"""
Бот записи на посещение (MAX Messenger).
Единый файл со всей логикой: FSM, регистрация, админ-панель,
напоминания, автоочистка, экспорт.
"""

import asyncio
import logging
import re
import sqlite3
import sys
from datetime import datetime, date, time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import aiosqlite
import yaml
from openpyxl import Workbook

from maxapi import Bot, Dispatcher
from maxapi.context import MemoryContext, State, StatesGroup
from maxapi.enums.attachment import AttachmentType
from maxapi.enums.upload_type import UploadType
from maxapi.types import (
    BotStarted,
    CallbackButton,
    Command,
    MessageCallback,
    MessageCreated,
)
from maxapi.types.attachments.attachment import ButtonsPayload
from maxapi.types.attachments.attachment import Attachment, OtherAttachmentPayload
from maxapi.types.attachments.upload import AttachmentPayload, AttachmentUpload
from maxapi.types.attachments.buttons.attachment_button import AttachmentButton

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("enrollment_bot")

# ---------------------------------------------------------------------------
#  Config
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("config.yaml")


def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


CFG = load_config()

BOT_TOKEN: str = CFG["bot"]["token"]
DB_PATH: str = CFG["database"].get("path", "bot.db")
MESSAGES: Dict[str, str] = CFG["messages"]
BUTTONS: Dict[str, str] = CFG["buttons"]
SLOTS_CFG: Dict[str, Any] = CFG["slots"]
_raw_ids = CFG.get("admin_ids", [])
ADMIN_IDS: List[int] = _raw_ids if isinstance(_raw_ids, list) else [_raw_ids]
CLEANUP_CFG: Dict[str, Any] = CFG.get("cleanup", {})
REMINDERS_CFG: Dict[str, Any] = CFG.get("reminders", {})
EXPORT_CFG: Dict[str, Any] = CFG.get("export", {})

DATE_FMT = SLOTS_CFG.get("date_format", "%d.%m.%Y")
TIME_FMT = SLOTS_CFG.get("time_format", "%H:%M")

# ---------------------------------------------------------------------------
#  FSM States
# ---------------------------------------------------------------------------


class UserStates(StatesGroup):
    wait_parent = State()
    wait_student = State()
    wait_grade = State()
    wait_birth = State()
    choose_slot = State()


class AdminStates(StatesGroup):
    menu = State()
    add_date = State()
    add_time = State()
    delete_slot = State()
    delete_confirm = State()
    export_choose = State()
    choose_slot_for_regs = State()
    choose_reg_to_delete = State()
    confirm_reg_delete = State()


# ---------------------------------------------------------------------------
#  Database helpers (aiosqlite)
# ---------------------------------------------------------------------------

db: Optional[aiosqlite.Connection] = None


def _row_factory(cursor: sqlite3.Cursor, row: tuple) -> Dict:
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row))


async def init_db() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(DB_PATH)
    conn.row_factory = _row_factory
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    return conn


async def init_tables():
    await db.execute("""
        CREATE TABLE IF NOT EXISTS slots (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            slot_date    TEXT NOT NULL,
            slot_time    TEXT NOT NULL,
            max_capacity INTEGER NOT NULL DEFAULT 1,
            created_by   INTEGER NOT NULL,
            created_at   TEXT DEFAULT (datetime('now', 'localtime')),
            UNIQUE (slot_date, slot_time)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS registrations (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       INTEGER NOT NULL,
            chat_id       INTEGER NOT NULL,
            username      TEXT DEFAULT '',
            parent_name   TEXT NOT NULL,
            student_name  TEXT NOT NULL,
            grade         TEXT NOT NULL,
            birth_date    TEXT NOT NULL,
            slot_id       INTEGER NOT NULL,
            registered_at TEXT DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (slot_id) REFERENCES slots(id) ON DELETE CASCADE,
            UNIQUE (user_id, slot_id)
        )
    """)

    cols = {r["name"] for r in await db_fetch_all("PRAGMA table_info(registrations)")}
    if "username" not in cols:
        await db.execute("ALTER TABLE registrations ADD COLUMN username TEXT DEFAULT ''")
    await db.execute("""
        CREATE TABLE IF NOT EXISTS reminders_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            registration_id INTEGER NOT NULL,
            reminder_type   TEXT NOT NULL,
            sent_at         TEXT DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (registration_id) REFERENCES registrations(id) ON DELETE CASCADE,
            UNIQUE (registration_id, reminder_type)
        )
    """)
    await db.commit()
    log.info("Таблицы БД проверены / созданы")


async def db_fetch_all(query: str, args: tuple = ()) -> List[Dict]:
    cursor = await db.execute(query, args)
    return await cursor.fetchall()


async def db_fetch_one(query: str, args: tuple = ()) -> Optional[Dict]:
    cursor = await db.execute(query, args)
    return await cursor.fetchone()


async def db_execute(query: str, args: tuple = ()) -> int:
    cursor = await db.execute(query, args)
    await db.commit()
    return cursor.lastrowid


# ---------------------------------------------------------------------------
#  Date/time formatting helpers
# ---------------------------------------------------------------------------


def fmt_date(val) -> str:
    if isinstance(val, date):
        return val.strftime(DATE_FMT)
    if isinstance(val, str):
        return datetime.strptime(val, "%Y-%m-%d").strftime(DATE_FMT)
    return str(val)


def fmt_time(val) -> str:
    if isinstance(val, timedelta):
        return (datetime.min + val).strftime(TIME_FMT)
    if isinstance(val, time):
        return val.strftime(TIME_FMT)
    if isinstance(val, str):
        return val[:5]
    return str(val)


def fmt_datetime(val) -> str:
    if isinstance(val, datetime):
        return val.strftime(f"{DATE_FMT} {TIME_FMT}")
    return str(val) if val else ""


# ---------------------------------------------------------------------------
#  Validation helpers
# ---------------------------------------------------------------------------

NAME_RE = re.compile(r"^[A-Za-zА-Яа-яЁёІіЇїЄєҐґ\-\s]{3,100}$")
GRADE_RE = re.compile(r"^(\d{1,2})\s*([А-Яа-яA-Za-z])?$")


def validate_name(text: str) -> bool:
    text = text.strip()
    if not NAME_RE.match(text):
        return False
    return len(text.split()) >= 2


def validate_grade(text: str) -> Optional[str]:
    m = GRADE_RE.match(text.strip())
    if not m:
        return None
    num = int(m.group(1))
    if num < 1 or num > 11:
        return None
    letter = (m.group(2) or "").upper()
    return f"{num}{letter}"


def validate_birth_date(text: str) -> Optional[date]:
    raw = text.strip().replace("/", ".").replace("-", ".")
    try:
        d = datetime.strptime(raw, DATE_FMT).date()
    except ValueError:
        return None
    if d >= date.today():
        return None
    return d


def validate_date(text: str) -> Optional[date]:
    try:
        d = datetime.strptime(text.strip(), DATE_FMT).date()
    except ValueError:
        return None
    if d < date.today():
        return None
    return d


def validate_times(text: str) -> List[time]:
    result = []
    for part in text.split(","):
        part = part.strip()
        try:
            result.append(datetime.strptime(part, TIME_FMT).time())
        except ValueError:
            pass
    return result


# ---------------------------------------------------------------------------
#  Keyboard helpers
# ---------------------------------------------------------------------------


def _kb(*rows: List[CallbackButton]) -> List[AttachmentButton]:
    return [AttachmentButton(type="inline_keyboard", payload=ButtonsPayload(buttons=list(rows)))]


def cancel_btn() -> CallbackButton:
    return CallbackButton(text=BUTTONS["cancel"], payload="cancel")


def back_btn() -> CallbackButton:
    return CallbackButton(text=BUTTONS["back"], payload="back")


# ---------------------------------------------------------------------------
#  Bot & dispatcher setup
# ---------------------------------------------------------------------------

bot = Bot(BOT_TOKEN)
dp = Dispatcher()


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------


async def send_welcome(chat_id: Optional[int] = None, user_id: Optional[int] = None):
    await bot.send_message(
        chat_id=chat_id,
        user_id=user_id,
        text=MESSAGES["welcome"],
        attachments=_kb([CallbackButton(text=BUTTONS["sign_up"], payload="sign_up")]),
    )


async def send_admin_menu(chat_id: Optional[int] = None, user_id: Optional[int] = None):
    await bot.send_message(
        chat_id=chat_id,
        user_id=user_id,
        text=MESSAGES["admin_welcome"],
        attachments=_kb(
            [CallbackButton(text=BUTTONS["add_slot"], payload="adm_add")],
            [CallbackButton(text=BUTTONS["remove_slot"], payload="adm_del")],
            [CallbackButton(text=BUTTONS["delete_reg"], payload="adm_del_reg")],
            [CallbackButton(text=BUTTONS["view_slots"], payload="adm_view")],
            [CallbackButton(text=BUTTONS["export_list"], payload="adm_export")],
        ),
    )


def fmt_user(reg: Dict) -> str:
    if reg.get("username"):
        return f"@{reg['username']}"
    return f"id:{reg['user_id']}"


# ---------------------------------------------------------------------------
#  /start  (BotStarted + command)
# ---------------------------------------------------------------------------


@dp.bot_started()
async def on_bot_started(event: BotStarted, context: MemoryContext):
    await context.clear()
    uid = getattr(event, "user_id", None) or 0
    if uid in ADMIN_IDS:
        await context.set_state(AdminStates.menu)
        await send_admin_menu(chat_id=event.chat_id)
    else:
        await send_welcome(chat_id=event.chat_id)


@dp.message_created(Command("start"))
async def on_start_command(event: MessageCreated, context: MemoryContext):
    await context.clear()
    uid = event.message.sender.user_id
    chat_id = event.message.recipient.chat_id
    user_id = uid if not chat_id else None
    if uid in ADMIN_IDS:
        await context.set_state(AdminStates.menu)
        await send_admin_menu(chat_id=chat_id, user_id=user_id)
    else:
        await send_welcome(chat_id=chat_id, user_id=user_id)


# ---------------------------------------------------------------------------
#  /admin, /myid
# ---------------------------------------------------------------------------


@dp.message_created(Command("myid"))
async def on_myid_command(event: MessageCreated):
    uid = event.message.sender.user_id
    chat_id = event.message.recipient.chat_id
    user_id = uid if not chat_id else None
    await bot.send_message(
        chat_id=chat_id,
        user_id=user_id,
        text=f"Ваш user_id: {uid}",
    )


@dp.message_created(Command("myusername"))
async def on_myusername_command(event: MessageCreated):
    sender = event.message.sender
    uid = sender.user_id
    uname = getattr(sender, "username", None) or ""
    chat_id = event.message.recipient.chat_id
    user_id = uid if not chat_id else None
    text = f"Ваш user_id: {uid}\nВаш username: {uname or '—'}"
    await bot.send_message(chat_id=chat_id, user_id=user_id, text=text)


@dp.message_created(Command("admin"))
async def on_admin_command(event: MessageCreated, context: MemoryContext):
    uid = event.message.sender.user_id
    chat_id = event.message.recipient.chat_id
    user_id = uid if not chat_id else None
    if uid not in ADMIN_IDS:
        await bot.send_message(chat_id=chat_id, user_id=user_id, text=MESSAGES["no_access"])
        return
    await context.clear()
    await context.set_state(AdminStates.menu)
    await send_admin_menu(chat_id=chat_id, user_id=user_id)


# ---------------------------------------------------------------------------
#  Callback router
# ---------------------------------------------------------------------------


@dp.message_callback()
async def on_callback(event: MessageCallback, context: MemoryContext):
    payload = event.callback.payload or ""
    uid = event.callback.user.user_id
    chat_id = event.message.recipient.chat_id
    user_id = uid if not chat_id else None

    if payload == "cancel":
        await context.clear()
        await event.answer(notification=MESSAGES["cancelled"])
        if uid in ADMIN_IDS:
            await context.set_state(AdminStates.menu)
            await send_admin_menu(chat_id=chat_id, user_id=user_id)
        else:
            await send_welcome(chat_id=chat_id, user_id=user_id)
        return

    if payload == "back":
        await context.clear()
        if uid in ADMIN_IDS:
            await context.set_state(AdminStates.menu)
            await event.answer()
            await send_admin_menu(chat_id=chat_id, user_id=user_id)
        else:
            await event.answer()
            await send_welcome(chat_id=chat_id, user_id=user_id)
        return

    if payload == "sign_up":
        await context.clear()
        await context.set_state(UserStates.wait_parent)
        await event.answer()
        await bot.send_message(
            chat_id=chat_id, user_id=user_id,
            text=MESSAGES["ask_parent_name"],
            attachments=_kb([cancel_btn()]),
        )
        return

    if payload.startswith("slot_"):
        await _handle_slot_selection(event, context, payload, chat_id, user_id, uid)
        return

    if uid not in ADMIN_IDS:
        await event.answer(notification=MESSAGES["no_access"])
        return

    if payload == "adm_add":
        await context.set_state(AdminStates.add_date)
        await event.answer()
        await bot.send_message(
            chat_id=chat_id, user_id=user_id,
            text=f"Введите дату ({DATE_FMT.replace('%d','ДД').replace('%m','ММ').replace('%Y','ГГГГ')}):",
            attachments=_kb([back_btn()]),
        )
        return

    if payload == "adm_del":
        await _show_delete_slots(event, context, chat_id, user_id)
        return

    if payload == "adm_del_reg":
        await _show_slots_for_reg_delete(event, context, chat_id, user_id)
        return

    if payload == "adm_view":
        await _show_all_slots(event, chat_id, user_id)
        return

    if payload == "adm_export":
        await context.set_state(AdminStates.export_choose)
        await event.answer()
        await bot.send_message(
            chat_id=chat_id, user_id=user_id,
            text="Выберите тип экспорта:",
            attachments=_kb(
                [CallbackButton(text="📅 Ближайшие", payload="export_upcoming")],
                [CallbackButton(text="📅 Все", payload="export_all")],
                [back_btn()],
            ),
        )
        return

    if payload.startswith("export_"):
        await _handle_export(event, context, payload, chat_id, user_id)
        return

    if payload.startswith("dslot_"):
        await _handle_delete_slot_select(event, context, payload, chat_id, user_id)
        return

    if payload == "confirm_delete":
        await _handle_delete_confirm(event, context, chat_id, user_id, uid)
        return

    if payload.startswith("regslot_"):
        await _show_regs_for_slot(event, context, payload, chat_id, user_id)
        return

    if payload.startswith("delreg_"):
        await _confirm_reg_delete(event, context, payload, chat_id, user_id)
        return

    if payload == "confirm_delreg":
        await _handle_reg_delete(event, context, chat_id, user_id)
        return


# ---------------------------------------------------------------------------
#  Slot selection by user
# ---------------------------------------------------------------------------


async def _handle_slot_selection(
    event: MessageCallback, context: MemoryContext, payload: str,
    chat_id: Optional[int], user_id: Optional[int], uid: int,
):
    state = await context.get_state()
    if state != UserStates.choose_slot:
        await event.answer()
        return

    try:
        slot_id = int(payload.removeprefix("slot_"))
    except ValueError:
        await event.answer()
        return

    try:
        slot = await db_fetch_one("SELECT * FROM slots WHERE id=?", (slot_id,))
        if not slot:
            await event.answer(notification="Слот не найден.")
            return

        cnt = await db_fetch_one(
            "SELECT COUNT(*) AS c FROM registrations WHERE slot_id=?", (slot_id,)
        )
        occupied = cnt["c"] if cnt else 0

        if occupied >= slot["max_capacity"]:
            await event.answer(notification=MESSAGES["slot_full"])
            await _show_available_slots(chat_id, user_id)
            return

        exists = await db_fetch_one(
            "SELECT id FROM registrations WHERE user_id=? AND slot_id=?", (uid, slot_id),
        )
        if exists:
            await event.answer(notification=MESSAGES["already_registered"])
            return

        data = await context.get_data()
        username = getattr(event.callback.user, "username", None) or ""
        await db_execute(
            """INSERT INTO registrations
               (user_id, chat_id, username, parent_name, student_name, grade, birth_date, slot_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (uid, chat_id or 0, username, data["parent_name"],
             data["student_name"], data["grade"], data["birth_date"], slot_id),
        )

        success_text = MESSAGES["success"].format(
            parent_name=data["parent_name"],
            student_name=data["student_name"],
            grade=data["grade"],
            slot_date=fmt_date(slot["slot_date"]),
            slot_time=fmt_time(slot["slot_time"]),
        )
        await context.clear()
        await event.answer()
        await bot.send_message(chat_id=chat_id, user_id=user_id, text=success_text)

    except Exception:
        log.exception("Ошибка при записи на слот")
        await event.answer(notification=MESSAGES["service_unavailable"])


async def _show_available_slots(chat_id: Optional[int], user_id: Optional[int]):
    try:
        rows = await db_fetch_all(
            """SELECT s.id, s.slot_date, s.slot_time
               FROM slots s
               LEFT JOIN registrations r ON r.slot_id = s.id
               WHERE s.slot_date >= date('now')
               GROUP BY s.id
               HAVING COUNT(r.id) = 0
               ORDER BY s.slot_date, s.slot_time""",
        )
    except Exception:
        log.exception("Ошибка при получении слотов")
        await bot.send_message(chat_id=chat_id, user_id=user_id, text=MESSAGES["service_unavailable"])
        return

    if not rows:
        await bot.send_message(chat_id=chat_id, user_id=user_id, text=MESSAGES["no_slots"])
        return

    buttons_rows: List[List[CallbackButton]] = []
    for row in rows:
        label = f"{fmt_date(row['slot_date'])} — {fmt_time(row['slot_time'])}"
        buttons_rows.append(
            [CallbackButton(text=label, payload=f"slot_{row['id']}")]
        )
    buttons_rows.append([cancel_btn()])

    await bot.send_message(
        chat_id=chat_id, user_id=user_id,
        text=MESSAGES["choose_slot"],
        attachments=_kb(*buttons_rows),
    )


# ---------------------------------------------------------------------------
#  Text messages
# ---------------------------------------------------------------------------


@dp.message_created()
async def on_text(event: MessageCreated, context: MemoryContext):
    state = await context.get_state()
    text = (event.message.body.text or "").strip()
    uid = event.message.sender.user_id
    chat_id = event.message.recipient.chat_id
    user_id = uid if not chat_id else None

    if not state:
        if uid in ADMIN_IDS:
            await context.set_state(AdminStates.menu)
            await send_admin_menu(chat_id=chat_id, user_id=user_id)
        else:
            await send_welcome(chat_id=chat_id, user_id=user_id)
        return

    # ---- User registration steps ----
    if state == UserStates.wait_parent:
        if not validate_name(text):
            await bot.send_message(chat_id=chat_id, user_id=user_id,
                                   text=MESSAGES["invalid_name"], attachments=_kb([cancel_btn()]))
            return
        await context.update_data(parent_name=text)
        await context.set_state(UserStates.wait_student)
        await bot.send_message(chat_id=chat_id, user_id=user_id,
                               text=MESSAGES["ask_student_name"], attachments=_kb([cancel_btn()]))
        return

    if state == UserStates.wait_student:
        if not validate_name(text):
            await bot.send_message(chat_id=chat_id, user_id=user_id,
                                   text=MESSAGES["invalid_name"], attachments=_kb([cancel_btn()]))
            return
        await context.update_data(student_name=text)
        await context.set_state(UserStates.wait_grade)
        await bot.send_message(chat_id=chat_id, user_id=user_id,
                               text=MESSAGES["ask_grade"], attachments=_kb([cancel_btn()]))
        return

    if state == UserStates.wait_grade:
        grade = validate_grade(text)
        if grade is None:
            await bot.send_message(chat_id=chat_id, user_id=user_id,
                                   text=MESSAGES["invalid_grade"], attachments=_kb([cancel_btn()]))
            return
        await context.update_data(grade=grade)
        await context.set_state(UserStates.wait_birth)
        await bot.send_message(chat_id=chat_id, user_id=user_id,
                               text=MESSAGES["ask_birth_date"], attachments=_kb([cancel_btn()]))
        return

    if state == UserStates.wait_birth:
        bd = validate_birth_date(text)
        if bd is None:
            await bot.send_message(chat_id=chat_id, user_id=user_id,
                                   text=MESSAGES["invalid_date"], attachments=_kb([cancel_btn()]))
            return
        await context.update_data(birth_date=bd.isoformat())
        await context.set_state(UserStates.choose_slot)
        await _show_available_slots(chat_id, user_id)
        return

    # ---- Admin steps ----
    if uid not in ADMIN_IDS:
        await send_welcome(chat_id=chat_id, user_id=user_id)
        return

    if state == AdminStates.add_date:
        d = validate_date(text)
        if d is None:
            await bot.send_message(chat_id=chat_id, user_id=user_id,
                                   text=MESSAGES["invalid_date"], attachments=_kb([back_btn()]))
            return
        await context.update_data(adm_date=d.isoformat())
        await context.set_state(AdminStates.add_time)
        await bot.send_message(chat_id=chat_id, user_id=user_id,
                               text="Введите время через запятую (ЧЧ:ММ):", attachments=_kb([back_btn()]))
        return

    if state == AdminStates.add_time:
        times = validate_times(text)
        if not times:
            await bot.send_message(chat_id=chat_id, user_id=user_id,
                                   text="❌ Неверный формат времени. Пример: 10:00, 14:00",
                                   attachments=_kb([back_btn()]))
            return
        data = await context.get_data()
        adm_date = date.fromisoformat(data["adm_date"])
        created, duplicates = [], []

        for t in times:
            try:
                await db_execute(
                    """INSERT INTO slots (slot_date, slot_time, max_capacity, created_by)
                       VALUES (?, ?, 1, ?)""",
                    (adm_date.isoformat(), t.isoformat(), uid),
                )
                created.append(t)
            except Exception as exc:
                if "UNIQUE" in str(exc):
                    duplicates.append(t)
                else:
                    log.exception("Ошибка при создании слота")

        lines = []
        if created:
            lines.append(f"✅ Создано {len(created)} слот(ов):")
            for t in created:
                lines.append(f"  • {adm_date.strftime(DATE_FMT)} — {t.strftime(TIME_FMT)}")
        if duplicates:
            lines.append(f"⚠️ Уже существуют: {', '.join(t.strftime(TIME_FMT) for t in duplicates)}")

        await context.clear()
        await context.set_state(AdminStates.menu)
        await bot.send_message(chat_id=chat_id, user_id=user_id, text="\n".join(lines))
        await send_admin_menu(chat_id=chat_id, user_id=user_id)
        return


# ---------------------------------------------------------------------------
#  Admin: delete slot
# ---------------------------------------------------------------------------


async def _show_delete_slots(event, context, chat_id, user_id):
    rows = await db_fetch_all(
        """SELECT s.id, s.slot_date, s.slot_time, COUNT(r.id) AS occupied
           FROM slots s LEFT JOIN registrations r ON r.slot_id = s.id
           WHERE s.slot_date >= date('now') GROUP BY s.id
           ORDER BY s.slot_date, s.slot_time""")
    if not rows:
        await event.answer(notification="Нет предстоящих слотов.")
        return
    await context.set_state(AdminStates.delete_slot)
    btn_rows = []
    for r in rows:
        label = f"{fmt_date(r['slot_date'])} {fmt_time(r['slot_time'])} — {r['occupied']} чел."
        btn_rows.append([CallbackButton(text=label, payload=f"dslot_{r['id']}")])
    btn_rows.append([back_btn()])
    await event.answer()
    await bot.send_message(chat_id=chat_id, user_id=user_id,
                           text="Выберите слот для удаления:", attachments=_kb(*btn_rows))


async def _handle_delete_slot_select(event, context, payload, chat_id, user_id):
    slot_id = int(payload.removeprefix("dslot_"))
    slot = await db_fetch_one("SELECT * FROM slots WHERE id=?", (slot_id,))
    if not slot:
        await event.answer(notification="Слот не найден.")
        return
    cnt = await db_fetch_one("SELECT COUNT(*) AS c FROM registrations WHERE slot_id=?", (slot_id,))
    occupied = cnt["c"] if cnt else 0
    await context.update_data(del_slot_id=slot_id)
    await context.set_state(AdminStates.delete_confirm)
    await event.answer()
    await bot.send_message(
        chat_id=chat_id, user_id=user_id,
        text=f"⚠️ Записей: {occupied}. Будут уведомлены.\n{fmt_date(slot['slot_date'])} {fmt_time(slot['slot_time'])}\nПодтвердить удаление?",
        attachments=_kb([CallbackButton(text="✅ Подтвердить", payload="confirm_delete")], [cancel_btn()]),
    )


async def _handle_delete_confirm(event, context, chat_id, user_id, uid):
    data = await context.get_data()
    slot_id = data.get("del_slot_id")
    if not slot_id:
        await event.answer()
        return
    slot = await db_fetch_one("SELECT * FROM slots WHERE id=?", (slot_id,))
    if not slot:
        await event.answer(notification="Слот уже удалён.")
        await context.clear()
        await context.set_state(AdminStates.menu)
        await send_admin_menu(chat_id=chat_id, user_id=user_id)
        return
    regs = await db_fetch_all("SELECT * FROM registrations WHERE slot_id=?", (slot_id,))
    sd, st_str = fmt_date(slot["slot_date"]), fmt_time(slot["slot_time"])
    await db_execute("DELETE FROM slots WHERE id=?", (slot_id,))
    notified = 0
    for reg in regs:
        try:
            text = MESSAGES["slot_cancelled"].format(slot_date=sd, slot_time=st_str, student_name=reg["student_name"])
            r_chat = reg["chat_id"] if reg["chat_id"] else None
            r_user = reg["user_id"] if not r_chat else None
            await bot.send_message(chat_id=r_chat, user_id=r_user, text=text)
            notified += 1
        except Exception:
            log.exception("Не удалось уведомить user_id=%s", reg["user_id"])
    await context.clear()
    await context.set_state(AdminStates.menu)
    await event.answer()
    await bot.send_message(chat_id=chat_id, user_id=user_id,
                           text=f"✅ Слот удалён. {notified} пользователям отправлено уведомление.")
    await send_admin_menu(chat_id=chat_id, user_id=user_id)


# ---------------------------------------------------------------------------
#  Admin: delete a registration
# ---------------------------------------------------------------------------


async def _show_slots_for_reg_delete(event, context, chat_id, user_id):
    rows = await db_fetch_all(
        """SELECT s.id, s.slot_date, s.slot_time, COUNT(r.id) AS occupied
           FROM slots s JOIN registrations r ON r.slot_id = s.id
           WHERE s.slot_date >= date('now') GROUP BY s.id
           ORDER BY s.slot_date, s.slot_time""")
    if not rows:
        await event.answer(notification="Нет слотов с записями.")
        return
    await context.set_state(AdminStates.choose_slot_for_regs)
    btn_rows = []
    for r in rows:
        label = f"{fmt_date(r['slot_date'])} {fmt_time(r['slot_time'])} — {r['occupied']} чел."
        btn_rows.append([CallbackButton(text=label, payload=f"regslot_{r['id']}")])
    btn_rows.append([back_btn()])
    await event.answer()
    await bot.send_message(chat_id=chat_id, user_id=user_id,
                           text="Выберите слот, чтобы увидеть записи:", attachments=_kb(*btn_rows))


async def _show_regs_for_slot(event, context, payload, chat_id, user_id):
    slot_id = int(payload.removeprefix("regslot_"))
    regs = await db_fetch_all("SELECT * FROM registrations WHERE slot_id=?", (slot_id,))
    if not regs:
        await event.answer(notification="Нет записей.")
        return
    await context.set_state(AdminStates.choose_reg_to_delete)
    btn_rows = []
    for r in regs:
        label = f"{r['student_name']} ({r['grade']}) — {fmt_user(r)}"
        btn_rows.append([CallbackButton(text=label, payload=f"delreg_{r['id']}")])
    btn_rows.append([back_btn()])
    await event.answer()
    await bot.send_message(chat_id=chat_id, user_id=user_id,
                           text="Выберите запись для удаления:", attachments=_kb(*btn_rows))


async def _confirm_reg_delete(event, context, payload, chat_id, user_id):
    reg_id = int(payload.removeprefix("delreg_"))
    reg = await db_fetch_one(
        """SELECT r.*, s.slot_date, s.slot_time FROM registrations r
           JOIN slots s ON s.id = r.slot_id WHERE r.id=?""", (reg_id,))
    if not reg:
        await event.answer(notification="Запись не найдена.")
        return
    await context.update_data(del_reg_id=reg_id)
    await context.set_state(AdminStates.confirm_reg_delete)
    info = (f"👤 {reg['parent_name']}\n🧒 {reg['student_name']} ({reg['grade']})\n"
            f"📅 {fmt_date(reg['slot_date'])} {fmt_time(reg['slot_time'])}\n🆔 {fmt_user(reg)}")
    await event.answer()
    await bot.send_message(
        chat_id=chat_id, user_id=user_id,
        text=f"Удалить эту запись?\n\n{info}",
        attachments=_kb([CallbackButton(text="✅ Подтвердить", payload="confirm_delreg")], [cancel_btn()]),
    )


async def _handle_reg_delete(event, context, chat_id, user_id):
    data = await context.get_data()
    reg_id = data.get("del_reg_id")
    if not reg_id:
        await event.answer()
        return
    reg = await db_fetch_one(
        """SELECT r.*, s.slot_date, s.slot_time FROM registrations r
           JOIN slots s ON s.id = r.slot_id WHERE r.id=?""", (reg_id,))
    if not reg:
        await event.answer(notification="Запись уже удалена.")
    else:
        await db_execute("DELETE FROM registrations WHERE id=?", (reg_id,))
        try:
            text = MESSAGES["slot_cancelled"].format(
                slot_date=fmt_date(reg["slot_date"]),
                slot_time=fmt_time(reg["slot_time"]),
                student_name=reg["student_name"])
            r_chat = reg["chat_id"] if reg["chat_id"] else None
            r_user = reg["user_id"] if not r_chat else None
            await bot.send_message(chat_id=r_chat, user_id=r_user, text=text)
        except Exception:
            log.exception("Не удалось уведомить user_id=%s", reg["user_id"])
    await context.clear()
    await context.set_state(AdminStates.menu)
    await event.answer()
    await bot.send_message(chat_id=chat_id, user_id=user_id, text="✅ Запись удалена.")
    await send_admin_menu(chat_id=chat_id, user_id=user_id)


# ---------------------------------------------------------------------------
#  Admin: view all slots
# ---------------------------------------------------------------------------


async def _show_all_slots(event, chat_id, user_id):
    rows = await db_fetch_all(
        """SELECT s.slot_date, s.slot_time, COUNT(r.id) AS occupied
           FROM slots s LEFT JOIN registrations r ON r.slot_id = s.id
           WHERE s.slot_date >= date('now') GROUP BY s.id
           ORDER BY s.slot_date, s.slot_time""")
    if not rows:
        await event.answer(notification="Нет предстоящих дат.")
        return
    grouped: Dict[str, List] = {}
    for r in rows:
        grouped.setdefault(r["slot_date"], []).append(r)
    lines = ["📋 Ближайшие даты:\n"]
    for d in sorted(grouped):
        lines.append(f"📅 {fmt_date(d)}")
        for r in grouped[d]:
            status = "🔴 занято" if r["occupied"] > 0 else "🟢 свободно"
            lines.append(f"   🕐 {fmt_time(r['slot_time'])} — {status}")
        lines.append("")
    await event.answer()
    await bot.send_message(chat_id=chat_id, user_id=user_id,
                           text="\n".join(lines), attachments=_kb([back_btn()]))


# ---------------------------------------------------------------------------
#  Admin: export
# ---------------------------------------------------------------------------


EXPORT_HEADERS = [
    "Дата", "Время", "Пользователь", "ФИО родителя",
    "ФИО ученика", "Класс", "Дата рождения", "Дата записи",
]


def _export_row(r: Dict) -> list:
    user = f"@{r['username']}" if r.get("username") else str(r["user_id"])
    return [
        fmt_date(r["slot_date"]), fmt_time(r["slot_time"]), user,
        r["parent_name"], r["student_name"], r["grade"],
        fmt_date(r["birth_date"]), fmt_datetime(r["registered_at"]),
    ]


async def _handle_export(event, context, payload, chat_id, user_id):
    upcoming_only = payload == "export_upcoming"
    where = "WHERE s.slot_date >= date('now')" if upcoming_only else ""
    rows = await db_fetch_all(
        f"""SELECT s.slot_date, s.slot_time, r.username, r.user_id,
                   r.parent_name, r.student_name, r.grade,
                   r.birth_date, r.registered_at
            FROM registrations r JOIN slots s ON s.id = r.slot_id
            {where} ORDER BY s.slot_date, s.slot_time, r.registered_at""")
    if not rows:
        await event.answer(notification="Нет записей для экспорта.")
        return
    fmt = EXPORT_CFG.get("format", "xlsx")
    if fmt == "xlsx":
        filepath = await _export_xlsx(rows)
    elif fmt == "csv":
        filepath = await _export_csv(rows)
    elif fmt == "pdf":
        filepath = await _export_pdf(rows)
    elif fmt in ("ods", "odd"):
        filepath = await _export_ods(rows)
    else:
        await event.answer(notification=f"Неизвестный формат экспорта: {fmt}")
        return
    await context.clear()
    await context.set_state(AdminStates.menu)
    await event.answer()
    try:
        file_payload = await _upload_file_as_attachment_payload(filepath)
        await bot.send_message(
            chat_id=chat_id,
            user_id=user_id,
            text="📊 Файл экспорта:",
            attachments=[Attachment(type=AttachmentType.FILE, payload=file_payload)],
        )
    except ModuleNotFoundError as e:
        log.exception("Не установлена зависимость для экспорта")
        await bot.send_message(
            chat_id=chat_id,
            user_id=user_id,
            text=f"⚠️ Не установлена библиотека для экспорта: {e.name}\n"
                 f"Установи зависимости: pip install -r requirements.txt",
        )
    except Exception:
        log.exception("Ошибка при загрузке/отправке файла экспорта")
        await bot.send_message(chat_id=chat_id, user_id=user_id, text=MESSAGES["service_unavailable"])
    await send_admin_menu(chat_id=chat_id, user_id=user_id)
    try:
        Path(filepath).unlink()
    except OSError:
        pass


async def _export_xlsx(rows: List[Dict]) -> str:
    wb = Workbook()
    ws = wb.active
    ws.title = "Записи"
    ws.append(EXPORT_HEADERS)
    for r in rows:
        ws.append(_export_row(r))
    filepath = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb.save(filepath)
    return filepath


async def _export_csv(rows: List[Dict]) -> str:
    import csv as csv_mod
    filepath = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        w = csv_mod.writer(f, delimiter=";")
        w.writerow(EXPORT_HEADERS)
        for r in rows:
            w.writerow(_export_row(r))
    return filepath


async def _export_pdf(rows: List[Dict]) -> str:
    # Требуется пакет reportlab
    try:
        from reportlab.lib import colors  # type: ignore
        from reportlab.lib.pagesizes import A4  # type: ignore
        from reportlab.lib.styles import getSampleStyleSheet  # type: ignore
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer  # type: ignore
        from reportlab.pdfbase import pdfmetrics  # type: ignore
        from reportlab.pdfbase.ttfonts import TTFont  # type: ignore
    except ModuleNotFoundError:
        raise

    filepath = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

    doc = SimpleDocTemplate(filepath, pagesize=A4)
    styles = getSampleStyleSheet()

    font_name = _ensure_cyrillic_pdf_font()
    if font_name:
        # Подменяем базовый шрифт, чтобы кириллица отображалась корректно
        styles["Normal"].fontName = font_name
        styles["Title"].fontName = font_name

    data = [EXPORT_HEADERS] + [_export_row(r) for r in rows]

    # Чуть поджимаем длинные строки (ФИО и т.п.) для влезания в PDF
    def _cell(v: object) -> str:
        s = "" if v is None else str(v)
        return s

    table = Table([[Paragraph(_cell(c), styles["Normal"]) for c in row] for row in data], repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), styles["Normal"].fontName),
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )

    doc.build([Paragraph("Экспорт записей", styles["Title"]), Spacer(1, 10), table])
    return filepath


@lru_cache(maxsize=1)
def _ensure_cyrillic_pdf_font() -> str:
    """
    ReportLab по умолчанию не поддерживает кириллицу (получаются квадраты).
    Регистрируем системный TTF-шрифт с кириллицей (Windows: Arial/DejaVu).
    Возвращает имя зарегистрированного шрифта или пустую строку.
    """
    try:
        from reportlab.pdfbase import pdfmetrics  # type: ignore
        from reportlab.pdfbase.ttfonts import TTFont  # type: ignore
    except Exception:
        return ""

    candidates = [
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/ARIAL.TTF"),
        Path("C:/Windows/Fonts/calibri.ttf"),
        Path("C:/Windows/Fonts/CALIBRI.TTF"),
        Path("C:/Windows/Fonts/times.ttf"),
        Path("C:/Windows/Fonts/tahoma.ttf"),
        Path("C:/Windows/Fonts/TAHOMA.TTF"),
    ]

    font_path = next((p for p in candidates if p.exists()), None)
    if not font_path:
        return ""

    font_name = "CyrillicFont"
    try:
        pdfmetrics.registerFont(TTFont(font_name, str(font_path)))
        return font_name
    except Exception:
        return ""


async def _export_ods(rows: List[Dict]) -> str:
    # Требуется пакет odfpy
    try:
        from odf.opendocument import OpenDocumentSpreadsheet  # type: ignore
        from odf.table import Table, TableRow, TableCell  # type: ignore
        from odf.text import P  # type: ignore
    except ModuleNotFoundError:
        raise

    filepath = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.ods"

    doc = OpenDocumentSpreadsheet()
    sheet = Table(name="Записи")

    def add_row(values: List[object]):
        tr = TableRow()
        for v in values:
            tc = TableCell()
            tc.addElement(P(text="" if v is None else str(v)))
            tr.addElement(tc)
        sheet.addElement(tr)

    add_row(EXPORT_HEADERS)
    for r in rows:
        add_row(_export_row(r))

    doc.spreadsheet.addElement(sheet)
    doc.save(filepath)
    return filepath


async def _upload_file_as_attachment_payload(filepath: str) -> object:
    """
    Обход бага maxapi с InputMedia на Windows: грузим файл вручную через /uploads,
    затем используем полученный payload как вложение типа FILE.
    """
    api_url = "https://platform-api.max.ru/uploads?type=file"
    headers = {"Authorization": BOT_TOKEN}

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.post(api_url) as r:
            r.raise_for_status()
            upload = await r.json()
            upload_url = upload["url"]

        with open(filepath, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("data", f)
            async with session.post(upload_url, data=form) as r2:
                r2.raise_for_status()
                payload = await r2.json()

    # MAX может вернуть разные payload для file upload:
    # - {"url": "...", "token": "..."}  (некоторые типы)
    # - {"fileId": 123, "token": "..."} (file upload)
    if not isinstance(payload, dict) or "token" not in payload:
        raise RuntimeError(f"Unexpected upload payload: {payload!r}")

    if "url" in payload:
        return OtherAttachmentPayload(url=payload["url"], token=payload.get("token"))

    # Для file upload достаточно token (fileId можно игнорировать)
    return AttachmentUpload(type=UploadType.FILE, payload=AttachmentPayload(token=payload["token"]))


# ---------------------------------------------------------------------------
#  Background: reminders
# ---------------------------------------------------------------------------


async def _reminder_loop():
    if not REMINDERS_CFG.get("enabled"):
        return
    interval = REMINDERS_CFG.get("run_check_interval_minutes", 30) * 60
    before_hours: List[int] = REMINDERS_CFG.get("before_hours", [])
    while True:
        try:
            now = datetime.now()
            for hours in before_hours:
                target = now + timedelta(hours=hours)
                reminder_type = f"{hours}h"
                regs = await db_fetch_all(
                    """SELECT r.id AS reg_id, r.user_id, r.chat_id, r.student_name,
                              s.slot_date, s.slot_time
                       FROM registrations r JOIN slots s ON s.id = r.slot_id
                       WHERE (s.slot_date || ' ' || s.slot_time) BETWEEN ? AND ?
                         AND r.id NOT IN (
                             SELECT registration_id FROM reminders_log WHERE reminder_type = ?
                         )""",
                    (now.strftime("%Y-%m-%d %H:%M:%S"), target.strftime("%Y-%m-%d %H:%M:%S"), reminder_type))
                for reg in regs:
                    try:
                        text = MESSAGES["reminder"].format(
                            slot_date=fmt_date(reg["slot_date"]),
                            slot_time=fmt_time(reg["slot_time"]),
                            student_name=reg["student_name"])
                        r_chat = reg["chat_id"] if reg["chat_id"] else None
                        r_user = reg["user_id"] if not r_chat else None
                        await bot.send_message(chat_id=r_chat, user_id=r_user, text=text)
                        await db_execute(
                            "INSERT OR IGNORE INTO reminders_log (registration_id, reminder_type) VALUES (?, ?)",
                            (reg["reg_id"], reminder_type))
                        log.info("Напоминание (%s) отправлено user_id=%s", reminder_type, reg["user_id"])
                    except Exception:
                        log.exception("Ошибка отправки напоминания reg_id=%s", reg["reg_id"])
        except Exception:
            log.exception("Ошибка в цикле напоминаний")
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
#  Background: cleanup
# ---------------------------------------------------------------------------


async def _cleanup_loop():
    if not CLEANUP_CFG.get("enabled"):
        return
    run_at_str = CLEANUP_CFG.get("run_at", "03:00")
    run_at_time = datetime.strptime(run_at_str, "%H:%M").time()
    delete_after = CLEANUP_CFG.get("delete_after_days", 30)
    while True:
        now = datetime.now()
        target = datetime.combine(now.date(), run_at_time)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            cutoff = (date.today() - timedelta(days=delete_after)).isoformat()
            await db_execute("DELETE FROM slots WHERE slot_date < ?", (cutoff,))
            log.info("Автоочистка: удалены слоты старше %s дней (до %s)", delete_after, cutoff)
        except Exception:
            log.exception("Ошибка автоочистки")


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------


async def main():
    global db
    db = await init_db()
    await init_tables()
    asyncio.create_task(_reminder_loop())
    asyncio.create_task(_cleanup_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
