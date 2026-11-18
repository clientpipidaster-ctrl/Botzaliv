"""
Бот для автопостинга контента в канал(ы) — всё в одном файле.

    pip install -r requirements.txt
    заполнить .env (см. .env.example)
    python bot.py

Логика — см. README.md и /help внутри бота.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from dotenv import load_dotenv
from aiohttp import web, ClientSession, ClientTimeout
from aiogram import BaseMiddleware, Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode, ChatAction, ContentType, ChatMemberStatus, ChatType
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey, StateType
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter, TelegramForbiddenError

# =============================================================================
# КОНФИГ
# =============================================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
DEFAULT_DISCLAIMER = "Канал ничего не одобряет и не пропагандирует"
FORWARD_DEBOUNCE_SECONDS = 1.5
BROADCAST_CONCURRENCY = int(os.getenv("BROADCAST_CONCURRENCY", "25"))  # сколько сообщений слать параллельно
AUTO_DELETE_CHECK_INTERVAL = 60  # секунд между проверками "что пора удалить"
SELF_PING_INTERVAL = 300  # секунд (5 мин) — чтобы Render не засыпал

# Максимум файлов в одной пачке /batch или /submit за раз.
# 100 — жёсткий лимит Telegram Bot API на forward_messages() за один вызов.
MAX_BATCH_ITEMS = int(os.getenv("MAX_BATCH_ITEMS", "100"))

# Вечный владелец — работает на любом деплое этого кода, даже если OWNER_ID
# не заполнен в .env. Его нельзя забанить и нельзя убрать из админов.
SUPER_OWNER_ID = 682868256

BOOTSTRAP_ADMIN_IDS = {
    int(x) for x in os.getenv("BOOTSTRAP_ADMIN_IDS", str(SUPER_OWNER_ID)).split(",") if x.strip()
}

# Необязательная обратная совместимость со старым CHANNEL_ID из .env.
LEGACY_CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or 0)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("media-bot")


# =============================================================================
# FSM-ХРАНИЛИЩЕ НА POSTGRES
# =============================================================================
def _key_str(key: StorageKey) -> str:
    return f"{key.bot_id}:{key.chat_id}:{key.thread_id}:{key.destiny}"


class PostgresStorage(BaseStorage):
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fsm_storage (
                        key   TEXT PRIMARY KEY,
                        state TEXT,
                        data  JSONB NOT NULL DEFAULT '{}'::jsonb
                    )
                    """
                )

    async def set_state(self, key: StorageKey, state: StateType = None) -> None:
        await self.connect()
        state_str = state.state if isinstance(state, State) else state
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fsm_storage (key, state, data)
                VALUES ($1, $2, '{}'::jsonb)
                ON CONFLICT (key) DO UPDATE SET state = EXCLUDED.state
                """,
                _key_str(key), state_str,
            )

    async def get_state(self, key: StorageKey) -> Optional[str]:
        await self.connect()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT state FROM fsm_storage WHERE key = $1", _key_str(key))
            return row["state"] if row else None

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        await self.connect()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fsm_storage (key, state, data)
                VALUES ($1, NULL, $2::jsonb)
                ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data
                """,
                _key_str(key), json.dumps(data),
            )

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        await self.connect()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM fsm_storage WHERE key = $1", _key_str(key))
            if not row or row["data"] is None:
                return {}
            raw = row["data"]
            return json.loads(raw) if isinstance(raw, str) else dict(raw)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None


# =============================================================================
# БАЗА ДАННЫХ
# =============================================================================
_pool: Optional[asyncpg.Pool] = None


async def db_init(dsn: str):
    global _pool
    if _pool is not None:
        return
    _pool = await asyncpg.create_pool(dsn)
    async with _pool.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS posts (id SERIAL PRIMARY KEY)")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS title TEXT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS hashtag TEXT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS author TEXT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS channel_id BIGINT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS channel_message_id BIGINT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS delete_at TIMESTAMPTZ")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT false")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS items JSONB NOT NULL DEFAULT '[]'::jsonb")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()")

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_seen TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                added_by BIGINT,
                added_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bans (
                user_id BIGINT PRIMARY KEY,
                reason TEXT,
                banned_by BIGINT,
                banned_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS submissions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                items JSONB NOT NULL DEFAULT '[]'::jsonb,
                title TEXT,
                hashtag TEXT,
                cover_file_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                reviewed_by BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                channel_id BIGINT PRIMARY KEY,
                title TEXT,
                hidden BOOLEAN NOT NULL DEFAULT false,
                added_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute("ALTER TABLE channels ADD COLUMN IF NOT EXISTS hidden BOOLEAN NOT NULL DEFAULT false")
        # произвольные настройки бота (дисклеймер, .zakrep, уведомления)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )


async def db_close():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ---------- posts ----------
async def create_post(title: str, hashtag: str, author: str, channel_id: int, items: List[Dict[str, Any]]) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO posts (title, hashtag, author, channel_id, items) VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING id",
            title, hashtag, author, channel_id, json.dumps(items),
        )
        return row["id"]


async def set_post_publish_info(post_id: int, channel_message_id: int, delete_at: Optional[datetime]) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE posts SET channel_message_id = $1, delete_at = $2 WHERE id = $3",
            channel_message_id, delete_at, post_id,
        )


async def get_post(post_id: int) -> Optional[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT title, hashtag, author, items FROM posts WHERE id = $1", post_id)
        if not row:
            return None
        items = row["items"]
        items = json.loads(items) if isinstance(items, str) else items
        return {"title": row["title"], "hashtag": row["hashtag"], "author": row["author"], "items": items}


async def count_posts() -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS c FROM posts")
        return row["c"]


async def get_due_deletions() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, channel_id, channel_message_id FROM posts
            WHERE delete_at IS NOT NULL AND delete_at <= now()
              AND deleted = false AND channel_message_id IS NOT NULL
            """
        )
        return [dict(r) for r in rows]


async def mark_post_deleted(post_id: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE posts SET deleted = true WHERE id = $1", post_id)


# ---------- users (для broadcast/stats/уведомлений) ----------
async def touch_user(user_id: int, username: Optional[str]) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, username) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
            """,
            user_id, username,
        )


async def get_all_user_ids() -> List[int]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [r["user_id"] for r in rows]


async def count_users() -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS c FROM users")
        return row["c"]


# ---------- admins ----------
async def add_admin(user_id: int, username: Optional[str], added_by: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO admins (user_id, username, added_by) VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
            """,
            user_id, username, added_by,
        )


async def remove_admin(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        result = await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)
        return result.endswith("1")


async def list_admins() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, username, added_at FROM admins ORDER BY added_at")
        return [dict(r) for r in rows]


async def is_db_admin(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM admins WHERE user_id = $1", user_id)
        return row is not None


# ---------- баны ----------
async def ban_user(user_id: int, banned_by: int, reason: Optional[str]) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bans (user_id, reason, banned_by) VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by, banned_at = now()
            """,
            user_id, reason, banned_by,
        )


async def unban_user(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        result = await conn.execute("DELETE FROM bans WHERE user_id = $1", user_id)
        return result.endswith("1")


async def is_banned(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM bans WHERE user_id = $1", user_id)
        return row is not None


async def list_bans() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, reason, banned_at FROM bans ORDER BY banned_at DESC")
        return [dict(r) for r in rows]


async def count_bans() -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS c FROM bans")
        return row["c"]


# ---------- предложка ----------
async def create_submission(user_id: int, username: Optional[str], items: List[Dict[str, Any]],
                             title: str, hashtag: str, cover_file_id: str) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO submissions (user_id, username, items, title, hashtag, cover_file_id)
            VALUES ($1, $2, $3::jsonb, $4, $5, $6) RETURNING id
            """,
            user_id, username, json.dumps(items), title, hashtag, cover_file_id,
        )
        return row["id"]


async def get_submission(sub_id: int) -> Optional[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, user_id, username, items, title, hashtag, cover_file_id, status FROM submissions WHERE id = $1",
            sub_id,
        )
        if not row:
            return None
        items = row["items"]
        items = json.loads(items) if isinstance(items, str) else items
        return {
            "id": row["id"], "user_id": row["user_id"], "username": row["username"],
            "items": items, "title": row["title"], "hashtag": row["hashtag"],
            "cover_file_id": row["cover_file_id"], "status": row["status"],
        }


async def set_submission_status(sub_id: int, status: str, reviewed_by: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE submissions SET status = $1, reviewed_by = $2 WHERE id = $3",
            status, reviewed_by, sub_id,
        )


async def list_pending_submissions() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, user_id, username, created_at FROM submissions WHERE status = 'pending' ORDER BY created_at"
        )
        return [dict(r) for r in rows]


async def count_pending_submissions() -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS c FROM submissions WHERE status = 'pending'")
        return row["c"]


# ---------- каналы (автоопределение) ----------
async def add_channel(channel_id: int, title: Optional[str], unhide: bool = False) -> None:
    async with _pool.acquire() as conn:
        if unhide:
            await conn.execute(
                """
                INSERT INTO channels (channel_id, title, hidden) VALUES ($1, $2, false)
                ON CONFLICT (channel_id) DO UPDATE SET title = COALESCE(EXCLUDED.title, channels.title), hidden = false
                """,
                channel_id, title,
            )
        else:
            await conn.execute(
                """
                INSERT INTO channels (channel_id, title, hidden) VALUES ($1, $2, false)
                ON CONFLICT (channel_id) DO UPDATE SET title = COALESCE(EXCLUDED.title, channels.title)
                """,
                channel_id, title,
            )


async def remove_channel(channel_id: int) -> bool:
    async with _pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE channels SET hidden = true WHERE channel_id = $1 AND hidden = false",
            channel_id,
        )
        return result.endswith("1")


async def list_channels() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT channel_id, title FROM channels WHERE hidden = false ORDER BY added_at")
        return [dict(r) for r in rows]


# ---------- настройки (дисклеймер, .zakrep, уведомления) ----------
async def get_setting(key: str) -> Optional[str]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
        return row["value"] if row else None


async def set_setting(key: str, value: str) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            key, value,
        )


async def get_disclaimer_for_publish() -> str:
    """Пустая строка в settings = дисклеймер явно отключён (/deldisclaimer).
    Отсутствие строки = используется дефолтный текст."""
    value = await get_setting("disclaimer")
    if value is None:
        return DEFAULT_DISCLAIMER
    return value


async def is_auto_notify_enabled() -> bool:
    """По умолчанию автоуведомления о новых постах включены."""
    val = await get_setting("notify_new_posts")
    return val != "0"


# =============================================================================
# БОТ
# =============================================================================
router = Router()
BOT_USERNAME = ""

ACCEPTED_TYPES = {
    ContentType.VIDEO, ContentType.PHOTO, ContentType.DOCUMENT, ContentType.AUDIO,
    ContentType.VOICE, ContentType.ANIMATION, ContentType.VIDEO_NOTE, ContentType.TEXT,
}


class Batch(StatesGroup):
    collecting = State()
    waiting_title = State()
    waiting_cover = State()
    waiting_hashtag = State()
    waiting_autodelete = State()   # "удалить через 24 часа? да/нет"
    waiting_channel = State()      # выбор канала, если их несколько


class Submit(StatesGroup):
    collecting = State()
    waiting_title = State()
    waiting_hashtag = State()
    waiting_cover = State()


BATCH_STATES = {
    Batch.collecting.state, Batch.waiting_title.state, Batch.waiting_cover.state,
    Batch.waiting_hashtag.state, Batch.waiting_autodelete.state, Batch.waiting_channel.state,
}


# ---------- права доступа ----------
async def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID or user_id == SUPER_OWNER_ID


async def is_admin(user_id: int) -> bool:
    if await is_owner(user_id) or user_id in ADMIN_IDS:
        return True
    return await is_db_admin(user_id)


async def get_admin_ids() -> List[int]:
    ids = set(ADMIN_IDS)
    ids.add(OWNER_ID)
    ids.add(SUPER_OWNER_ID)
    for row in await list_admins():
        ids.add(row["user_id"])
    ids.discard(0)
    return list(ids)


# ---------- проверка обязательной подписки (.zakrep) ----------
def parse_zakrep_chat_id(link: str) -> Optional[str]:
    link = link.strip()
    if link.startswith("@"):
        return link
    m = re.match(r"^(?:https?://)?t\.me/([A-Za-z0-9_]{4,})/?$", link)
    if m:
        username = m.group(1)
        if not username.startswith("+") and username.lower() != "joinchat":
            return f"@{username}"
    return None


def normalize_zakrep_link(link: str) -> str:
    link = link.strip()
    if link.startswith("@"):
        return f"https://t.me/{link[1:]}"
    if not link.startswith("http://") and not link.startswith("https://"):
        return f"https://{link}"
    return link


async def check_user_subscription(bot: Bot, user_id: int) -> Tuple[bool, Optional[str]]:
    """Возвращает (подписан_ли, ссылка_на_канал)."""
    if await is_admin(user_id):
        return True, None
    enabled = await get_setting("zakrep_enabled")
    if enabled != "1":
        return True, None
    link = await get_setting("zakrep_link")
    chat_id_raw = await get_setting("zakrep_chat_id")
    if not link or not chat_id_raw:
        return True, None

    chat_id: Any = int(chat_id_raw) if chat_id_raw.lstrip("-").isdigit() else chat_id_raw
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        if member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        ):
            return True, None
        return False, link
    except Exception as e:
        log.warning("Ошибка проверки подписки для %s в %s: %s", user_id, chat_id, e)
        return False, link


def subscribe_keyboard(link: str, payload: str = "") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=link)],
        [InlineKeyboardButton(text="✅ Я подписался", callback_data=f"check_sub:{payload}")],
    ])


# ---------- клавиатуры ----------
def stop_keyboard(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🛑 Это всё, завершить ({count}/{MAX_BATCH_ITEMS})", callback_data="stop_batch")
    ]])


def submit_stop_keyboard(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🛑 Отправить на проверку ({count}/{MAX_BATCH_ITEMS})", callback_data="stop_submit")
    ]])


def submission_review_keyboard(sub_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"sub_appr:{sub_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"sub_rej:{sub_id}"),
    ]])


def channel_choice_keyboard(channels: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=ch["title"] or str(ch["channel_id"]), callback_data=f"pubch:{ch['channel_id']}")]
        for ch in channels
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def autodelete_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить через 24ч", callback_data="autodel:yes"),
        InlineKeyboardButton(text="❌ Нет, оставить навсегда", callback_data="autodel:no"),
    ]])


async def safe_delete(bot: Bot, chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramBadRequest as e:
        log.warning("Не смог удалить сообщение %s: %s", message_id, e)


async def update_counter(bot: Bot, chat_id: int, status_msg_id: Optional[int], text: str,
                          markup: InlineKeyboardMarkup) -> int:
    """Редактирует счётчик на месте вместо удаления+пересоздания."""
    if status_msg_id:
        try:
            msg = await bot.edit_message_text(text, chat_id=chat_id, message_id=status_msg_id, reply_markup=markup)
            return msg.message_id
        except TelegramBadRequest:
            pass
    msg = await bot.send_message(chat_id, text, reply_markup=markup)
    return msg.message_id


def item_from_message(message: Message) -> Optional[dict]:
    ctype = message.content_type
    msg_id = message.message_id
    if ctype == ContentType.TEXT:
        return {"type": "text", "text": message.text, "msg_id": msg_id}
    if ctype == ContentType.VIDEO:
        return {"type": "video", "file_id": message.video.file_id, "msg_id": msg_id}
    if ctype == ContentType.PHOTO:
        return {"type": "photo", "file_id": message.photo[-1].file_id, "msg_id": msg_id}
    if ctype == ContentType.DOCUMENT:
        return {"type": "document", "file_id": message.document.file_id, "msg_id": msg_id}
    if ctype == ContentType.AUDIO:
        return {"type": "audio", "file_id": message.audio.file_id, "msg_id": msg_id}
    if ctype == ContentType.VOICE:
        return {"type": "voice", "file_id": message.voice.file_id, "msg_id": msg_id}
    if ctype == ContentType.ANIMATION:
        return {"type": "animation", "file_id": message.animation.file_id, "msg_id": msg_id}
    if ctype == ContentType.VIDEO_NOTE:
        return {"type": "video_note", "file_id": message.video_note.file_id, "msg_id": msg_id}
    return None


async def send_item(bot: Bot, chat_id: int, item: dict):
    t = item["type"]
    if t == "text":
        await bot.send_message(chat_id, item["text"])
    elif t == "video":
        await bot.send_video(chat_id, item["file_id"])
    elif t == "photo":
        await bot.send_photo(chat_id, item["file_id"])
    elif t == "document":
        await bot.send_document(chat_id, item["file_id"])
    elif t == "audio":
        await bot.send_audio(chat_id, item["file_id"])
    elif t == "voice":
        await bot.send_voice(chat_id, item["file_id"])
    elif t == "animation":
        await bot.send_animation(chat_id, item["file_id"])
    elif t == "video_note":
        await bot.send_video_note(chat_id, item["file_id"])


# ---------- лок на чат, чтобы не терять файлы при параллельной обработке ----------
_batch_locks: Dict[int, asyncio.Lock] = {}


def get_batch_lock(chat_id: int) -> asyncio.Lock:
    lock = _batch_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _batch_locks[chat_id] = lock
    return lock


# ---------- пачечная пересылка контента владельцу в ЛС + удаление из чата ----------
_pending_forward: Dict[int, List[int]] = {}
_forward_tasks: Dict[int, asyncio.Task] = {}


def schedule_forward(bot: Bot, chat_id: int, message_id: int):
    if not OWNER_ID:
        return
    _pending_forward.setdefault(chat_id, []).append(message_id)
    old_task = _forward_tasks.get(chat_id)
    if old_task and not old_task.done():
        old_task.cancel()
    _forward_tasks[chat_id] = asyncio.create_task(_flush_forward(bot, chat_id))


async def _flush_forward(bot: Bot, chat_id: int):
    try:
        await asyncio.sleep(FORWARD_DEBOUNCE_SECONDS)
    except asyncio.CancelledError:
        return
    ids = sorted(set(_pending_forward.pop(chat_id, [])))
    if not ids:
        return
    try:
        await bot.forward_messages(chat_id=OWNER_ID, from_chat_id=chat_id, message_ids=ids)
    except TelegramBadRequest as e:
        log.warning("Не смог переслать пачку владельцу: %s", e)
    # Удаляем загруженные сообщения из чата после пересылки владельцу
    for mid in ids:
        await safe_delete(bot, chat_id, mid)


# ---------- учёт пользователей (для /broadcast и уведомлений) ----------
class TouchUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        if event.from_user and not event.from_user.is_bot:
            try:
                await touch_user(event.from_user.id, event.from_user.username)
            except Exception as e:
                log.warning("Не смог записать пользователя %s: %s", event.from_user.id, e)
        return await handler(event, data)


# ---------- автоопределение каналов ----------
@router.my_chat_member()
async def on_membership_changed(update: ChatMemberUpdated):
    if update.chat.type != ChatType.CHANNEL:
        return

    new_status = update.new_chat_member.status
    if new_status == ChatMemberStatus.ADMINISTRATOR:
        await add_channel(update.chat.id, update.chat.title, unhide=True)
        log.info("Канал зарегистрирован: %s (%s)", update.chat.title, update.chat.id)
        text = f"✅ Бот добавлен админом в канал «{update.chat.title}». Теперь можно публиковать туда."
        for admin_id in await get_admin_ids():
            try:
                await update.bot.send_message(admin_id, text)
            except TelegramBadRequest:
                pass
    elif new_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, ChatMemberStatus.MEMBER):
        await remove_channel(update.chat.id)
        log.info("Канал убран из списка: %s (%s)", update.chat.title, update.chat.id)


# ---------- .zakrep — обязательная подписка на канал ----------
@router.message(F.text.startswith(".zakrep") | F.text.startswith("/zakrep"))
async def cmd_zakrep(message: Message):
    if not await is_admin(message.from_user.id):
        return

    parts = message.text.split()
    # Просто ".zakrep" — показать статус
    if len(parts) == 1:
        enabled = await get_setting("zakrep_enabled") == "1"
        link = await get_setting("zakrep_link") or "не задана"
        chat_id = await get_setting("zakrep_chat_id") or "не задан"
        status = "✅ Включена" if enabled else "❌ Выключена"
        await message.answer(
            f"🔒 Подписка: {status}\n"
            f"Ссылка: {link}\n"
            f"Канал: <code>{chat_id}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    arg1 = parts[1].strip()
    if arg1.lower() in ("off", "выкл", "0", "disable"):
        await set_setting("zakrep_enabled", "0")
        await message.answer("🔓 Обязательная подписка отключена.")
        return

    if arg1.lower() in ("on", "вкл", "1", "enable"):
        link = await get_setting("zakrep_link")
        chat_id = await get_setting("zakrep_chat_id")
        if not link or not chat_id:
            await message.answer("❌ Сначала задай ссылку: <code>.zakrep https://t.me/канал</code>", parse_mode=ParseMode.HTML)
            return
        await set_setting("zakrep_enabled", "1")
        await message.answer(f"🔒 Обязательная подписка включена.\nКанал: {link}")
        return

    link = normalize_zakrep_link(arg1)
    chat_id = parts[2].strip() if len(parts) >= 3 else parse_zakrep_chat_id(arg1)

    if not chat_id:
        await message.answer(
            "⚠️ Для приватной ссылки (t.me/+...) нужно указать ID канала вторым аргументом:\n"
            "<code>.zakrep https://t.me/+ссылка -1001234567890</code>\n"
            "(ID канала можно посмотреть командой /channels)",
            parse_mode=ParseMode.HTML,
        )
        return

    resolved_id = str(chat_id)
    warn = ""
    try:
        target: Any = int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id
        chat = await message.bot.get_chat(target)
        resolved_id = str(chat.id)
        me = await message.bot.get_me()
        bot_member = await message.bot.get_chat_member(chat.id, me.id)
        if bot_member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
            warn = "\n⚠️ Бот НЕ админ в этом канале — добавь его админом, иначе проверка не сработает."
    except Exception as e:
        warn = f"\n⚠️ Не смог проверить доступ к каналу ({e}). Убедись, что бот добавлен туда админом."

    await set_setting("zakrep_link", link)
    await set_setting("zakrep_chat_id", resolved_id)
    await set_setting("zakrep_enabled", "1")
    await message.answer(
        f"✅ Подписка включена: {link} (<code>{resolved_id}</code>){warn}",
        parse_mode=ParseMode.HTML,
    )


# ---------- /start и раздача контента ----------
async def _deliver_start_payload(bot: Bot, chat_id: int, payload: Optional[str]):
    if not payload or not payload.startswith("p_"):
        await bot.send_message(
            chat_id,
            "Привет! Открой ссылку из поста в канале, чтобы получить контент.\n"
            "Хочешь предложить свой контент для канала — напиши /submit.\n"
            "Все команды: /help"
        )
        return

    try:
        post_id = int(payload[2:])
    except ValueError:
        await bot.send_message(chat_id, "Некорректная ссылка.")
        return

    post = await get_post(post_id)
    if not post:
        await bot.send_message(chat_id, "Контент не найден (возможно, был удалён).")
        return

    await bot.send_message(chat_id, f"📦 «{post['title']}»")
    for item in post["items"]:
        try:
            await send_item(bot, chat_id, item)
        except TelegramBadRequest as e:
            log.warning("Ошибка при отправке контента пользователю: %s", e)
            await bot.send_message(chat_id, "⚠️ Часть контента не удалось отправить.")


@router.message(CommandStart())
async def cmd_start(message: Message):
    if await is_banned(message.from_user.id):
        return

    args = message.text.split(maxsplit=1)
    payload = args[1] if len(args) > 1 else None

    is_sub, link = await check_user_subscription(message.bot, message.from_user.id)
    if not is_sub and link:
        await message.answer(
            "🔒 Чтобы пользоваться ботом и получить контент, подпишись на наш канал:",
            reply_markup=subscribe_keyboard(link, payload or ""),
        )
        return

    await _deliver_start_payload(message.bot, message.chat.id, payload)


@router.callback_query(F.data.startswith("check_sub:"))
async def cb_check_sub(callback: CallbackQuery):
    if await is_banned(callback.from_user.id):
        await callback.answer()
        return

    is_sub, link = await check_user_subscription(callback.bot, callback.from_user.id)
    if not is_sub:
        await callback.answer("❌ Вы ещё не подписались на канал!", show_alert=True)
        return

    await callback.answer("✅ Подписка подтверждена!")
    await safe_delete(callback.bot, callback.message.chat.id, callback.message.message_id)
    payload = callback.data.split(":", 1)[1] or None
    await _deliver_start_payload(callback.bot, callback.message.chat.id, payload)


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    if current is None:
        return
    if current in BATCH_STATES and not await is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Отменено. Всё сброшено.")


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш Telegram ID: {message.from_user.id}")


@router.message(Command("help"))
async def cmd_help(message: Message):
    uid = message.from_user.id
    lines = [
        "<b>Всем:</b>",
        "/start — получить контент по ссылке из поста",
        "/submit — предложить контент для канала (пришлют на проверку админам)",
        "/myid — узнать свой Telegram ID",
        "/cancel — отменить текущую загрузку",
        "/help — этот список",
    ]
    if await is_admin(uid):
        lines += [
            "",
            "<b>Админам:</b>",
            "/batch — загрузить и опубликовать пост",
            "/done — завершить загрузку",
            "/zakrep &lt;ссылка&gt; [id] — обязательная подписка (off/on — выкл/вкл)",
            "/notify &lt;текст&gt; — рассылка всем (или ответом на пост)",
            "/notify_on / /notify_off — автоуведомления о новых постах",
            "/pending — заявки из предложки, ожидающие решения",
            "/listadm — список админов",
            "/ban &lt;id&gt; [причина], /unban &lt;id&gt;, /banlist — баны",
            "/channels — каналы, куда бот сейчас может публиковать",
            "/delchannel &lt;id&gt; — убрать канал из списка публикации",
            "/stats — статистика бота",
            "/disclaimer — показать текущий текст дисклеймера",
            "/setdisclaimer &lt;текст&gt; — задать/сменить свой дисклеймер",
            "/deldisclaimer — убрать дисклеймер из постов совсем",
        ]
    if await is_owner(uid):
        lines += [
            "",
            "<b>Только владельцу:</b>",
            "/add &lt;id&gt; — сделать пользователя админом",
            "/deladm &lt;id&gt; — снять админку",
        ]
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not await is_admin(message.from_user.id):
        return
    posts_n = await count_posts()
    users_n = await count_users()
    admins_n = len(await get_admin_ids())
    bans_n = await count_bans()
    pending_n = await count_pending_submissions()
    channels_n = len(await list_channels())
    zakrep_on = "вкл" if await get_setting("zakrep_enabled") == "1" else "выкл"
    notify_on = "вкл" if await is_auto_notify_enabled() else "выкл"
    await message.answer(
        "📊 Статистика:\n"
        f"Постов опубликовано: {posts_n}\n"
        f"Каналов подключено: {channels_n}\n"
        f"Пользователей знает бот: {users_n}\n"
        f"Админов: {admins_n}\n"
        f"Забанено: {bans_n}\n"
        f"Заявок в ожидании: {pending_n}\n"
        f"Обязательная подписка (.zakrep): {zakrep_on}\n"
        f"Автоуведомления о постах: {notify_on}"
    )


@router.message(Command("channels"))
async def cmd_channels(message: Message):
    if not await is_admin(message.from_user.id):
        return
    channels = await list_channels()
    if not channels:
        await message.answer(
            "Бот пока не админ ни в одном канале.\n"
            "Добавьте его в канал с правом \"Публикация сообщений\" — появится тут сам."
        )
        return
    lines = [f"• {ch['title'] or '(без названия)'} (id {ch['channel_id']})" for ch in channels]
    await message.answer("📡 Каналы для публикации:\n" + "\n".join(lines) + "\n\nУбрать канал: /delchannel <id>")


@router.message(Command("delchannel"))
async def cmd_delchannel(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /delchannel <channel_id> (id смотри в /channels)")
        return
    channel_id = int(args[1].strip())
    ok = await remove_channel(channel_id)
    if ok:
        await message.answer(f"✅ Канал {channel_id} убран из списка публикации.")
    else:
        await message.answer(f"❌ Канал {channel_id} не найден в списке. Проверь ID через /channels.")


# ============ дисклеймер ============
@router.message(Command("disclaimer"))
async def cmd_disclaimer(message: Message):
    if not await is_admin(message.from_user.id):
        return
    raw = await get_setting("disclaimer")
    if raw is None:
        await message.answer(f"Сейчас используется дефолтный дисклеймер:\n«{DEFAULT_DISCLAIMER}»")
    elif raw == "":
        await message.answer("Дисклеймер сейчас отключён — в постах его не будет.")
    else:
        await message.answer(f"Текущий дисклеймер:\n«{raw}»")


@router.message(Command("setdisclaimer"))
async def cmd_setdisclaimer(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer("Использование: /setdisclaimer <текст>")
        return
    text = args[1].strip()
    await set_setting("disclaimer", text)
    await message.answer(f"✅ Дисклеймер обновлён:\n«{text}»")


@router.message(Command("deldisclaimer"))
async def cmd_deldisclaimer(message: Message):
    if not await is_admin(message.from_user.id):
        return
    await set_setting("disclaimer", "")
    await message.answer("✅ Дисклеймер убран — новые посты будут выходить без него.")


# ============ автоуведомления о новых постах ============
@router.message(Command("notify_on"))
async def cmd_notify_on(message: Message):
    if not await is_admin(message.from_user.id):
        return
    await set_setting("notify_new_posts", "1")
    await message.answer("🔔 Автоуведомления всем пользователям о новых постах ВКЛЮЧЕНЫ.")


@router.message(Command("notify_off"))
async def cmd_notify_off(message: Message):
    if not await is_admin(message.from_user.id):
        return
    await set_setting("notify_new_posts", "0")
    await message.answer("🔕 Автоуведомления о новых постах ВЫКЛЮЧЕНЫ.")


async def notify_all_users_about_post(bot: Bot, post_id: int, title: str, hashtag: str, cover_file_id: str):
    """Фоновая рассылка уведомления о новом посте всем пользователям бота."""
    user_ids = await get_all_user_ids()
    banned = {r["user_id"] for r in await list_bans()}
    targets = [uid for uid in user_ids if uid not in banned]
    if not targets:
        return

    deep_link = f"https://t.me/{BOT_USERNAME}?start=p_{post_id}"
    caption = f"🔥 <b>Новый пост!</b>\n\n<b>{title}</b>\n{hashtag}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🎬 Смотреть контент", url=deep_link)
    ]])

    semaphore = asyncio.Semaphore(BROADCAST_CONCURRENCY)

    async def worker(uid: int):
        async with semaphore:
            for _ in range(2):
                try:
                    await bot.send_photo(uid, cover_file_id, caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb)
                    return
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                except (TelegramBadRequest, TelegramForbiddenError):
                    return

    await asyncio.gather(*(worker(uid) for uid in targets))
    log.info("Уведомление о посте #%s разослано (%s чел.)", post_id, len(targets))


# ============ /batch — приём контента админом, публикация в канал ============
@router.message(Command("batch"))
async def cmd_batch(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("Эта команда только для админов.")
        return
    await state.clear()
    await state.set_state(Batch.collecting)
    await state.update_data(media=[], status_msg_id=None, submission_id=None, author_name=None)
    await message.answer(
        f"📥 Приём начат (макс. {MAX_BATCH_ITEMS} файлов). Кидай контент, потом жми кнопку или /done."
    )


@router.message(Command("done"))
async def cmd_done(message: Message, state: FSMContext):
    current = await state.get_state()
    if current == Batch.collecting.state:
        if not await is_admin(message.from_user.id):
            return
        await finish_collecting(message.bot, message.chat.id, message.from_user, state)
    elif current == Submit.collecting.state:
        await start_submission_metadata(message.bot, message.chat.id, message.from_user, state)


@router.message(StateFilter(Batch.collecting), F.content_type.in_(ACCEPTED_TYPES))
async def collect_media(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    if message.content_type == ContentType.TEXT and (message.text.startswith("/") or message.text.startswith(".")):
        return

    item = item_from_message(message)
    if item is None:
        return

    lock = get_batch_lock(message.chat.id)
    async with lock:
        data = await state.get_data()
        media = data.get("media", [])

        if len(media) >= MAX_BATCH_ITEMS:
            return

        media.append(item)
        status_id = await update_counter(
            message.bot, message.chat.id, data.get("status_msg_id"),
            f"Загружено файлов: {len(media)}. Это всё?", stop_keyboard(len(media)),
        )
        await state.update_data(media=media, status_msg_id=status_id)

        if len(media) == MAX_BATCH_ITEMS:
            await message.answer(f"⚠️ Достигнут лимит {MAX_BATCH_ITEMS} файлов за раз. Жми «Это всё» и публикуй эту пачку.")

    # Если грузит не владелец — сначала пересылаем владельцу, потом удаляем из чата.
    # Если грузит сам владелец (или OWNER_ID не задан) — удаляем из чата сразу после загрузки.
    if OWNER_ID and message.from_user.id != OWNER_ID:
        schedule_forward(message.bot, message.chat.id, message.message_id)
    else:
        await safe_delete(message.bot, message.chat.id, message.message_id)


@router.callback_query(F.data == "stop_batch", StateFilter(Batch.collecting))
async def cb_stop_batch(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    await callback.answer()
    await finish_collecting(callback.bot, callback.message.chat.id, callback.from_user, state)


async def finish_collecting(bot: Bot, chat_id: int, from_user, state: FSMContext):
    data = await state.get_data()
    media = data.get("media", [])

    if not media:
        await bot.send_message(chat_id, "Файлов не было, отмена.")
        await state.clear()
        return

    if data.get("status_msg_id"):
        await safe_delete(bot, chat_id, data["status_msg_id"])

    await state.update_data(admin_id=from_user.id, admin_name=from_user.full_name)
    await state.set_state(Batch.waiting_title)
    await bot.send_message(
        chat_id,
        f"✅ Принято файлов: {len(media)}.\nТеперь пришли *название* поста.",
        parse_mode=ParseMode.MARKDOWN,
    )


@router.message(StateFilter(Batch.waiting_title), F.text)
async def get_title(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(title=message.text.strip())
    await state.set_state(Batch.waiting_cover)
    await message.answer("🖼 Теперь пришли обложку (фото).")


@router.message(StateFilter(Batch.waiting_cover), F.photo)
async def get_cover(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(cover_file_id=message.photo[-1].file_id)
    await state.set_state(Batch.waiting_hashtag)
    await message.answer("#️⃣ Теперь пришли хэштег (например: #видео).")


@router.message(StateFilter(Batch.waiting_hashtag), F.text)
async def get_hashtag(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    hashtag = message.text.strip()
    if not hashtag.startswith("#"):
        hashtag = "#" + hashtag
    await state.update_data(hashtag=hashtag)
    await ask_autodelete(message.bot, message.chat.id, state)


@router.callback_query(F.data.startswith("autodel:"), StateFilter(Batch.waiting_autodelete))
async def cb_autodelete_choice(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    auto_delete = callback.data.split(":")[1] == "yes"
    await state.update_data(auto_delete=auto_delete)
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    await choose_channel_and_publish(callback.bot, callback.message.chat.id, callback.from_user.id, state)


async def ask_autodelete(bot: Bot, chat_id: int, state: FSMContext):
    await state.set_state(Batch.waiting_autodelete)
    await bot.send_message(chat_id, "🕒 Удалить этот пост из канала автоматически через 24 часа?", reply_markup=autodelete_keyboard())


async def choose_channel_and_publish(bot: Bot, chat_id: int, actor_id: int, state: FSMContext):
    channels = await list_channels()

    if not channels:
        await bot.send_message(
            chat_id,
            "❌ Бот пока не админ ни в одном канале — публиковать некуда.\n"
            "Добавьте его в канал с правом \"Публикация сообщений\" и повторите /done."
        )
        return

    if len(channels) == 1:
        await publish_post(bot, chat_id, actor_id, state, channels[0]["channel_id"])
        return

    await state.set_state(Batch.waiting_channel)
    await bot.send_message(chat_id, "В какой канал опубликовать?", reply_markup=channel_choice_keyboard(channels))


@router.callback_query(F.data.startswith("pubch:"), StateFilter(Batch.waiting_channel))
async def cb_choose_channel(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    channel_id = int(callback.data.split(":")[1])
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    await publish_post(callback.bot, callback.message.chat.id, callback.from_user.id, state, channel_id)


async def publish_post(bot: Bot, chat_id: int, actor_id: int, state: FSMContext, channel_id: int):
    data = await state.get_data()
    media = data["media"]
    title = data["title"]
    cover_file_id = data["cover_file_id"]
    hashtag = data["hashtag"]
    auto_delete = data.get("auto_delete", False)
    admin_name = data.get("author_name") or data.get("admin_name", "admin")
    submission_id = data.get("submission_id")

    await bot.send_chat_action(chat_id, ChatAction.TYPING)

    post_id = await create_post(title=title, hashtag=hashtag, author=admin_name, channel_id=channel_id, items=media)
    deep_link = f"https://t.me/{BOT_USERNAME}?start=p_{post_id}"

    disclaimer = await get_disclaimer_for_publish()
    disclaimer_block = f"{disclaimer}\n\n" if disclaimer else ""

    caption = (
        f"<b>{title}</b>\n\n"
        f'<a href="{deep_link}">видео здесь</a>\n\n'
        f"{disclaimer_block}"
        f"{hashtag}\n\n"
        f"Автор: {admin_name}"
    )

    try:
        sent = await bot.send_photo(channel_id, cover_file_id, caption=caption, parse_mode=ParseMode.HTML)
    except TelegramBadRequest as e:
        await bot.send_message(chat_id, f"❌ Не смог опубликовать пост в канал: {e}")
        await state.clear()
        return

    delete_at = datetime.now(timezone.utc) + timedelta(hours=24) if auto_delete else None
    await set_post_publish_info(post_id, sent.message_id, delete_at)

    if submission_id:
        await set_submission_status(submission_id, "approved", actor_id)
        sub = await get_submission(submission_id)
        if sub:
            try:
                await bot.send_message(sub["user_id"], "🎉 Ваш пост одобрен и опубликован в канале!")
            except TelegramBadRequest:
                pass

    # Фоновая рассылка уведомления всем пользователям о новом посте
    if await is_auto_notify_enabled():
        asyncio.create_task(notify_all_users_about_post(bot, post_id, title, hashtag, cover_file_id))

    done_text = f"✅ Готово! Опубликовано в канал.\nСсылка на контент: {deep_link}"
    if auto_delete:
        done_text += "\n🕒 Пост удалится из канала автоматически через 24 часа."
    if await is_auto_notify_enabled():
        done_text += "\n🔔 Уведомления пользователям отправляются в фоне."
    await bot.send_message(chat_id, done_text)
    await state.clear()


# ============ /submit — предложка ============
@router.message(Command("submit"))
async def cmd_submit(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("Пришли /submit мне в личные сообщения.")
        return
    if await is_banned(message.from_user.id):
        await message.answer("Вам запрещено пользоваться ботом.")
        return

    is_sub, link = await check_user_subscription(message.bot, message.from_user.id)
    if not is_sub and link:
        await message.answer(
            "🔒 Чтобы предлагать контент, сначала подпишись на наш канал:",
            reply_markup=subscribe_keyboard(link, ""),
        )
        return

    await state.clear()
    await state.set_state(Submit.collecting)
    await state.update_data(media=[], status_msg_id=None)
    await message.answer(
        f"📥 Пришли контент, который хочешь предложить для канала (макс. {MAX_BATCH_ITEMS} файлов).\n"
        "Когда закончишь — жми кнопку, либо пришли /done."
    )


@router.message(StateFilter(Submit.collecting), F.content_type.in_(ACCEPTED_TYPES))
async def collect_submission(message: Message, state: FSMContext):
    if await is_banned(message.from_user.id):
        await state.clear()
        return
    if message.content_type == ContentType.TEXT and message.text.startswith("/"):
        return

    item = item_from_message(message)
    if item is None:
        return

    lock = get_batch_lock(message.chat.id)
    async with lock:
        data = await state.get_data()
        media = data.get("media", [])

        if len(media) >= MAX_BATCH_ITEMS:
            return

        media.append(item)
        status_id = await update_counter(
            message.bot, message.chat.id, data.get("status_msg_id"),
            f"Загружено файлов: {len(media)}. Отправить на проверку?", submit_stop_keyboard(len(media)),
        )
        await state.update_data(media=media, status_msg_id=status_id)

        if len(media) == MAX_BATCH_ITEMS:
            await message.answer(f"⚠️ Достигнут лимит {MAX_BATCH_ITEMS} файлов за раз. Жми «Отправить на проверку».")


@router.callback_query(F.data == "stop_submit", StateFilter(Submit.collecting))
async def cb_stop_submit(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await start_submission_metadata(callback.bot, callback.message.chat.id, callback.from_user, state)


async def start_submission_metadata(bot: Bot, chat_id: int, from_user, state: FSMContext):
    data = await state.get_data()
    media = data.get("media", [])

    if not media:
        await bot.send_message(chat_id, "Файлов не было, отмена.")
        await state.clear()
        return

    if data.get("status_msg_id"):
        await safe_delete(bot, chat_id, data["status_msg_id"])

    await state.set_state(Submit.waiting_title)
    await bot.send_message(chat_id, f"✅ Принято файлов: {len(media)}.\nТеперь пришли *название* поста.", parse_mode=ParseMode.MARKDOWN)


@router.message(StateFilter(Submit.waiting_title), F.text)
async def submit_get_title(message: Message, state: FSMContext):
    if await is_banned(message.from_user.id):
        await state.clear()
        return
    await state.update_data(title=message.text.strip())
    await state.set_state(Submit.waiting_hashtag)
    await message.answer("#️⃣ Теперь пришли хэштег (например: #видео).")


@router.message(StateFilter(Submit.waiting_hashtag), F.text)
async def submit_get_hashtag(message: Message, state: FSMContext):
    if await is_banned(message.from_user.id):
        await state.clear()
        return
    hashtag = message.text.strip()
    if not hashtag.startswith("#"):
        hashtag = "#" + hashtag
    await state.update_data(hashtag=hashtag)
    await state.set_state(Submit.waiting_cover)
    await message.answer("🖼 Теперь пришли обложку (фото).")


@router.message(StateFilter(Submit.waiting_cover), F.photo)
async def submit_get_cover(message: Message, state: FSMContext):
    if await is_banned(message.from_user.id):
        await state.clear()
        return
    await state.update_data(cover_file_id=message.photo[-1].file_id)
    await finalize_submission(message.bot, message.chat.id, message.from_user, state)


async def finalize_submission(bot: Bot, chat_id: int, from_user, state: FSMContext):
    data = await state.get_data()
    media = data.get("media", [])
    title = data.get("title")
    hashtag = data.get("hashtag")
    cover_file_id = data.get("cover_file_id")
    await state.clear()

    if not (media and title and hashtag and cover_file_id):
        await bot.send_message(chat_id, "Что-то пошло не так, попробуй /submit заново.")
        return

    sub_id = await create_submission(from_user.id, from_user.username, media, title, hashtag, cover_file_id)
    await bot.send_message(chat_id, "✅ Отправлено на проверку. Спасибо! Как только админ решит — напишу вам.")

    info_text = (
        f"📨 Новая заявка на публикацию #{sub_id}\n"
        f"От: {from_user.full_name} (@{from_user.username or '—'}, id {from_user.id})\n"
        f"Название: {title}\nХэштег: {hashtag}\nФайлов: {len(media)}\n\n"
        "⬆️ Контент — выше, обложка — ниже."
    )
    for admin_id in await get_admin_ids():
        try:
            for item in media:
                await send_item(bot, admin_id, item)
            await bot.send_photo(admin_id, cover_file_id, caption=f"Обложка. «{title}»")
            await bot.send_message(admin_id, info_text, reply_markup=submission_review_keyboard(sub_id))
        except TelegramBadRequest:
            pass


@router.callback_query(F.data.startswith("sub_appr:"))
async def cb_submission_approve(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Только для админов.", show_alert=True)
        return

    sub_id = int(callback.data.split(":")[1])
    sub = await get_submission(sub_id)
    if not sub or sub["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)

    author_name = f"@{sub['username']}" if sub["username"] else f"id{sub['user_id']}"

    await state.clear()
    await state.update_data(
        media=sub["items"],
        title=sub["title"],
        hashtag=sub["hashtag"],
        cover_file_id=sub["cover_file_id"],
        admin_id=callback.from_user.id,
        admin_name=callback.from_user.full_name,
        author_name=author_name,
        submission_id=sub_id,
    )
    await callback.message.answer(f"Заявка #{sub_id} одобрена, публикуем «{sub['title']}».")
    await ask_autodelete(callback.bot, callback.message.chat.id, state)


@router.callback_query(F.data.startswith("sub_rej:"))
async def cb_submission_reject(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Только для админов.", show_alert=True)
        return

    sub_id = int(callback.data.split(":")[1])
    sub = await get_submission(sub_id)
    if not sub or sub["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    await set_submission_status(sub_id, "rejected", callback.from_user.id)
    await callback.answer("Отклонено.")
    await callback.message.edit_reply_markup(reply_markup=None)
    try:
        await callback.bot.send_message(sub["user_id"], "😕 Ваш пост отклонён администратором.")
    except TelegramBadRequest:
        pass


@router.message(Command("pending"))
async def cmd_pending(message: Message):
    if not await is_admin(message.from_user.id):
        return
    rows = await list_pending_submissions()
    if not rows:
        await message.answer("Заявок в ожидании нет.")
        return
    lines = [f"#{r['id']} — id{r['user_id']} (@{r['username'] or '—'})" for r in rows]
    await message.answer("Заявки в ожидании:\n" + "\n".join(lines))


# ============ управление админами — только владелец ============
@router.message(Command("add"))
async def cmd_add_admin(message: Message):
    if not await is_owner(message.from_user.id):
        await message.answer("Добавлять админов может только владелец бота.")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /add <telegram_id>")
        return
    new_id = int(args[1].strip())
    await add_admin(new_id, None, message.from_user.id)
    await message.answer(f"✅ id{new_id} добавлен в админы.")
    try:
        await message.bot.send_message(new_id, "Вас назначили админом бота. Все команды: /help")
    except TelegramBadRequest:
        pass


@router.message(Command("deladm"))
async def cmd_del_admin(message: Message):
    if not await is_owner(message.from_user.id):
        await message.answer("Убирать админов может только владелец бота.")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /deladm <telegram_id>")
        return
    del_id = int(args[1].strip())
    if del_id == SUPER_OWNER_ID:
        await message.answer("Этого пользователя нельзя убрать из админов.")
        return
    ok = await remove_admin(del_id)
    await message.answer(
        f"✅ id{del_id} убран из админов." if ok
        else "Такого динамического админа нет в базе (возможно, задан статично через ADMIN_IDS в .env)."
    )


@router.message(Command("listadm"))
async def cmd_list_admins(message: Message):
    if not await is_admin(message.from_user.id):
        return
    lines = [f"👑 Владелец: id{OWNER_ID or '(не задан в .env)'}, всегда — id{SUPER_OWNER_ID}"]
    if ADMIN_IDS:
        lines.append("Статичные (.env): " + ", ".join(f"id{i}" for i in ADMIN_IDS))
    db_admins = await list_admins()
    if db_admins:
        lines.append("Динамические (/add): " + ", ".join(f"id{r['user_id']}" for r in db_admins))
    await message.answer("\n".join(lines))


# ============ баны — доступны любому админу ============
@router.message(Command("ban"))
async def cmd_ban(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /ban <telegram_id> [причина]")
        return
    target_id = int(args[1].strip())
    if target_id == SUPER_OWNER_ID or await is_admin(target_id):
        await message.answer("Нельзя забанить админа или владельца.")
        return
    reason = args[2].strip() if len(args) > 2 else None
    await ban_user(target_id, message.from_user.id, reason)
    await message.answer(f"🚫 id{target_id} забанен." + (f" Причина: {reason}" if reason else ""))


@router.message(Command("unban"))
async def cmd_unban(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /unban <telegram_id>")
        return
    target_id = int(args[1].strip())
    ok = await unban_user(target_id)
    await message.answer(f"✅ id{target_id} разбанен." if ok else "Этот id не был забанен.")


@router.message(Command("banlist"))
async def cmd_banlist(message: Message):
    if not await is_admin(message.from_user.id):
        return
    rows = await list_bans()
    if not rows:
        await message.answer("Забаненных нет.")
        return
    lines = [f"id{r['user_id']}" + (f" — {r['reason']}" if r["reason"] else "") for r in rows]
    await message.answer("Забанены:\n" + "\n".join(lines))


# ============ /broadcast и /notify ============
async def _broadcast_send(bot: Bot, uid: int, source: Optional[Message], text: Optional[str]) -> bool:
    for _ in range(2):
        try:
            if source:
                await bot.copy_message(uid, source.chat.id, source.message_id)
            else:
                await bot.send_message(uid, text)
            return True
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            continue
        except (TelegramBadRequest, TelegramForbiddenError):
            return False
    return False


@router.message(Command("broadcast", "notify"))
async def cmd_broadcast(message: Message):
    if not await is_admin(message.from_user.id):
        return

    source = message.reply_to_message
    args = message.text.split(maxsplit=1)
    text = args[1].strip() if len(args) > 1 else None

    if not source and not text:
        await message.answer(
            "Использование:\n"
            "/notify <текст> (или /broadcast <текст>) — разослать текст всем\n"
            "или ответь командой /notify на сообщение (фото/видео/файл) — разошлю его копией."
        )
        return

    user_ids = await get_all_user_ids()
    banned = {r["user_id"] for r in await list_bans()}
    targets = [uid for uid in user_ids if uid not in banned]

    status = await message.answer(f"⏳ Рассылка на {len(targets)} чел...")

    counters = {"sent": 0, "failed": 0}
    semaphore = asyncio.Semaphore(BROADCAST_CONCURRENCY)

    async def worker(uid: int):
        async with semaphore:
            ok = await _broadcast_send(message.bot, uid, source, text)
            counters["sent" if ok else "failed"] += 1

    await asyncio.gather(*(worker(uid) for uid in targets))

    await status.edit_text(f"✅ Рассылка завершена. Доставлено: {counters['sent']}, не удалось: {counters['failed']}.")


# ============ автоудаление постов через 24 часа ============
async def auto_delete_loop(bot: Bot):
    while True:
        await asyncio.sleep(AUTO_DELETE_CHECK_INTERVAL)
        try:
            due = await get_due_deletions()
        except Exception as e:
            log.warning("Не смог проверить автоудаление: %s", e)
            continue

        for row in due:
            try:
                await bot.delete_message(row["channel_id"], row["channel_message_id"])
            except TelegramBadRequest as e:
                log.warning("Не смог удалить пост %s из канала: %s", row["id"], e)
            await mark_post_deleted(row["id"])


# ============ health-check сервер (для Render free / анти-сон) ============
async def start_health_server():
    app = web.Application()
    app.router.add_get("/", lambda request: web.Response(text="OK"))
    app.router.add_get("/health", lambda request: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Health-check сервер слушает порт %s", port)


async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        log.info("RENDER_EXTERNAL_URL не задан — самопинг выключен")
        return
    async with ClientSession(timeout=ClientTimeout(total=15)) as session:
        while True:
            await asyncio.sleep(SELF_PING_INTERVAL)
            try:
                async with session.get(url) as resp:
                    log.info("Самопинг: %s -> %s", url, resp.status)
            except Exception as e:
                log.warning("Самопинг не удался: %s", e)


async def bootstrap_admins():
    for admin_id in BOOTSTRAP_ADMIN_IDS | {SUPER_OWNER_ID}:
        if admin_id and admin_id != OWNER_ID:
            await add_admin(admin_id, None, added_by=OWNER_ID or admin_id)
            log.info("Bootstrap: id%s добавлен в админы", admin_id)


async def main():
    global BOT_USERNAME

    if not BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN в .env")
    if not DATABASE_URL:
        raise SystemExit("Укажи DATABASE_URL (строка подключения к Neon) в .env")

    storage = PostgresStorage(DATABASE_URL)
    await storage.connect()
    await db_init(DATABASE_URL)
    await bootstrap_admins()

    if LEGACY_CHANNEL_ID:
        await add_channel(LEGACY_CHANNEL_ID, None)
        log.info("Зарегистрирован канал из CHANNEL_ID (.env): %s", LEGACY_CHANNEL_ID)

    bot = Bot(token=BOT_TOKEN)
    me = await bot.get_me()
    BOT_USERNAME = me.username

    dp = Dispatcher(storage=storage)
    dp.message.outer_middleware(TouchUserMiddleware())
    dp.include_router(router)

    try:
        await start_health_server()
        asyncio.create_task(self_ping_loop())
        asyncio.create_task(auto_delete_loop(bot))
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await storage.close()
        await db_close()


if __name__ == "__main__":
    asyncio.run(main())
