"""
Бот для автопостинга контента в канал(ы) — всё в одном файле.

    pip install -r requirements.txt
    заполнить .env
    python bot.py
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
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat,
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
BROADCAST_CONCURRENCY = int(os.getenv("BROADCAST_CONCURRENCY", "25"))
AUTO_DELETE_CHECK_INTERVAL = 60
SCHEDULE_CHECK_INTERVAL = 30
SELF_PING_INTERVAL = 300
MAX_BATCH_ITEMS = int(os.getenv("MAX_BATCH_ITEMS", "100"))

# Дефолтное фото пилы для /start (можно заменить своей фоткой командой /setwelcome)
DEFAULT_WELCOME_PHOTO = os.getenv(
    "WELCOME_PHOTO_URL",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3c/Handsaw.jpg/640px-Handsaw.jpg",
)

SUPER_OWNER_ID = 682868256

BOOTSTRAP_ADMIN_IDS = {
    int(x) for x in os.getenv("BOOTSTRAP_ADMIN_IDS", str(SUPER_OWNER_ID)).split(",") if x.strip()
}

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
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS cover_file_id TEXT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS delete_at TIMESTAMPTZ")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT false")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS views INT NOT NULL DEFAULT 0")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS items JSONB NOT NULL DEFAULT '[]'::jsonb")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()")

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                hashtag TEXT NOT NULL,
                author TEXT NOT NULL,
                channel_id BIGINT NOT NULL,
                cover_file_id TEXT NOT NULL,
                items JSONB NOT NULL DEFAULT '[]'::jsonb,
                auto_delete BOOLEAN NOT NULL DEFAULT false,
                publish_at TIMESTAMPTZ NOT NULL,
                actor_id BIGINT,
                submission_id INT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                notify_enabled BOOLEAN NOT NULL DEFAULT true,
                first_seen TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_enabled BOOLEAN NOT NULL DEFAULT true")

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
async def create_post(
    title: str, hashtag: str, author: str, channel_id: int, items: List[Dict[str, Any]], cover_file_id: Optional[str] = None
) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO posts (title, hashtag, author, channel_id, cover_file_id, items) VALUES ($1, $2, $3, $4, $5, $6::jsonb) RETURNING id",
            title, hashtag, author, channel_id, cover_file_id, json.dumps(items),
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
        row = await conn.fetchrow(
            "SELECT id, title, hashtag, author, channel_id, channel_message_id, views, items FROM posts WHERE id = $1",
            post_id,
        )
        if not row:
            return None
        items = row["items"]
        items = json.loads(items) if isinstance(items, str) else items
        return {
            "id": row["id"],
            "title": row["title"],
            "hashtag": row["hashtag"],
            "author": row["author"],
            "channel_id": row["channel_id"],
            "channel_message_id": row["channel_message_id"],
            "views": row["views"],
            "items": items,
        }


async def increment_post_views(post_id: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE posts SET views = views + 1 WHERE id = $1", post_id)


async def delete_post_from_db(post_id: int) -> Optional[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM posts WHERE id = $1 RETURNING channel_id, channel_message_id, title",
            post_id,
        )
        return dict(row) if row else None


async def count_posts() -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS c FROM posts")
        return row["c"]


async def total_views() -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COALESCE(SUM(views), 0) AS s FROM posts")
        return row["s"]


async def get_top_posts(limit: int = 10) -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, title, hashtag, views FROM posts ORDER BY views DESC, id DESC LIMIT $1",
            limit,
        )
        return [dict(r) for r in rows]


async def get_recent_posts(limit: int = 10) -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, title, hashtag, views FROM posts ORDER BY id DESC LIMIT $1",
            limit,
        )
        return [dict(r) for r in rows]


async def get_hashtags(limit: int = 20) -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT hashtag, COUNT(*) AS cnt FROM posts
            WHERE hashtag IS NOT NULL AND hashtag <> ''
            GROUP BY hashtag ORDER BY cnt DESC LIMIT $1
            """,
            limit,
        )
        return [dict(r) for r in rows]


async def get_posts_by_hashtag(hashtag: str, limit: int = 15) -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, title, views FROM posts WHERE lower(hashtag) = lower($1) ORDER BY id DESC LIMIT $2",
            hashtag, limit,
        )
        return [dict(r) for r in rows]


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


# ---------- scheduled posts ----------
async def create_scheduled_post(
    title: str, hashtag: str, author: str, channel_id: int, cover_file_id: str,
    items: List[Dict[str, Any]], auto_delete: bool, publish_at: datetime,
    actor_id: int, submission_id: Optional[int],
) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO scheduled_posts
            (title, hashtag, author, channel_id, cover_file_id, items, auto_delete, publish_at, actor_id, submission_id)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10) RETURNING id
            """,
            title, hashtag, author, channel_id, cover_file_id, json.dumps(items),
            auto_delete, publish_at, actor_id, submission_id,
        )
        return row["id"]


async def get_due_scheduled_posts() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM scheduled_posts WHERE status = 'pending' AND publish_at <= now() ORDER BY publish_at"
        )
        res = []
        for r in rows:
            d = dict(r)
            d["items"] = json.loads(d["items"]) if isinstance(d["items"], str) else d["items"]
            res.append(d)
        return res


async def mark_scheduled_done(sched_id: int, status: str = "published") -> None:
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE scheduled_posts SET status = $1 WHERE id = $2", status, sched_id)


async def list_pending_scheduled() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, title, publish_at FROM scheduled_posts WHERE status = 'pending' ORDER BY publish_at"
        )
        return [dict(r) for r in rows]


# ---------- users ----------
async def touch_user(user_id: int, username: Optional[str]) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, username) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
            """,
            user_id, username,
        )


async def get_user_notify(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT notify_enabled FROM users WHERE user_id = $1", user_id)
        return row["notify_enabled"] if row else True


async def set_user_notify(user_id: int, enabled: bool) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, notify_enabled) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET notify_enabled = EXCLUDED.notify_enabled
            """,
            user_id, enabled,
        )


async def get_all_user_ids(only_notify_enabled: bool = False) -> List[int]:
    async with _pool.acquire() as conn:
        if only_notify_enabled:
            rows = await conn.fetch("SELECT user_id FROM users WHERE notify_enabled = true")
        else:
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


# ---------- каналы ----------
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


# ---------- настройки ----------
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
    value = await get_setting("disclaimer")
    if value is None:
        return DEFAULT_DISCLAIMER
    return value


async def is_auto_notify_enabled() -> bool:
    val = await get_setting("notify_new_posts")
    return val != "0"


async def is_protect_content_enabled() -> bool:
    val = await get_setting("protect_content")
    return val == "1"


async def get_zakrep_channels() -> List[Dict[str, str]]:
    """Список каналов для мульти-ОП. Поддерживает миграцию со старого формата."""
    raw = await get_setting("zakrep_list")
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    # Миграция со старых ключей zakrep_link / zakrep_chat_id
    old_link = await get_setting("zakrep_link")
    old_chat = await get_setting("zakrep_chat_id")
    if old_link and old_chat:
        items = [{"link": old_link, "chat_id": old_chat}]
        await set_setting("zakrep_list", json.dumps(items))
        return items
    return []


async def save_zakrep_channels(items: List[Dict[str, str]]) -> None:
    await set_setting("zakrep_list", json.dumps(items))


# =============================================================================
# БОТ И КОМАНДЫ В МЕНЮ (/)
# =============================================================================
router = Router()
BOT_USERNAME = ""

ACCEPTED_TYPES = {
    ContentType.VIDEO, ContentType.PHOTO, ContentType.DOCUMENT, ContentType.AUDIO,
    ContentType.VOICE, ContentType.ANIMATION, ContentType.VIDEO_NOTE, ContentType.TEXT,
}

USER_COMMANDS = [
    BotCommand(command="start", description="🏠 Главное меню"),
    BotCommand(command="catalog", description="📂 Каталог и хэштеги"),
    BotCommand(command="submit", description="📤 Предложить контент"),
    BotCommand(command="myid", description="🆔 Узнать свой ID"),
    BotCommand(command="cancel", description="❌ Отменить действие"),
]

ADMIN_COMMANDS = USER_COMMANDS + [
    BotCommand(command="batch", description="📥 Загрузить и опубликовать пост"),
    BotCommand(command="done", description="✅ Завершить загрузку"),
    BotCommand(command="zakrep", description="🔒 Обязательная подписка (мульти-ОП)"),
    BotCommand(command="protect", description="🛡 Защита контента от пересылки"),
    BotCommand(command="notify", description="📢 Рассылка всем пользователям"),
    BotCommand(command="notify_on", description="🔔 Вкл автоуведомления о постах"),
    BotCommand(command="notify_off", description="🔕 Выкл автоуведомления о постах"),
    BotCommand(command="top", description="🔥 Топ постов по просмотрам"),
    BotCommand(command="delpost", description="🗑 Удалить пост из базы и канала"),
    BotCommand(command="scheduled", description="🕒 Список отложенных постов"),
    BotCommand(command="pending", description="📨 Заявки в предложке"),
    BotCommand(command="channels", description="📡 Список каналов"),
    BotCommand(command="delchannel", description="➖ Убрать канал из списка"),
    BotCommand(command="stats", description="📊 Статистика бота"),
    BotCommand(command="setwelcome", description="🖼 Задать фото для /start"),
    BotCommand(command="disclaimer", description="📝 Текущий дисклеймер"),
    BotCommand(command="setdisclaimer", description="✏️ Изменить дисклеймер"),
    BotCommand(command="deldisclaimer", description="🚫 Убрать дисклеймер"),
    BotCommand(command="ban", description="🚫 Забанить пользователя"),
    BotCommand(command="unban", description="✅ Разбанить пользователя"),
    BotCommand(command="banlist", description="📋 Список банов"),
    BotCommand(command="listadm", description="👥 Список админов"),
]

OWNER_COMMANDS = ADMIN_COMMANDS + [
    BotCommand(command="add", description="➕ Добавить админа"),
    BotCommand(command="deladm", description="➖ Снять админа"),
]


async def setup_bot_commands(bot: Bot):
    """Устанавливает всплывающие подсказки команд при вводе / в Telegram."""
    try:
        await bot.set_my_commands(USER_COMMANDS, scope=BotCommandScopeDefault())
    except Exception as e:
        log.warning("Не смог установить USER_COMMANDS: %s", e)

    for admin_id in await get_admin_ids():
        try:
            cmds = OWNER_COMMANDS if await is_owner(admin_id) else ADMIN_COMMANDS
            await bot.set_my_commands(cmds, scope=BotCommandScopeChat(chat_id=admin_id))
        except Exception:
            pass


class Batch(StatesGroup):
    collecting = State()
    waiting_title = State()
    waiting_cover = State()
    waiting_hashtag = State()
    waiting_autodelete = State()
    waiting_schedule = State()
    waiting_schedule_time = State()
    waiting_channel = State()


class Submit(StatesGroup):
    collecting = State()
    waiting_title = State()
    waiting_hashtag = State()
    waiting_cover = State()


BATCH_STATES = {
    Batch.collecting.state, Batch.waiting_title.state, Batch.waiting_cover.state,
    Batch.waiting_hashtag.state, Batch.waiting_autodelete.state,
    Batch.waiting_schedule.state, Batch.waiting_schedule_time.state,
    Batch.waiting_channel.state,
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


# ---------- проверка мульти-ОП (.zakrep / /zakrep) ----------
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


async def check_user_subscription(bot: Bot, user_id: int) -> Tuple[bool, List[Dict[str, str]]]:
    """Возвращает (подписан_ли_на_все, список_неподписанных_каналов)."""
    if await is_admin(user_id):
        return True, []
    if await get_setting("zakrep_enabled") != "1":
        return True, []
    channels = await get_zakrep_channels()
    if not channels:
        return True, []

    missing = []
    for ch in channels:
        chat_id_raw = str(ch["chat_id"])
        chat_id: Any = int(chat_id_raw) if chat_id_raw.lstrip("-").isdigit() else chat_id_raw
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status not in (
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.CREATOR,
            ):
                missing.append(ch)
        except Exception as e:
            log.warning("Ошибка проверки подписки %s в %s: %s", user_id, chat_id, e)
            missing.append(ch)
    return len(missing) == 0, missing


def subscribe_keyboard(missing: List[Dict[str, str]], payload: str = "") -> InlineKeyboardMarkup:
    rows = []
    for i, ch in enumerate(missing, 1):
        label = f"📢 Подписаться на канал #{i}" if len(missing) > 1 else "📢 Подписаться на канал"
        rows.append([InlineKeyboardButton(text=label, url=ch["link"])])
    rows.append([InlineKeyboardButton(text="✅ Я подписался", callback_data=f"check_sub:{payload}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------- клавиатуры ----------
def welcome_keyboard(notify_on: bool) -> InlineKeyboardMarkup:
    notify_label = "🔔 Уведомления: Вкл" if notify_on else "🔕 Уведомления: Выкл"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 Каталог и хэштеги", callback_data="menu:catalog")],
        [InlineKeyboardButton(text="🔥 Популярное", callback_data="menu:top")],
        [InlineKeyboardButton(text="📤 Предложить контент", callback_data="menu:submit")],
        [InlineKeyboardButton(text=notify_label, callback_data="menu:toggle_notify")],
    ])


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
        InlineKeyboardButton(text="✅ Да, через 24ч", callback_data="autodel:yes"),
        InlineKeyboardButton(text="❌ Оставить навсегда", callback_data="autodel:no"),
    ]])


def schedule_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🚀 Опубликовать сейчас", callback_data="sched:now"),
        InlineKeyboardButton(text="🕒 Отложить", callback_data="sched:later"),
    ]])


async def safe_delete(bot: Bot, chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramBadRequest as e:
        log.warning("Не смог удалить сообщение %s: %s", message_id, e)


async def update_counter(bot: Bot, chat_id: int, status_msg_id: Optional[int], text: str,
                          markup: InlineKeyboardMarkup) -> int:
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


async def send_item(bot: Bot, chat_id: int, item: dict, protect: bool = False):
    t = item["type"]
    if t == "text":
        await bot.send_message(chat_id, item["text"], protect_content=protect)
    elif t == "video":
        await bot.send_video(chat_id, item["file_id"], protect_content=protect)
    elif t == "photo":
        await bot.send_photo(chat_id, item["file_id"], protect_content=protect)
    elif t == "document":
        await bot.send_document(chat_id, item["file_id"], protect_content=protect)
    elif t == "audio":
        await bot.send_audio(chat_id, item["file_id"], protect_content=protect)
    elif t == "voice":
        await bot.send_voice(chat_id, item["file_id"], protect_content=protect)
    elif t == "animation":
        await bot.send_animation(chat_id, item["file_id"], protect_content=protect)
    elif t == "video_note":
        await bot.send_video_note(chat_id, item["file_id"], protect_content=protect)


# ---------- лок на чат ----------
_batch_locks: Dict[int, asyncio.Lock] = {}


def get_batch_lock(chat_id: int) -> asyncio.Lock:
    lock = _batch_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _batch_locks[chat_id] = lock
    return lock


# ---------- пачечная пересылка контента владельцу + удаление из чата ----------
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
    for mid in ids:
        await safe_delete(bot, chat_id, mid)


# ---------- учёт пользователей ----------
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
        text = f"✅ Бот добавлен админом в канал «{update.chat.title}»."
        for admin_id in await get_admin_ids():
            try:
                await update.bot.send_message(admin_id, text)
            except TelegramBadRequest:
                pass
    elif new_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, ChatMemberStatus.MEMBER):
        await remove_channel(update.chat.id)
        log.info("Канал убран из списка: %s (%s)", update.chat.title, update.chat.id)


# =============================================================================
# ПРИВЕТСТВИЕ (/start), МЕНЮ И КАТАЛОГ
# =============================================================================
async def send_welcome(bot: Bot, chat_id: int, user_id: int):
    notify_on = await get_user_notify(user_id)
    photo = await get_setting("welcome_photo") or DEFAULT_WELCOME_PHOTO
    caption = (
        "🪚 <b>Добро пожаловать!</b>\n\n"
        "Здесь ты можешь мгновенно получать весь контент из наших постов, "
        "искать видео по хэштегам и предлагать свои материалы.\n\n"
        "Выбери нужный раздел ниже 👇"
    )
    kb = welcome_keyboard(notify_on)
    try:
        await bot.send_photo(chat_id, photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception:
        await bot.send_message(chat_id, caption, parse_mode=ParseMode.HTML, reply_markup=kb)


@router.message(Command("setwelcome"))
async def cmd_setwelcome(message: Message):
    if not await is_admin(message.from_user.id):
        return
    src = message.reply_to_message
    if not src or not src.photo:
        await message.answer("Ответь командой /setwelcome на нужное фото.")
        return
    await set_setting("welcome_photo", src.photo[-1].file_id)
    await message.answer("✅ Фото для /start обновлено.")


async def _deliver_post(bot: Bot, chat_id: int, post_id: int):
    post = await get_post(post_id)
    if not post:
        await bot.send_message(chat_id, "Контент не найден или был удалён.")
        return

    await increment_post_views(post_id)
    protect = await is_protect_content_enabled()
    await bot.send_message(chat_id, f"📦 <b>{post['title']}</b>", parse_mode=ParseMode.HTML, protect_content=protect)
    for item in post["items"]:
        try:
            await send_item(bot, chat_id, item, protect=protect)
        except TelegramBadRequest as e:
            log.warning("Ошибка при отправке контента: %s", e)
            await bot.send_message(chat_id, "⚠️ Часть контента не удалось отправить.")


@router.message(CommandStart())
async def cmd_start(message: Message):
    if await is_banned(message.from_user.id):
        return

    args = message.text.split(maxsplit=1)
    payload = args[1] if len(args) > 1 else None

    is_sub, missing = await check_user_subscription(message.bot, message.from_user.id)
    if not is_sub and missing:
        await message.answer(
            "🔒 Чтобы пользоваться ботом и получить контент, подпишись на канал(ы):",
            reply_markup=subscribe_keyboard(missing, payload or ""),
        )
        return

    if payload and payload.startswith("p_"):
        try:
            post_id = int(payload[2:])
            await _deliver_post(message.bot, message.chat.id, post_id)
        except ValueError:
            await message.answer("Некорректная ссылка.")
        return

    await send_welcome(message.bot, message.chat.id, message.from_user.id)


@router.callback_query(F.data.startswith("check_sub:"))
async def cb_check_sub(callback: CallbackQuery):
    if await is_banned(callback.from_user.id):
        await callback.answer()
        return

    is_sub, missing = await check_user_subscription(callback.bot, callback.from_user.id)
    if not is_sub:
        await callback.answer("❌ Вы подписались не на все каналы!", show_alert=True)
        return

    await callback.answer("✅ Подписка подтверждена!")
    await safe_delete(callback.bot, callback.message.chat.id, callback.message.message_id)
    payload = callback.data.split(":", 1)[1] or None
    if payload and payload.startswith("p_"):
        try:
            await _deliver_post(callback.bot, callback.message.chat.id, int(payload[2:]))
            return
        except ValueError:
            pass
    await send_welcome(callback.bot, callback.message.chat.id, callback.from_user.id)


# ---------- быстрые кнопки главного меню ----------
@router.callback_query(F.data == "menu:toggle_notify")
async def cb_menu_toggle_notify(callback: CallbackQuery):
    uid = callback.from_user.id
    current = await get_user_notify(uid)
    new_val = not current
    await set_user_notify(uid, new_val)
    await callback.answer("🔔 Уведомления включены" if new_val else "🔕 Уведомления отключены")
    try:
        await callback.message.edit_reply_markup(reply_markup=welcome_keyboard(new_val))
    except TelegramBadRequest:
        pass


@router.callback_query(F.data == "user_notify_off")
async def cb_user_notify_off(callback: CallbackQuery):
    await set_user_notify(callback.from_user.id, False)
    await callback.answer("🔕 Уведомления о новых постах отключены. Включить обратно можно в /start.", show_alert=True)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass


@router.callback_query(F.data == "menu:submit")
async def cb_menu_submit(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if await is_banned(callback.from_user.id):
        return
    is_sub, missing = await check_user_subscription(callback.bot, callback.from_user.id)
    if not is_sub and missing:
        await callback.message.answer(
            "🔒 Сначала подпишись на канал(ы):",
            reply_markup=subscribe_keyboard(missing, ""),
        )
        return
    await state.clear()
    await state.set_state(Submit.collecting)
    await state.update_data(media=[], status_msg_id=None)
    await callback.message.answer(
        f"📥 Пришли контент для канала (макс. {MAX_BATCH_ITEMS} файлов). Когда закончишь — жми кнопку или /done."
    )


async def show_catalog(bot: Bot, chat_id: int):
    tags = await get_hashtags(20)
    if not tags:
        await bot.send_message(chat_id, "📂 В каталоге пока нет постов.")
        return
    rows = []
    row = []
    for t in tags:
        row.append(InlineKeyboardButton(text=f"{t['hashtag']} ({t['cnt']})", callback_data=f"cat_tag:{t['hashtag']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🆕 Последние посты", callback_data="cat_recent")])
    await bot.send_message(chat_id, "📂 <b>Выбери хэштег:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.message(Command("catalog"))
async def cmd_catalog(message: Message):
    if await is_banned(message.from_user.id):
        return
    is_sub, missing = await check_user_subscription(message.bot, message.from_user.id)
    if not is_sub and missing:
        await message.answer("🔒 Сначала подпишись на канал(ы):", reply_markup=subscribe_keyboard(missing, ""))
        return
    await show_catalog(message.bot, message.chat.id)


@router.callback_query(F.data == "menu:catalog")
async def cb_menu_catalog(callback: CallbackQuery):
    await callback.answer()
    await show_catalog(callback.bot, callback.message.chat.id)


@router.callback_query(F.data == "menu:top")
async def cb_menu_top(callback: CallbackQuery):
    await callback.answer()
    posts = await get_top_posts(10)
    if not posts:
        await callback.message.answer("Постов пока нет.")
        return
    rows = [
        [InlineKeyboardButton(text=f"🔥 {p['title']} (👁 {p['views']})", callback_data=f"open_post:{p['id']}")]
        for p in posts
    ]
    await callback.message.answer("🔥 <b>Популярные посты:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data == "cat_recent")
async def cb_cat_recent(callback: CallbackQuery):
    await callback.answer()
    posts = await get_recent_posts(10)
    if not posts:
        await callback.message.answer("Постов пока нет.")
        return
    rows = [
        [InlineKeyboardButton(text=f"🎬 {p['title']}", callback_data=f"open_post:{p['id']}")]
        for p in posts
    ]
    await callback.message.answer("🆕 <b>Последние посты:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data.startswith("cat_tag:"))
async def cb_cat_tag(callback: CallbackQuery):
    await callback.answer()
    tag = callback.data.split(":", 1)[1]
    posts = await get_posts_by_hashtag(tag, 15)
    if not posts:
        await callback.message.answer(f"По хэштегу {tag} ничего не найдено.")
        return
    rows = [
        [InlineKeyboardButton(text=f"🎬 {p['title']}", callback_data=f"open_post:{p['id']}")]
        for p in posts
    ]
    await callback.message.answer(f"📂 <b>Посты по {tag}:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data.startswith("open_post:"))
async def cb_open_post(callback: CallbackQuery):
    if await is_banned(callback.from_user.id):
        await callback.answer()
        return
    post_id = int(callback.data.split(":", 1)[1])
    is_sub, missing = await check_user_subscription(callback.bot, callback.from_user.id)
    if not is_sub and missing:
        await callback.answer("Нужна подписка!", show_alert=True)
        await callback.message.answer(
            "🔒 Чтобы получить контент, подпишись на канал(ы):",
            reply_markup=subscribe_keyboard(missing, f"p_{post_id}"),
        )
        return
    await callback.answer()
    await _deliver_post(callback.bot, callback.message.chat.id, post_id)


# =============================================================================
# МУЛЬТИ-ОП (.zakrep / /zakrep) И ЗАЩИТА КОНТЕНТА (/protect)
# =============================================================================
@router.message(F.text.startswith(".zakrep") | F.text.startswith("/zakrep"))
async def cmd_zakrep(message: Message):
    if not await is_admin(message.from_user.id):
        return

    parts = message.text.split()
    channels = await get_zakrep_channels()

    if len(parts) == 1:
        enabled = await get_setting("zakrep_enabled") == "1"
        status = "✅ Включена" if enabled else "❌ Выключена"
        if not channels:
            ch_text = "Список пуст."
        else:
            ch_text = "\n".join(f"{i}. {c['link']} (<code>{c['chat_id']}</code>)" for i, c in enumerate(channels, 1))
        await message.answer(
            f"🔒 <b>Обязательная подписка:</b> {status}\n\n{ch_text}\n\n"
            "<code>/zakrep ссылка [id]</code> — добавить\n"
            "<code>/zakrep del 1</code> — удалить по номеру\n"
            "<code>/zakrep clear</code> — очистить всё\n"
            "<code>/zakrep on</code> / <code>off</code> — вкл/выкл",
            parse_mode=ParseMode.HTML,
        )
        return

    arg1 = parts[1].strip()
    low = arg1.lower()

    if low in ("off", "выкл", "0", "disable"):
        await set_setting("zakrep_enabled", "0")
        await message.answer("🔓 Обязательная подписка отключена.")
        return

    if low in ("on", "вкл", "1", "enable"):
        if not channels:
            await message.answer("❌ Сначала добавь канал: <code>/zakrep https://t.me/канал</code>", parse_mode=ParseMode.HTML)
            return
        await set_setting("zakrep_enabled", "1")
        await message.answer(f"🔒 Обязательная подписка включена (каналов: {len(channels)}).")
        return

    if low == "clear":
        await save_zakrep_channels([])
        await set_setting("zakrep_enabled", "0")
        await message.answer("🗑 Список каналов ОП очищен, подписка выключена.")
        return

    if low == "del" and len(parts) >= 3 and parts[2].isdigit():
        idx = int(parts[2]) - 1
        if 0 <= idx < len(channels):
            removed = channels.pop(idx)
            await save_zakrep_channels(channels)
            await message.answer(f"✅ Удалён: {removed['link']}")
        else:
            await message.answer("❌ Неверный номер.")
        return

    # Поддержка "/zakrep add <link> [id]" и "/zakrep <link> [id]"
    link_idx = 2 if low == "add" and len(parts) >= 3 else 1
    raw_link = parts[link_idx].strip()
    link = normalize_zakrep_link(raw_link)
    chat_id = parts[link_idx + 1].strip() if len(parts) > link_idx + 1 else parse_zakrep_chat_id(raw_link)

    if not chat_id:
        await message.answer(
            "⚠️ Для приватной ссылки укажи ID канала:\n<code>/zakrep https://t.me/+ссылка -1001234567890</code>",
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
            warn = "\n⚠️ Бот НЕ админ в этом канале — добавь его админом."
    except Exception as e:
        warn = f"\n⚠️ Не смог проверить доступ к каналу ({e})."

    # Обновляем, если уже есть, или добавляем новый
    channels = [c for c in channels if str(c["chat_id"]) != resolved_id]
    channels.append({"link": link, "chat_id": resolved_id})
    await save_zakrep_channels(channels)
    await set_setting("zakrep_enabled", "1")
    await message.answer(
        f"✅ Канал добавлен в ОП: {link} (<code>{resolved_id}</code>)\nВсего в ОП: {len(channels)}{warn}",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("protect"))
async def cmd_protect(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        state_str = "✅ Включена" if await is_protect_content_enabled() else "❌ Выключена"
        await message.answer(f"🛡 Защита контента от пересылки: {state_str}\nВкл/выкл: /protect on | /protect off")
        return
    val = args[1].strip().lower()
    if val in ("on", "1", "вкл"):
        await set_setting("protect_content", "1")
        await message.answer("🛡 Защита от пересылки и сохранения ВКЛЮЧЕНА.")
    elif val in ("off", "0", "выкл"):
        await set_setting("protect_content", "0")
        await message.answer("🔓 Защита от пересылки ВЫКЛЮЧЕНА.")


# =============================================================================
# БАЗОВЫЕ И АДМИН КОМАНДЫ
# =============================================================================
@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    if current is None:
        return
    if current in BATCH_STATES and not await is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Отменено.")


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>", parse_mode=ParseMode.HTML)


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not await is_admin(message.from_user.id):
        return
    posts_n = await count_posts()
    views_n = await total_views()
    users_n = await count_users()
    admins_n = len(await get_admin_ids())
    bans_n = await count_bans()
    pending_n = await count_pending_submissions()
    channels_n = len(await list_channels())
    zakrep_on = "вкл" if await get_setting("zakrep_enabled") == "1" else "выкл"
    zakrep_cnt = len(await get_zakrep_channels())
    notify_on = "вкл" if await is_auto_notify_enabled() else "выкл"
    protect_on = "вкл" if await is_protect_content_enabled() else "выкл"
    await message.answer(
        "📊 <b>Статистика:</b>\n"
        f"Постов: {posts_n} (👁 просмотров: {views_n})\n"
        f"Каналов: {channels_n}\n"
        f"Пользователей: {users_n}\n"
        f"Админов: {admins_n} | Банов: {bans_n}\n"
        f"Заявок в предложке: {pending_n}\n"
        f"ОП (.zakrep): {zakrep_on} ({zakrep_cnt} кан.)\n"
        f"Защита контента: {protect_on}\n"
        f"Автоуведомления: {notify_on}",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("top"))
async def cmd_top(message: Message):
    if not await is_admin(message.from_user.id):
        return
    posts = await get_top_posts(10)
    if not posts:
        await message.answer("Постов пока нет.")
        return
    lines = [f"{i}. #{p['id']} «{p['title']}» — 👁 {p['views']}" for i, p in enumerate(posts, 1)]
    await message.answer("🔥 <b>Топ постов:</b>\n" + "\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("delpost"))
async def cmd_delpost(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().isdigit():
        await message.answer("Использование: /delpost <id_поста> (ID смотри в /top или в ссылке p_123)")
        return
    post_id = int(args[1].strip())
    deleted = await delete_post_from_db(post_id)
    if not deleted:
        await message.answer(f"❌ Пост #{post_id} не найден.")
        return
    if deleted.get("channel_id") and deleted.get("channel_message_id"):
        await safe_delete(message.bot, deleted["channel_id"], deleted["channel_message_id"])
    await message.answer(f"🗑 Пост #{post_id} «{deleted.get('title')}» удалён из базы и канала.")


@router.message(Command("scheduled"))
async def cmd_scheduled(message: Message):
    if not await is_admin(message.from_user.id):
        return
    rows = await list_pending_scheduled()
    if not rows:
        await message.answer("Отложенных постов нет.")
        return
    lines = [f"#{r['id']} «{r['title']}» — {r['publish_at'].strftime('%d.%m %H:%M UTC')}" for r in rows]
    await message.answer("🕒 <b>Отложенные посты:</b>\n" + "\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("channels"))
async def cmd_channels(message: Message):
    if not await is_admin(message.from_user.id):
        return
    channels = await list_channels()
    if not channels:
        await message.answer("Бот пока не админ ни в одном канале.")
        return
    lines = [f"• {ch['title'] or '(без названия)'} (<code>{ch['channel_id']}</code>)" for ch in channels]
    await message.answer("📡 <b>Каналы для публикации:</b>\n" + "\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("delchannel"))
async def cmd_delchannel(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /delchannel <channel_id>")
        return
    channel_id = int(args[1].strip())
    ok = await remove_channel(channel_id)
    if ok:
        await message.answer(f"✅ Канал {channel_id} убран из списка публикации.")
    else:
        await message.answer(f"❌ Канал {channel_id} не найден. Проверь ID через /channels.")


# ============ дисклеймер ============
@router.message(Command("disclaimer"))
async def cmd_disclaimer(message: Message):
    if not await is_admin(message.from_user.id):
        return
    raw = await get_setting("disclaimer")
    if raw is None:
        await message.answer(f"Дефолтный дисклеймер:\n«{DEFAULT_DISCLAIMER}»")
    elif raw == "":
        await message.answer("Дисклеймер отключён.")
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
    await message.answer("✅ Дисклеймер убран.")


# ============ автоуведомления о новых постах ============
@router.message(Command("notify_on"))
async def cmd_notify_on(message: Message):
    if not await is_admin(message.from_user.id):
        return
    await set_setting("notify_new_posts", "1")
    await message.answer("🔔 Автоуведомления о новых постах ВКЛЮЧЕНЫ.")


@router.message(Command("notify_off"))
async def cmd_notify_off(message: Message):
    if not await is_admin(message.from_user.id):
        return
    await set_setting("notify_new_posts", "0")
    await message.answer("🔕 Автоуведомления о новых постах ВЫКЛЮЧЕНЫ.")


async def notify_all_users_about_post(bot: Bot, post_id: int, title: str, hashtag: str, cover_file_id: str):
    user_ids = await get_all_user_ids(only_notify_enabled=True)
    banned = {r["user_id"] for r in await list_bans()}
    targets = [uid for uid in user_ids if uid not in banned]
    if not targets:
        return

    deep_link = f"https://t.me/{BOT_USERNAME}?start=p_{post_id}"
    caption = f"🔥 <b>Новый пост!</b>\n\n<b>{title}</b>\n{hashtag}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Смотреть контент", url=deep_link)],
        [InlineKeyboardButton(text="🔕 Отключить уведомления", callback_data="user_notify_off")],
    ])

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


# =============================================================================
# /batch — ПРИЁМ КОНТЕНТА, ОТЛОЖКА И ПУБЛИКАЦИЯ
# =============================================================================
@router.message(Command("batch"))
async def cmd_batch(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.clear()
    await state.set_state(Batch.collecting)
    await state.update_data(media=[], status_msg_id=None, submission_id=None, author_name=None, publish_at=None)
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
            await message.answer(f"⚠️ Лимит {MAX_BATCH_ITEMS} файлов. Жми «Это всё».")

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
    await bot.send_message(chat_id, f"✅ Принято файлов: {len(media)}.\nТеперь пришли *название* поста.", parse_mode=ParseMode.MARKDOWN)


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


async def ask_autodelete(bot: Bot, chat_id: int, state: FSMContext):
    await state.set_state(Batch.waiting_autodelete)
    await bot.send_message(chat_id, "🕒 Удалить пост из канала через 24 часа?", reply_markup=autodelete_keyboard())


@router.callback_query(F.data.startswith("autodel:"), StateFilter(Batch.waiting_autodelete))
async def cb_autodelete_choice(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    auto_delete = callback.data.split(":")[1] == "yes"
    await state.update_data(auto_delete=auto_delete)
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(Batch.waiting_schedule)
    await callback.message.answer("📅 Когда опубликовать?", reply_markup=schedule_keyboard())


@router.callback_query(F.data.startswith("sched:"), StateFilter(Batch.waiting_schedule))
async def cb_schedule_choice(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    choice = callback.data.split(":")[1]
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    if choice == "now":
        await state.update_data(publish_at=None)
        await choose_channel_and_publish(callback.bot, callback.message.chat.id, callback.from_user.id, state)
    else:
        await state.set_state(Batch.waiting_schedule_time)
        await callback.message.answer(
            "🕒 Через сколько опубликовать?\nПримеры: <code>30m</code> (30 мин), <code>2h</code> (2 часа), <code>1d</code> (1 день)",
            parse_mode=ParseMode.HTML,
        )


def parse_delay(text: str) -> Optional[timedelta]:
    m = re.match(r"^(\d+)\s*([mhdмчд])$", text.strip().lower())
    if not m:
        return None
    val = int(m.group(1))
    unit = m.group(2)
    if unit in ("m", "м"):
        return timedelta(minutes=val)
    if unit in ("h", "ч"):
        return timedelta(hours=val)
    if unit in ("d", "д"):
        return timedelta(days=val)
    return None


@router.message(StateFilter(Batch.waiting_schedule_time), F.text)
async def get_schedule_time(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    delta = parse_delay(message.text)
    if not delta or delta.total_seconds() < 60:
        await message.answer("❌ Неверный формат. Пример: <code>30m</code>, <code>2h</code>, <code>1d</code>", parse_mode=ParseMode.HTML)
        return
    publish_at = datetime.now(timezone.utc) + delta
    await state.update_data(publish_at=publish_at.isoformat())
    await choose_channel_and_publish(message.bot, message.chat.id, message.from_user.id, state)


async def choose_channel_and_publish(bot: Bot, chat_id: int, actor_id: int, state: FSMContext):
    channels = await list_channels()
    if not channels:
        await bot.send_message(chat_id, "❌ Бот не админ ни в одном канале.")
        return
    if len(channels) == 1:
        await finalize_publish_or_schedule(bot, chat_id, actor_id, state, channels[0]["channel_id"])
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
    await finalize_publish_or_schedule(callback.bot, callback.message.chat.id, callback.from_user.id, state, channel_id)


async def execute_publish(
    bot: Bot, title: str, hashtag: str, admin_name: str, channel_id: int,
    cover_file_id: str, media: List[Dict[str, Any]], auto_delete: bool,
    actor_id: int, submission_id: Optional[int],
) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    post_id = await create_post(
        title=title, hashtag=hashtag, author=admin_name,
        channel_id=channel_id, items=media, cover_file_id=cover_file_id,
    )
    deep_link = f"https://t.me/{BOT_USERNAME}?start=p_{post_id}"
    disclaimer = await get_disclaimer_for_publish()
    disclaimer_block = f"{disclaimer}\n\n" if disclaimer else ""

    caption = (
        f"<b>{title}</b>\n\n"
        f"{disclaimer_block}"
        f"{hashtag}\n\n"
        f"Автор: {admin_name}"
    )
    channel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🎬 Смотреть видео", url=deep_link)
    ]])

    try:
        sent = await bot.send_photo(
            channel_id, cover_file_id, caption=caption,
            parse_mode=ParseMode.HTML, reply_markup=channel_kb,
        )
    except TelegramBadRequest as e:
        return None, None, str(e)

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

    if await is_auto_notify_enabled():
        asyncio.create_task(notify_all_users_about_post(bot, post_id, title, hashtag, cover_file_id))

    return post_id, deep_link, None


async def finalize_publish_or_schedule(bot: Bot, chat_id: int, actor_id: int, state: FSMContext, channel_id: int):
    data = await state.get_data()
    media = data["media"]
    title = data["title"]
    cover_file_id = data["cover_file_id"]
    hashtag = data["hashtag"]
    auto_delete = data.get("auto_delete", False)
    admin_name = data.get("author_name") or data.get("admin_name", "admin")
    submission_id = data.get("submission_id")
    publish_at_raw = data.get("publish_at")

    if publish_at_raw:
        publish_at = datetime.fromisoformat(publish_at_raw)
        sched_id = await create_scheduled_post(
            title, hashtag, admin_name, channel_id, cover_file_id,
            media, auto_delete, publish_at, actor_id, submission_id,
        )
        await bot.send_message(
            chat_id,
            f"🕒 Пост «{title}» отложен (#{sched_id}) на {publish_at.strftime('%d.%m %H:%M UTC')}.",
        )
        await state.clear()
        return

    await bot.send_chat_action(chat_id, ChatAction.TYPING)
    post_id, deep_link, err = await execute_publish(
        bot, title, hashtag, admin_name, channel_id, cover_file_id, media, auto_delete, actor_id, submission_id
    )
    if err:
        await bot.send_message(chat_id, f"❌ Не смог опубликовать: {err}")
        await state.clear()
        return

    done_text = f"✅ Опубликовано (#{post_id}).\nСсылка: {deep_link}"
    if auto_delete:
        done_text += "\n🕒 Удалится через 24ч."
    await bot.send_message(chat_id, done_text)
    await state.clear()


# =============================================================================
# /submit — ПРЕДЛОЖКА
# =============================================================================
@router.message(Command("submit"))
async def cmd_submit(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("Пришли /submit мне в личные сообщения.")
        return
    if await is_banned(message.from_user.id):
        await message.answer("Вам запрещено пользоваться ботом.")
        return

    is_sub, missing = await check_user_subscription(message.bot, message.from_user.id)
    if not is_sub and missing:
        await message.answer("🔒 Сначала подпишись на канал(ы):", reply_markup=subscribe_keyboard(missing, ""))
        return

    await state.clear()
    await state.set_state(Submit.collecting)
    await state.update_data(media=[], status_msg_id=None)
    await message.answer(
        f"📥 Пришли контент для канала (макс. {MAX_BATCH_ITEMS} файлов). Когда закончишь — жми кнопку или /done."
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
    await bot.send_message(chat_id, "✅ Отправлено на проверку. Как только админ решит — напишу вам.")

    info_text = (
        f"📨 Заявка #{sub_id}\n"
        f"От: {from_user.full_name} (@{from_user.username or '—'}, id {from_user.id})\n"
        f"Название: {title}\nХэштег: {hashtag}\nФайлов: {len(media)}"
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
        media=sub["items"], title=sub["title"], hashtag=sub["hashtag"],
        cover_file_id=sub["cover_file_id"], admin_id=callback.from_user.id,
        admin_name=callback.from_user.full_name, author_name=author_name,
        submission_id=sub_id, publish_at=None,
    )
    await callback.message.answer(f"Заявка #{sub_id} одобрена: «{sub['title']}».")
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


# =============================================================================
# АДМИНЫ, БАНЫ И РАССЫЛКА
# =============================================================================
@router.message(Command("add"))
async def cmd_add_admin(message: Message):
    if not await is_owner(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /add <telegram_id>")
        return
    new_id = int(args[1].strip())
    await add_admin(new_id, None, message.from_user.id)
    await setup_bot_commands(message.bot)
    await message.answer(f"✅ id{new_id} добавлен в админы.")
    try:
        await message.bot.send_message(new_id, "Вас назначили админом бота.")
    except TelegramBadRequest:
        pass


@router.message(Command("deladm"))
async def cmd_del_admin(message: Message):
    if not await is_owner(message.from_user.id):
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
    if ok:
        try:
            await message.bot.set_my_commands(USER_COMMANDS, scope=BotCommandScopeChat(chat_id=del_id))
        except Exception:
            pass
    await message.answer(f"✅ id{del_id} убран из админов." if ok else "Такого админа нет в базе.")


@router.message(Command("listadm"))
async def cmd_list_admins(message: Message):
    if not await is_admin(message.from_user.id):
        return
    lines = [f"👑 Владелец: id{OWNER_ID or '(не задан)'}, вечный — id{SUPER_OWNER_ID}"]
    if ADMIN_IDS:
        lines.append("Статичные: " + ", ".join(f"id{i}" for i in ADMIN_IDS))
    db_admins = await list_admins()
    if db_admins:
        lines.append("Динамические: " + ", ".join(f"id{r['user_id']}" for r in db_admins))
    await message.answer("\n".join(lines))


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
        await message.answer("Использование: /notify <текст> (или ответом на сообщение)")
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


# =============================================================================
# ФОНОВЫЕ ЦИКЛЫ (АВТОУДАЛЕНИЕ, ОТЛОЖКА, АНТИ-СОН)
# =============================================================================
async def auto_delete_loop(bot: Bot):
    while True:
        await asyncio.sleep(AUTO_DELETE_CHECK_INTERVAL)
        try:
            due = await get_due_deletions()
        except Exception as e:
            log.warning("Ошибка проверки автоудаления: %s", e)
            continue
        for row in due:
            try:
                await bot.delete_message(row["channel_id"], row["channel_message_id"])
            except TelegramBadRequest as e:
                log.warning("Не смог удалить пост %s: %s", row["id"], e)
            await mark_post_deleted(row["id"])


async def scheduled_posts_loop(bot: Bot):
    while True:
        await asyncio.sleep(SCHEDULE_CHECK_INTERVAL)
        try:
            due = await get_due_scheduled_posts()
        except Exception as e:
            log.warning("Ошибка проверки отложенных постов: %s", e)
            continue
        for row in due:
            post_id, deep_link, err = await execute_publish(
                bot, row["title"], row["hashtag"], row["author"], row["channel_id"],
                row["cover_file_id"], row["items"], row["auto_delete"],
                row["actor_id"] or 0, row["submission_id"],
            )
            if err:
                log.warning("Не смог опубликовать отложенный пост #%s: %s", row["id"], err)
                await mark_scheduled_done(row["id"], "failed")
            else:
                await mark_scheduled_done(row["id"], "published")
                log.info("Отложенный пост #%s опубликован как пост #%s", row["id"], post_id)


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
        raise SystemExit("Укажи DATABASE_URL в .env")

    storage = PostgresStorage(DATABASE_URL)
    await storage.connect()
    await db_init(DATABASE_URL)
    await bootstrap_admins()

    if LEGACY_CHANNEL_ID:
        await add_channel(LEGACY_CHANNEL_ID, None)

    bot = Bot(token=BOT_TOKEN)
    me = await bot.get_me()
    BOT_USERNAME = me.username

    await setup_bot_commands(bot)

    dp = Dispatcher(storage=storage)
    dp.message.outer_middleware(TouchUserMiddleware())
    dp.include_router(router)

    try:
        await start_health_server()
        asyncio.create_task(self_ping_loop())
        asyncio.create_task(auto_delete_loop(bot))
        asyncio.create_task(scheduled_posts_loop(bot))
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await storage.close()
        await db_close()


if __name__ == "__main__":
    asyncio.run(main())
