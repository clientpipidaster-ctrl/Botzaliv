"""
Бот для автопостинга контента в канал(ы) — всё в одном файле.

    pip install -r requirements.txt
    заполнить .env
    python bot.py
"""

import asyncio
import base64
import gc
import hashlib
import html as html_lib
import io
import json
import logging
import os
import re
import struct
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import asyncpg
from dotenv import load_dotenv
from aiohttp import web, ClientSession, ClientTimeout, CookieJar
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
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
    BotCommandScopeChat,
    BufferedInputFile,
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

# Дефолтное фото пилы для /start (можно заменить своей фоткой командой /setwelcome)
DEFAULT_WELCOME_PHOTO = os.getenv(
    "WELCOME_PHOTO_URL",
    "https://upload.wikimedia.org/wikipedia/en/6/64/Billy_the_Puppet.jpg",
)

SUPER_OWNER_ID = 682868256

BOOTSTRAP_ADMIN_IDS = {
    int(x) for x in os.getenv("BOOTSTRAP_ADMIN_IDS", str(SUPER_OWNER_ID)).split(",") if x.strip()
}

LEGACY_CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or 0)

# Автообновление бота прямо из Telegram через GitHub API (/update)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "clientpipidaster-ctrl/Botzaliv")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")

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
            self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=2, max_inactive_connection_lifetime=60)
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
    _pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4, max_inactive_connection_lifetime=60)
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
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites (
                user_id BIGINT NOT NULL,
                post_id INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
                added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (user_id, post_id)
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ad_campaigns (
                id SERIAL PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                text TEXT NOT NULL,
                link TEXT NOT NULL,
                btn_text TEXT NOT NULL DEFAULT '🔗 Перейти',
                photo_file_id TEXT,
                interval_minutes INT NOT NULL DEFAULT 0,
                next_run TIMESTAMPTZ NOT NULL,
                active BOOLEAN NOT NULL DEFAULT true,
                created_by BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
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


async def delete_posts_by_hashtag(hashtag: str) -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "DELETE FROM posts WHERE lower(hashtag) = lower($1) RETURNING id, channel_id, channel_message_id",
            hashtag,
        )
        return [dict(r) for r in rows]


async def search_posts_by_title(query: str, limit: int = 15) -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, title, views FROM posts WHERE title ILIKE $1 ORDER BY id DESC LIMIT $2",
            f"%{query}%", limit,
        )
        return [dict(r) for r in rows]


async def get_random_post_id() -> Optional[int]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM posts ORDER BY random() LIMIT 1")
        return row["id"] if row else None


# ---------- избранное ----------
async def is_favorite(user_id: int, post_id: int) -> bool:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM favorites WHERE user_id = $1 AND post_id = $2", user_id, post_id)
        return row is not None


async def toggle_favorite(user_id: int, post_id: int) -> bool:
    """Возвращает True если добавлено, False если убрано."""
    async with _pool.acquire() as conn:
        exists = await conn.fetchrow("SELECT 1 FROM favorites WHERE user_id = $1 AND post_id = $2", user_id, post_id)
        if exists:
            await conn.execute("DELETE FROM favorites WHERE user_id = $1 AND post_id = $2", user_id, post_id)
            return False
        await conn.execute(
            "INSERT INTO favorites (user_id, post_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            user_id, post_id,
        )
        return True


async def get_user_favorites(user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id, p.title, p.views FROM favorites f
            JOIN posts p ON p.id = f.post_id
            WHERE f.user_id = $1
            ORDER BY f.added_at DESC LIMIT $2
            """,
            user_id, limit,
        )
        return [dict(r) for r in rows]


# ---------- реклама по расписанию (/reklama) ----------
async def create_ad_campaign(
    channel_id: int, text: str, link: str, btn_text: str,
    photo_file_id: Optional[str], interval_minutes: int, next_run: datetime, created_by: int,
) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO ad_campaigns (channel_id, text, link, btn_text, photo_file_id, interval_minutes, next_run, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id
            """,
            channel_id, text, link, btn_text, photo_file_id, interval_minutes, next_run, created_by,
        )
        return row["id"]


async def get_due_ads() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM ad_campaigns WHERE active = true AND next_run <= now() ORDER BY next_run"
        )
        return [dict(r) for r in rows]


async def update_ad_after_run(ad_id: int, interval_minutes: int) -> None:
    async with _pool.acquire() as conn:
        if interval_minutes > 0:
            next_run = datetime.now(timezone.utc) + timedelta(minutes=interval_minutes)
            await conn.execute("UPDATE ad_campaigns SET next_run = $1 WHERE id = $2", next_run, ad_id)
        else:
            await conn.execute("UPDATE ad_campaigns SET active = false WHERE id = $1", ad_id)


async def list_active_ads() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM ad_campaigns WHERE active = true ORDER BY next_run")
        return [dict(r) for r in rows]


async def delete_ad_campaign(ad_id: int) -> bool:
    async with _pool.acquire() as conn:
        res = await conn.execute("DELETE FROM ad_campaigns WHERE id = $1", ad_id)
        return res.endswith("1")


async def export_users_txt() -> bytes:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, username, first_seen FROM users ORDER BY first_seen DESC")
    lines = [f"{r['user_id']} | @{r['username'] or '-'} | {r['first_seen'].strftime('%Y-%m-%d %H:%M')}" for r in rows]
    return ("\n".join(lines) if lines else "Пользователей нет.").encode("utf-8")


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
BOT_START_TIME = time.monotonic()

ACCEPTED_TYPES = {
    ContentType.VIDEO, ContentType.PHOTO, ContentType.DOCUMENT, ContentType.AUDIO,
    ContentType.VOICE, ContentType.ANIMATION, ContentType.VIDEO_NOTE, ContentType.TEXT,
}

USER_COMMANDS = [
    BotCommand(command="start", description="🏠 Главное меню"),
    BotCommand(command="catalog", description="📂 Каталог и хэштеги"),
    BotCommand(command="search", description="🔍 Поиск по названию"),
    BotCommand(command="fav", description="⭐ Избранное"),
    BotCommand(command="submit", description="📤 Предложить контент"),
    BotCommand(command="myid", description="🆔 Узнать свой ID"),
    BotCommand(command="cancel", description="❌ Отменить действие"),
]

ADMIN_COMMANDS = USER_COMMANDS + [
    BotCommand(command="batch", description="📥 Загрузить и опубликовать пост"),
    BotCommand(command="grab", description="🌐 Скачать видео с сайта по ссылке"),
    BotCommand(command="done", description="✅ Завершить загрузку"),
    BotCommand(command="zakrep", description="🔒 Обязательная подписка (мульти-ОП)"),
    BotCommand(command="protect", description="🛡 Защита контента от пересылки"),
    BotCommand(command="notify", description="📢 Рассылка всем пользователям"),
    BotCommand(command="notify_on", description="🔔 Вкл автоуведомления о постах"),
    BotCommand(command="notify_off", description="🔕 Выкл автоуведомления о постах"),
    BotCommand(command="top", description="🔥 Топ постов по просмотрам"),
    BotCommand(command="delpost", description="🗑 Удалить пост из базы и канала"),
    BotCommand(command="scheduled", description="🕒 Список отложенных постов"),
    BotCommand(command="reklama", description="📣 Реклама по расписанию в канал"),
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

SUPER_OWNER_COMMANDS = OWNER_COMMANDS + [
    BotCommand(command="owner", description="👑 Панель Супер-Владельца"),
    BotCommand(command="restart", description="🔄 Перезапустить бота"),
    BotCommand(command="update", description="🚀 Обновить bot.py на GitHub"),
]


async def setup_bot_commands(bot: Bot):
    """Устанавливает всплывающие подсказки команд при вводе / в Telegram.
    Обычные пользователи видят только USER_COMMANDS, админы — ADMIN/OWNER_COMMANDS."""
    for scope in (BotCommandScopeDefault(), BotCommandScopeAllPrivateChats(), BotCommandScopeAllGroupChats()):
        try:
            await bot.set_my_commands(USER_COMMANDS, scope=scope)
        except Exception as e:
            log.warning("Не смог установить USER_COMMANDS (%s): %s", scope, e)

    for admin_id in await get_admin_ids():
        try:
            if admin_id == SUPER_OWNER_ID:
                cmds = SUPER_OWNER_COMMANDS
            elif await is_owner(admin_id):
                cmds = OWNER_COMMANDS
            else:
                cmds = ADMIN_COMMANDS
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


class SearchState(StatesGroup):
    waiting_query = State()


class ReklamaState(StatesGroup):
    waiting_text = State()
    waiting_link = State()
    waiting_photo = State()
    waiting_schedule = State()
    waiting_channel = State()


class GrabState(StatesGroup):
    waiting_url = State()


BATCH_STATES = {
    Batch.collecting.state, Batch.waiting_title.state, Batch.waiting_cover.state,
    Batch.waiting_hashtag.state, Batch.waiting_autodelete.state,
    Batch.waiting_schedule.state, Batch.waiting_schedule_time.state,
    Batch.waiting_channel.state,
}


# ---------- права доступа ----------
def is_super_owner(user_id: int) -> bool:
    return user_id == SUPER_OWNER_ID


async def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID or user_id == SUPER_OWNER_ID


_maint_cache: Optional[bool] = None


async def is_maintenance_enabled() -> bool:
    global _maint_cache
    if _maint_cache is not None:
        return _maint_cache
    _maint_cache = (await get_setting("maintenance_mode") == "1")
    return _maint_cache


def get_ram_mb() -> int:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except Exception:
        pass
    return 0


async def get_auto_restart_hours() -> int:
    val = await get_setting("auto_restart_hours")
    if val is None:
        return 6  # по умолчанию перезапуск каждые 6 часов от лагов хоста
    try:
        return int(val)
    except ValueError:
        return 6


async def trigger_restart(bot: Optional[Bot], reason: str, notify_owner: bool = False):
    log.warning("Перезапуск бота: %s", reason)
    if notify_owner and bot:
        try:
            await asyncio.wait_for(
                bot.send_message(
                    SUPER_OWNER_ID,
                    f"♻️ <b>Автоперезапуск бота</b>\nПричина: {reason}\nВернётся в строй через 10–15 сек.",
                    parse_mode=ParseMode.HTML,
                    disable_notification=True,
                ),
                timeout=3.0,
            )
        except Exception:
            pass
    try:
        if bot:
            await asyncio.wait_for(bot.session.close(), timeout=2.0)
    except Exception:
        pass
    try:
        await asyncio.wait_for(db_close(), timeout=2.0)
    except Exception:
        pass
    gc.collect()
    os._exit(1)


async def is_admin_logs_enabled() -> bool:
    val = await get_setting("admin_logs")
    return val != "0"  # включено по умолчанию


async def get_dm_autodelete_minutes() -> int:
    val = await get_setting("dm_autodelete_min")
    try:
        return int(val) if val else 0
    except ValueError:
        return 0


async def log_admin_action(bot: Bot, actor_id: int, actor_name: str, action_text: str):
    """Отправляет тихий лог действий админов Супер-Владельцу."""
    if actor_id == SUPER_OWNER_ID:
        return
    if not await is_admin_logs_enabled():
        return
    try:
        await bot.send_message(
            SUPER_OWNER_ID,
            f"🕵️ <b>Лог админа</b>\n"
            f"Кто: {actor_name} (<code>{actor_id}</code>)\n"
            f"Действие: {action_text}",
            parse_mode=ParseMode.HTML,
            disable_notification=True,
        )
    except Exception:
        pass


async def clear_all_fsm() -> int:
    async with _pool.acquire() as conn:
        res = await conn.execute("DELETE FROM fsm_storage")
        try:
            return int(res.split()[-1])
        except Exception:
            return 0


async def export_backup_json() -> bytes:
    async with _pool.acquire() as conn:
        posts = [dict(r) for r in await conn.fetch("SELECT id, title, hashtag, author, channel_id, views, created_at FROM posts ORDER BY id")]
        channels = [dict(r) for r in await conn.fetch("SELECT channel_id, title, hidden FROM channels")]
        admins = [dict(r) for r in await conn.fetch("SELECT user_id, username, added_by FROM admins")]
        bans = [dict(r) for r in await conn.fetch("SELECT user_id, reason FROM bans")]
        settings = [dict(r) for r in await conn.fetch("SELECT key, value FROM settings")]
        users_cnt = (await conn.fetchrow("SELECT COUNT(*) AS c FROM users"))["c"]
    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "users_count": users_cnt,
        "channels": channels,
        "admins": admins,
        "bans": bans,
        "settings": settings,
        "posts": posts,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")


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
        [
            InlineKeyboardButton(text="📂 Каталог", callback_data="menu:catalog"),
            InlineKeyboardButton(text="🔍 Поиск", callback_data="menu:search"),
        ],
        [
            InlineKeyboardButton(text="🔥 Популярное", callback_data="menu:top"),
            InlineKeyboardButton(text="⭐ Избранное", callback_data="menu:fav"),
        ],
        [InlineKeyboardButton(text="🎲 Сыграть в игру (Случайный пост)", callback_data="menu:random")],
        [InlineKeyboardButton(text="📤 Предложить контент", callback_data="menu:submit")],
        [InlineKeyboardButton(text=notify_label, callback_data="menu:toggle_notify")],
    ])


def stop_keyboard(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🛑 Это всё, завершить ({count} шт.)", callback_data="stop_batch")
    ]])


def submit_stop_keyboard(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🛑 Отправить на проверку ({count} шт.)", callback_data="stop_submit")
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
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            await bot.forward_messages(chat_id=OWNER_ID, from_chat_id=chat_id, message_ids=chunk)
        except TelegramBadRequest as e:
            log.warning("Не смог переслать пачку владельцу: %s", e)
    for mid in ids:
        await safe_delete(bot, chat_id, mid)


# ---------- учёт пользователей ----------
_touched_cache: Dict[int, float] = {}


class TouchUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        if event.from_user and not event.from_user.is_bot:
            uid = event.from_user.id
            now_m = time.monotonic()
            if now_m - _touched_cache.get(uid, 0.0) > 600:
                _touched_cache[uid] = now_m
                if len(_touched_cache) > 5000:
                    _touched_cache.clear()
                try:
                    await touch_user(uid, event.from_user.username)
                except Exception as e:
                    log.warning("Не смог записать пользователя %s: %s", uid, e)
            if await is_maintenance_enabled() and not await is_admin(uid):
                await event.answer("🛠 Бот сейчас на техническом обслуживании. Попробуй позже.")
                return
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
        "🤡 <b>Добро пожаловать!</b>\n\n"
        "<i>Я хочу сыграть с тобой в одну игру...</i>\n"
        "Здесь ты можешь мгновенно получать контент из наших постов, "
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


async def _delete_dm_later(bot: Bot, chat_id: int, msg_ids: List[int], minutes: int):
    await asyncio.sleep(minutes * 60)
    for mid in msg_ids:
        await safe_delete(bot, chat_id, mid)


async def send_item_ret(bot: Bot, chat_id: int, item: dict, protect: bool = False) -> Optional[Message]:
    t = item["type"]
    if t == "text":
        return await bot.send_message(chat_id, item["text"], protect_content=protect)
    if t == "video":
        return await bot.send_video(chat_id, item["file_id"], protect_content=protect)
    if t == "photo":
        return await bot.send_photo(chat_id, item["file_id"], protect_content=protect)
    if t == "document":
        return await bot.send_document(chat_id, item["file_id"], protect_content=protect)
    if t == "audio":
        return await bot.send_audio(chat_id, item["file_id"], protect_content=protect)
    if t == "voice":
        return await bot.send_voice(chat_id, item["file_id"], protect_content=protect)
    if t == "animation":
        return await bot.send_animation(chat_id, item["file_id"], protect_content=protect)
    if t == "video_note":
        return await bot.send_video_note(chat_id, item["file_id"], protect_content=protect)
    return None


def fav_button_kb(post_id: int, in_fav: bool) -> InlineKeyboardMarkup:
    text = "💔 Убрать из избранного" if in_fav else "❤️ В избранное"
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=text, callback_data=f"fav_toggle:{post_id}")
    ]])


async def _deliver_post(bot: Bot, chat_id: int, post_id: int):
    post = await get_post(post_id)
    if not post:
        await bot.send_message(chat_id, "Контент не найден или был удалён.")
        return

    await increment_post_views(post_id)
    protect = await is_protect_content_enabled()
    in_fav = await is_favorite(chat_id, post_id)
    sent_ids: List[int] = []

    head = await bot.send_message(
        chat_id,
        f"📦 <b>{post['title']}</b>",
        parse_mode=ParseMode.HTML,
        protect_content=protect,
        reply_markup=fav_button_kb(post_id, in_fav),
    )
    sent_ids.append(head.message_id)

    failed = 0
    for item in post["items"]:
        sent_msg = None
        for _ in range(3):
            try:
                sent_msg = await send_item_ret(bot, chat_id, item, protect=protect)
                break
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.5)
            except TelegramBadRequest as e:
                log.warning("Ошибка при отправке контента (post #%s): %s", post_id, e)
                break
            except Exception as e:
                log.warning("Неожиданная ошибка отправки (post #%s): %s", post_id, e)
                break
        if sent_msg:
            sent_ids.append(sent_msg.message_id)
        else:
            failed += 1
        await asyncio.sleep(0.15)

    if failed:
        await bot.send_message(chat_id, f"⚠️ Не удалось отправить файлов: {failed} (возможно, они были загружены через старого бота).")

    dm_del = await get_dm_autodelete_minutes()
    if dm_del > 0 and sent_ids:
        note = await bot.send_message(chat_id, f"⏳ Контент автоматически удалится из чата через {dm_del} мин.")
        sent_ids.append(note.message_id)
        asyncio.create_task(_delete_dm_later(bot, chat_id, sent_ids, dm_del))


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
        "📥 Пришли контент для канала. Когда закончишь — жми кнопку или /done."
    )


def _tag_hash(tag: str) -> str:
    return hashlib.md5(tag.lower().encode("utf-8")).hexdigest()[:12]


async def show_catalog(bot: Bot, chat_id: int):
    tags = await get_hashtags(20)
    if not tags:
        await bot.send_message(chat_id, "📂 В каталоге пока нет постов.")
        return
    rows = []
    row = []
    for t in tags:
        label = t["hashtag"][:24]
        row.append(InlineKeyboardButton(text=f"{label} ({t['cnt']})", callback_data=f"cat_tag:{_tag_hash(t['hashtag'])}"))
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


def build_top_keyboard(posts: List[Dict[str, Any]], admin_mode: bool) -> InlineKeyboardMarkup:
    rows = []
    for p in posts:
        title = (p["title"] or "Без названия")[:35]
        row = [InlineKeyboardButton(text=f"🔥 {title} (👁 {p['views']})", callback_data=f"open_post:{p['id']}")]
        if admin_mode:
            row.append(InlineKeyboardButton(text="🗑", callback_data=f"qdel_post:{p['id']}"))
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "menu:top")
async def cb_menu_top(callback: CallbackQuery):
    await callback.answer()
    posts = await get_top_posts(10)
    if not posts:
        await callback.message.answer("Постов пока нет.")
        return
    admin_mode = await is_admin(callback.from_user.id)
    await callback.message.answer(
        "🔥 <b>Популярные посты:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_top_keyboard(posts, admin_mode),
    )


@router.callback_query(F.data.startswith("qdel_post:"))
async def cb_quick_delete_post(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Только для админов.", show_alert=True)
        return
    post_id = int(callback.data.split(":", 1)[1])
    deleted = await delete_post_from_db(post_id)
    if not deleted:
        await callback.answer("Пост уже удалён.", show_alert=True)
    else:
        if deleted.get("channel_id") and deleted.get("channel_message_id"):
            await safe_delete(callback.bot, deleted["channel_id"], deleted["channel_message_id"])
        await callback.answer(f"🗑 Пост #{post_id} удалён!")
    posts = await get_top_posts(10)
    try:
        if posts:
            await callback.message.edit_reply_markup(reply_markup=build_top_keyboard(posts, True))
        else:
            await callback.message.edit_text("Постов пока нет.")
    except TelegramBadRequest:
        pass


def build_catalog_posts_kb(posts: List[Dict[str, Any]], admin_mode: bool, ctx: str, tag_hash: Optional[str] = None) -> InlineKeyboardMarkup:
    rows = []
    for p in posts:
        title = (p["title"] or "Без названия")[:35]
        row = [InlineKeyboardButton(text=f"🎬 {title}", callback_data=f"open_post:{p['id']}")]
        if admin_mode:
            row.append(InlineKeyboardButton(text="🗑", callback_data=f"qdel_cat:{p['id']}:{ctx}"))
        rows.append(row)
    if admin_mode and tag_hash:
        rows.append([InlineKeyboardButton(text="🗑 Удалить весь этот хэштег", callback_data=f"qdel_tag:{tag_hash}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "cat_recent")
async def cb_cat_recent(callback: CallbackQuery):
    await callback.answer()
    posts = await get_recent_posts(10)
    if not posts:
        await callback.message.answer("Постов пока нет.")
        return
    admin_mode = await is_admin(callback.from_user.id)
    await callback.message.answer(
        "🆕 <b>Последние посты:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_catalog_posts_kb(posts, admin_mode, "recent"),
    )


@router.callback_query(F.data.startswith("cat_tag:"))
async def cb_cat_tag(callback: CallbackQuery):
    await callback.answer()
    thash = callback.data.split(":", 1)[1]
    tags = await get_hashtags(100)
    tag = next((t["hashtag"] for t in tags if _tag_hash(t["hashtag"]) == thash), None)
    if not tag:
        await callback.message.answer("Хэштег не найден.")
        return
    posts = await get_posts_by_hashtag(tag, 15)
    if not posts:
        await callback.message.answer(f"По хэштегу {tag} ничего не найдено.")
        return
    admin_mode = await is_admin(callback.from_user.id)
    await callback.message.answer(
        f"📂 <b>Посты по {tag}:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_catalog_posts_kb(posts, admin_mode, f"t_{thash}", tag_hash=thash),
    )


@router.callback_query(F.data.startswith("qdel_cat:"))
async def cb_quick_delete_cat_post(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Только для админов.", show_alert=True)
        return
    _, post_id_str, ctx = callback.data.split(":", 2)
    post_id = int(post_id_str)
    deleted = await delete_post_from_db(post_id)
    if not deleted:
        await callback.answer("Пост уже удалён.", show_alert=True)
    else:
        if deleted.get("channel_id") and deleted.get("channel_message_id"):
            await safe_delete(callback.bot, deleted["channel_id"], deleted["channel_message_id"])
        await callback.answer(f"🗑 Пост #{post_id} удалён!")

    if ctx == "recent":
        posts = await get_recent_posts(10)
        kb = build_catalog_posts_kb(posts, True, "recent") if posts else None
    else:
        thash = ctx[2:]
        tags = await get_hashtags(100)
        tag = next((t["hashtag"] for t in tags if _tag_hash(t["hashtag"]) == thash), None)
        posts = await get_posts_by_hashtag(tag, 15) if tag else []
        kb = build_catalog_posts_kb(posts, True, ctx, tag_hash=thash) if posts else None

    try:
        if kb:
            await callback.message.edit_reply_markup(reply_markup=kb)
        else:
            await callback.message.edit_text("Постов в этом разделе больше нет.")
    except TelegramBadRequest:
        pass


@router.callback_query(F.data.startswith("qdel_tag:"))
async def cb_quick_delete_tag(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Только для админов.", show_alert=True)
        return
    thash = callback.data.split(":", 1)[1]
    tags = await get_hashtags(100)
    tag = next((t["hashtag"] for t in tags if _tag_hash(t["hashtag"]) == thash), None)
    if not tag:
        await callback.answer("Хэштег уже пуст.", show_alert=True)
        return
    deleted_rows = await delete_posts_by_hashtag(tag)
    for r in deleted_rows:
        if r.get("channel_id") and r.get("channel_message_id"):
            await safe_delete(callback.bot, r["channel_id"], r["channel_message_id"])
    await callback.answer(f"🗑 Удалено постов: {len(deleted_rows)}", show_alert=True)
    try:
        await callback.message.edit_text(f"🗑 Раздел {tag} полностью удалён ({len(deleted_rows)} пост.).")
    except TelegramBadRequest:
        pass


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

# ---------- избранное, поиск и случайный пост ----------
@router.callback_query(F.data.startswith("fav_toggle:"))
async def cb_fav_toggle(callback: CallbackQuery):
    post_id = int(callback.data.split(":", 1)[1])
    added = await toggle_favorite(callback.from_user.id, post_id)
    await callback.answer("❤️ Добавлено в избранное!" if added else "💔 Убрано из избранного.")
    try:
        await callback.message.edit_reply_markup(reply_markup=fav_button_kb(post_id, added))
    except TelegramBadRequest:
        pass


async def show_favorites(bot: Bot, chat_id: int, user_id: int):
    posts = await get_user_favorites(user_id, 20)
    if not posts:
        await bot.send_message(chat_id, "⭐ В избранном пока пусто. Нажми «❤️ В избранное» под любым постом.")
        return
    rows = [
        [InlineKeyboardButton(text=f"⭐ {(p['title'] or 'Без названия')[:42]}", callback_data=f"open_post:{p['id']}")]
        for p in posts
    ]
    await bot.send_message(chat_id, "⭐ <b>Твоё избранное:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.message(Command("fav"))
async def cmd_fav(message: Message):
    if await is_banned(message.from_user.id):
        return
    await show_favorites(message.bot, message.chat.id, message.from_user.id)


@router.callback_query(F.data == "menu:fav")
async def cb_menu_fav(callback: CallbackQuery):
    await callback.answer()
    await show_favorites(callback.bot, callback.message.chat.id, callback.from_user.id)


@router.callback_query(F.data == "menu:random")
async def cb_menu_random(callback: CallbackQuery):
    if await is_banned(callback.from_user.id):
        await callback.answer()
        return
    is_sub, missing = await check_user_subscription(callback.bot, callback.from_user.id)
    if not is_sub and missing:
        await callback.answer("Нужна подписка!", show_alert=True)
        await callback.message.answer("🔒 Сначала подпишись на канал(ы):", reply_markup=subscribe_keyboard(missing, ""))
        return
    post_id = await get_random_post_id()
    if not post_id:
        await callback.answer("Постов пока нет.", show_alert=True)
        return
    await callback.answer("🎲 Игра началась!")
    await _deliver_post(callback.bot, callback.message.chat.id, post_id)


@router.message(Command("search"))
async def cmd_search(message: Message, state: FSMContext):
    if await is_banned(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) >= 2 and args[1].strip():
        await _run_search(message, args[1].strip())
        return
    await state.set_state(SearchState.waiting_query)
    await message.answer("🔍 Введи слово или часть названия для поиска:")


@router.callback_query(F.data == "menu:search")
async def cb_menu_search(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if await is_banned(callback.from_user.id):
        return
    await state.set_state(SearchState.waiting_query)
    await callback.message.answer("🔍 Введи слово или часть названия для поиска:")


@router.message(StateFilter(SearchState.waiting_query), F.text)
async def handle_search_query(message: Message, state: FSMContext):
    if message.text.startswith("/"):
        await state.clear()
        return
    await state.clear()
    await _run_search(message, message.text.strip())


async def _run_search(message: Message, query: str):
    posts = await search_posts_by_title(query, 15)
    if not posts:
        await message.answer(f"🔍 По запросу «{query}» ничего не найдено.")
        return
    rows = [
        [InlineKeyboardButton(text=f"🎬 {(p['title'] or 'Без названия')[:45]}", callback_data=f"open_post:{p['id']}")]
        for p in posts
    ]
    await message.answer(
        f"🔍 <b>Результаты по «{query}»:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


# =============================================================================
# 📣 РЕКЛАМА ПО РАСПИСАНИЮ (/reklama)
# =============================================================================
@router.message(Command("reklama"))
async def cmd_reklama(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) >= 2:
        sub = parts[1].lower()
        if sub == "list":
            ads = await list_active_ads()
            if not ads:
                await message.answer("📣 Активных рекламных кампаний нет.")
                return
            lines = []
            for a in ads:
                rep = f"каждые {a['interval_minutes']} мин" if a["interval_minutes"] > 0 else "один раз"
                lines.append(
                    f"#{a['id']} | {rep} | след: {a['next_run'].strftime('%d.%m %H:%M UTC')}\n"
                    f"Ссылка: {a['link']}\nТекст: {a['text'][:50]}..."
                )
            await message.answer("📣 <b>Активная реклама:</b>\n\n" + "\n\n".join(lines) + "\n\nУдалить: <code>/reklama del ID</code>", parse_mode=ParseMode.HTML)
            return
        if sub == "del" and len(parts) >= 3 and parts[2].isdigit():
            ad_id = int(parts[2])
            ok = await delete_ad_campaign(ad_id)
            if ok:
                await log_admin_action(message.bot, message.from_user.id, message.from_user.full_name, f"Удалил рекламу #{ad_id}")
            await message.answer(f"🗑 Реклама #{ad_id} удалена." if ok else f"❌ Реклама #{ad_id} не найдена.")
            return

    await state.clear()
    await state.set_state(ReklamaState.waiting_text)
    await message.answer(
        "📣 <b>Создание рекламы в канал</b>\n\n"
        "Пришли <b>текст/описание</b> рекламного поста.\n"
        "<i>(Список активной рекламы: /reklama list, отмена: /cancel)</i>",
        parse_mode=ParseMode.HTML,
    )


@router.message(StateFilter(ReklamaState.waiting_text), F.text)
async def reklama_get_text(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(ad_text=message.text.strip())
    await state.set_state(ReklamaState.waiting_link)
    await message.answer("🔗 Теперь пришли <b>ссылку</b> (и через пробел текст кнопки, если хочешь).\nПример: <code>https://t.me/kanal Подписаться</code>", parse_mode=ParseMode.HTML)


@router.message(StateFilter(ReklamaState.waiting_link), F.text)
async def reklama_get_link(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=1)
    link = normalize_zakrep_link(parts[0])
    btn_text = parts[1].strip() if len(parts) > 1 else "🔗 Перейти"
    await state.update_data(ad_link=link, ad_btn=btn_text)
    await state.set_state(ReklamaState.waiting_photo)
    await message.answer("🖼 Пришли <b>фото</b> для рекламы, или напиши <code>-</code> (минус), чтобы без фото.", parse_mode=ParseMode.HTML)


@router.message(StateFilter(ReklamaState.waiting_photo), F.photo)
async def reklama_get_photo(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(ad_photo=message.photo[-1].file_id)
    await state.set_state(ReklamaState.waiting_schedule)
    await message.answer(
        "🕒 <b>Когда и как публиковать?</b>\n"
        "• <code>30m</code>, <code>2h</code>, <code>1d</code> — опубликовать один раз через это время\n"
        "• <code>every 6h</code> (или <code>каждые 6h</code>) — повторять регулярно",
        parse_mode=ParseMode.HTML,
    )


@router.message(StateFilter(ReklamaState.waiting_photo), F.text)
async def reklama_skip_photo(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(ad_photo=None)
    await state.set_state(ReklamaState.waiting_schedule)
    await message.answer(
        "🕒 <b>Когда и как публиковать?</b>\n"
        "• <code>30m</code>, <code>2h</code>, <code>1d</code> — опубликовать один раз через это время\n"
        "• <code>every 6h</code> (или <code>каждые 6h</code>) — повторять регулярно",
        parse_mode=ParseMode.HTML,
    )


@router.message(StateFilter(ReklamaState.waiting_schedule), F.text)
async def reklama_get_schedule(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    raw = message.text.strip().lower()
    recurring = False
    for prefix in ("every ", "каждые ", "раз в "):
        if raw.startswith(prefix):
            recurring = True
            raw = raw[len(prefix):].strip()
            break
    delta = parse_delay(raw)
    if not delta or delta.total_seconds() < 60:
        await message.answer("❌ Неверный формат. Пример: <code>2h</code> или <code>every 6h</code>", parse_mode=ParseMode.HTML)
        return

    interval_min = int(delta.total_seconds() // 60) if recurring else 0
    next_run = datetime.now(timezone.utc) + delta
    await state.update_data(ad_interval=interval_min, ad_next_run=next_run.isoformat())

    channels = await list_channels()
    if not channels:
        await message.answer("❌ Бот не админ ни в одном канале.")
        await state.clear()
        return
    if len(channels) == 1:
        await _save_reklama(message.bot, message.chat.id, message.from_user, state, channels[0]["channel_id"])
        return

    await state.set_state(ReklamaState.waiting_channel)
    rows = [
        [InlineKeyboardButton(text=ch["title"] or str(ch["channel_id"]), callback_data=f"adch:{ch['channel_id']}")]
        for ch in channels
    ]
    await message.answer("В какой канал публиковать рекламу?", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data.startswith("adch:"), StateFilter(ReklamaState.waiting_channel))
async def cb_reklama_channel(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    channel_id = int(callback.data.split(":", 1)[1])
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    await _save_reklama(callback.bot, callback.message.chat.id, callback.from_user, state, channel_id)


async def _save_reklama(bot: Bot, chat_id: int, from_user, state: FSMContext, channel_id: int):
    data = await state.get_data()
    next_run = datetime.fromisoformat(data["ad_next_run"])
    interval_min = data["ad_interval"]
    ad_id = await create_ad_campaign(
        channel_id, data["ad_text"], data["ad_link"], data["ad_btn"],
        data.get("ad_photo"), interval_min, next_run, from_user.id,
    )
    await state.clear()
    rep = f"каждые {interval_min} мин" if interval_min > 0 else "один раз"
    await bot.send_message(
        chat_id,
        f"✅ Реклама #{ad_id} запланирована ({rep}).\nПервый выход: {next_run.strftime('%d.%m %H:%M UTC')}.",
    )
    await log_admin_action(bot, from_user.id, from_user.full_name, f"Создал рекламу #{ad_id} ({rep})")



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
    await message.answer(
        "🔥 <b>Топ постов (нажми 🗑 чтобы удалить):</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_top_keyboard(posts, True),
    )


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
    await log_admin_action(message.bot, message.from_user.id, message.from_user.full_name, f"Удалил пост #{post_id} «{deleted.get('title')}»")
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
        "📥 Приём начат. Кидай контент, потом жми кнопку или /done."
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

        media.append(item)
        status_id = await update_counter(
            message.bot, message.chat.id, data.get("status_msg_id"),
            f"Загружено файлов: {len(media)}. Это всё?", stop_keyboard(len(media)),
        )
        await state.update_data(media=media, status_msg_id=status_id)

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


def _grab_cover_kb(data: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
    if data.get("grab_cover_url"):
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🖼 Взять обложку с сайта", callback_data="grab_use:cover")
        ]])
    return None


@router.message(StateFilter(Batch.waiting_title), F.text)
async def get_title(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(title=message.text.strip())
    data = await state.get_data()
    await state.set_state(Batch.waiting_cover)
    await message.answer("🖼 Теперь пришли обложку (фото).", reply_markup=_grab_cover_kb(data))


@router.callback_query(F.data == "grab_use:title", StateFilter(Batch.waiting_title))
async def cb_grab_use_title(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    data = await state.get_data()
    site_title = data.get("grab_title")
    if not site_title:
        await callback.answer("Название с сайта не найдено.", show_alert=True)
        return
    await callback.answer("✅ Название взято с сайта")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    await state.update_data(title=site_title)
    await state.set_state(Batch.waiting_cover)
    await callback.message.answer(
        f"📝 Название: <b>{html_lib.escape(site_title)}</b>\n\n🖼 Теперь пришли обложку (фото).",
        parse_mode=ParseMode.HTML,
        reply_markup=_grab_cover_kb(data),
    )


@router.message(StateFilter(Batch.waiting_cover), F.photo)
async def get_cover(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(cover_file_id=message.photo[-1].file_id)
    await state.set_state(Batch.waiting_hashtag)
    await message.answer("#️⃣ Теперь пришли хэштег (например: #видео).")


@router.callback_query(F.data == "grab_use:cover", StateFilter(Batch.waiting_cover))
async def cb_grab_use_cover(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    data = await state.get_data()
    cover_url = data.get("grab_cover_url")
    page_url = data.get("grab_page_url", "")
    if not cover_url:
        await callback.answer("Обложка с сайта не найдена.", show_alert=True)
        return
    await callback.answer("⏳ Загружаю обложку с сайта...")
    try:
        file_id = await _upload_image_from_url(callback.bot, callback.message.chat.id, cover_url, page_url)
    except Exception as e:
        await callback.message.answer(f"❌ Не смог скачать обложку с сайта ({e}). Пришли фото вручную:")
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    await state.update_data(cover_file_id=file_id)
    await state.set_state(Batch.waiting_hashtag)
    await callback.message.answer("#️⃣ Обложка принята! Теперь пришли хэштег (например: #видео).")


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

    await log_admin_action(bot, actor_id, admin_name, f"Опубликовал пост #{post_id} «{title}»")
    done_text = f"✅ Опубликовано (#{post_id}).\nСсылка: {deep_link}"
    if auto_delete:
        done_text += "\n🕒 Удалится через 24ч."
    await bot.send_message(chat_id, done_text)
    await state.clear()


# =============================================================================
# 🌐 ПАРСЕР ВИДЕО С САЙТА ПО ССЫЛКЕ И HTML-КОДУ (РЕЖИМ 1 — /grab)
# =============================================================================
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
MAX_GRAB_VIDEO_BYTES = 49 * 1024 * 1024  # 49 МБ (лимит Bot API — 50 МБ)
_grab_cache: Dict[int, Dict[str, Any]] = {}

_AD_IFRAME_HOSTS = (
    "googleads", "doubleclick", "googlesyndication", "googletagmanager",
    "yandex", "mc.yandex", "facebook.com/plugins", "vk.com/widget",
    "twitter.com/widgets", "addthis", "sharethis", "recaptcha", "hcaptcha",
)

_HTML_EMBED_RE = re.compile(r"<(?:iframe|video|source|embed|object)\b", re.IGNORECASE)


def _looks_like_html_embed(text: str) -> bool:
    return bool(_HTML_EMBED_RE.search(text or ""))


def _extract_url_from_text(text: str) -> Optional[str]:
    m = re.search(r"https?://[^\s<>\"']+", text.strip())
    if m:
        return m.group(0).rstrip(".,);]")
    m_proto = re.search(r"(?:src|href)=[\"'](//[^\s\"']+)[\"']", text, re.IGNORECASE)
    if m_proto:
        return "https:" + m_proto.group(1)
    return None


def _is_direct_video_url(url: str) -> bool:
    path = urlparse(url).path.lower().rstrip("/")
    return path.endswith((".mp4", ".mov", ".webm", ".m4v"))


def _root_site_url(url: str) -> str:
    """Возвращает корень основного домена (например, https://cdn.site.com/a/b -> https://site.com/)."""
    p = urlparse(url)
    if not p.scheme or not p.netloc:
        return ""
    host = p.netloc.split(":")[0]
    parts = host.split(".")
    if len(parts) >= 3 and parts[0] in ("cdn", "cdn1", "cdn2", "s1", "s2", "v1", "v2", "static", "media", "dl", "stream", "video"):
        base_host = ".".join(parts[1:])
    else:
        base_host = p.netloc
    return f"{p.scheme}://{base_host}/"


def _video_url_priority(url: str) -> Tuple[int, int]:
    """Приоритет: сначала стандартный .mp4 (H.264), затем .mov/.m4v, в конце .webm и HEVC."""
    low = url.lower()
    path = urlparse(low).path.rstrip("/")
    is_hevc = int(any(k in low for k in ("hevc", "h265", "h.265", "vp9", "av1")))
    if path.endswith(".mp4"):
        ext_rank = 0
    elif path.endswith((".m4v", ".mov")):
        ext_rank = 1
    elif path.endswith(".webm"):
        ext_rank = 2
    else:
        ext_rank = 3
    return (is_hevc, ext_rank)


def _extract_html_title(html_text: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html_text, re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return re.sub(r"\s+", " ", html_lib.unescape(m.group(1))).strip()[:80]


def _extract_videos_from_html(html_text: str, base_url: str) -> Tuple[List[str], bool]:
    """Извлекает все прямые и относительные ссылки на видео из HTML и JS-скриптов."""
    found: List[str] = []
    seen = set()
    has_hls = bool(re.search(r"\.m3u8(?:\?|\b|\"|\')", html_text, re.IGNORECASE))

    def add_candidate(raw_u: str):
        u = html_lib.unescape(raw_u).replace("\\/", "/").strip()
        if not u or u.startswith(("data:", "blob:", "javascript:", "#")):
            return
        if u.startswith("//"):
            u = "https:" + u
        full = urljoin(base_url, u) if base_url else u
        if not full.startswith(("http://", "https://")):
            return
        low = full.lower()
        if ".m3u8" in low or ".mpd" in low:
            return
        if full not in seen:
            seen.add(full)
            found.append(full)

    # 1. OpenGraph / Twitter meta video
    for pat in (
        r'<meta[^>]+(?:property|name)=["\'](?:og:video(?::url|:secure_url)?|twitter:player:stream)["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\'](?:og:video(?::url|:secure_url)?|twitter:player:stream)["\']',
    ):
        for m in re.finditer(pat, html_text, re.IGNORECASE):
            add_candidate(m.group(1))

    # 2. <video src/data-src> и <source src/data-src>
    for m in re.finditer(r'<(?:video|source)[^>]+(?:data-src|data-video|data-url|src)=["\']([^"\']+)["\']', html_text, re.IGNORECASE):
        add_candidate(m.group(1))

    # 3. Атрибуты data-video / data-mp4 / data-file / href на видеофайлы
    for m in re.finditer(
        r'(?:data-(?:video|mp4|file|stream|source|src)|href)=["\']([^"\']+?\.(?:mp4|webm|mov|m4v)(?:/|\?[^"\']*)?)["\']',
        html_text,
        re.IGNORECASE,
    ):
        add_candidate(m.group(1))

    # 4. Абсолютные, протокол-относительные (//...) и относительные (/...) пути к видео в JS/JSON
    unescaped_html = html_text.replace("\\/", "/")
    for m in re.finditer(
        r'(?:https?:)?//[^\s"\'<>\\]+?\.(?:mp4|webm|mov|m4v)(?:/|\?[^\s"\'<>\\]*)?',
        unescaped_html,
        re.IGNORECASE,
    ):
        add_candidate(m.group(0))

    for m in re.finditer(
        r'["\'](/[a-zA-Z0-9_./\-]+?\.(?:mp4|webm|mov|m4v)(?:/|\?[^"\']*)?)["\']',
        unescaped_html,
        re.IGNORECASE,
    ):
        add_candidate(m.group(1))

    # 5. Ключи видеоплееров в JSON/JS ("video_url", "mp4", "hd", "sd", "file", "src")
    for m in re.finditer(
        r'["\'](?:video_url|videoUrl|mp4|hd|sd|file|src|stream_url|streamUrl)["\']\s*:\s*["\']((?:https?:)?//[^"\']+?|/[^"\']+?)["\']',
        unescaped_html,
        re.IGNORECASE,
    ):
        cand = m.group(1)
        if any(ext in cand.lower() for ext in (".mp4", ".webm", ".mov", ".m4v", "/get_file", "/stream", "/media")):
            add_candidate(cand)

    return found, has_hls


def _extract_iframe_urls(html_text: str, base_url: str) -> List[str]:
    iframes: List[str] = []
    seen = set()
    for m in re.finditer(r'<iframe[^>]+(?:data-src|src)=["\']([^"\']+)["\']', html_text, re.IGNORECASE):
        raw_u = html_lib.unescape(m.group(1)).strip()
        if not raw_u or raw_u.startswith(("about:", "javascript:", "data:")):
            continue
        if raw_u.startswith("//"):
            raw_u = "https:" + raw_u
        full = urljoin(base_url, raw_u) if base_url else raw_u
        if not full.startswith(("http://", "https://")):
            continue
        low = full.lower()
        if any(ad in low for ad in _AD_IFRAME_HOSTS):
            continue
        if full not in seen:
            seen.add(full)
            iframes.append(full)
    return iframes[:3]


def _extract_with_ytdlp_sync(url: str) -> Optional[Dict[str, Any]]:
    """Резервный экстрактор через yt-dlp (если библиотека установлена в окружении)."""
    try:
        import yt_dlp  # type: ignore
    except ImportError:
        return None

    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "socket_timeout": 15,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        log.info("yt-dlp не смог извлечь %s: %s", url, e)
        return None

    if not info:
        return None

    if "entries" in info and info["entries"]:
        entries = [e for e in info["entries"] if e]
        if not entries:
            return None
        info = entries[0]

    formats = info.get("formats") or []
    candidates: List[Tuple[int, int, str, Dict[str, str]]] = []
    for f in formats:
        f_url = f.get("url")
        if not f_url or not f_url.startswith("http"):
            continue
        proto = (f.get("protocol") or "").lower()
        if "m3u8" in proto or "dash" in proto or ".m3u8" in f_url.lower():
            continue
        vcodec = f.get("vcodec") or "none"
        acodec = f.get("acodec") or "none"
        if vcodec == "none" or acodec == "none":
            continue
        fsize = f.get("filesize") or f.get("filesize_approx") or 0
        if fsize and fsize > MAX_GRAB_VIDEO_BYTES:
            continue
        ext = (f.get("ext") or "").lower()
        is_h264 = 0 if ("avc" in vcodec.lower() or ext == "mp4") else 1
        height = int(f.get("height") or 0)
        candidates.append((is_h264, -height, f_url, dict(f.get("http_headers") or {})))

    if not candidates and info.get("url") and str(info["url"]).startswith("http"):
        if ".m3u8" not in str(info["url"]).lower():
            candidates.append((0, 0, str(info["url"]), dict(info.get("http_headers") or {})))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1]))
    seen = set()
    urls: List[str] = []
    headers_map: Dict[str, Dict[str, str]] = {}
    for _, _, u, hdrs in candidates:
        if u not in seen:
            seen.add(u)
            urls.append(u)
            if hdrs:
                headers_map[u] = hdrs

    return {
        "videos": urls[:10],
        "title": (info.get("title") or "")[:120] or None,
        "cover_url": info.get("thumbnail"),
        "has_hls": False,
        "ytdlp_headers": headers_map,
    }


def _inspect_video_bytes(raw: bytes) -> Dict[str, Any]:
    """Проверяет, что скачанный файл — реальное видео (а не HTML), и достаёт размеры/кодек."""
    if len(raw) < 10240:
        raise RuntimeError("файл слишком маленький (меньше 10 КБ) — сайт отдал заглушку вместо видео")

    head_probe = raw[:1024].lstrip().lower()
    if (
        head_probe.startswith((b"<!doctype", b"<html", b"<head", b"<script", b"<?xml", b"{"))
        or b"<html" in head_probe
    ):
        raise RuntimeError("сайт отдал HTML-страницу вместо видеофайла")

    info: Dict[str, Any] = {
        "container": "unknown",
        "codec": "unknown",
        "width": None,
        "height": None,
    }

    if len(raw) >= 12 and raw[4:8] == b"ftyp":
        info["container"] = "mp4"
        scan = raw[:3 * 1024 * 1024] + (raw[-3 * 1024 * 1024:] if len(raw) > 3 * 1024 * 1024 else b"")

        if b"avc1" in scan or b"avc3" in scan:
            info["codec"] = "h264"
        elif b"hvc1" in scan or b"hev1" in scan:
            info["codec"] = "hevc"
        elif b"vp09" in scan:
            info["codec"] = "vp9"
        elif b"av01" in scan:
            info["codec"] = "av1"

        pos = 0
        while True:
            idx = scan.find(b"tkhd", pos)
            if idx == -1 or idx < 4:
                break
            box_start = idx - 4
            if box_start + 92 <= len(scan):
                version = scan[box_start + 8]
                wh_offset = box_start + (84 if version == 0 else 96)
                if wh_offset + 8 <= len(scan):
                    w_fixed, h_fixed = struct.unpack(">II", scan[wh_offset:wh_offset + 8])
                    w, h = w_fixed >> 16, h_fixed >> 16
                    if 64 <= w <= 7680 and 64 <= h <= 4320:
                        info["width"] = w
                        info["height"] = h
                        break
            pos = idx + 4
    elif raw[:4] == b"\x1a\x45\xdf\xa3":
        info["container"] = "webm"
        info["codec"] = "vp8/vp9"

    return info


def _dump_cookie_jar(jar: CookieJar) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    for c in jar:
        cookies[c.key] = c.value
    return cookies


async def parse_html_embed_snippet(snippet: str) -> Dict[str, Any]:
    """Разбирает HTML-код для вставки («Поделиться HTML-кодом»): <iframe ...> или <video ...>."""
    # 1. Ищем прямые <video>/<source> прямо внутри вставленного HTML-кода
    direct_vids, has_hls = _extract_videos_from_html(snippet, "")

    title = None
    m_title = re.search(r'\btitle=["\']([^"\']+)["\']', snippet, re.IGNORECASE)
    if m_title:
        title = html_lib.unescape(m_title.group(1)).strip()[:120] or None

    cover_url = None
    m_poster = re.search(r'\bposter=["\']([^"\']+)["\']', snippet, re.IGNORECASE)
    if m_poster:
        p_u = html_lib.unescape(m_poster.group(1)).strip()
        if p_u.startswith("//"):
            p_u = "https:" + p_u
        if p_u.startswith(("http://", "https://")):
            cover_url = p_u

    iframes = _extract_iframe_urls(snippet, "")
    embed_url = iframes[0] if iframes else (_extract_url_from_text(snippet) or "")

    if direct_vids:
        direct_vids.sort(key=_video_url_priority)
        return {
            "videos": direct_vids[:15],
            "title": title,
            "cover_url": cover_url,
            "has_hls": has_hls,
            "cookies": {},
            "ytdlp_headers": {},
            "source_url": embed_url or direct_vids[0],
        }

    if embed_url:
        parsed = await parse_site_video_page(embed_url)
        if title and not parsed.get("title"):
            parsed["title"] = title
        if cover_url and not parsed.get("cover_url"):
            parsed["cover_url"] = cover_url
        parsed["source_url"] = embed_url
        return parsed

    raise RuntimeError("в переданном HTML-коде не найдено ни ссылки на видео, ни <iframe> плеера")


async def parse_site_video_page(page_url: str) -> Dict[str, Any]:
    """Парсит веб-страницу (и вложенные iframe), сохраняя cookies сессии."""
    jar = CookieJar(unsafe=True)
    root_ref = _root_site_url(page_url)
    headers = {
        "User-Agent": BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Upgrade-Insecure-Requests": "1",
    }
    if root_ref:
        headers["Referer"] = root_ref

    if _is_direct_video_url(page_url):
        if root_ref:
            try:
                async with ClientSession(cookie_jar=jar, timeout=ClientTimeout(total=10)) as s:
                    async with s.get(root_ref, headers=headers, allow_redirects=True) as r:
                        await r.content.read(65536)
            except Exception:
                pass
        return {
            "videos": [page_url],
            "title": None,
            "cover_url": None,
            "has_hls": False,
            "cookies": _dump_cookie_jar(jar),
            "ytdlp_headers": {},
            "source_url": page_url,
        }

    html_text = ""
    final_url = page_url
    fetch_err: Optional[str] = None

    try:
        async with ClientSession(cookie_jar=jar, timeout=ClientTimeout(total=20)) as session:
            async with session.get(page_url, headers=headers, allow_redirects=True) as resp:
                if resp.status >= 400:
                    fetch_err = f"Сайт вернул код {resp.status}"
                else:
                    final_url = str(resp.url)
                    ctype = resp.headers.get("Content-Type", "").lower()
                    if ctype.startswith("video/") or _is_direct_video_url(final_url):
                        return {
                            "videos": [final_url],
                            "title": None,
                            "cover_url": None,
                            "has_hls": False,
                            "cookies": _dump_cookie_jar(jar),
                            "ytdlp_headers": {},
                            "source_url": final_url,
                        }
                    raw = await resp.content.read(2 * 1024 * 1024)
                    html_text = raw.decode("utf-8", errors="ignore")

            if html_text:
                found, has_hls = _extract_videos_from_html(html_text, final_url)
                if not found:
                    for iframe_url in _extract_iframe_urls(html_text, final_url):
                        try:
                            if_headers = dict(headers)
                            if_headers["Referer"] = final_url
                            async with session.get(iframe_url, headers=if_headers, allow_redirects=True) as if_resp:
                                if if_resp.status < 400:
                                    if_final = str(if_resp.url)
                                    if_ctype = if_resp.headers.get("Content-Type", "").lower()
                                    if if_ctype.startswith("video/") or _is_direct_video_url(if_final):
                                        found.append(if_final)
                                        break
                                    if_raw = await if_resp.content.read(1024 * 1024)
                                    if_html = if_raw.decode("utf-8", errors="ignore")
                                    sub_found, sub_hls = _extract_videos_from_html(if_html, if_final)
                                    has_hls = has_hls or sub_hls
                                    for u in sub_found:
                                        if u not in found:
                                            found.append(u)
                        except Exception:
                            continue
            else:
                found, has_hls = [], False
    except Exception as e:
        fetch_err = str(e)
        found, has_hls = [], False

    title = None
    if html_text:
        for pat in (
            r'<meta[^>]+(?:property|name)=["\'](?:og:title|twitter:title)["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\'](?:og:title|twitter:title)["\']',
            r"<title[^>]*>(.*?)</title>",
        ):
            m = re.search(pat, html_text, re.IGNORECASE | re.DOTALL)
            if m:
                cleaned = re.sub(r"\s+", " ", html_lib.unescape(m.group(1))).strip()
                if cleaned:
                    title = cleaned[:120]
                    break

    cover_url = None
    if html_text:
        for pat in (
            r'<meta[^>]+(?:property|name)=["\'](?:og:image|twitter:image)["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\'](?:og:image|twitter:image)["\']',
            r'<video[^>]+poster=["\']([^"\']+)["\']',
        ):
            m = re.search(pat, html_text, re.IGNORECASE)
            if m:
                cand = html_lib.unescape(m.group(1).strip())
                if cand and not cand.startswith("data:"):
                    cover_url = urljoin(final_url, cand)
                    break

    ytdlp_headers: Dict[str, Dict[str, str]] = {}
    if not found:
        yt_res = await asyncio.to_thread(_extract_with_ytdlp_sync, page_url)
        if yt_res and yt_res.get("videos"):
            found = yt_res["videos"]
            title = title or yt_res.get("title")
            cover_url = cover_url or yt_res.get("cover_url")
            ytdlp_headers = yt_res.get("ytdlp_headers") or {}
        elif fetch_err:
            raise RuntimeError(fetch_err)

    found.sort(key=_video_url_priority)

    return {
        "videos": found[:15],
        "title": title,
        "cover_url": cover_url,
        "has_hls": has_hls,
        "cookies": _dump_cookie_jar(jar),
        "ytdlp_headers": ytdlp_headers,
        "source_url": final_url,
    }


def _extract_redirect_from_html_interstitial(html_text: str, base_url: str, exclude_url: str = "") -> Optional[str]:
    """Если сервер вернул HTML-страницу плеера/редиректа, вытаскивает из неё прямую ссылку на видео."""
    m = re.search(
        r'<meta[^>]+http-equiv=["\']refresh["\'][^>]+content=["\'][^"\']*?url=([^"\'>\s]+)',
        html_text,
        re.IGNORECASE,
    )
    if m:
        cand = urljoin(base_url, html_lib.unescape(m.group(1).strip()))
        if cand != exclude_url:
            return cand

    m = re.search(
        r'(?:window\.)?location(?:\.href)?\s*=\s*["\']([^"\']+?\.(?:mp4|webm|mov|m4v)(?:/|\?[^"\']*)?)["\']',
        html_text,
        re.IGNORECASE,
    )
    if m:
        cand = urljoin(base_url, html_lib.unescape(m.group(1).strip()))
        if cand != exclude_url:
            return cand

    vids, _ = _extract_videos_from_html(html_text, base_url)
    for v in vids:
        if v != exclude_url and v != base_url:
            return v

    iframes = _extract_iframe_urls(html_text, base_url)
    for ifr in iframes:
        if ifr != exclude_url and ifr != base_url:
            return ifr

    return None


async def _download_video_bytes_with_retries(
    video_url: str,
    page_url: str,
    saved_cookies: Optional[Dict[str, str]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> bytes:
    """Скачивает видео, подбирая Referer, cookies сессии и разворачивая HTML-плееры."""
    jar = CookieJar(unsafe=True)
    if saved_cookies:
        jar.update_cookies(saved_cookies)

    root_ref = _root_site_url(page_url or video_url)
    video_root_ref = _root_site_url(video_url)

    primary_ref = page_url if (page_url and page_url != video_url) else (root_ref or video_root_ref)
    strategies: List[Tuple[str, Optional[str], bool]] = [
        ("page_ref", primary_ref, False),
    ]
    if video_root_ref and video_root_ref != primary_ref:
        strategies.append(("cdn_root_ref", video_root_ref, False))
    strategies.append(("warmup_cookies", primary_ref, True))
    strategies.append(("no_ref", None, False))

    last_diag = "сайт вернул text/html вместо видео"
    current_url = video_url

    async with ClientSession(cookie_jar=jar, timeout=ClientTimeout(total=120)) as session:
        for _, ref_url, do_warmup in strategies:
            if do_warmup:
                for warm_target in dict.fromkeys(u for u in (root_ref, page_url) if u and u != current_url):
                    try:
                        w_hdrs = {"User-Agent": BROWSER_UA, "Accept": "text/html,*/*;q=0.8"}
                        async with session.get(warm_target, headers=w_hdrs, allow_redirects=True) as w_resp:
                            await w_resp.content.read(131072)
                    except Exception:
                        pass

            headers: Dict[str, str] = {
                "User-Agent": BROWSER_UA,
                "Accept": "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Sec-Fetch-Dest": "video",
                "Sec-Fetch-Mode": "no-cors",
            }
            if extra_headers:
                for k, v in extra_headers.items():
                    if k.lower() not in ("host", "content-length"):
                        headers[k] = v

            if ref_url:
                headers["Referer"] = ref_url
                p_ref = urlparse(ref_url)
                if p_ref.scheme and p_ref.netloc:
                    headers["Origin"] = f"{p_ref.scheme}://{p_ref.netloc}"
            else:
                headers.pop("Referer", None)
                headers.pop("Origin", None)

            async with session.get(current_url, headers=headers, allow_redirects=True) as resp:
                if resp.status >= 400:
                    last_diag = f"сервер видео ответил кодом {resp.status}"
                    continue

                ctype = resp.headers.get("Content-Type", "").lower()
                if ctype.startswith(("text/", "application/json", "application/xml", "application/javascript")):
                    html_probe = (await resp.content.read(512 * 1024)).decode("utf-8", errors="ignore")
                    unwrapped = _extract_redirect_from_html_interstitial(html_probe, str(resp.url), exclude_url=current_url)
                    if unwrapped and unwrapped != current_url:
                        inner_headers = dict(headers)
                        inner_headers["Referer"] = str(resp.url)
                        current_url = unwrapped
                        async with session.get(current_url, headers=inner_headers, allow_redirects=True) as resp2:
                            ctype2 = resp2.headers.get("Content-Type", "").lower()
                            if resp2.status < 400 and not ctype2.startswith(("text/", "application/json")):
                                resp = resp2
                                ctype = ctype2
                            else:
                                html_title = _extract_html_title(html_probe)
                                last_diag = f"сайт вернул text/html{f' («{html_title}»)' if html_title else ''} вместо видео"
                                continue
                    else:
                        html_title = _extract_html_title(html_probe)
                        last_diag = f"сайт вернул text/html{f' («{html_title}»)' if html_title else ''} вместо видео"
                        continue

                cl = resp.headers.get("Content-Length")
                if cl and cl.isdigit() and int(cl) > MAX_GRAB_VIDEO_BYTES:
                    mb = round(int(cl) / (1024 * 1024), 1)
                    raise RuntimeError(f"видео весит {mb} МБ (лимит Telegram для ботов — 50 МБ)")

                buf = io.BytesIO()
                total = 0
                async for chunk in resp.content.iter_chunked(256 * 1024):
                    total += len(chunk)
                    if total > MAX_GRAB_VIDEO_BYTES:
                        buf.close()
                        raise RuntimeError("видео больше 50 МБ (лимит Telegram для ботов)")
                    buf.write(chunk)

                raw_bytes = buf.getvalue()
                buf.close()

                head_probe = raw_bytes[:1024].lstrip().lower()
                if head_probe.startswith((b"<!doctype", b"<html", b"<head")) or b"<html" in head_probe:
                    html_title = _extract_html_title(raw_bytes[:65536].decode("utf-8", errors="ignore"))
                    last_diag = f"сайт вернул HTML-страницу{f' («{html_title}»)' if html_title else ''} вместо видео"
                    continue

                return raw_bytes

    raise RuntimeError(last_diag)


async def _upload_video_from_url(
    bot: Bot,
    chat_id: int,
    video_url: str,
    page_url: str,
    saved_cookies: Optional[Dict[str, str]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Tuple[Message, Dict[str, Any]]:
    """Скачивает видео с подбором Referer/cookies, проверяет кодек и грузит со стримингом."""
    raw_bytes = await _download_video_bytes_with_retries(
        video_url, page_url, saved_cookies=saved_cookies, extra_headers=extra_headers
    )
    try:
        vinfo = _inspect_video_bytes(raw_bytes)
        base_name = os.path.splitext(os.path.basename(urlparse(video_url).path.rstrip("/")))[0] or "video"
        ext = ".webm" if vinfo["container"] == "webm" else ".mp4"
        fname = f"{base_name}{ext}"

        sent = await bot.send_video(
            chat_id,
            video=BufferedInputFile(raw_bytes, filename=fname),
            width=vinfo["width"],
            height=vinfo["height"],
            supports_streaming=True,
        )
        return sent, vinfo
    finally:
        del raw_bytes
        gc.collect()


def _item_from_sent_media(sent: Message) -> Dict[str, Any]:
    if sent.video:
        return {"type": "video", "file_id": sent.video.file_id, "msg_id": sent.message_id}
    if sent.animation:
        return {"type": "animation", "file_id": sent.animation.file_id, "msg_id": sent.message_id}
    if sent.document:
        return {"type": "document", "file_id": sent.document.file_id, "msg_id": sent.message_id}
    raise RuntimeError("Telegram не принял файл как видео")


def _codec_warning_text(vinfo: Dict[str, Any]) -> str:
    if vinfo.get("container") == "webm":
        return "\n⚠️ <i>Формат видео WebM — встроенный плеер Telegram на телефоне может его не открыть.</i>"
    if vinfo.get("codec") in ("hevc", "vp9", "av1"):
        return f"\n⚠️ <i>Кодек {vinfo['codec'].upper()} — встроенный плеер Telegram на некоторых телефонах может потребовать внешний плеер.</i>"
    return ""


async def _upload_image_from_url(bot: Bot, chat_id: int, img_url: str, page_url: str) -> str:
    headers = {"User-Agent": BROWSER_UA, "Referer": page_url or img_url}
    async with ClientSession(timeout=ClientTimeout(total=20)) as session:
        async with session.get(img_url, headers=headers, allow_redirects=True) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"код {resp.status}")
            raw = await resp.content.read(10 * 1024 * 1024)
    sent = await bot.send_photo(chat_id, photo=BufferedInputFile(raw, filename="cover.jpg"))
    file_id = sent.photo[-1].file_id
    await safe_delete(bot, chat_id, sent.message_id)
    return file_id


def _grab_ready_kb(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🚀 Опубликовать в канал ({count} шт.)", callback_data="grab:pub")],
        [InlineKeyboardButton(text="➕ Добавить в копилку /batch", callback_data="grab:to_batch")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="grab:cancel")],
    ])


async def _process_site_input(message: Message, raw_input: str):
    chat_id = message.chat.id
    is_html = _looks_like_html_embed(raw_input)
    status = await message.answer("🔍 Разбираю HTML-код плеера..." if is_html else "🔍 Сканирую страницу и ищу видео...")
    try:
        if is_html:
            parsed = await parse_html_embed_snippet(raw_input)
            source_url = parsed.get("source_url") or ""
        else:
            url = _extract_url_from_text(raw_input)
            if not url:
                await status.edit_text("❌ Не вижу ссылки или HTML-кода плеера.")
                return
            parsed = await parse_site_video_page(url)
            source_url = parsed.get("source_url") or url
    except Exception as e:
        await status.edit_text(f"❌ Не удалось обработать: {e}")
        return

    videos = parsed["videos"]
    if not videos:
        if parsed.get("has_hls"):
            await status.edit_text(
                "⚠️ На этой странице видео отдаётся потоком <code>.m3u8</code> (HLS), "
                "прямого <code>.mp4</code> файла нет — Telegram Bot API не принимает потоковые плейлисты.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await status.edit_text("❌ Прямых видеофайлов не найдено (возможно, сайт подгружает плеер через JS или защищён антиботом).")
        return

    saved_cookies = parsed.get("cookies") or {}
    ytdlp_headers = parsed.get("ytdlp_headers") or {}

    _grab_cache[chat_id] = {
        "page_url": source_url,
        "videos": videos,
        "title": parsed["title"],
        "cover_url": parsed["cover_url"],
        "cookies": saved_cookies,
        "ytdlp_headers": ytdlp_headers,
        "items": [],
        "preview_msg_ids": [],
    }

    if len(videos) == 1:
        await status.edit_text("⏳ Нашёл видео! Проверяю и загружаю в Telegram...")
        try:
            sent, vinfo = await _upload_video_from_url(
                message.bot,
                chat_id,
                videos[0],
                source_url,
                saved_cookies=saved_cookies,
                extra_headers=ytdlp_headers.get(videos[0]),
            )
            item = _item_from_sent_media(sent)
        except Exception as e:
            await status.edit_text(f"❌ Не удалось загрузить видео: {e}")
            return
        await safe_delete(message.bot, chat_id, status.message_id)
        _grab_cache[chat_id]["items"] = [item]
        _grab_cache[chat_id]["preview_msg_ids"] = [sent.message_id]
        title_hint = f"\n📝 Заголовок с сайта: <i>{html_lib.escape(parsed['title'])}</i>" if parsed["title"] else ""
        codec_warn = _codec_warning_text(vinfo)
        await message.answer(
            f"✅ <b>Видео готово!</b>{title_hint}{codec_warn}\nЧто делаем дальше?",
            parse_mode=ParseMode.HTML,
            reply_markup=_grab_ready_kb(1),
        )
        return

    rows = []
    row = []
    for idx in range(len(videos)):
        row.append(InlineKeyboardButton(text=f"🎬 Видео #{idx + 1}", callback_data=f"grab:pick:{idx}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=f"📥 Забрать все ({len(videos)} шт.)", callback_data="grab:pick_all")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="grab:cancel")])

    await status.edit_text(
        f"🎬 Найдено видео: <b>{len(videos)} шт.</b>\nВыбери нужное или забери все сразу:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.message(Command("grab"))
async def cmd_grab(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    rest = message.text.split(maxsplit=1)[1].strip() if len(message.text.split(maxsplit=1)) > 1 else ""
    if rest and (_looks_like_html_embed(rest) or _extract_url_from_text(rest)):
        await _process_site_input(message, rest)
        return
    await state.set_state(GrabState.waiting_url)
    await message.answer("🌐 Пришли ссылку на страницу с видео или HTML-код для вставки (<code>&lt;iframe ...&gt;</code>):", parse_mode=ParseMode.HTML)


@router.message(StateFilter(GrabState.waiting_url), F.text)
async def handle_grab_url_state(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    if message.text.startswith("/"):
        await state.clear()
        return
    raw = message.text.strip()
    if not (_looks_like_html_embed(raw) or _extract_url_from_text(raw)):
        await message.answer("❌ Не вижу ссылки или HTML-кода плеера. Пришли URL <code>https://...</code>, код <code>&lt;iframe ...&gt;</code> или /cancel.", parse_mode=ParseMode.HTML)
        return
    await state.clear()
    await _process_site_input(message, raw)


@router.message(StateFilter(None), F.chat.type == ChatType.PRIVATE, F.text.regexp(r"https?://|<(?:iframe|video|source|embed|object)\b"))
async def auto_grab_link_in_pm(message: Message):
    if not await is_admin(message.from_user.id):
        return
    if message.text.startswith("/") or message.text.startswith("."):
        return
    raw = message.text.strip()
    if _looks_like_html_embed(raw):
        await _process_site_input(message, raw)
        return
    url = _extract_url_from_text(raw)
    if not url:
        return
    host = urlparse(url).netloc.lower()
    if host in ("t.me", "telegram.me", "telegram.dog"):
        return
    await _process_site_input(message, raw)


@router.callback_query(F.data.startswith("grab:"))
async def cb_grab_actions(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Только для админов.", show_alert=True)
        return

    chat_id = callback.message.chat.id
    entry = _grab_cache.get(chat_id)
    parts = callback.data.split(":")
    action = parts[1]

    if action == "cancel":
        await callback.answer("Отменено.")
        if entry:
            for mid in entry.get("preview_msg_ids", []):
                await safe_delete(callback.bot, chat_id, mid)
            _grab_cache.pop(chat_id, None)
        await safe_delete(callback.bot, chat_id, callback.message.message_id)
        return

    if not entry:
        await callback.answer("Сессия устарела, отправь ссылку заново.", show_alert=True)
        return

    saved_cookies = entry.get("cookies") or {}
    ytdlp_headers = entry.get("ytdlp_headers") or {}

    if action == "pick":
        idx = int(parts[2])
        videos = entry["videos"]
        if idx < 0 or idx >= len(videos):
            await callback.answer("Неверный номер.", show_alert=True)
            return
        await callback.answer("⏳ Загружаю видео...")
        await callback.message.edit_text(f"⏳ Проверяю и загружаю видео #{idx + 1}...")
        try:
            sent, vinfo = await _upload_video_from_url(
                callback.bot,
                chat_id,
                videos[idx],
                entry["page_url"],
                saved_cookies=saved_cookies,
                extra_headers=ytdlp_headers.get(videos[idx]),
            )
            item = _item_from_sent_media(sent)
        except Exception as e:
            await callback.message.edit_text(f"❌ Не удалось загрузить видео #{idx + 1}: {e}")
            return
        entry["items"] = [item]
        entry["preview_msg_ids"] = [sent.message_id]
        codec_warn = _codec_warning_text(vinfo)
        await callback.message.edit_text(
            f"✅ <b>Видео готово!</b>{codec_warn}\nЧто делаем дальше?",
            parse_mode=ParseMode.HTML,
            reply_markup=_grab_ready_kb(1),
        )
        return

    if action == "pick_all":
        videos = entry["videos"]
        await callback.answer("⏳ Загружаю все видео...")
        await callback.message.edit_text(f"⏳ Загружаю {len(videos)} видео с сайта...")
        items = []
        msg_ids = []
        failed = 0
        for v_url in videos:
            try:
                sent, _ = await _upload_video_from_url(
                    callback.bot,
                    chat_id,
                    v_url,
                    entry["page_url"],
                    saved_cookies=saved_cookies,
                    extra_headers=ytdlp_headers.get(v_url),
                )
                items.append(_item_from_sent_media(sent))
                msg_ids.append(sent.message_id)
            except Exception as e:
                log.warning("Ошибка загрузки одного из видео (%s): %s", v_url, e)
                failed += 1
            await asyncio.sleep(0.2)

        if not items:
            await callback.message.edit_text("❌ Ни одно видео не удалось загрузить (возможно, сайт отдаёт защищённые или слишком большие файлы).")
            return

        entry["items"] = items
        entry["preview_msg_ids"] = msg_ids
        warn = f" (пропущено: {failed})" if failed else ""
        await callback.message.edit_text(
            f"✅ <b>Загружено видео: {len(items)} шт.{warn}</b>\nЧто делаем дальше?",
            parse_mode=ParseMode.HTML,
            reply_markup=_grab_ready_kb(len(items)),
        )
        return

    if action == "to_batch":
        items = entry.get("items", [])
        if not items:
            await callback.answer("Сначала выбери видео.", show_alert=True)
            return
        await callback.answer("➕ Добавлено в /batch!")
        for mid in entry.get("preview_msg_ids", []):
            await safe_delete(callback.bot, chat_id, mid)
        _grab_cache.pop(chat_id, None)
        await safe_delete(callback.bot, chat_id, callback.message.message_id)

        cur_state = await state.get_state()
        data = await state.get_data()
        media = data.get("media", []) if cur_state == Batch.collecting.state else []
        media.extend(items)
        await state.set_state(Batch.collecting)
        status_id = await update_counter(
            callback.bot, chat_id, data.get("status_msg_id") if cur_state == Batch.collecting.state else None,
            f"Загружено файлов: {len(media)}. Кидай ещё или жми «Это всё».",
            stop_keyboard(len(media)),
        )
        await state.update_data(media=media, status_msg_id=status_id)
        return

    if action == "pub":
        items = entry.get("items", [])
        if not items:
            await callback.answer("Сначала выбери видео.", show_alert=True)
            return
        await callback.answer()
        for mid in entry.get("preview_msg_ids", []):
            await safe_delete(callback.bot, chat_id, mid)
        await safe_delete(callback.bot, chat_id, callback.message.message_id)
        _grab_cache.pop(chat_id, None)

        await state.clear()
        await state.update_data(
            media=items,
            admin_id=callback.from_user.id,
            admin_name=callback.from_user.full_name,
            submission_id=None,
            author_name=None,
            publish_at=None,
            grab_title=entry.get("title"),
            grab_cover_url=entry.get("cover_url"),
            grab_page_url=entry.get("page_url"),
        )
        await state.set_state(Batch.waiting_title)

        kb = None
        if entry.get("title"):
            short_t = entry["title"][:32]
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text=f"📝 Взять: «{short_t}»", callback_data="grab_use:title")
            ]])
        await callback.bot.send_message(
            chat_id,
            f"✅ Принято видео: {len(items)} шт.\nТеперь пришли *название* поста (или нажми кнопку ниже):",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb,
        )


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
        "📥 Пришли контент для канала. Когда закончишь — жми кнопку или /done."
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
    await log_admin_action(message.bot, message.from_user.id, message.from_user.full_name, f"Забанил id{target_id}")
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
# 👑 OWNER PANEL (ТОЛЬКО SUPER_OWNER_ID)
# =============================================================================
async def build_owner_panel() -> Tuple[str, InlineKeyboardMarkup]:
    maint = await is_maintenance_enabled()
    prot = await is_protect_content_enabled()
    zakrep = await get_setting("zakrep_enabled") == "1"
    notify = await is_auto_notify_enabled()
    admlogs = await is_admin_logs_enabled()
    dm_del = await get_dm_autodelete_minutes()
    auto_rst = await get_auto_restart_hours()
    ads_cnt = len(await list_active_ads())
    ram_mb = get_ram_mb()

    uptime_sec = int(time.monotonic() - BOT_START_TIME)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)

    dm_del_str = f"{dm_del} мин" if dm_del > 0 else "ВЫКЛ"
    auto_rst_str = f"каждые {auto_rst}ч" if auto_rst > 0 else "ВЫКЛ"
    ram_line = f"🧠 Память RAM: <code>{ram_mb} / 512 МБ</code>\n" if ram_mb > 0 else ""

    text = (
        "👑 <b>Панель Супер-Владельца</b>\n\n"
        f"⏱ Аптайм: <code>{h}ч {m}м {s}с</code>\n"
        f"{ram_line}"
        f"♻️ Авто-рестарт: <code>{auto_rst_str}</code>\n"
        f"🛠 Техработы: {'✅ ВКЛ' if maint else '❌ ВЫКЛ'}\n"
        f"🛡 Защита контента: {'✅ ВКЛ' if prot else '❌ ВЫКЛ'}\n"
        f"🔒 ОП (.zakrep): {'✅ ВКЛ' if zakrep else '❌ ВЫКЛ'}\n"
        f"🔔 Автоуведомления: {'✅ ВКЛ' if notify else '❌ ВЫКЛ'}\n"
        f"🕵️ Логи админов: {'✅ ВКЛ' if admlogs else '❌ ВЫКЛ'}\n"
        f"⏳ Автоудаление в ЛС: <code>{dm_del_str}</code>\n"
        f"📣 Активной рекламы: <code>{ads_cnt}</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Перезапустить бота", callback_data="own:restart"),
            InlineKeyboardButton(text=f"♻️ Авто-рестарт: {auto_rst_str}", callback_data="own:cycle_autorestart"),
        ],
        [
            InlineKeyboardButton(text=f"🛠 Техработы: {'ВКЛ' if maint else 'ВЫКЛ'}", callback_data="own:toggle_maint"),
            InlineKeyboardButton(text=f"🛡 Защита: {'ВКЛ' if prot else 'ВЫКЛ'}", callback_data="own:toggle_prot"),
        ],
        [
            InlineKeyboardButton(text=f"🔒 ОП: {'ВКЛ' if zakrep else 'ВЫКЛ'}", callback_data="own:toggle_zakrep"),
            InlineKeyboardButton(text=f"🔔 Пуши: {'ВКЛ' if notify else 'ВЫКЛ'}", callback_data="own:toggle_notify"),
        ],
        [
            InlineKeyboardButton(text=f"🕵️ Логи админов: {'ВКЛ' if admlogs else 'ВЫКЛ'}", callback_data="own:toggle_logs"),
            InlineKeyboardButton(text=f"⏳ Удаление в ЛС: {dm_del_str}", callback_data="own:cycle_dmdel"),
        ],
        [
            InlineKeyboardButton(text="📡 Статус и пинг БД", callback_data="own:status"),
            InlineKeyboardButton(text="💾 Бэкап базы (JSON)", callback_data="own:backup"),
        ],
        [
            InlineKeyboardButton(text="📋 Экспорт юзеров (TXT)", callback_data="own:export_users"),
            InlineKeyboardButton(text="👥 Админы (снятие)", callback_data="own:admins"),
        ],
        [
            InlineKeyboardButton(text="🧹 Сбросить FSM", callback_data="own:clear_fsm"),
            InlineKeyboardButton(text="🔄 Обновить меню (/)", callback_data="own:sync_cmds"),
        ],
    ])
    return text, kb


@router.message(Command("owner"))
async def cmd_owner_panel(message: Message):
    if not is_super_owner(message.from_user.id):
        return
    text, kb = await build_owner_panel()
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@router.message(Command("restart"))
async def cmd_restart_bot(message: Message):
    if not is_super_owner(message.from_user.id):
        return
    await message.answer("♻️ Перезапускаю бота... Через 10–15 секунд он снова в строю!")
    await trigger_restart(message.bot, "ручной перезапуск командой /restart", notify_owner=False)

@router.message(Command("update"))
@router.message(F.document & F.caption.startswith("/update"))
async def cmd_update_bot(message: Message):
    if not is_super_owner(message.from_user.id):
        return

    if not GITHUB_TOKEN:
        await message.answer(
            "❌ Не задан <code>GITHUB_TOKEN</code> в переменных Render.\n"
            "Добавь его в Environment Variables на Render и перезапусти бота.",
            parse_mode=ParseMode.HTML,
        )
        return

    doc = message.document or (message.reply_to_message.document if message.reply_to_message else None)
    if not doc or not (doc.file_name or "").endswith(".py"):
        await message.answer(
            "🚀 <b>Обновление бота через Telegram</b>\n\n"
            "Отправь новый <code>.py</code> файл с подписью <code>/update</code> "
            "(или ответь командой <code>/update</code> на уже отправленный <code>.py</code> файл).",
            parse_mode=ParseMode.HTML,
        )
        return

    status = await message.answer("⏳ Скачиваю файл и проверяю синтаксис...")

    buf = io.BytesIO()
    try:
        await message.bot.download(doc, destination=buf)
        raw_bytes = buf.getvalue()
        new_code = raw_bytes.decode("utf-8")
    except Exception as e:
        await status.edit_text(f"❌ Не смог прочитать файл: {e}")
        return

    try:
        compile(new_code, "bot.py", "exec")
    except SyntaxError as e:
        await status.edit_text(f"❌ Ошибка синтаксиса в файле (строка {e.lineno}):\n<code>{e.msg}</code>", parse_mode=ParseMode.HTML)
        return

    await status.edit_text("🚀 Синтаксис ОК! Отправляю обновление на GitHub...")

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/bot.py"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        async with ClientSession(timeout=ClientTimeout(total=30)) as session:
            sha = None
            async with session.get(f"{api_url}?ref={GITHUB_BRANCH}", headers=headers) as resp:
                if resp.status == 200:
                    info = await resp.json()
                    sha = info.get("sha")
                elif resp.status != 404:
                    err_text = await resp.text()
                    await status.edit_text(f"❌ Ошибка GitHub GET ({resp.status}):\n<code>{err_text[:300]}</code>", parse_mode=ParseMode.HTML)
                    return

            payload = {
                "message": f"Update bot.py via Telegram /update ({doc.file_name})",
                "content": base64.b64encode(raw_bytes).decode("ascii"),
                "branch": GITHUB_BRANCH,
            }
            if sha:
                payload["sha"] = sha

            async with session.put(api_url, headers=headers, json=payload) as resp:
                if resp.status in (200, 201):
                    res_data = await resp.json()
                    commit_sha = res_data.get("commit", {}).get("sha", "")[:7]
                    await status.edit_text(
                        f"✅ <b>Обновление залито на GitHub!</b>\n"
                        f"Репозиторий: <code>{GITHUB_REPO}</code> ({GITHUB_BRANCH})\n"
                        f"Коммит: <code>{commit_sha}</code>\n\n"
                        f"⏳ Render сам подхватит новый код и перезапустит бота через 1–2 минуты.",
                        parse_mode=ParseMode.HTML,
                    )
                else:
                    err_text = await resp.text()
                    await status.edit_text(f"❌ Ошибка GitHub PUT ({resp.status}):\n<code>{err_text[:300]}</code>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await status.edit_text(f"❌ Ошибка при отправке на GitHub: {e}")



@router.callback_query(F.data.startswith("own:"))
async def cb_owner_panel(callback: CallbackQuery):
    if not is_super_owner(callback.from_user.id):
        await callback.answer("⛔ Доступ только у Супер-Владельца.", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]

    global _maint_cache
    if action == "restart":
        await callback.answer("♻️ Перезапускаю...")
        await callback.message.answer("♻️ Перезапускаю бота... Через 10–15 секунд он снова в строю!")
        await trigger_restart(callback.bot, "ручной перезапуск из /owner", notify_owner=False)
        return
    elif action == "cycle_autorestart":
        cur = await get_auto_restart_hours()
        nxt = {6: 12, 12: 24, 24: 0, 0: 6}.get(cur, 6)
        await set_setting("auto_restart_hours", str(nxt))
        await callback.answer(f"♻️ Авто-рестарт: каждые {nxt}ч" if nxt else "♻️ Авто-рестарт: ВЫКЛ")
    elif action == "toggle_maint":
        new_val = "0" if await is_maintenance_enabled() else "1"
        await set_setting("maintenance_mode", new_val)
        _maint_cache = (new_val == "1")
        await callback.answer("🛠 Техработы ВКЛ" if new_val == "1" else "✅ Техработы ВЫКЛ")
    elif action == "toggle_prot":
        new_val = "0" if await is_protect_content_enabled() else "1"
        await set_setting("protect_content", new_val)
        await callback.answer("🛡 Защита ВКЛ" if new_val == "1" else "🔓 Защита ВЫКЛ")
    elif action == "toggle_zakrep":
        new_val = "0" if await get_setting("zakrep_enabled") == "1" else "1"
        await set_setting("zakrep_enabled", new_val)
        await callback.answer("🔒 ОП ВКЛ" if new_val == "1" else "🔓 ОП ВЫКЛ")
    elif action == "toggle_notify":
        new_val = "0" if await is_auto_notify_enabled() else "1"
        await set_setting("notify_new_posts", new_val)
        await callback.answer("🔔 Автоуведомления ВКЛ" if new_val == "1" else "🔕 Автоуведомления ВЫКЛ")
    elif action == "status":
        t0 = time.monotonic()
        async with _pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ms = int((time.monotonic() - t0) * 1000)
        users_n = await count_users()
        posts_n = await count_posts()
        views_n = await total_views()
        url = os.environ.get("RENDER_EXTERNAL_URL") or "не задан"
        await callback.answer()
        await callback.message.answer(
            f"📡 <b>Статус системы</b>\n\n"
            f"🗄 Пинг Neon БД: <code>{db_ms} мс</code>\n"
            f"🌐 Render URL: <code>{url}</code>\n"
            f"👥 Пользователей: <code>{users_n}</code>\n"
            f"📦 Постов: <code>{posts_n}</code> (👁 {views_n})",
            parse_mode=ParseMode.HTML,
        )
        return
    elif action == "backup":
        await callback.answer("⏳ Формирую бэкап...")
        data = await export_backup_json()
        fname = f"backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json"
        await callback.message.answer_document(
            BufferedInputFile(data, filename=fname),
            caption="💾 Бэкап базы данных (JSON).",
        )
        return
    elif action == "clear_fsm":
        cnt = await clear_all_fsm()
        await callback.answer(f"🧹 Очищено зависших состояний: {cnt}", show_alert=True)
        return
    elif action == "sync_cmds":
        await setup_bot_commands(callback.bot)
        await callback.answer("🔄 Меню команд (/) обновлено для всех!", show_alert=True)
        return
    elif action == "toggle_logs":
        new_val = "0" if await is_admin_logs_enabled() else "1"
        await set_setting("admin_logs", new_val)
        await callback.answer("🕵️ Логи админов ВКЛ" if new_val == "1" else "🕵️ Логи админов ВЫКЛ")
    elif action == "cycle_dmdel":
        cur = await get_dm_autodelete_minutes()
        nxt = {0: 10, 10: 30, 30: 0}.get(cur, 0)
        await set_setting("dm_autodelete_min", str(nxt))
        await callback.answer(f"⏳ Автоудаление в ЛС: {nxt} мин" if nxt else "⏳ Автоудаление в ЛС: ВЫКЛ")
    elif action == "export_users":
        await callback.answer("⏳ Выгружаю юзеров...")
        data = await export_users_txt()
        fname = f"users_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.txt"
        await callback.message.answer_document(BufferedInputFile(data, filename=fname), caption="📋 Список пользователей бота.")
        return
    elif action == "admins":
        db_admins = await list_admins()
        rows = []
        for a in db_admins:
            uid = a["user_id"]
            if uid == SUPER_OWNER_ID:
                continue
            label = f"❌ Снять id{uid}" + (f" (@{a['username']})" if a.get("username") else "")
            rows.append([InlineKeyboardButton(text=label, callback_data=f"own:rmadm:{uid}")])
        rows.append([InlineKeyboardButton(text="⬅️ Назад в панель", callback_data="own:back")])
        await callback.answer()
        await callback.message.edit_text(
            "👥 <b>Динамические админы:</b>\nНажми, чтобы быстро снять права." if len(rows) > 1 else "Динамических админов нет.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )
        return
    elif action.startswith("rmadm:"):
        del_id = int(action.split(":", 1)[1])
        if del_id != SUPER_OWNER_ID:
            await remove_admin(del_id)
            try:
                await callback.bot.set_my_commands(USER_COMMANDS, scope=BotCommandScopeChat(chat_id=del_id))
            except Exception:
                pass
            await callback.answer(f"✅ Админ id{del_id} снят!")
        text, kb = await build_owner_panel()
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return
    elif action == "back":
        await callback.answer()

    text, kb = await build_owner_panel()
    try:
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except TelegramBadRequest:
        pass


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


async def ads_loop(bot: Bot):
    while True:
        await asyncio.sleep(SCHEDULE_CHECK_INTERVAL)
        try:
            due = await get_due_ads()
        except Exception as e:
            log.warning("Ошибка проверки рекламы: %s", e)
            continue
        for ad in due:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text=ad["btn_text"], url=ad["link"])
            ]])
            try:
                if ad.get("photo_file_id"):
                    await bot.send_photo(ad["channel_id"], ad["photo_file_id"], caption=ad["text"], parse_mode=ParseMode.HTML, reply_markup=kb)
                else:
                    await bot.send_message(ad["channel_id"], ad["text"], parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                log.info("Реклама #%s опубликована в канал %s", ad["id"], ad["channel_id"])
            except TelegramBadRequest as e:
                log.warning("Не смог опубликовать рекламу #%s: %s", ad["id"], e)
            await update_ad_after_run(ad["id"], ad["interval_minutes"])


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


async def watchdog_and_autorestart_loop(bot: Bot):
    """Следит за лагами цикла событий, переполнением RAM и плановым автоперезапуском."""
    high_lag_strikes = 0
    while True:
        t0 = time.monotonic()
        await asyncio.sleep(20)
        lag = (time.monotonic() - t0) - 20.0

        # 1. Детектор зависаний хоста
        if lag > 10.0:
            high_lag_strikes += 1
            log.warning("Обнаружен лаг цикла событий: %.1f сек (удар %s/3)", lag, high_lag_strikes)
            if high_lag_strikes >= 3:
                await trigger_restart(bot, f"зависание хоста (лаг {lag:.1f} сек)", notify_owner=True)
        else:
            high_lag_strikes = 0

        # 2. Защита от переполнения памяти на Render Free (512 МБ)
        ram_mb = get_ram_mb()
        if ram_mb >= 430:
            await trigger_restart(bot, f"высокое потребление памяти ({ram_mb} / 512 МБ)", notify_owner=True)

        # 3. Плановый перезапуск по таймеру из /owner
        try:
            hours = await get_auto_restart_hours()
            if hours > 0 and (time.monotonic() - BOT_START_TIME) >= hours * 3600:
                await trigger_restart(bot, f"плановый перезапуск каждые {hours}ч", notify_owner=True)
        except Exception as e:
            log.warning("Ошибка проверки авто-рестарта: %s", e)


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
        asyncio.create_task(watchdog_and_autorestart_loop(bot))
        asyncio.create_task(auto_delete_loop(bot))
        asyncio.create_task(scheduled_posts_loop(bot))
        asyncio.create_task(ads_loop(bot))
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await storage.close()
        await db_close()


if __name__ == "__main__":
    asyncio.run(main())
