import asyncio
import email
import html
import imaplib
import logging
import os
import re
import sqlite3
import ssl
from contextlib import closing
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.message import Message
from email.utils import parseaddr, parsedate_to_datetime
from io import BytesIO
from pathlib import Path
from typing import Iterable
from typing import Iterator

from telegram import Update
from telegram.constants import ChatType, ParseMode
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("gmail-telegram-forwarder")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)

def load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "bot_data.sqlite3"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
MAX_FETCH_PER_POLL = int(os.getenv("MAX_FETCH_PER_POLL", "25"))
MAX_PARALLEL_GMAIL_CHECKS = int(os.getenv("MAX_PARALLEL_GMAIL_CHECKS", "5"))
IMAP_TIMEOUT_SECONDS = int(os.getenv("IMAP_TIMEOUT_SECONDS", "20"))
MAX_MESSAGE_CHARS = 3300
MAX_ATTACHMENT_BYTES = 49 * 1024 * 1024
CONNECT_EMAIL, CONNECT_PASSWORD = range(2)
GMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@gmail\.com$", re.IGNORECASE)


@dataclass(frozen=True)
class Connection:
    id: int
    chat_id: int
    chat_title: str
    gmail_address: str
    app_password: str
    last_seen_uid: int
    enabled: bool


def init_db() -> None:
    with closing(sqlite3.connect(DATABASE_PATH, timeout=30)) as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL UNIQUE,
                chat_title TEXT NOT NULL,
                gmail_address TEXT NOT NULL,
                app_password TEXT NOT NULL,
                last_seen_uid INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS forwarded_emails (
                connection_id INTEGER NOT NULL,
                uid INTEGER NOT NULL,
                message_id TEXT,
                forwarded_at TEXT NOT NULL,
                PRIMARY KEY (connection_id, uid)
            )
            """
        )
        db.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_connection(chat_id: int, chat_title: str, gmail_address: str, app_password: str, last_seen_uid: int) -> None:
    with closing(sqlite3.connect(DATABASE_PATH, timeout=30)) as db:
        db.execute(
            """
            INSERT INTO connections (
                chat_id, chat_title, gmail_address, app_password, last_seen_uid,
                enabled, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                gmail_address = excluded.gmail_address,
                app_password = excluded.app_password,
                last_seen_uid = excluded.last_seen_uid,
                enabled = 1,
                updated_at = excluded.updated_at
            """,
            (chat_id, chat_title, gmail_address, app_password, last_seen_uid, now_iso(), now_iso()),
        )
        db.commit()


def disable_connection(chat_id: int) -> bool:
    with closing(sqlite3.connect(DATABASE_PATH, timeout=30)) as db:
        cursor = db.execute(
            "UPDATE connections SET enabled = 0, updated_at = ? WHERE chat_id = ? AND enabled = 1",
            (now_iso(), chat_id),
        )
        db.commit()
        return cursor.rowcount > 0


def get_connection_by_chat(chat_id: int) -> Connection | None:
    with closing(sqlite3.connect(DATABASE_PATH, timeout=30)) as db:
        row = db.execute(
            """
            SELECT id, chat_id, chat_title, gmail_address, app_password, last_seen_uid, enabled
            FROM connections WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()
    return Connection(*row) if row else None


def get_enabled_connections() -> list[Connection]:
    with closing(sqlite3.connect(DATABASE_PATH, timeout=30)) as db:
        rows = db.execute(
            """
            SELECT id, chat_id, chat_title, gmail_address, app_password, last_seen_uid, enabled
            FROM connections WHERE enabled = 1
            """
        ).fetchall()
    return [Connection(*row) for row in rows]


def mark_forwarded(connection_id: int, uid: int, message_id: str | None) -> None:
    with closing(sqlite3.connect(DATABASE_PATH, timeout=30)) as db:
        db.execute(
            """
            INSERT OR IGNORE INTO forwarded_emails (connection_id, uid, message_id, forwarded_at)
            VALUES (?, ?, ?, ?)
            """,
            (connection_id, uid, message_id, now_iso()),
        )
        db.execute(
            "UPDATE connections SET last_seen_uid = MAX(last_seen_uid, ?), updated_at = ? WHERE id = ?",
            (uid, now_iso(), connection_id),
        )
        db.commit()


def was_forwarded(connection_id: int, uid: int) -> bool:
    with closing(sqlite3.connect(DATABASE_PATH, timeout=30)) as db:
        row = db.execute(
            "SELECT 1 FROM forwarded_emails WHERE connection_id = ? AND uid = ?",
            (connection_id, uid),
        ).fetchone()
    return row is not None


def forwarded_uids(connection_id: int, uids: list[int]) -> set[int]:
    if not uids:
        return set()
    placeholders = ",".join("?" for _ in uids)
    with closing(sqlite3.connect(DATABASE_PATH, timeout=30)) as db:
        rows = db.execute(
            f"""
            SELECT uid FROM forwarded_emails
            WHERE connection_id = ? AND uid IN ({placeholders})
            """,
            [connection_id, *uids],
        ).fetchall()
    return {int(row[0]) for row in rows}


def decode_mime(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def clean_text(value: str) -> str:
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in value.splitlines()]
    return "\n".join(lines).strip()


def message_body(msg: Message) -> str:
    plain_parts: list[str] = []
    html_parts: list[str] = []

    if msg.is_multipart():
        parts = msg.walk()
    else:
        parts = [msg]

    for part in parts:
        if part.get_content_maintype() == "multipart":
            continue
        if part.get_filename():
            continue

        content_type = part.get_content_type()
        payload = part.get_payload(decode=True)
        if not payload:
            continue

        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, errors="replace")
        except LookupError:
            text = payload.decode("utf-8", errors="replace")

        if content_type == "text/plain":
            plain_parts.append(text)
        elif content_type == "text/html":
            html_parts.append(html_to_text(text))

    body = "\n\n".join(plain_parts or html_parts)
    return clean_text(body)


def html_to_text(value: str) -> str:
    text = value
    for tag in ("br", "p", "div", "tr"):
        text = text.replace(f"<{tag}>", "\n").replace(f"<{tag}/>", "\n").replace(f"<{tag} />", "\n")
    in_tag = False
    output: list[str] = []
    for char in text:
        if char == "<":
            in_tag = True
        elif char == ">":
            in_tag = False
        elif not in_tag:
            output.append(char)
    return html.unescape("".join(output))


def email_date(msg: Message) -> str:
    raw_date = msg.get("Date")
    if not raw_date:
        return "Unknown"
    try:
        parsed = parsedate_to_datetime(raw_date)
        return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return raw_date


def email_summary(msg: Message) -> str:
    from_name, from_email = parseaddr(decode_mime(msg.get("From")))
    subject = decode_mime(msg.get("Subject")) or "(No subject)"
    body = message_body(msg) or "(No message body)"
    if len(body) > MAX_MESSAGE_CHARS:
        body = body[:MAX_MESSAGE_CHARS].rstrip() + "\n\n..."

    return (
        f"<b>New Email</b>\n\n"
        f"<b>Sender Name:</b> {html.escape(from_name or 'Unknown')}\n"
        f"<b>Sender Email:</b> {html.escape(from_email or 'Unknown')}\n"
        f"<b>Subject:</b> {html.escape(subject)}\n"
        f"<b>Date & Time:</b> {html.escape(email_date(msg))}\n\n"
        f"<b>Email Message:</b>\n{html.escape(body)}"
    )


def iter_attachments(msg: Message) -> Iterable[tuple[str, bytes]]:
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        filename = decode_mime(part.get_filename())
        if not filename:
            continue
        payload = part.get_payload(decode=True)
        if payload:
            yield filename, payload


def connect_imap(connection: Connection) -> imaplib.IMAP4_SSL:
    context = ssl.create_default_context()
    imap = imaplib.IMAP4_SSL("imap.gmail.com", 993, ssl_context=context, timeout=IMAP_TIMEOUT_SECONDS)
    imap.login(connection.gmail_address, connection.app_password)
    imap.select("INBOX")
    return imap


@contextmanager
def imap_session(connection: Connection) -> Iterator[imaplib.IMAP4_SSL]:
    imap = connect_imap(connection)
    try:
        yield imap
    finally:
        try:
            imap.close()
        except imaplib.IMAP4.error:
            pass
        imap.logout()


def current_highest_uid(connection: Connection) -> int:
    with imap_session(connection) as imap:
        status, data = imap.uid("search", None, "ALL")
        if status != "OK" or not data or not data[0]:
            return 0
        return max(int(uid_bytes) for uid_bytes in data[0].split())


def fetch_new_messages(connection: Connection) -> list[tuple[int, Message]]:
    messages: list[tuple[int, Message]] = []
    with imap_session(connection) as imap:
        search_from = connection.last_seen_uid + 1
        status, data = imap.uid("search", None, f"UID {search_from}:*")
        if status != "OK" or not data or not data[0]:
            return []

        candidate_uids = sorted(
            int(uid_bytes)
            for uid_bytes in data[0].split()
            if int(uid_bytes) > connection.last_seen_uid
        )
        skipped_uids = forwarded_uids(connection.id, candidate_uids)
        for uid in candidate_uids[:MAX_FETCH_PER_POLL]:
            if uid in skipped_uids:
                continue
            status, payload = imap.uid("fetch", str(uid), "(RFC822)")
            if status != "OK" or not payload:
                continue
            raw = next((item[1] for item in payload if isinstance(item, tuple)), None)
            if not raw:
                continue
            messages.append((uid, email.message_from_bytes(raw)))

    return messages


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "Hi. Add me as an admin in a Telegram group, then run /connect in that group."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "Commands:\n"
        "/connect - guided Gmail setup\n"
        "/connect gmail app_password - quick setup\n"
        "/cancel - cancel guided setup\n"
        "/disconnect - stop forwarding for this group\n"
        "/status - show current link status\n"
        "/help - show this help"
    )


async def connect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat = update.effective_chat
    message = update.effective_message
    if not chat or chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        await message.reply_text("Use /connect inside the Telegram group you want emails forwarded to.")
        return ConversationHandler.END
    if not await is_group_admin(update, context):
        await message.reply_text("Only group admins can connect Gmail forwarding.")
        return ConversationHandler.END

    if len(context.args) >= 2:
        gmail_address = context.args[0].strip()
        app_password = " ".join(context.args[1:]).replace(" ", "").strip()
        await delete_sensitive_message(message)
        await finish_gmail_connect(update, context, gmail_address, app_password)
        return ConversationHandler.END

    context.user_data["connect_chat_id"] = chat.id
    context.user_data["connect_chat_title"] = chat.title or str(chat.id)
    await message.reply_text(
        "Gmail setup started.\n\n"
        "Step 1 of 2: send the Gmail address to connect.\n"
        "Example: yourname@gmail.com\n\n"
        "Send /cancel anytime to stop."
    )
    return CONNECT_EMAIL


async def connect_email_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    gmail_address = (message.text or "").strip()
    if not GMAIL_RE.match(gmail_address):
        await message.reply_text("Please send a valid Gmail address, like yourname@gmail.com. Send /cancel to stop.")
        return CONNECT_EMAIL

    context.user_data["connect_gmail_address"] = gmail_address
    await message.reply_text(
        "Step 2 of 2: send the Gmail App Password.\n\n"
        "Use the 16-character Google App Password, not your normal Gmail password. Spaces are okay; I will remove them."
    )
    return CONNECT_PASSWORD


async def connect_password_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    chat = update.effective_chat
    if not chat:
        return ConversationHandler.END

    gmail_address = context.user_data.get("connect_gmail_address")
    app_password = (message.text or "").replace(" ", "").strip()
    await delete_sensitive_message(message)

    if not gmail_address:
        await message.reply_text("Setup expired. Run /connect again.")
        clear_connect_state(context)
        return ConversationHandler.END
    if len(app_password) < 12:
        await context.bot.send_message(chat_id=chat.id, text="That App Password looks too short. Send it again, or /cancel.")
        return CONNECT_PASSWORD

    await finish_gmail_connect(update, context, gmail_address, app_password)
    clear_connect_state(context)
    return ConversationHandler.END


async def finish_gmail_connect(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    gmail_address: str,
    app_password: str,
) -> bool:
    chat = update.effective_chat
    if not chat:
        return False
    if not GMAIL_RE.match(gmail_address):
        await context.bot.send_message(chat_id=chat.id, text="Please use a valid @gmail.com address.")
        return False

    status_message = await context.bot.send_message(chat_id=chat.id, text="Checking Gmail login and IMAP access...")
    test_connection = Connection(0, chat.id, chat.title or str(chat.id), gmail_address, app_password, 0, True)
    try:
        last_seen_uid = await asyncio.to_thread(current_highest_uid, test_connection)
    except Exception as exc:
        logger.warning("Gmail login failed for %s: %s", gmail_address, exc)
        await status_message.edit_text(
            "Gmail connection failed.\n\n"
            "Check these three things:\n"
            "1. The address is a Gmail account.\n"
            "2. IMAP is enabled in Gmail settings.\n"
            "3. You used a Google App Password, not the normal Gmail password.\n\n"
            "Run /connect to try again."
        )
        return False

    upsert_connection(chat.id, chat.title or str(chat.id), gmail_address, app_password, last_seen_uid)
    await status_message.edit_text(
        f"Connected to {gmail_address}.\n\n"
        "Only new emails received after this setup will be forwarded here."
    )
    return True


async def cancel_connect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    clear_connect_state(context)
    await update.effective_message.reply_text("Gmail setup cancelled.")
    return ConversationHandler.END


def clear_connect_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in ("connect_chat_id", "connect_chat_title", "connect_gmail_address"):
        context.user_data.pop(key, None)


async def delete_sensitive_message(message) -> None:
    try:
        await message.delete()
    except TelegramError:
        logger.info("Could not delete sensitive setup message in chat %s", message.chat_id)


async def disconnect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat:
        return
    if chat.type in {ChatType.GROUP, ChatType.SUPERGROUP} and not await is_group_admin(update, context):
        await update.effective_message.reply_text("Only group admins can disconnect Gmail forwarding.")
        return
    if disable_connection(chat.id):
        await update.effective_message.reply_text("Disconnected. Emails will no longer be forwarded to this group.")
    else:
        await update.effective_message.reply_text("This chat is not currently connected.")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat:
        return
    connection = get_connection_by_chat(chat.id)
    if not connection or not connection.enabled:
        await update.effective_message.reply_text("Not connected.")
        return
    await update.effective_message.reply_text(
        f"Connected to {connection.gmail_address}\n"
        f"Last forwarded Gmail UID: {connection.last_seen_uid}"
    )


async def is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return False
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
    except TelegramError:
        return False
    return member.status in {"administrator", "creator"}


async def monitor_inboxes(application: Application) -> None:
    semaphore = asyncio.Semaphore(MAX_PARALLEL_GMAIL_CHECKS)
    logger.info(
        "Inbox monitor started: interval=%ss, parallel_checks=%s, max_fetch_per_poll=%s",
        POLL_INTERVAL_SECONDS,
        MAX_PARALLEL_GMAIL_CHECKS,
        MAX_FETCH_PER_POLL,
    )
    while True:
        connections = get_enabled_connections()
        await asyncio.gather(
            *(process_connection(application, connection, semaphore) for connection in connections),
            return_exceptions=True,
        )
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def process_connection(application: Application, connection: Connection, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        try:
            messages = await asyncio.to_thread(fetch_new_messages, connection)
            for uid, msg in messages:
                await forward_email(application, connection, uid, msg)
        except Exception as exc:
            logger.warning("Polling failed for chat %s / %s: %s", connection.chat_id, connection.gmail_address, exc)


async def forward_email(application: Application, connection: Connection, uid: int, msg: Message) -> None:
    await application.bot.send_message(
        chat_id=connection.chat_id,
        text=email_summary(msg),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

    attachment_errors: list[str] = []
    for filename, payload in iter_attachments(msg):
        if len(payload) > MAX_ATTACHMENT_BYTES:
            attachment_errors.append(f"{filename} is too large for Telegram.")
            continue
        document = BytesIO(payload)
        document.name = filename
        try:
            await application.bot.send_document(chat_id=connection.chat_id, document=document, filename=filename)
        except TelegramError as exc:
            logger.warning("Attachment send failed for chat %s / UID %s / %s: %s", connection.chat_id, uid, filename, exc)
            attachment_errors.append(f"{filename} could not be sent.")

    if attachment_errors:
        await application.bot.send_message(
            chat_id=connection.chat_id,
            text="Attachment notice:\n" + "\n".join(f"- {error}" for error in attachment_errors),
        )

    mark_forwarded(connection.id, uid, msg.get("Message-ID"))


async def post_init(application: Application) -> None:
    application.bot_data["monitor_task"] = asyncio.create_task(monitor_inboxes(application))


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN before starting the bot.")

    init_db()
    application = Application.builder().token(token).post_init(post_init).build()
    connect_conversation = ConversationHandler(
        entry_points=[CommandHandler("connect", connect_command)],
        states={
            CONNECT_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, connect_email_step)],
            CONNECT_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, connect_password_step)],
        },
        fallbacks=[CommandHandler("cancel", cancel_connect)],
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(connect_conversation)
    application.add_handler(CommandHandler("disconnect", disconnect_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cancel", cancel_connect))
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
