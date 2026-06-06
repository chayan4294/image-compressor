# Gmail to Telegram Forward Bot

A lightweight Python bot that links a Gmail inbox to a Telegram group and forwards new emails, including attachments.

## Features

- `/start`, `/connect`, `/disconnect`, `/status`, `/help`
- Supports multiple Telegram groups and Gmail accounts
- Stores linked Gmail/group information in SQLite
- Polls Gmail through IMAP using Gmail address and App Password
- Prevents forwarding the same email twice
- Auto-recovers from temporary Gmail/Telegram connection failures
- Forwards sender name, sender email, subject, date/time, message text, and attachments

## Setup

1. Create a Telegram bot using `@BotFather`.
2. Enable Gmail IMAP in Gmail settings.
3. Create a Gmail App Password:
   - Google Account
   - Security
   - 2-Step Verification
   - App passwords
4. Install dependencies:

```powershell
py -3 -m pip install -r requirements.txt
```

5. Copy `.env.example` to `.env` and set your bot token, or export environment variables manually.

```powershell
$env:TELEGRAM_BOT_TOKEN="your_bot_token"
py -3 bot.py
```

## Telegram Usage

Add the bot as an admin in a Telegram group, then run:

```text
/connect
```

The bot will ask for the Gmail address first, then the Gmail App Password. The password message is deleted when the bot has permission.

Quick setup is still supported:

```text
/connect your@gmail.com your_app_password
```

Cancel guided setup:

```text
/cancel
```

Check the connection:

```text
/status
```

Stop forwarding:

```text
/disconnect
```

Important: `/connect` contains a Gmail App Password. Use it only in a private/admin-only setup. The bot tries to delete the command message when it has permission.

## Run 24/7

Run on a VPS, PC, or cloud server with a process manager such as `systemd`, `pm2`, Docker, or a hosting platform that supports long-running Python processes.

## Speed Settings

The bot checks Gmail every 10 seconds by default.

```text
POLL_INTERVAL_SECONDS=10
MAX_FETCH_PER_POLL=25
MAX_PARALLEL_GMAIL_CHECKS=5
IMAP_TIMEOUT_SECONDS=20
```

Lower `POLL_INTERVAL_SECONDS` for faster forwarding. Keep it at `10` or higher for normal Gmail usage.
