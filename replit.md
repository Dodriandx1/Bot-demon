# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.
Also contains a Python Telegram Bot for downloading media from social networks.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Telegram Bot (telegram-bot/)

A Pyrogram-based Telegram bot for downloading media from social networks and file hosts.

### Features
- Download from: YouTube, TikTok, Instagram, Twitter/X, Facebook, Mega.nz, Mediafire, and many more via yt-dlp
- Styled progress bars for: Download → Encoding → Upload phases
- Single message that edits in real-time for each phase
- `/cancel_<task_id>` command to stop any active download
- `/stat` command for live server statistics panel
- `/reset` command to cancel all downloads and free storage
- Queue system: processes downloads one at a time

### Dependencies
- Python 3.11
- pyrogram 2.0.106, tgcrypto, httpx, beautifulsoup4, psutil, yt-dlp
- System: ffmpeg, megatools

### Secrets Required
- `API_ID` — Telegram App API ID from my.telegram.org
- `API_HASH` — Telegram App API Hash from my.telegram.org
- `BOT_TOKEN` — Bot token from @BotFather
- `MEGA_EMAIL` — MEGA.nz account email
- `MEGA_PASSWORD` — MEGA.nz account password

### Workflow
- **Telegram Bot** — `python3 telegram-bot/bot.py`

## Key Commands

- `pnpm run typecheck` — full typecheck across all packages
- `pnpm run build` — typecheck + build all packages
- `pnpm --filter @workspace/api-spec run codegen` — regenerate API hooks and Zod schemas from OpenAPI spec
- `pnpm --filter @workspace/db run push` — push DB schema changes (dev only)
- `pnpm --filter @workspace/api-server run dev` — run API server locally

See the `pnpm-workspace` skill for workspace structure, TypeScript setup, and package details.
