# AI Grid Indonesia — Unified Bot

Investor portal + channel broadcaster for the AI Grid Indonesia 50MW Tier-IV Batam Compute Hub.

## What It Does

**Investor portal** (DMs)
- Tier browsing and allocation
- Crypto payment via NOWPayments
- Registration with Investor ID + PIN
- Portfolio login and PIN recovery
- Deploy more capital anytime

**Channel broadcaster** (`@xUniverseUpdates`)
- Automated scheduled posts (8 AM UTC, 6 PM UTC, Sunday 6 PM UTC)
- Live RSS feed integration (Tesla, SpaceX, AI news)
- Curated news dataset (20+ items)
- Allocator feedback pool (130+ entries)
- Engagement polls
- Manual admin `/post` command

## Stack

- Python 3.12
- FastAPI + Uvicorn
- python-telegram-bot 21.x
- PostgreSQL + SQLAlchemy (async) + asyncpg
- NOWPayments API
- APScheduler
- feedparser

## Environment Variables

| Name | Purpose |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Bot authentication |
| `TELEGRAM_CHANNEL_ID` | Target channel for broadcasts |
| `TELEGRAM_ADMIN_IDS` | Comma-separated admin user IDs |
| `NETLIFY_URL` | Public website URL |
| `NOWPAYMENTS_API_KEY` | Payment gateway |
| `NOWPAYMENTS_IPN_SECRET` | Webhook signature verification |
| `DATABASE_URL` | PostgreSQL connection string |

## Admin Commands

- `/post <text>` — broadcast to channel
- `/postmedia <caption>` — reply to photo/video to broadcast it
- `/quiet` — pause auto-scheduler
- `/resume` — resume auto-scheduler
- `/chanstat` — view scheduler status

## Investor Commands

- `/start` — main menu
- `/register` — register allocation after payment
- `/login` — access portfolio with ID + PIN
- `/recover` — recover PIN via email/phone
- `/founder` — founder bio
- `/risk` — risk disclosure
- `/status` — project milestones

## Deployment

Railway. Push to `main` → auto-deploy.
