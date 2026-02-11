# latex-proxy-bot-prod

Production-ready Telegram bot for issuing MTProxy access.

## Structure
- `app/` — Telegraf bot code
- `docker-compose.yml` — runtime container config
- `.env.example` — required env vars template

## Quick start (local)
1. Copy `.env.example` to `.env` and fill values.
2. Run: `docker compose up -d --force-recreate`

## Admin commands
- `/admin` panel
- `/stats`
- `/diag`
- `/turbo`
- `/stable`
- `/safe`

## Notes
- Default fast profile is port `8443`.
- Stable fallback profile is `443`.
