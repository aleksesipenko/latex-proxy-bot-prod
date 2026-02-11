# DEPLOYMENT

## Prod host
- `45.140.146.233:2222`
- path: `/opt/latex-proxy-bot`

## Manual deploy
1. Sync repository snapshot to server
2. Ensure `.env` exists (never commit secrets)
3. `docker compose up -d --force-recreate`

## Auto-pull design
- systemd oneshot service: `latex-proxy-bot-sync.service`
- systemd timer: `latex-proxy-bot-sync.timer` (e.g. every 5 min)
- script pulls from private repo and recreates container on change
