# PROD_CHECKLIST

## Daily
- [ ] `docker ps` shows `latex-proxy-bot-bot-1` Up
- [ ] `docker ps` shows `latex-mtproxy-official` Up
- [ ] `docker ps` shows `latex-mtproxy-8443` Up
- [ ] Bot `/admin` responds for admin
- [ ] Test proxy link (turbo 8443)

## Weekly
- [ ] `systemctl status latex-proxy-bot-sync.timer`
- [ ] `systemctl status latex-proxy-bot-sync.service`
- [ ] `journalctl -u latex-proxy-bot-sync.service -n 100`
- [ ] rotate backups in `/opt/latex-proxy-bot.pre-git.*`

## On incidents
- [ ] Check bot logs: `docker compose logs --tail=200 bot`
- [ ] Check token validity (`getMe`)
- [ ] Check for duplicate pollers (409)
