# DISASTER_RECOVERY

## 1) Bot down
```bash
cd /opt/latex-proxy-bot
cp .env.example .env   # if .env lost, then restore real secrets manually
docker compose up -d --force-recreate
```

## 2) Rollback code
```bash
cd /opt/latex-proxy-bot
git log --oneline -n 20
git reset --hard <known-good-commit>
docker compose up -d --force-recreate
```

## 3) Disable auto-sync temporarily
```bash
systemctl stop latex-proxy-bot-sync.timer
systemctl disable latex-proxy-bot-sync.timer
```

## 4) Re-enable auto-sync
```bash
systemctl enable --now latex-proxy-bot-sync.timer
```

## 5) Restore from server backup snapshot
```bash
ls -dt /opt/latex-proxy-bot.pre-git.* | head
rm -rf /opt/latex-proxy-bot
cp -a /opt/latex-proxy-bot.pre-git.<timestamp> /opt/latex-proxy-bot
cd /opt/latex-proxy-bot && docker compose up -d --force-recreate
```
