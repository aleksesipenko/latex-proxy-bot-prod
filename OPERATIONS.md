# OPERATIONS (Swiss-Style Runbook)

## Health checks
- `docker compose ps`
- `docker compose logs --tail=200 bot`

## Known failure signatures
- `409 Conflict` -> another bot poller with same token
- `401 Unauthorized` -> invalid/revoked bot token

## Proxy routing mode
- Turbo: `:8443`
- Stable: `:443`

## Incident playbook
1. Validate bot token via `getMe`.
2. Ensure single poller instance.
3. Validate MTProxy container `Up` and links work.
4. If media slow: compare 8443 vs 443.
