# tg-webhook-relay

Receives Telegram webhooks for several bots and forwards each update to that bot's own
endpoint. Updates are spooled on disk first and forwarded in `update_id` order with
retries, so a bot that is down or redeploying loses nothing. Inbound updates are also
logged to ClickHouse `telegram_bots.bot_requests` (the rows
ktgbotapi-clickhouse-logging-middleware used to write from `getUpdates`).

## Config (env)

| var | |
|---|---|
| `BOTS_JSON` (or `BOTS_FILE`, default `/data/bots.json`) | `[{"name","token","target","allowed_updates"?}]` |
| `PUBLIC_URL` | public base URL; webhooks are set to `<PUBLIC_URL>/webhook/<name>` on start |
| `CLICKHOUSE_JDBC_URL` | optional; without it nothing is logged |
| `SPOOL_DIR` | default `/data/spool` (mount a volume) |

## Forwarded request

Identical to a Telegram webhook call: the raw update as the JSON body, plus
`X-Telegram-Bot-Api-Secret-Token: hex(sha256("tg-webhook-relay:" + token))`. Any standard
webhook receiver works as a target. A non-2xx answer is retried (backoff up to 30 s)
and blocks later updates of that bot, which keeps them in order.

`GET /health` reports the queue depth per bot.

## Rollout

Add a bot to `BOTS_JSON` and switch the bot from long polling to a webhook receiver
in the same step. Once a bot has moved, the middleware stops logging its inbound rows
by itself, since it only logs them from `getUpdates`.
