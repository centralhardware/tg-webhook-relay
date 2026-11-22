# tg-webhook-relay

Telegram relay for several bots: webhook receiver, Bot API proxy and request log.

**Inbound.** Receives Telegram webhooks for several bots and forwards each update to that bot's own
endpoint. Updates are spooled on disk first and forwarded in `update_id` order with
retries, so a bot that is down or redeploying loses nothing. Each update gets a log id
(a Snowflake) that travels with it as `X-Log-Update-Id`.

**Outbound.** Bots use the relay as their Bot API URL (`/bot<token>/<method>`, file
downloads `/file/bot<token>/...`). Calls go on to api.telegram.org unchanged. A call that
carries `X-Log-Update-Id` is logged under that id, so a handler's calls join the update
that triggered them; the header is stripped before Telegram sees the call. Only tokens
in the config are proxied.

**Log.** Everything lands in ClickHouse `bot_requests`, with the row shapes
ktgbotapi-clickhouse-logging-middleware used: inbound rows (`MessageUpdate`…, request = Telegram's
update_id) and outbound rows (method, params, response, success/error, duration). `getMe` is
skipped. A proxy-only bot that still long-polls gets its updates logged from the `getUpdates`
results.

## Config (env)

| var | |
|---|---|
| `BOTS_JSON` (or `BOTS_FILE`, default `/data/bots.json`) | `[{"name","token","target"?,"allowed_updates"?}]`. No `target` = proxy only |
| `PUBLIC_URL` | public base URL; webhooks are set to `<PUBLIC_URL>/webhook/<name>` on start |
| `CLICKHOUSE_JDBC_URL` | optional; without it nothing is logged |
| `SNOWFLAKE_MACHINE_ID` | default 1 |
| `SPOOL_DIR` | default `/data/spool` (mount a volume) |

## Forwarded request

The raw update as the JSON body, plus `X-Log-Update-Id`. Targets are on the private
network, so this hop carries no secret (the public Telegram → relay hop is checked
against `X-Telegram-Bot-Api-Secret-Token`). Any standard webhook receiver works as a target. A non-2xx answer is retried (backoff up to 30 s)
and blocks later updates of that bot, which keeps them in order.

`GET /health` reports the queue depth per bot.

## Rollout

Per bot: add it to `BOTS_JSON`, point its API URL at the relay, switch it to the
ktgbotapi-commons webhook receiver (which carries `X-Log-Update-Id` onto every call the
handler makes) and drop the ClickHouse middleware, all in one step.
