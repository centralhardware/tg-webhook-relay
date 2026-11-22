package relay

import dev.inmo.micro_utils.common.Warning
import dev.inmo.tgbotapi.extensions.utils.extensions.raw.from
import dev.inmo.tgbotapi.types.chat.User
import dev.inmo.tgbotapi.types.update.*
import dev.inmo.tgbotapi.types.update.abstracts.Update
import dev.inmo.tgbotapi.types.update.abstracts.UpdateDeserializationStrategy
import dev.inmo.tgbotapi.utils.RiskFeature
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.json.*
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * The bots' request log, `bot_requests` (ktgbotapi-clickhouse-logging-middleware's
 * table and row shapes):
 * - inbound update: method = ktgbotapi Update class name (MessageUpdate…), request =
 *   Telegram's update_id, response = the update JSON, duration 0, success true.
 * - outbound call: method = Bot API method, request = its params, response = Telegram's
 *   answer, success/error from it, duration measured at the proxy.
 * update_id is the relay's log id; user columns come from the update that id belongs to.
 * Fire-and-forget: a slow or down ClickHouse drops rows, never delays a bot.
 */
class RequestLog(private val jdbcUrl: String) {
    private class Who(val id: Long, val username: String, val first: String, val last: String, val at: Long)
    private class Row(
        val ts: Instant, val bot: String, val logId: Long, val who: Who?, val method: String,
        val request: String, val response: String, val success: Boolean, val error: String, val ms: Long,
    )

    private val rows = Channel<Row>(10_000)
    private val users = ConcurrentHashMap<Long, Who>()
    private var conn: Connection? = null
    private val ids = snowflake(System.getenv("SNOWFLAKE_MACHINE_ID")?.toIntOrNull()?.plus(1) ?: 2)

    fun start(scope: CoroutineScope) {
        scope.launch(Dispatchers.IO) {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
            // One INSERT per row: bot_requests is a Buffer table, it does the batching.
            // The channel only keeps a slow ClickHouse off the bots' path.
            for (row in rows) {
                runCatching { insert(row) }.onFailure {
                    log("clickhouse insert failed: $it")
                    runCatching { conn?.close() }
                    conn = null
                }
            }
        }
        // Handlers rarely run longer than this; past it the user columns stay empty.
        scope.launch {
            while (isActive) {
                delay(60_000)
                val cutoff = System.currentTimeMillis() - 15 * 60_000
                users.entries.removeIf { it.value.at < cutoff }
            }
        }
    }

    fun inbound(bot: String, logId: Long, body: String) {
        val update = try {
            updateJson.decodeFromString(UpdateDeserializationStrategy, body)
        } catch (e: Exception) {
            log("$bot: update not parsed for the log: ${e.message}"); return
        }
        inbound(bot, logId, update, body)
    }

    private fun inbound(bot: String, logId: Long, update: Update, raw: String) {
        val who = update.extractUser()?.let {
            Who(it.id.chatId.long, it.username?.withoutAt.orEmpty(), it.firstName, it.lastName, System.currentTimeMillis())
        }
        who?.let { users[logId] = it }
        offer(Row(Instant.now(), bot, logId, who, update::class.simpleName.orEmpty(), update.updateId.long.toString(), raw, true, "", 0))
    }

    fun outbound(bot: String, logId: Long, method: String, type: ContentType?, body: ByteArray, response: String, httpOk: Boolean, failure: String, ms: Long) {
        // getMe is a health-check heartbeat in most bots; it says nothing about traffic.
        if (method == "getMe") return
        val answer = runCatching { updateJson.parseToJsonElement(response).jsonObject }.getOrNull()
        val ok = failure.isEmpty() && httpOk && answer?.get("ok")?.jsonPrimitive?.booleanOrNull != false
        // A bot still long-polling through the proxy: log the updates, not the poll.
        if (method == "getUpdates") {
            if (ok) answer?.get("result")?.jsonArray?.forEach { u ->
                runCatching { updateJson.decodeFromJsonElement(UpdateDeserializationStrategy, u) }
                    .onSuccess { inbound(bot, ids.next(), it, u.toString()) }
            }
            return
        }
        val error = failure.ifEmpty {
            if (ok) "" else answer?.let { "${it["error_code"]?.jsonPrimitive?.content}: ${it["description"]?.jsonPrimitive?.content}" } ?: "HTTP error"
        }
        offer(Row(Instant.now(), bot, logId, users[logId], method, paramsOf(type, body), response, ok, error, ms))
    }

    private fun offer(r: Row) { rows.trySend(r) }

    private fun insert(r: Row) {
        val c = conn?.takeIf { !it.isClosed } ?: DriverManager.getConnection(jdbcUrl).also { conn = it }
        c.prepareStatement(
            "INSERT INTO bot_requests (timestamp, bot, update_id, user_id, username, first_name, last_name, " +
                "method, request, response, success, error, duration_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ).use { ps ->
            run {
                ps.setTimestamp(1, Timestamp.from(r.ts))
                ps.setString(2, r.bot)
                ps.setLong(3, r.logId)
                ps.setLong(4, r.who?.id ?: 0L)
                ps.setString(5, r.who?.username.orEmpty())
                ps.setString(6, r.who?.first.orEmpty())
                ps.setString(7, r.who?.last.orEmpty())
                ps.setString(8, r.method)
                ps.setString(9, r.request)
                ps.setString(10, r.response)
                ps.setBoolean(11, r.success)
                ps.setString(12, r.error)
                ps.setLong(13, r.ms)
            }
            ps.executeUpdate()
        }
    }
}

/** Request params as JSON text. Multipart file parts become "<file name, N bytes>". */
fun paramsOf(type: ContentType?, body: ByteArray): String {
    if (body.isEmpty()) return "{}"
    return when {
        type == null || type.match(ContentType.Application.Json) -> body.decodeToString()
        type.match(ContentType.Application.FormUrlEncoded) -> buildJsonObject {
            parseQueryString(body.decodeToString()).forEach { k, v -> put(k, v.firstOrNull()) }
        }.toString()
        type.match(ContentType.MultiPart.FormData) -> {
            val boundary = type.parameter("boundary") ?: return "<multipart ${body.size} bytes>"
            // ISO-8859-1 maps bytes 1:1, so offsets survive; text values are re-decoded as UTF-8.
            val text = String(body, Charsets.ISO_8859_1)
            buildJsonObject {
                for (part in text.split("--$boundary")) {
                    val sep = part.indexOf("\r\n\r\n").takeIf { it >= 0 } ?: continue
                    val headers = part.substring(0, sep)
                    val name = Regex("""name="([^"]*)"""").find(headers)?.groupValues?.get(1) ?: continue
                    val value = part.substring(sep + 4).removeSuffix("\r\n")
                    val file = Regex("""filename="([^"]*)"""").find(headers)?.groupValues?.get(1)
                    if (file != null) put(name, "<file $file, ${value.length} bytes>")
                    else put(name, String(value.toByteArray(Charsets.ISO_8859_1), Charsets.UTF_8))
                }
            }.toString()
        }
        else -> "<${type.contentType}/${type.contentSubtype} ${body.size} bytes>"
    }
}

private val updateJson = Json { ignoreUnknownKeys = true; isLenient = true; explicitNulls = false }

@OptIn(Warning::class, RiskFeature::class)
private fun Update.extractUser(): User? = when (this) {
    is MessageUpdate -> data.from
    is EditMessageUpdate -> data.from
    is ChannelPostUpdate -> data.from
    is EditChannelPostUpdate -> data.from
    is InlineQueryUpdate -> data.from
    is CallbackQueryUpdate -> data.from
    is ChosenInlineResultUpdate -> data.from
    is ShippingQueryUpdate -> data.from
    is PreCheckoutQueryUpdate -> data.from
    is BusinessMessageUpdate -> data.from
    is EditBusinessMessageUpdate -> data.from
    is MyChatMemberUpdatedUpdate -> data.user
    is CommonChatMemberUpdatedUpdate -> data.user
    is ChatJoinRequestUpdate -> data.user
    is BusinessConnectionUpdate -> data.user
    is PaidMediaPurchasedUpdate -> data.from
    is PollAnswerUpdate -> data.user
    is ChatMessageReactionUpdatedUpdate -> data.reactedUser
    is ChatBoostUpdatedUpdate -> data.boost.source.user
    is ChatBoostRemovedUpdate -> data.source.user
    is ManagedBotUpdate -> data.user
    else -> null
}
