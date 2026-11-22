package relay

import dev.inmo.tgbotapi.types.chat.User
import dev.inmo.tgbotapi.types.update.*
import dev.inmo.tgbotapi.types.update.abstracts.Update
import dev.inmo.tgbotapi.types.update.abstracts.UpdateDeserializationStrategy
import dev.inmo.tgbotapi.utils.RiskFeature
import dev.inmo.tgbotapi.extensions.utils.extensions.raw.from
import kotlinx.serialization.json.Json
import dev.inmo.micro_utils.common.Warning
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant
import java.util.Properties

/**
 * Inbound half of the bots' request log (`telegram_bots.bot_requests`), written here
 * instead of by ktgbotapi-clickhouse-logging-middleware. Rows match what the middleware
 * wrote from getUpdates results: method = the ktgbotapi Update class name, request =
 * Telegram's update_id, response = the parsed update, update_id = a fresh Snowflake id.
 * Fire-and-forget: a slow or down ClickHouse drops rows, it never delays forwarding.
 */
class ClickHouseLog(private val jdbcUrl: String) {
    private class Row(val ts: Instant, val bot: String, val user: User?, val method: String, val request: String, val response: String)

    private val rows = Channel<Row>(10_000)
    private var conn: Connection? = null
    private val props = Properties().apply {
        setProperty("clickhouse_setting_async_insert", "1")
        setProperty("clickhouse_setting_wait_for_async_insert", "0")
    }

    fun start(scope: CoroutineScope) = scope.launch(Dispatchers.IO) {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
        val batch = ArrayList<Row>(500)
        for (first in rows) {
            batch += first
            while (batch.size < 500) batch += rows.tryReceive().getOrNull() ?: break
            runCatching { insert(batch) }.onFailure {
                log("clickhouse insert of ${batch.size} row(s) failed: $it")
                runCatching { conn?.close() }
                conn = null
            }
            batch.clear()
        }
    }

    fun inbound(bot: String, body: String) {
        val update = try {
            updateJson.decodeFromString(UpdateDeserializationStrategy, body)
        } catch (e: Exception) {
            log("$bot: update not parsed for the log: ${e.message}"); return
        }
        rows.trySend(Row(Instant.now(), bot, update.extractUser(), update::class.simpleName.orEmpty(), update.updateId.toString(), update.toString()))
    }

    private fun insert(batch: List<Row>) {
        val c = conn?.takeIf { !it.isClosed } ?: DriverManager.getConnection(jdbcUrl, props).also { conn = it }
        // update_id comes from ClickHouse's Snowflake generator, as in the middleware.
        c.prepareStatement(
            "INSERT INTO bot_requests (timestamp, bot, update_id, user_id, username, first_name, last_name, " +
                "method, request, response, success, error, duration_ms) " +
                "SELECT ?, ?, generateSnowflakeID(), ?, ?, ?, ?, ?, ?, ?, true, '', 0",
        ).use { ps ->
            for (r in batch) {
                ps.setTimestamp(1, Timestamp.from(r.ts))
                ps.setString(2, r.bot)
                ps.setLong(3, r.user?.id?.chatId?.long ?: 0L)
                ps.setString(4, r.user?.username?.withoutAt.orEmpty())
                ps.setString(5, r.user?.firstName.orEmpty())
                ps.setString(6, r.user?.lastName.orEmpty())
                ps.setString(7, r.method)
                ps.setString(8, r.request)
                ps.setString(9, r.response)
                ps.executeUpdate()
            }
        }
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
