package relay

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO as ClientCIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.content.ByteArrayContent
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import kotlin.system.exitProcess

/**
 * Telegram relay for several bots: webhook receiver + Bot API proxy + request log.
 *
 * Inbound: Telegram POSTs each update to /webhook/<bot>. The relay gives it a log id
 * (a Snowflake), logs it, writes it to a per-bot spool on disk and answers 200. A
 * worker per bot forwards the spool to that bot's target in update order, retrying
 * while the target is down. The forwarded request looks like a Telegram webhook call
 * plus X-Log-Update-Id: <log id>.
 *
 * Outbound: bots use the relay as their Bot API URL (/bot<token>/<method>). Each call
 * is forwarded to api.telegram.org and logged with its duration and result. A call that
 * carries X-Log-Update-Id is logged under that id (so a handler's calls join the update
 * that triggered them); the header is stripped before Telegram sees the call.
 *
 * Only tokens listed in the config are proxied. The webhook secret is derived from the
 * token: hex(sha256("tg-webhook-relay:" + token)).
 */

@Serializable
data class Bot(
    val name: String,
    val token: String,
    /** Webhook forward target. Null = proxy only (the bot still long-polls, through the relay). */
    val target: String? = null,
    /** Passed to setWebhook. Null keeps Telegram's default set (no reactions etc.). */
    val allowed_updates: List<String>? = null,
)

const val LOG_ID_HEADER = "X-Log-Update-Id"
private const val TELEGRAM = "https://api.telegram.org"
private val json = Json { ignoreUnknownKeys = true }

fun log(msg: String) = System.err.println("[relay] $msg")

fun secretFor(token: String): String =
    MessageDigest.getInstance("SHA-256").digest("tg-webhook-relay:$token".toByteArray())
        .joinToString("") { "%02x".format(it) }

private val NAME_RE = Regex("^[A-Za-z0-9_-]{1,64}$")

private fun env(k: String): String? = System.getenv(k)?.trim()?.ifBlank { null }

fun loadBots(): List<Bot> {
    val raw = env("BOTS_JSON") ?: File(env("BOTS_FILE") ?: "/data/bots.json").takeIf { it.exists() }?.readText()
    ?: run { log("no bots configured: set BOTS_JSON or BOTS_FILE"); exitProcess(1) }
    val bots = json.decodeFromString<List<Bot>>(raw)
    bots.forEach { require(NAME_RE.matches(it.name)) { "bad bot name: ${it.name}" } }
    require(bots.map { it.name }.toSet().size == bots.size) { "duplicate bot names" }
    return bots
}

class Spool(root: File, val bot: Bot, private val target: String) {
    val dir = File(root, bot.name).apply { mkdirs() }
    private val wake = Channel<Unit>(Channel.CONFLATED)

    /** File name: <update_id>-<nanos>-<log id>.json — sorts in update order, carries the log id. */
    fun put(body: String, updateId: Long?, logId: Long) {
        val base = "%020d-%d-%d".format(updateId ?: System.currentTimeMillis(), System.nanoTime(), logId)
        val tmp = File(dir, "$base.tmp")
        tmp.writeText(body)
        Files.move(tmp.toPath(), File(dir, "$base.json").toPath(), StandardCopyOption.ATOMIC_MOVE)
        wake.trySend(Unit)
    }

    fun pending(): List<File> = dir.listFiles { f -> f.name.endsWith(".json") }?.sortedBy { it.name }.orEmpty()

    suspend fun forward(http: HttpClient) {
        val secret = secretFor(bot.token)
        var failures = 0
        while (currentCoroutineContext().isActive) {
            val files = pending()
            if (files.isEmpty()) {
                withTimeoutOrNull(5_000) { wake.receive() }
                continue
            }
            for (f in files) {
                val logId = f.name.removeSuffix(".json").split('-').getOrNull(2)
                val ok = try {
                    val r = http.post(target) {
                        contentType(ContentType.Application.Json)
                        header("X-Telegram-Bot-Api-Secret-Token", secret)
                        logId?.let { header(LOG_ID_HEADER, it) }
                        setBody(f.readText())
                    }
                    r.status.isSuccess().also { if (!it) log("${bot.name}: target answered ${r.status} for ${f.name}") }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    if (failures == 0) log("${bot.name}: target unreachable: ${e.message}")
                    false
                }
                if (!ok) {
                    failures++
                    delay(minOf(1000L * failures, 30_000L))
                    break // retry the same file, keep order
                }
                if (failures > 0) log("${bot.name}: target back after $failures failed attempts")
                failures = 0
                f.delete()
            }
        }
    }
}

suspend fun setWebhook(http: HttpClient, bot: Bot, publicUrl: String) {
    val body = buildJsonObject {
        put("url", "$publicUrl/webhook/${bot.name}")
        put("secret_token", secretFor(bot.token))
        put("max_connections", 40)
        bot.allowed_updates?.let { putJsonArray("allowed_updates") { it.forEach { u -> add(u) } } }
    }
    repeat(10) { attempt ->
        try {
            val r = http.post("$TELEGRAM/bot${bot.token}/setWebhook") {
                contentType(ContentType.Application.Json)
                setBody(body.toString())
            }
            if (r.status.isSuccess()) { log("${bot.name}: webhook set"); return }
            log("${bot.name}: setWebhook failed: ${r.bodyAsText()}")
        } catch (e: Exception) {
            log("${bot.name}: setWebhook error: ${e.message}")
        }
        delay(2000L * (attempt + 1))
    }
}

/** Hop-by-hop and per-connection headers that must not be copied across the proxy. */
private val SKIP_HEADERS = setOf(
    HttpHeaders.Host, HttpHeaders.ContentLength, HttpHeaders.ContentType, HttpHeaders.TransferEncoding,
    HttpHeaders.Connection, HttpHeaders.AcceptEncoding, "Keep-Alive", LOG_ID_HEADER,
).map { it.lowercase() }.toSet()

fun main() {
    val bots = loadBots()
    val publicUrl = env("PUBLIC_URL")?.trimEnd('/')
    if (publicUrl == null && bots.any { it.target != null }) { log("PUBLIC_URL is required for webhook bots"); exitProcess(1) }
    val root = File(env("SPOOL_DIR") ?: "/data/spool")
    val byToken = bots.associateBy { it.token }
    val spools = bots.filter { it.target != null }.associate { it.name to Spool(root, it, it.target!!) }
    val http = HttpClient(ClientCIO) {
        // getUpdates long-polls through the proxy; leave room for its timeout.
        install(HttpTimeout) { requestTimeoutMillis = 120_000; connectTimeoutMillis = 5_000 }
        expectSuccess = false
        followRedirects = false
    }
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val chLog = env("CLICKHOUSE_JDBC_URL")?.let { RequestLog(it).also { l -> l.start(scope) } }
        ?: null.also { log("CLICKHOUSE_JDBC_URL not set — nothing is logged") }
    val ids = Snowflake(env("SNOWFLAKE_MACHINE_ID")?.toIntOrNull() ?: 1)
    for (s in spools.values) {
        scope.launch { setWebhook(http, s.bot, publicUrl!!) }
        scope.launch { s.forward(http) }
    }
    log("bots: ${bots.joinToString { "${it.name}${it.target?.let { t -> " -> $t" } ?: " (proxy only)"}" }}")

    embeddedServer(CIO, port = env("PORT")?.toIntOrNull() ?: 8080, host = "0.0.0.0") {
        routing {
            post("/webhook/{bot}") {
                val s = spools[call.parameters["bot"]]
                if (s == null) { call.respond(HttpStatusCode.NotFound); return@post }
                if (call.request.header("X-Telegram-Bot-Api-Secret-Token") != secretFor(s.bot.token)) {
                    call.respond(HttpStatusCode.Unauthorized); return@post
                }
                val body = call.receiveText()
                val updateId = runCatching { json.parseToJsonElement(body).jsonObject["update_id"]!!.jsonPrimitive.long }.getOrNull()
                val logId = ids.next()
                // Answer 200 only once the update is on disk; a failed write makes Telegram retry.
                s.put(body, updateId, logId)
                call.respond(HttpStatusCode.OK)
                chLog?.inbound(s.bot.name, logId, body)
            }

            // Bot API proxy: /bot<token>/<method> and file downloads /file/bot<token>/<path>.
            route("/{segment}/{method...}") {
                handle {
                    val segment = call.parameters["segment"]!!
                    val isFile = segment == "file"
                    val token = (if (isFile) call.parameters.getAll("method")?.firstOrNull() else segment)
                        ?.takeIf { it.startsWith("bot") }?.removePrefix("bot")
                    val bot = token?.let { byToken[it] }
                    if (bot == null) { call.respond(HttpStatusCode.NotFound); return@handle }
                    val path = call.request.uri // already /bot<token>/... or /file/bot<token>/...
                    if (System.getenv("RELAY_DEBUG") != null) log("proxy ${call.request.httpMethod.value} ${path.replace(bot.token, "<t>")}")
                    val method = if (isFile) "getFile:download" else call.parameters.getAll("method")!!.joinToString("/")
                    val logId = call.request.header(LOG_ID_HEADER)?.toLongOrNull() ?: 0L
                    val reqType = call.request.contentType()
                    val reqBody = call.receive<ByteArray>()

                    val started = System.nanoTime()
                    val resp = try {
                        http.request("$TELEGRAM$path") {
                            this.method = call.request.httpMethod
                            call.request.headers.forEach { k, v -> if (k.lowercase() !in SKIP_HEADERS) v.forEach { header(k, it) } }
                            if (reqBody.isNotEmpty()) setBody(ByteArrayContent(reqBody, reqType))
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        val ms = (System.nanoTime() - started) / 1_000_000
                        if (!isFile) chLog?.outbound(bot.name, logId, method, reqType, reqBody, "", false, e.toString(), ms)
                        call.respond(HttpStatusCode.BadGateway, "relay: ${e.message}")
                        return@handle
                    }
                    val respBody = resp.readRawBytes()
                    val ms = (System.nanoTime() - started) / 1_000_000
                    call.respondBytes(respBody, resp.contentType(), resp.status)
                    if (!isFile) chLog?.outbound(bot.name, logId, method, reqType, reqBody, respBody.decodeToString(), resp.status.isSuccess(), "", ms)
                }
            }

            get("/health") {
                val q = spools.values.joinToString(",") { "\"${it.bot.name}\":${it.pending().size}" }
                call.respondText("{\"ok\":true,\"queued\":{$q}}", ContentType.Application.Json)
            }
        }
    }.start(wait = true)
}

/**
 * ClickHouse-compatible Snowflake: 41 bits of Unix ms, 10 bits machine id, 12 bits
 * sequence — the same layout generateSnowflakeID() produces, so ids from the relay
 * and from the old middleware sort together.
 */
class Snowflake(private val machine: Int) {
    private var lastMs = 0L
    private var seq = 0L

    @Synchronized fun next(): Long {
        var now = System.currentTimeMillis()
        if (now == lastMs) {
            seq = (seq + 1) and 0xFFF
            if (seq == 0L) { while (now <= lastMs) now = System.currentTimeMillis() }
        } else seq = 0
        if (now < lastMs) now = lastMs // clock went back: stay monotonic
        lastMs = now
        return (now shl 22) or ((machine.toLong() and 0x3FF) shl 12) or seq
    }
}
