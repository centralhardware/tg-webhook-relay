package relay

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO as ClientCIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import kotlin.system.exitProcess

/**
 * Telegram webhook relay for several bots.
 *
 * Telegram POSTs each update to /webhook/<bot>. The relay writes it to a per-bot
 * spool on disk and answers 200 at once, and a worker per bot forwards the spool
 * to that bot's target in update order. If the target is down, updates wait on
 * disk and the worker keeps retrying, so a restart or deploy loses nothing.
 *
 * The forwarded request looks exactly like a Telegram webhook call (same body,
 * same X-Telegram-Bot-Api-Secret-Token header), so a target can use any standard
 * webhook receiver. The secret is derived from the bot token, so both sides can
 * compute it without extra config: hex(sha256("tg-webhook-relay:" + token)).
 */

@Serializable
data class Bot(
    val name: String,
    val token: String,
    val target: String,
    /** Passed to setWebhook. Null keeps Telegram's default set (no reactions etc.). */
    val allowed_updates: List<String>? = null,
)

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

class Spool(root: File, val bot: Bot) {
    val dir = File(root, bot.name).apply { mkdirs() }
    private val wake = kotlinx.coroutines.channels.Channel<Unit>(kotlinx.coroutines.channels.Channel.CONFLATED)

    fun put(body: String) {
        // update_id orders the spool; fall back to arrival time for anything odd.
        val id = runCatching { json.parseToJsonElement(body).jsonObject["update_id"]!!.jsonPrimitive.long }.getOrNull()
        val base = "%020d".format(id ?: System.currentTimeMillis()) + "-" + System.nanoTime()
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
                val ok = try {
                    val r = http.post(bot.target) {
                        contentType(ContentType.Application.Json)
                        header("X-Telegram-Bot-Api-Secret-Token", secret)
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
            val r = http.post("https://api.telegram.org/bot${bot.token}/setWebhook") {
                contentType(ContentType.Application.Json)
                setBody(body.toString())
            }
            val text = r.bodyAsText()
            if (r.status.isSuccess()) { log("${bot.name}: webhook set"); return }
            log("${bot.name}: setWebhook failed: $text")
        } catch (e: Exception) {
            log("${bot.name}: setWebhook error: ${e.message}")
        }
        delay(2000L * (attempt + 1))
    }
}

fun main() {
    val bots = loadBots()
    val publicUrl = env("PUBLIC_URL")?.trimEnd('/') ?: run { log("PUBLIC_URL is required"); exitProcess(1) }
    val root = File(env("SPOOL_DIR") ?: "/data/spool")
    val spools = bots.associate { it.name to Spool(root, it) }
    val http = HttpClient(ClientCIO) {
        install(HttpTimeout) { requestTimeoutMillis = 30_000; connectTimeoutMillis = 5_000 }
        expectSuccess = false
    }
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val chLog = env("CLICKHOUSE_JDBC_URL")?.let { ClickHouseLog(it).also { l -> l.start(scope) } }
        ?: null.also { log("CLICKHOUSE_JDBC_URL not set — inbound updates are not logged") }
    for (s in spools.values) {
        scope.launch { setWebhook(http, s.bot, publicUrl) }
        scope.launch { s.forward(http) }
    }
    log("relaying ${bots.joinToString { "${it.name} -> ${it.target}" }}")

    embeddedServer(CIO, port = env("PORT")?.toIntOrNull() ?: 8080, host = "0.0.0.0") {
        routing {
            post("/webhook/{bot}") {
                val s = spools[call.parameters["bot"]]
                if (s == null) { call.respond(HttpStatusCode.NotFound); return@post }
                if (call.request.header("X-Telegram-Bot-Api-Secret-Token") != secretFor(s.bot.token)) {
                    call.respond(HttpStatusCode.Unauthorized); return@post
                }
                // Answer 200 only once the update is on disk; a failed write makes Telegram retry.
                val body = call.receiveText()
                s.put(body)
                call.respond(HttpStatusCode.OK)
                chLog?.inbound(s.bot.name, body)
            }
            get("/health") {
                val q = spools.values.joinToString(",") { "\"${it.bot.name}\":${it.pending().size}" }
                call.respondText("{\"ok\":true,\"queued\":{$q}}", ContentType.Application.Json)
            }
        }
    }.start(wait = true)
}
