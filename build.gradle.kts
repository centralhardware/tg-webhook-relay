plugins {
    kotlin("jvm") version "2.4.21"
    kotlin("plugin.serialization") version "2.4.21"
}

repositories { mavenCentral() }

dependencies {
    implementation("io.ktor:ktor-server-cio:3.6.0")
    implementation("io.ktor:ktor-client-cio:3.6.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("dev.inmo:tgbotapi:37.1.0")
    implementation("com.clickhouse:clickhouse-jdbc:0.11.2")
    implementation("org.apache.httpcomponents.client5:httpclient5:5.6.5")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")
}

kotlin { compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21) } }
java { targetCompatibility = JavaVersion.VERSION_21 }

val fatJar by tasks.registering(Jar::class) {
    archiveFileName.set("relay.jar")
    destinationDirectory.set(layout.buildDirectory.dir("out"))
    manifest { attributes["Main-Class"] = "relay.MainKt" }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({ configurations.runtimeClasspath.get().filter { it.name.endsWith(".jar") }.map { zipTree(it) } })
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "META-INF/versions/9/module-info.class")
}
