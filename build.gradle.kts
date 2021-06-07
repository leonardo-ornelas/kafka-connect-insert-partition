plugins {
    java
}

group = "dev.ornelas.kafka.connect.smt"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(JavaVersion.VERSION_11.majorVersion))
    }
}

dependencies {
    compileOnly("org.apache.kafka:connect-api:2.7.1")
    implementation("org.apache.kafka:connect-transforms:2.7.1")
}