#!/usr/bin/env python3
"""
create_service.py — Scaffold a Spring Boot + Kafka + Avro microservice.

Genera la misma estructura multi-módulo que `kafkaSpringUserService` y `rol_service`:
- POM padre + módulo de eventos Avro + módulo core con Spring Boot
- Layout DDD por capas (application / domain / infrastructure)

Uso:
    python create_service.py <nombre> [--output-dir DIR]

Ejemplo:
    python create_service.py product
    → crea ./product_service/ con la estructura completa
"""

import argparse
import re
import sys
from pathlib import Path


PARENT_POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
\t<modelVersion>4.0.0</modelVersion>

\t<parent>
\t\t<groupId>org.springframework.boot</groupId>
\t\t<artifactId>spring-boot-starter-parent</artifactId>
\t\t<version>3.5.14</version>
\t\t<relativePath/>
\t</parent>

\t<groupId>com.sergioricart</groupId>
\t<artifactId>sergioricart-__PROJECT__</artifactId>
\t<version>0.0.1-SNAPSHOT</version>
\t<packaging>pom</packaging>
\t<name>sergioricart-__PROJECT__</name>
\t<url>https://github.com/SergioRicart/sergioricart-__PROJECT__</url>

\t<modules>
\t\t<module>__MODULE__-events</module>
\t\t<module>__MODULE__-core</module>
\t</modules>

\t<properties>
\t\t<java.version>21</java.version>
\t\t<mapstruct.version>1.6.3</mapstruct.version>
\t\t<avro.version>1.12.1</avro.version>
\t\t<avro.plugin.version>1.12.1</avro.plugin.version>
\t\t<confluent.version>7.9.0</confluent.version>
\t</properties>

\t<repositories>
\t\t<repository>
\t\t\t<id>confluent</id>
\t\t\t<url>https://packages.confluent.io/maven/</url>
\t\t</repository>
\t\t<repository>
\t\t\t<id>sergioricart-commons</id>
\t\t\t<name>GitHub Packages</name>
\t\t\t<url>https://maven.pkg.github.com/SergioRicart/microservice-commons</url>
\t\t</repository>
\t</repositories>

\t<distributionManagement>
\t\t<repository>
\t\t\t<id>github</id>
\t\t\t<name>GitHub Packages</name>
\t\t\t<url>https://maven.pkg.github.com/SergioRicart/sergioricart-__PROJECT__</url>
\t\t</repository>
\t</distributionManagement>

</project>
"""

EVENTS_POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
\t<modelVersion>4.0.0</modelVersion>

\t<parent>
\t\t<groupId>com.sergioricart</groupId>
\t\t<artifactId>sergioricart-__PROJECT__</artifactId>
\t\t<version>0.0.1-SNAPSHOT</version>
\t</parent>

\t<artifactId>sergioricart-__NAME__-events</artifactId>
\t<packaging>jar</packaging>
\t<name>sergioricart-__NAME__-events</name>
\t<description>Avro event schemas for __NAME__-service — consumible as a Maven dependency</description>

\t<dependencies>
\t\t<dependency>
\t\t\t<groupId>org.apache.avro</groupId>
\t\t\t<artifactId>avro</artifactId>
\t\t\t<version>${avro.version}</version>
\t\t</dependency>
\t</dependencies>

\t<build>
\t\t<plugins>
\t\t\t<plugin>
\t\t\t\t<groupId>org.apache.avro</groupId>
\t\t\t\t<artifactId>avro-maven-plugin</artifactId>
\t\t\t\t<version>${avro.plugin.version}</version>
\t\t\t\t<executions>
\t\t\t\t\t<execution>
\t\t\t\t\t\t<id>schemas</id>
\t\t\t\t\t\t<phase>generate-sources</phase>
\t\t\t\t\t\t<goals>
\t\t\t\t\t\t\t<goal>schema</goal>
\t\t\t\t\t\t</goals>
\t\t\t\t\t\t<configuration>
\t\t\t\t\t\t\t<sourceDirectory>${project.basedir}/src/main/resources/</sourceDirectory>
\t\t\t\t\t\t\t<outputDirectory>${project.basedir}/src/main/java/</outputDirectory>
\t\t\t\t\t\t</configuration>
\t\t\t\t\t</execution>
\t\t\t\t</executions>
\t\t\t</plugin>
\t\t</plugins>
\t</build>

</project>
"""

CORE_POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
\t<modelVersion>4.0.0</modelVersion>

\t<parent>
\t\t<groupId>com.sergioricart</groupId>
\t\t<artifactId>sergioricart-__PROJECT__</artifactId>
\t\t<version>0.0.1-SNAPSHOT</version>
\t</parent>

\t<artifactId>__MODULE__-core</artifactId>
\t<packaging>jar</packaging>
\t<name>__MODULE__-core</name>
\t<description>Kafka project with Avro and Schema Registry</description>

\t<properties>
\t\t<maven.deploy.skip>true</maven.deploy.skip>
\t</properties>

\t<dependencies>

\t\t<dependency>
\t\t\t<groupId>com.sergioricart</groupId>
\t\t\t<artifactId>sergioricart-__NAME__-events</artifactId>
\t\t\t<version>${project.version}</version>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>com.sergioricart</groupId>
\t\t\t<artifactId>sergioricart-microservice-commons</artifactId>
\t\t\t<version>1.0.2</version>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.springframework.boot</groupId>
\t\t\t<artifactId>spring-boot-starter-actuator</artifactId>
\t\t</dependency>
\t\t<dependency>
\t\t\t<groupId>org.springframework.kafka</groupId>
\t\t\t<artifactId>spring-kafka</artifactId>
\t\t</dependency>
\t\t<dependency>
\t\t\t<groupId>org.springframework.boot</groupId>
\t\t\t<artifactId>spring-boot-starter-web</artifactId>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.projectlombok</groupId>
\t\t\t<artifactId>lombok</artifactId>
\t\t\t<optional>true</optional>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.springframework.boot</groupId>
\t\t\t<artifactId>spring-boot-starter-data-jpa</artifactId>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.springframework.boot</groupId>
\t\t\t<artifactId>spring-boot-starter-validation</artifactId>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.postgresql</groupId>
\t\t\t<artifactId>postgresql</artifactId>
\t\t\t<scope>runtime</scope>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.mapstruct</groupId>
\t\t\t<artifactId>mapstruct</artifactId>
\t\t\t<version>${mapstruct.version}</version>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>io.confluent</groupId>
\t\t\t<artifactId>kafka-avro-serializer</artifactId>
\t\t\t<version>${confluent.version}</version>
\t\t</dependency>
\t\t<dependency>
\t\t\t<groupId>io.confluent</groupId>
\t\t\t<artifactId>kafka-schema-registry-client</artifactId>
\t\t\t<version>${confluent.version}</version>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.springframework.boot</groupId>
\t\t\t<artifactId>spring-boot-starter-test</artifactId>
\t\t\t<scope>test</scope>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.springframework.kafka</groupId>
\t\t\t<artifactId>spring-kafka-test</artifactId>
\t\t\t<scope>test</scope>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>me.paulschwarz</groupId>
\t\t\t<artifactId>spring-dotenv</artifactId>
\t\t\t<version>4.0.0</version>
\t\t</dependency>

\t\t<dependency>
\t\t\t<groupId>org.springframework.security</groupId>
\t\t\t<artifactId>spring-security-crypto</artifactId>
\t\t</dependency>

\t</dependencies>

\t<build>
\t\t<plugins>

\t\t\t<plugin>
\t\t\t\t<groupId>org.springframework.boot</groupId>
\t\t\t\t<artifactId>spring-boot-maven-plugin</artifactId>
\t\t\t\t<configuration>
\t\t\t\t\t<classifier>exec</classifier>
\t\t\t\t\t<excludes>
\t\t\t\t\t\t<exclude>
\t\t\t\t\t\t\t<groupId>org.projectlombok</groupId>
\t\t\t\t\t\t\t<artifactId>lombok</artifactId>
\t\t\t\t\t\t</exclude>
\t\t\t\t\t</excludes>
\t\t\t\t</configuration>
\t\t\t</plugin>

\t\t\t<plugin>
\t\t\t\t<groupId>org.apache.maven.plugins</groupId>
\t\t\t\t<artifactId>maven-compiler-plugin</artifactId>
\t\t\t\t<configuration>
\t\t\t\t\t<annotationProcessorPaths>
\t\t\t\t\t\t<path>
\t\t\t\t\t\t\t<groupId>org.projectlombok</groupId>
\t\t\t\t\t\t\t<artifactId>lombok</artifactId>
\t\t\t\t\t\t</path>
\t\t\t\t\t\t<path>
\t\t\t\t\t\t\t<groupId>org.mapstruct</groupId>
\t\t\t\t\t\t\t<artifactId>mapstruct-processor</artifactId>
\t\t\t\t\t\t\t<version>${mapstruct.version}</version>
\t\t\t\t\t\t</path>
\t\t\t\t\t\t<path>
\t\t\t\t\t\t\t<groupId>org.projectlombok</groupId>
\t\t\t\t\t\t\t<artifactId>lombok-mapstruct-binding</artifactId>
\t\t\t\t\t\t\t<version>0.2.0</version>
\t\t\t\t\t\t</path>
\t\t\t\t\t</annotationProcessorPaths>
\t\t\t\t</configuration>
\t\t\t</plugin>

\t\t</plugins>
\t</build>

</project>
"""

APPLICATION_JAVA = """package com.sergioricart.__PROJECT__;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class __CAP__ServiceApplication {

\tpublic static void main(String[] args) {
\t\tSpringApplication.run(__CAP__ServiceApplication.class, args);
\t}

}
"""

APPLICATION_CONFIG_JAVA = """package com.sergioricart.__PROJECT__;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@Configuration
@EnableKafka
public class ApplicationConfig {

}
"""

APPLICATION_TESTS_JAVA = """package com.sergioricart.__PROJECT__;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class __CAP__ServiceApplicationTests {

\t@Test
\tvoid contextLoads() {
\t}

}
"""

APPLICATION_YAML = """spring:
  application:
    name: __PROJECT__

  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/${DB_NAME:__NAME___db}
    username: ${DB_USER:sricart}
    password: ${DB_PASSWORD:}
    driver-class-name: org.postgresql.Driver

  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
    properties:
      hibernate:
        format_sql: true
    database-platform: org.hibernate.dialect.PostgreSQLDialect

app:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    group-id: spring-service-group
    topics:
      __NAME__: __NAME__.events
    schema-registry-url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}

server:
  port: ${SERVER_PORT:8080}
"""


DOMAIN_FOLDERS = [
    "application/http/created",
    "application/http/update",
    "application/http/delete",
    "domain/constant",
    "domain/entity",
    "domain/event",
    "domain/exception",
    "domain/port",
    "infrastructure/api/contoller",
    "infrastructure/api/dto/request",
    "infrastructure/api/dto/response",
    "infrastructure/api/mapper",
    "infrastructure/database",
    "infrastructure/event/mapper",
    "infrastructure/event/producer",
]


def render(template: str, ctx: dict) -> str:
    out = template
    for key, value in ctx.items():
        out = out.replace(f"__{key}__", value)
    return out


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")



def validate_name(name: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9]*", name):
        sys.exit(
            f"Error: '{name}' no es un nombre válido. "
            "Usa solo letras ASCII minúsculas y dígitos, empezando por letra."
        )
    return name


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Genera un microservicio Spring Boot + Kafka + Avro con la estructura estándar.",
    )
    parser.add_argument(
        "name",
        help="Nombre de la entidad/dominio en minúsculas (ej: product, order, role)",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directorio donde crear el proyecto (por defecto: directorio actual)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Sobrescribir si la carpeta ya existe",
    )
    args = parser.parse_args()

    name = validate_name(args.name.strip().lower())
    cap = name.capitalize()
    project = f"{name}_service"
    module = f"{name}-service"

    ctx = {
        "NAME": name,
        "CAP": cap,
        "PROJECT": project,
        "MODULE": module,
    }

    base = Path(args.output_dir).resolve() / project
    if base.exists() and not args.force:
        sys.exit(f"Error: '{base}' ya existe. Usa --force para sobrescribir.")

    base.mkdir(parents=True, exist_ok=True)

    # POM padre
    write_file(base / "pom.xml", render(PARENT_POM, ctx))

    # Módulo events
    events = base / f"{module}-events"
    write_file(events / "pom.xml", render(EVENTS_POM, ctx))
    (events / "src" / "main" / "java").mkdir(parents=True, exist_ok=True)
    (events / "src" / "main" / "resources" / "schemas" / name).mkdir(parents=True, exist_ok=True)

    # Módulo core
    core = base / f"{module}-core"
    write_file(core / "pom.xml", render(CORE_POM, ctx))

    java_root = core / "src" / "main" / "java" / "com" / "sergioricart" / project
    write_file(java_root / f"{cap}ServiceApplication.java", render(APPLICATION_JAVA, ctx))
    write_file(java_root / "ApplicationConfig.java", render(APPLICATION_CONFIG_JAVA, ctx))

    test_root = core / "src" / "test" / "java" / "com" / "sergioricart" / project
    write_file(test_root / f"{cap}ServiceApplicationTests.java", render(APPLICATION_TESTS_JAVA, ctx))

    write_file(core / "src" / "main" / "resources" / "application.yaml", render(APPLICATION_YAML, ctx))

    # Esqueleto DDD por capas
    for sub in DOMAIN_FOLDERS:
        (java_root / name / Path(sub)).mkdir(parents=True, exist_ok=True)

    print(f"OK: proyecto creado en {base}")
    print(f"   - Padre:  sergioricart-{project} 0.0.1-SNAPSHOT")
    print(f"   - Events: sergioricart-{name}-events")
    print(f"   - Core:   {module}-core (com.sergioricart.{project}.{cap}ServiceApplication)")


if __name__ == "__main__":
    main()
