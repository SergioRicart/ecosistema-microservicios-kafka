# Sergio Ricart

Backend developer especializado en **arquitecturas event-driven** con Java y Spring Boot. Me interesa diseñar sistemas donde los servicios se comuniquen de forma asíncrona y desacoplada, con contratos de datos versionados y pipelines de CI/CD que no necesiten supervisión.

Este perfil recoge el trabajo que hice construyendo un ecosistema de microservicios desde cero: infraestructura compartida, servicios de dominio independientes, propagación de eventos entre servicios y finalmente la consolidación en un monolito modular.

---

## Ecosistema de microservicios

Una plataforma backend construida sobre **arquitectura hexagonal**, **DDD** y **comunicación asíncrona** mediante Apache Kafka con esquemas Avro registrados en Confluent Schema Registry.

```
                           ┌──────────────────────────┐
                           │       API Gateway         │
                           │  Spring Cloud · :8080     │
                           │  CORS · Routing · Health  │
                           └────────────┬─────────────┘
                                        │
         ┌──────────────┬───────────────┼───────────────┬──────────────┐
         │              │               │               │              │
   :8084 ▼        :8082 ▼         :8083 ▼         :8090 ▼       :8091 ▼
┌──────────────┐ ┌────────────┐ ┌──────────────┐ ┌───────────┐ ┌───────────────┐
│ Auth Service │ │User Service│ │  Category    │ │   Role    │ │   Product     │
│ JWT · JPA    │ │ CRUD+Kafka │ │  Service     │ │  Service  │ │   Service     │
│ Spring Sec.  │ │            │ │  CRUD+Kafka  │ │CRUD+Kafka │ │  CRUD+Kafka   │
└──────────────┘ └─────┬──────┘ └──────┬───────┘ └─────┬─────┘ └──────┬────────┘
                        │               │               │              │
                        └───────────────┴───────────────┴──────────────┘
                                        │  Kafka topics (Avro)
                                        ▼
                           ┌────────────────────────┐
                           │  Notification Service   │
                           │  Consume · SMTP · JPA   │
                           └────────────────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │   microservice-commons   │
                           │  Mediator · Kafka · JWT  │
                           │  (GitHub Packages lib)   │
                           └─────────────────────────┘
```

---

## Repositorios

### Infraestructura compartida

#### [microservice-commons](https://github.com/SergioRicart/microservice-commons)

Librería Spring Boot publicada en **GitHub Packages** y consumida por todos los servicios como dependencia Maven. Elimina el boilerplate de Kafka y el patrón Mediator en cada servicio.

Lo que provee:

| Componente | Descripción |
|------------|-------------|
| `Mediator` | Descubre automáticamente todos los beans `CommandHandler` del contexto y los enruta por tipo. Añadir un caso de uso = crear un `Command` + su `Handler @Component`. |
| `Command` / `CommandHandler<C,R>` | Interfaces genéricas del patrón. `VoidResponse` para handlers sin retorno. |
| `KafkaProducerConfig` | Autoconfiguración de productor Avro con Schema Registry. |
| `KafkaConsumerConfig` | Autoconfiguración de consumidor Avro con Schema Registry. |
| `KafkaEventConsumer` | Consumidor genérico: recibe el evento Avro, lo delega al `EventSpecificConsumer` registrado para ese tipo. |
| `KafkaProducer` | Wrapper sobre `KafkaTemplate` tipado con Avro. |
| `JwtSecurityAutoConfig` | Autoconfigura Spring Security + filtro JWT en cualquier servicio que incluya la lib. |
| `JwtService` | Firma, valida y extrae claims de tokens JWT. |

```xml
<dependency>
    <groupId>com.sergioricart</groupId>
    <artifactId>sergioricart-microservice-commons</artifactId>
    <version>1.0.4</version>
</dependency>
```

---

#### [api-gateway](https://github.com/SergioRicart/api-gatway)

Punto de entrada único al ecosistema. Enruta cada path al servicio correspondiente y expone CORS y health checks centralizados.

| Path entrante | Servicio destino | Puerto |
|---------------|-----------------|--------|
| `/api/v1/auth/**` | auth-service | 8084 |
| `/api/v1/users/**` | user-service | 8082 |
| `/api/v1/role/**` | role-service | 8090 |
| `/api/v1/page/**` | role-service | 8090 |
| `/api/v1/product/**` | product-service | 8091 |
| `/api/v1/category/**` | category-service | 8083 |

Cada URL de upstream se configura mediante variables de entorno (`AUTH_SERVICE_URL`, `USER_SERVICE_URL`, etc.), lo que permite despliegues en distintos entornos sin tocar el código.

**Stack:** Spring Cloud Gateway 2024.0.1 · Spring Boot 3.4 · Java 21 · Actuator (endpoints `/health`, `/info`, `/gateway`)

---

### Servicios de dominio

Todos los servicios de dominio siguen la misma estructura y los mismos principios:

- **Arquitectura hexagonal** (Ports & Adapters): la lógica de negocio no conoce Spring ni JPA.
- **Patrón Mediator** para el despacho de comandos desde el controller.
- **Maven multi-módulo**: módulo `*-events` (esquemas Avro, publicado en GitHub Packages) + módulo `*-core` (aplicación Spring Boot, no se publica).
- **Soft delete**: las eliminaciones establecen `deletedAt`; los listados filtran automáticamente los registros borrados.
- **CI contra PostgreSQL real**: los tests de integración no mockean la base de datos.

---

#### [user-service](https://github.com/SergioRicart/kafka-spring-user-service)

Gestión del ciclo de vida de usuarios. Es el productor principal de eventos: el `notification-service` reacciona a todos los eventos que publica este servicio.

**Arquitectura interna:**

```
UserController
    └── Mediator
            ├── CreateUserHandler   → UserRepository (PostgreSQL) + UserEvent (Kafka)
            ├── UpdateUserHandler   → UserRepository + UserEvent
            └── DeleteUserHandler   → UserRepository + UserEvent
```

**API REST** — `/api/v1/users`

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| `POST` | `/create` | Crea usuario y publica `UserCreatedEvent` |
| `PATCH` | `/update/{id}` | Actualiza usuario y publica `UserUpdatedEvent` |
| `DELETE` | `/delete/{id}` | Elimina usuario y publica `UserDeletedEvent` |

**Eventos Kafka** — topic `user.events`

| Evento | Campos clave |
|--------|-------------|
| `UserCreatedEvent` | `firstName`, `lastName`, `email`, `role`, `password`, `timestamp` |
| `UserUpdatedEvent` | `id`, `firstName`, `lastName`, `email`, `role`, `password`, `timestamp` |
| `UserDeletedEvent` | `id`, `timestamp` |
| `UserPasswordUpdatedEvent` | `id`, `password`, `timestamp` |

El módulo `user-service-events` se publica en GitHub Packages y otros servicios lo consumen como dependencia Maven con tipado fuerte en compilación.

---

#### [product-service](https://github.com/SergioRicart/kafka-spring-product-service)

Gestión de productos con CRUD completo y publicación de eventos de dominio.

**API REST** — `/api/v1/product`

| Método | Endpoint | Cuerpo |
|--------|----------|--------|
| `POST` | `/create` | `code`, `name`, `description`, `idCategory`, `retailPrice`, `costPrice`, `iva`, `profitMargin`, `idSupplier` |
| `GET` | `/` | — |
| `GET` | `/{id}` | — |
| `PATCH` | `/{id}` | campos a actualizar (parcial) |
| `DELETE` | `/{id}` | — (soft delete) |

**Eventos Kafka** — topic `product.events`: `ProductCreatedEvent` · `ProductUpdatedEvent` · `ProductDeletedEvent`

---

#### [category-service](https://github.com/SergioRicart/category-service)

Gestión de categorías. Mismo modelo que product-service.

**API REST** — `/api/v1/category`

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| `POST` | `/create` | Crea categoría (`name`, `description`) |
| `GET` | `/` | Lista categorías activas |
| `GET` | `/{id}` | Busca por ID |
| `PATCH` | `/{id}` | Actualización parcial |
| `DELETE` | `/{id}` | Soft delete |

**Eventos Kafka** — topic `category.events`: `CategoryCreatedEvent` · `CategoryUpdatedEvent` · `CategoryDeletedEvent`

---

#### [role-service](https://github.com/SergioRicart/kafka-spring-rol-service)

Gestión de roles y páginas (permisos por pantalla). Expone dos recursos: `Role` y `Page`.

**API REST**

| Recurso | Endpoints |
|---------|-----------|
| Roles — `/api/v1/role` | `POST /create` · `GET /` · `GET /{id}` · `PATCH /{id}` · `DELETE /{id}` |
| Páginas — `/api/v1/page` | `GET /role/{roleId}` (páginas asignadas a un rol) |

**Eventos Kafka** — topic `role.events`: `RoleCreatedEvent` · `RoleUpdatedEvent` · `RoleDeletedEvent`

---

#### [auth-service](https://github.com/SergioRicart/auth_service)

Autenticación y autorización basada en JWT. Consulta la base de datos de usuarios y emite tokens firmados que el resto de servicios validan usando `microservice-commons`.

**API REST** — `/api/v1/auth`

| Endpoint | Descripción |
|----------|-------------|
| `POST /login` | Recibe `email` + `password`, devuelve `{ token }` |
| `GET /me` | Devuelve el usuario autenticado a partir del token |

**Dominio:**

```
LoginCommand → LoginHandler → AuthUserRepository (JPA) → JwtService → LoginResponse { token }
GetMeQuery   → GetMeHandler → JwtService → UserAuthResponse { id, email, role }
```

Excepciones: `AuthUserNotFoundException` · `InvalidCredentialsException` — ambas capturadas por `GlobalExceptionHandler` y mapeadas a respuestas HTTP adecuadas.

**Stack:** Spring Security · JJWT 0.12.6 · Spring Data JPA · PostgreSQL · MapStruct

---

### Servicio de soporte

#### [notification-service](https://github.com/SergioRicart/kafka-spring-notification-service)

Consumidor de eventos Kafka puro: no expone API REST de negocio, solo reacciona a lo que publican los demás servicios.

**Pipeline de procesamiento:**

```
Kafka (user.events)
    │ Avro + Schema Registry
    ▼
UserKafkaEventConsumer  ←─ hereda de KafkaEventConsumer (microservice-commons)
    │ delega a EventSpecificConsumer por tipo
    ▼
MapStruct (Avro → Command)
    ▼
Mediator.dispatch(Command)
    ▼
Handler
    ├── EmailPort  →  SMTP (Spring Mail / Mailtrap)
    └── NotificationRepository  →  PostgreSQL (registro histórico)
```

**Eventos consumidos:**

| Evento | Acción |
|--------|--------|
| `UserCreatedEvent` | Email de bienvenida + registro en BD |
| `UserUpdatedEvent` | Email de actualización + registro en BD |
| `UserDeletedEvent` | Email de despedida + registro en BD |
| `UserPasswordUpdatedEvent` | Email de contraseña cambiada + registro en BD |

Los esquemas Avro se importan desde `user-service-events` (GitHub Packages), no se duplican.

---

## Diseño de eventos: esquema por módulo Maven

Cada servicio productor tiene un módulo `*-events` separado de la aplicación:

```
kafka-spring-user-service/
├── user-service-events/        ← JAR ligero, sin Spring Boot
│   └── src/main/resources/
│       └── schemas/user/
│           ├── UserCreatedEvent.avsc
│           ├── UserUpdatedEvent.avsc
│           └── UserDeletedEvent.avsc
└── user-service-core/          ← Aplicación Spring Boot (no se publica)
```

El módulo `*-events` **no arrastra dependencias de la aplicación** (sin JPA, sin web, sin seguridad). Cualquier servicio consumidor solo declara:

```xml
<dependency>
    <groupId>com.sergioricart</groupId>
    <artifactId>user-service-events</artifactId>
    <version>1.0.0</version>
</dependency>
```

y obtiene las clases Java generadas por el plugin Avro con tipado fuerte en compilación. Si el productor cambia un schema, el consumidor falla al compilar — el error llega en build, no en producción.

---

## CI/CD

Todos los servicios con tests comparten el mismo patrón de pipelines:

**CI (Pull Request → `main` / `development`)**

1. Setup Java 21 (Temurin) con caché de Maven
2. Levanta PostgreSQL como Docker service
3. Ejecuta `./mvnw clean test` **contra la base de datos real** — sin mocks de infraestructura
4. Falla el PR si algún test no pasa

**CD (merge → `main` / `development`)**

1. Tests
2. `./mvnw deploy` — publica el módulo `*-events` en **GitHub Packages**
3. El módulo `*-core` tiene `maven.deploy.skip=true`

El secret `GH_PACKAGES_TOKEN` (scope `write:packages`) se configura una vez en cada repositorio.

---

## Stack tecnológico

| Capa | Tecnología |
|------|-----------|
| Lenguaje | Java 21 |
| Framework | Spring Boot 3.5 · Spring Cloud Gateway 2024.0.1 |
| Seguridad | Spring Security · JJWT 0.12.6 |
| Mensajería | Apache Kafka · Apache Avro 1.12 · Confluent Schema Registry |
| Persistencia | Spring Data JPA · PostgreSQL 16 |
| Mapping | MapStruct · Lombok |
| Build | Maven multi-módulo · Maven Wrapper |
| Infra local | Docker · Docker Compose |
| CI/CD | GitHub Actions · GitHub Packages (Maven registry) |

---

## Del ecosistema al monolito modular

La plataforma de microservicios está marcada como `deprecated`. La evolución natural fue migrar a un **monolito modular** ([rial-sync](https://github.com/SergioRicart/rial-sync)) que mantiene la separación de dominios pero elimina el overhead operacional de gestionar múltiples servicios, bases de datos y brokers independientes.

Los principios de diseño se preservaron: arquitectura hexagonal, patrón Mediator y separación entre dominio e infraestructura. Solo cambió la frontera de despliegue.
