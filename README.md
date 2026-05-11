# Infraestructura de Microservicios — SergioRicart

Ecosistema de microservicios construido con **Spring Boot 3.5**, **Apache Kafka**, **Avro** y **Confluent Schema Registry**, siguiendo arquitectura hexagonal y el patrón Mediator.

---

## Tabla de contenidos

- [Visión general](#visión-general)
- [Diagrama de arquitectura](#diagrama-de-arquitectura)
- [Repositorios](#repositorios)
- [microservice-commons](#1-microservice-commons)
- [kafka-spring-user-service](#2-kafka-spring-user-service)
- [kafka-spring-notification-service](#3-kafka-spring-notification-service)
- [Tecnologías comunes](#tecnologías-comunes)
- [Flujo de eventos](#flujo-de-eventos)

---

## Visión general

La infraestructura está compuesta por tres proyectos que colaboran entre sí mediante eventos Kafka con esquemas Avro:

| Proyecto | Rol | Versión |
|---|---|---|
| `microservice-commons` | Librería compartida (Mediator + Kafka/Avro) | 1.0.2 |
| `kafka-spring-user-service` | Microservicio de usuarios (productor de eventos) | 1.0.1 |
| `kafka-spring-notification-service` | Microservicio de notificaciones (consumidor de eventos) | 1.0.0 |

---

## Diagrama de arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                        HTTP Client                              │
└──────────────────────────┬──────────────────────────────────────┘
                           │ REST API
                           ▼
┌──────────────────────────────────────────────────────────────┐
│               kafka-spring-user-service                       │
│                                                              │
│  UserController ──► Mediator ──► CreateUserHandler           │
│                               ──► UpdateUserHandler          │
│                               ──► DeleteUserHandler          │
│                                        │                     │
│                                 UserRepository               │
│                                 (PostgreSQL)                 │
│                                        │                     │
│                               UserEventProducer              │
└────────────────────────────────────────┬─────────────────────┘
                                         │ Avro Events
                                         ▼
                         ┌───────────────────────────┐
                         │   Apache Kafka + Schema   │
                         │       Registry            │
                         └───────────────┬───────────┘
                                         │ Avro Events
                                         ▼
┌──────────────────────────────────────────────────────────────┐
│            kafka-spring-notification-service                  │
│                                                              │
│  UserKafkaEventConsumer ──► UserCreatedConsumer              │
│                          ──► UserUpdatedConsumer             │
│                          ──► UserDeletedConsumer             │
│                                    │                         │
│                                 Mediator                     │
│                          ──► UserCreatedHandler ──► Email    │
│                          ──► UserUpdatedHandler ──► Email    │
│                          ──► UserDeletedHandler ──► Email    │
│                                    │                         │
│                          NotificationRepository              │
│                                 (PostgreSQL)                 │
└──────────────────────────────────────────────────────────────┘

         ┌──────────────────────────────────────┐
         │         microservice-commons          │
         │  (usado como dependencia Maven por   │
         │   user-service y notification-service)│
         └──────────────────────────────────────┘
```

---

## Repositorios

| Proyecto | GitHub | GitHub Packages |
|---|---|---|
| microservice-commons | [SergioRicart/microservice-commons](https://github.com/SergioRicart/microservice-commons) | [maven.pkg.github.com](https://maven.pkg.github.com/SergioRicart/microservice-commons) |
| kafka-spring-user-service | [SergioRicart/kafka-spring-user-service](https://github.com/SergioRicart/kafka-spring-user-service) | [maven.pkg.github.com](https://maven.pkg.github.com/SergioRicart/kafka-spring-user-service) |
| kafka-spring-notification-service | [SergioRicart/-kafka-spring-notification-service](https://github.com/SergioRicart/-kafka-spring-notification-service) | — |

---

## 1. microservice-commons

**Librería compartida** que provee la infraestructura base reutilizable para todos los microservicios del ecosistema.

### Dependencia Maven

```xml
<dependency>
    <groupId>com.sergioricart</groupId>
    <artifactId>sergioricart-microservice-commons</artifactId>
    <version>1.0.2</version>
</dependency>
```

### Componentes

#### Capa de aplicación — Patrón Mediator

| Clase | Descripción |
|---|---|
| `Mediator` | Despacha un `Command` al `CommandHandler` correspondiente usando su tipo como clave |
| `Command<R>` | Interfaz marcadora de un comando con tipo de respuesta `R` |
| `CommandHandler<T, R>` | Contrato que deben implementar todos los handlers |
| `VoidResponse` | Tipo de respuesta para comandos sin valor de retorno |

**Uso básico:**

```java
// Definir comando
public class CreateUserCommand extends Command<VoidResponse> { ... }

// Implementar handler
@Component
public class CreateUserHandler implements CommandHandler<CreateUserCommand, VoidResponse> {
    public VoidResponse handle(CreateUserCommand command) { ... }
}

// Despachar desde el controlador
mediator.dispatch(command);
```

#### Capa de infraestructura — Kafka / Avro

| Clase | Descripción |
|---|---|
| `KafkaProducer` | Envía registros Avro genéricos a un topic con UUID como key; logging de offset en callback asíncrono |
| `KafkaEventConsumer` | Consumer abstracto que recibe `GenericRecord`, resuelve el esquema Avro y delega en el `EventSpecificConsumer` correspondiente |
| `EventSpecificConsumer<T>` | Contrato para consumidores de un tipo de evento concreto |
| `KafkaProducerConfig` | Configuración del `KafkaTemplate` con serialización Avro |
| `KafkaConsumerConfig` | Configuración del consumidor Kafka con deserialización Avro |
| `MessagingUtil` | Convierte `GenericRecord` a `SpecificRecord` y construye `Message<SpecificRecord>` |
| `MapperUtils` | Utilidades de mapeo para transformaciones Avro ↔ dominio |

### Tecnologías

- Java 21
- Spring Boot 3.5.14
- Spring Kafka
- Apache Avro 1.12.1
- Confluent Kafka Avro Serializer 7.9.0
- MapStruct 1.6.3
- Lombok

---

## 2. kafka-spring-user-service

Microservicio responsable de la **gestión del ciclo de vida de usuarios**. Expone una API REST y publica eventos de dominio en Kafka cada vez que un usuario es creado, actualizado o eliminado.

Proyecto multi-módulo Maven:

```
kafka-spring-user-service/
├── user-service-events/   ← Esquemas Avro (artefacto publicado en GitHub Packages)
└── user-service-core/     ← Aplicación Spring Boot principal
```

### Módulo: user-service-events

Contiene los esquemas Avro de los eventos de usuario. Se publica como artefacto independiente para que otros microservicios puedan consumirlo sin depender del core.

```xml
<dependency>
    <groupId>com.sergioricart</groupId>
    <artifactId>sergioricart-user-events</artifactId>
    <version>1.0.1</version>
</dependency>
```

| Evento Avro | Descripción |
|---|---|
| `UserCreatedEvent` | Usuario creado con sus datos completos |
| `UserUpdatedEvent` | Usuario actualizado con los campos modificados |
| `UserDeletedEvent` | Usuario eliminado (contiene el ID) |

### Módulo: user-service-core

Aplicación Spring Boot que implementa la lógica de negocio y expone la API REST.

#### API REST — `POST /api/v1/users/create`

Crea un nuevo usuario.

```http
POST /api/v1/users/create
Content-Type: application/json

{
  "username": "john_doe",
  "email": "john@example.com",
  "password": "secret",
  "role": "USER"
}
```

**Respuesta:** `201 Created`

#### API REST — `PATCH /api/v1/users/update/{id}`

Actualiza los datos de un usuario existente.

```http
PATCH /api/v1/users/update/abc-123
Content-Type: application/json

{
  "email": "newemail@example.com"
}
```

**Respuesta:** `204 No Content`

#### API REST — `DELETE /api/v1/users/delete/{id}`

Elimina un usuario por ID.

```http
DELETE /api/v1/users/delete/abc-123
```

**Respuesta:** `204 No Content`

### Arquitectura interna (hexagonal)

```
infrastructure/api/         ← Controller, DTOs, mappers HTTP
application/http/           ← Commands y Handlers (CreateUser, UpdateUser, DeleteUser)
domain/                     ← User, Role, puertos (UserRepository, UserEvent), excepciones
infrastructure/database/    ← UserEntity, JPA repository, mapper entidad↔dominio
infrastructure/event/       ← UserEventProducer (publica eventos Avro en Kafka)
```

### Tecnologías

- Java 21
- Spring Boot 3.5.14 (Web, Data JPA, Validation, Actuator, Security Crypto)
- Spring Kafka + Confluent Avro Serializer 7.9.0
- PostgreSQL
- MapStruct 1.6.3
- Lombok
- spring-dotenv (variables de entorno desde `.env`)
- `sergioricart-microservice-commons` 1.0.2
- `sergioricart-user-events` 1.0.1

---

## 3. kafka-spring-notification-service

Microservicio responsable de **enviar notificaciones por correo electrónico** y **registrar cada notificación en base de datos** como trazabilidad. Actúa como consumidor puro de eventos Kafka: no expone API REST hacia otros microservicios.

### Responsabilidades

- Escuchar los topics de usuario en Kafka (`UserCreatedEvent`, `UserUpdatedEvent`, `UserDeletedEvent`)
- Enviar un correo electrónico personalizado al usuario afectado por cada evento
- Persistir un registro de cada notificación enviada en PostgreSQL (estado, destinatario, tipo de evento, timestamp)

### Flujo interno por evento

```
UserKafkaEventConsumer
        │
        ├── UserCreatedConsumer ──► UserCreatedHandler ──► EmailService ──► SMTP
        │                                               └──► NotificationRepository ──► PostgreSQL
        │
        ├── UserUpdatedConsumer ──► UserUpdatedHandler ──► EmailService ──► SMTP
        │                                               └──► NotificationRepository ──► PostgreSQL
        │
        └── UserDeletedConsumer ──► UserDeletedHandler ──► EmailService ──► SMTP
                                                        └──► NotificationRepository ──► PostgreSQL
```

### Eventos consumidos

| Evento | Acción | Correo enviado |
|---|---|---|
| `UserCreatedEvent` | Bienvenida al nuevo usuario | "¡Bienvenido a la plataforma!" con datos de acceso |
| `UserUpdatedEvent` | Notificación de cambio de datos | "Tus datos han sido actualizados" |
| `UserDeletedEvent` | Confirmación de baja | "Tu cuenta ha sido eliminada" |

### Registro de notificaciones (base de datos)

Cada notificación enviada queda registrada con los siguientes campos:

| Campo | Tipo | Descripción |
|---|---|---|
| `id` | UUID | Identificador único de la notificación |
| `userId` | String | ID del usuario afectado |
| `eventType` | Enum | `USER_CREATED`, `USER_UPDATED`, `USER_DELETED` |
| `recipient` | String | Email del destinatario |
| `status` | Enum | `SENT`, `FAILED` |
| `sentAt` | Timestamp | Fecha y hora del envío |

### Tecnologías

- Java 21
- Spring Boot 3.5.14 (Web, Data JPA, Validation, Actuator, Mail)
- Spring Kafka + Confluent Avro Serializer 7.9.0
- Apache Avro 1.12.1
- PostgreSQL
- MapStruct 1.6.3
- Lombok
- spring-dotenv
- `sergioricart-microservice-commons` 1.0.2
- `sergioricart-user-events` 1.0.1

---

## Tecnologías comunes

| Tecnología | Versión | Uso |
|---|---|---|
| Java | 21 | Lenguaje base |
| Spring Boot | 3.5.14 | Framework principal |
| Apache Kafka | — | Bus de eventos asíncrono |
| Apache Avro | 1.12.1 | Serialización de eventos con tipado fuerte |
| Confluent Schema Registry | 7.9.0 | Registro y validación de esquemas Avro |
| PostgreSQL | — | Persistencia relacional |
| MapStruct | 1.6.3 | Mapeo entre capas (dominio ↔ infraestructura) |
| Lombok | — | Reducción de boilerplate |
| Maven | — | Gestión de dependencias y build |
| GitHub Packages | — | Registro privado de artefactos Maven |

---

## Flujo de eventos

```
1. Cliente HTTP ──► POST /api/v1/users/create
2. UserController ──► CreateUserCommand ──► Mediator
3. CreateUserHandler ──► guarda User en PostgreSQL
4. CreateUserHandler ──► UserEventProducer.publish(UserCreatedEvent)
5. Kafka broker recibe el evento (esquema validado en Schema Registry)
6. UserKafkaEventConsumer (notification-service) recibe el GenericRecord
7. KafkaEventConsumer (commons) resuelve el SpecificRecord → UserCreatedEvent
8. UserCreatedConsumer ──► UserCreatedCommand ──► Mediator
9. UserCreatedHandler ──► EmailService.send(...)  →  correo enviado al usuario
10. UserCreatedHandler ──► NotificationRepository.save(...)  →  registro en PostgreSQL
```
