# Диалект {{ ydb-short-name }} для Kotlin Exposed

## Введение {#overview}

Это руководство описывает использование [JetBrains Exposed](https://github.com/JetBrains/Exposed) с {{ ydb-short-name }}.

JetBrains Exposed - SQL-библиотека для Kotlin, которая предоставляет типобезопасный DSL для создания запросов, взаимодействие с базой данных в стиле ORM и DAO-фреймворк.

## Установка диалекта {{ ydb-short-name }} {#install-dialect}

Примеры для различных систем сборки:

{% list tabs %}

- Maven

    ```xml
    <!-- Set actual versions -->
    <dependency>
        <groupId>tech.ydb.dialects</groupId>
        <artifactId>kotlin-exposed-ydb-dialect</artifactId>
        <version>${exposed.ydb.dialect.version}</version>
    </dependency>

    <dependency>
        <groupId>tech.ydb.jdbc</groupId>
        <artifactId>ydb-jdbc-driver</artifactId>
        <version>${ydb.jdbc.version}</version>
    </dependency>
    ```

- Gradle

    ```groovy
    dependencies {
        // Set actual versions
        implementation "tech.ydb.dialects:kotlin-exposed-ydb-dialect:$ydbDialectVersion"
        implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
    }
    ```

{% endlist %}

Диалект рассчитан на JDK 17+ и Exposed 1.x.

## Конфигурация диалекта {#configuration-dialect}

Для подключения используйте `YdbDialectProvider.connect(...)`. Он:

- регистрирует JDBC-драйвер YDB в Exposed;
- регистрирует metadata provider диалекта;
- создает `Database` с `YdbDialect`;
- по умолчанию использует `SERIALIZABLE` и отключает nested transactions.

```kotlin
import tech.ydb.exposed.dialect.YdbDialectProvider
import tech.ydb.exposed.dialect.ydbTransaction

val db = YdbDialectProvider.connect(
    url = "jdbc:ydb:grpc://localhost:2136/local"
)

ydbTransaction(db) {
    // Exposed DSL / DAO code
}
```

По умолчанию диалект использует расширенные временные типы YDB: `Date32`, `Datetime64`, `Timestamp64`.

Если нужно работать с уже существующей схемой, использующей legacy-типы `Date`, `Datetime`, `Timestamp`, передайте флаг `forceLegacyDatetimes = true`:

```kotlin
val db = YdbDialectProvider.connect(
    url = "jdbc:ydb:grpc://localhost:2136/local",
    forceLegacyDatetimes = true
)
```

## Использование {#using}

Используйте диалект так же, как и любой другой JDBC dialect Exposed: описывайте таблицы, открывайте транзакции и выполняйте DSL-запросы.

Для YDB-специфичных возможностей удобно наследоваться от `YdbTable` или `YdbIdTable`.

YDB требует, чтобы каждая таблица имела явный `PRIMARY KEY`.

### Пример описания таблицы {#table-example}

```kotlin
import tech.ydb.exposed.dialect.YdbIndexScope
import tech.ydb.exposed.dialect.YdbIndexSyncMode
import tech.ydb.exposed.dialect.YdbTable
import tech.ydb.exposed.dialect.types.ydbDecimal

object Products : YdbTable("products") {
    val id = integer("id")
    val sku = varchar("sku", 64)
    val name = varchar("name", 255)
    val category = varchar("category", 128)
    val price = ydbDecimal("price", precision = 10, scale = 2)

    override val primaryKey = PrimaryKey(id)

    init {
        index(isUnique = false, sku)

        secondaryIndex(
            name = "products_category_idx",
            category,
            unique = false,
            scope = YdbIndexScope.GLOBAL,
            syncMode = YdbIndexSyncMode.ASYNC,
            coverColumns = listOf(name, price)
        )
    }
}
```

Создание схемы:

```kotlin
import org.jetbrains.exposed.v1.jdbc.SchemaUtils

ydbTransaction(db) {
    SchemaUtils.create(Products)
}
```

Будет сгенерирована таблица YDB с обязательным `PRIMARY KEY`, а secondary index будет включен в DDL. Упрощенно это выглядит так:

```yql
CREATE TABLE IF NOT EXISTS products (
    id Int32 NOT NULL,
    sku Text NOT NULL,
    name Text NOT NULL,
    category Text NOT NULL,
    price Decimal(10, 2) NOT NULL,
    INDEX products_category_idx GLOBAL ASYNC ON (category) COVER (name, price),
    PRIMARY KEY (id)
)
```

### Пример операций чтения и записи {#crud-example}

```kotlin
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.replace
import org.jetbrains.exposed.v1.jdbc.upsert
import java.math.BigDecimal

ydbTransaction(db) {
    Products.upsert {
        it[id] = 1
        it[sku] = "BOOK-001"
        it[name] = "Kotlin in Action"
        it[category] = "books"
        it[price] = BigDecimal("39.90")
    }

    Products.replace {
        it[id] = 1
        it[sku] = "BOOK-001"
        it[name] = "Kotlin in Action, 2nd edition"
        it[category] = "books"
        it[price] = BigDecimal("44.90")
    }

    val row = Products
        .selectAll()
        .where { Products.id eq 1 }
        .single()
}
```

Диалект использует native YDB-синтаксис:

- `UPSERT INTO ...`
- `REPLACE INTO ...`

ANSI `MERGE` диалект не поддерживает.

## Типы данных {#types}

Таблица сопоставления стандартных типов Exposed с типами {{ ydb-short-name }}:

| Exposed type | {{ ydb-short-name }} type |
| --- | --- |
| `byte`, `ubyte` | `Int8`, `Uint8` |
| `short`, `ushort` | `Int16`, `Uint16` |
| `integer`, `uinteger` | `Int32`, `Uint32` |
| `long` | `Int64` |
| `float`, `double` | `Float`, `Double` |
| `bool` | `Bool` |
| `varchar`, `text` | `Text` |
| `binary`, `blob` | `String` |
| `uuid` | `Uuid` |
| `date` | `Date32` |
| `datetime` | `Datetime64` |
| `timestamp` | `Timestamp64` |
| `json` | `JsonDocument` |

Дополнительные YDB-специфичные column helpers:

| Helper | {{ ydb-short-name }} type |
| --- | --- |
| `ydbDecimal(name, precision, scale)` | `Decimal(p, s)` |
| `ydbInterval(name)` | `Interval` |
| `ydbJson(name)` | `Json` |
| `ydbJsonDocument(name)` | `JsonDocument` |
| `ydbUuid(name)` | `Uuid` |
| `ydbUint64(name)` | `Uint64` |

Пример:

```kotlin
import tech.ydb.exposed.dialect.types.ydbDecimal
import tech.ydb.exposed.dialect.types.ydbInterval
import tech.ydb.exposed.dialect.types.ydbJson
import tech.ydb.exposed.dialect.types.ydbJsonDocument
import tech.ydb.exposed.dialect.types.ydbUint64

object Events : YdbTable("events") {
    val id = integer("id")
    val duration = ydbInterval("duration")
    val payload = ydbJson("payload")
    val indexedPayload = ydbJsonDocument("indexed_payload")
    val version = ydbUint64("version")
    val price = ydbDecimal("price", 10, 2)

    override val primaryKey = PrimaryKey(id)
}
```

`JsonDocument` можно использовать как индексируемый JSON, аналогично `jsonb` в PostgreSQL.

### Ограничение `Uint64`

`ydbUint64()` отображается в Kotlin `Long`, поэтому поддерживает только диапазон `0..Long.MAX_VALUE`. Если требуется полная ширина `Uint64`, понадобится отдельное отображение на более широкий тип.

## Повторяемые транзакции {#retryable-transactions}

YDB использует Optimistic Concurrency Control, поэтому под нагрузкой транзакции могут завершаться retriable-ошибками, например `ABORTED`, `OVERLOADED` или `BAD_SESSION`.

Рекомендуемый способ выполнять операции — `ydbTransaction(...)`:

```kotlin
import tech.ydb.exposed.dialect.ydbReadOnlyTransaction
import tech.ydb.exposed.dialect.ydbTransaction

ydbTransaction(db) {
    // read-write transaction
}

ydbTransaction(db, idempotent = true) {
    // single UPSERT / REPLACE or another idempotent unit of work
}

ydbReadOnlyTransaction(db) {
    // retryable read-only transaction
}
```

Если `idempotent = true`, диалект также повторяет ошибки с не до конца определенным исходом, например `TIMEOUT` и `UNDETERMINED`.

## Secondary indexes {#secondary-indexes}

Диалект поддерживает:

- стандартный Exposed `index(...)`;
- YDB-специфичный `secondaryIndex(...)`.

Пример:

```kotlin
secondaryIndex(
    name = "products_category_idx",
    category,
    unique = false,
    scope = YdbIndexScope.GLOBAL,
    syncMode = YdbIndexSyncMode.ASYNC,
    coverColumns = listOf(name, price),
    withParams = mapOf("compression" to "off")
)
```

Поддерживаются:

- `GLOBAL` indexes;
- `UNIQUE` secondary indexes;
- `ASYNC` и `SYNC`;
- `COVER (...)`;
- `WITH (...)`.

## TTL {#ttl}

TTL можно объявлять прямо в определении таблицы:

```kotlin
import org.jetbrains.exposed.v1.javatime.timestamp
import tech.ydb.exposed.dialect.YdbTable
import tech.ydb.exposed.dialect.YdbTtlColumnMode
import tech.ydb.exposed.dialect.types.ydbUint64

object Sessions : YdbTable("sessions") {
    val id = integer("id")
    val expireAt = timestamp("expire_at")

    override val primaryKey = PrimaryKey(id)

    init {
        ttl(expireAt, "PT1H")
    }
}

object EventLog : YdbTable("event_log") {
    val id = integer("id")
    val createdAtEpoch = ydbUint64("created_at_epoch")

    override val primaryKey = PrimaryKey(id)

    init {
        ttl(createdAtEpoch, "PT24H", YdbTtlColumnMode.SECONDS)
    }
}
```

Поддерживаемые режимы TTL:

- `DATE_TYPE`;
- `SECONDS`;
- `MILLISECONDS`;
- `MICROSECONDS`;
- `NANOSECONDS`.

## Идентификаторы {#generated-identifiers}

YDB не поддерживает `AUTO_INCREMENT`, поэтому диалект явно отклоняет `autoIncrement()`.

Для таблиц с generated identifiers доступны:

- `YdbUuidIdTable` — native `Uuid`, генерируется через `UUID.randomUUID()`;
- `YdbUlidTable` — строковый ULID длиной 26 символов;
- `YdbStringIdTable` — строковый бизнес-ключ, задается вызывающим кодом;
- top-level helper `ydbUlid()` — если ULID нужен вне `IdTable`.

Пример:

```kotlin
import tech.ydb.exposed.dialect.YdbUlidTable

object Events : YdbUlidTable("events") {
    val payload = text("payload")
}
```

## Совместимость с Exposed DAO {#dao}

Диалект работает не только с DSL, но и с Exposed DAO. Для этого используются `YdbIdTable` и его специализации.

Это позволяет описывать сущности DAO поверх таблиц:

- с UUID первичным ключом типа через `YdbUuidIdTable`;
- с ULID первичным ключом через `YdbUlidTable`;
- со строковым первичным ключом через `YdbStringIdTable`.

## Ограничения {#limitations}

- Для каждой таблицы требуется явный `PRIMARY KEY`.
- `AUTO_INCREMENT` не поддерживается.
- ANSI `MERGE` не поддерживается; вместо него используйте `UPSERT` или `REPLACE`.
- Functional indexes не поддерживаются.
- Secondary indexes в рамках диалекта ориентированы на `GLOBAL` indexes для row-oriented таблиц YDB.
- `ydbUint64()` поддерживает только диапазон `0..Long.MAX_VALUE`.

