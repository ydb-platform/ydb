# Диалект YDB для Kotlin Exposed

## Введение {#overview}

Это руководство описывает использование [JetBrains Exposed](https://github.com/JetBrains/Exposed) с YDB.

Exposed - это Kotlin-библиотека для работы с базой данных через SQL DSL и DAO API поверх JDBC. Диалект YDB добавляет в Exposed поддержку YDB-совместимого SQL, отображение типов данных YDB, retry-aware транзакции для optimistic concurrency control и YDB-специфичные особенности схемы.

Поддержка поставляется в двух артефактах:

- основной JDBC-диалект для Exposed;
- отдельный опциональный модуль `spring-boot-starter` для приложений на Spring Boot 3.

## Установка диалекта YDB {#install-dialect}

Примеры для различных систем сборки:

{% list tabs %}

- Maven

  ```xml
  <!-- Укажите актуальные версии -->
  <dependency>
      <groupId>tech.ydb.jdbc</groupId>
      <artifactId>ydb-jdbc-driver</artifactId>
      <version>${ydb.jdbc.version}</version>
  </dependency>

  <dependency>
      <groupId>tech.ydb.dialects</groupId>
      <artifactId>kotlin-exposed-ydb-dialect</artifactId>
      <version>${exposed.ydb.dialect.version}</version>
  </dependency>
  ```

- Gradle

  ```groovy
  dependencies {
      // Укажите актуальные версии
      implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
      implementation "tech.ydb.dialects:kotlin-exposed-ydb-dialect:$ydbDialectVersion"
  }
  ```

{% endlist %}

Если приложение использует DAO, JSON DSL или другие модули Exposed, подключайте их стандартным способом вместе с основным диалектом.

## Подключение и конфигурация {#configuration-dialect}

Сначала зарегистрируйте диалект и драйвер, затем откройте `Database` через обычный `Database.connect(...)`:

```kotlin
import org.jetbrains.exposed.v1.jdbc.Database
import tech.ydb.exposed.dialect.registerYdbDialect
import tech.ydb.exposed.dialect.ydbDatabaseConfig

registerYdbDialect()

val db = Database.connect(
    url = "jdbc:ydb:grpc://localhost:2136/local",
    driver = "tech.ydb.jdbc.YdbDriver",
    databaseConfig = ydbDatabaseConfig()
)
```

`registerYdbDialect()`:

- регистрирует JDBC-драйвер `tech.ydb.jdbc.YdbDriver` для URL с префиксом `jdbc:ydb:`;
- регистрирует `YdbDialect` в Exposed;
- регистрирует metadata bridge для чтения существующих secondary indexes через JDBC metadata.

`ydbDatabaseConfig()` возвращает рекомендуемую конфигурацию Exposed для YDB:

- `defaultIsolationLevel = SERIALIZABLE`;
- `useNestedTransactions = false`;
- явный `YdbDialect(...)`.

### Signed temporal types {#signed-temporal-types}

По умолчанию стандартные Exposed temporal-типы отображаются в:

- `Date`
- `Datetime`
- `Timestamp`

Если требуется signed-режим, включите его и для диалекта, и для JDBC URL:

```kotlin
import org.jetbrains.exposed.v1.jdbc.Database
import tech.ydb.exposed.dialect.registerYdbDialect
import tech.ydb.exposed.dialect.ydbJdbcUrl

registerYdbDialect(enableSignedDatetimes = true)

val db = Database.connect(
    url = ydbJdbcUrl(
        url = "jdbc:ydb:grpc://localhost:2136/local",
        enableSignedDatetimes = true
    ),
    driver = "tech.ydb.jdbc.YdbDriver",
    databaseConfig = ydbDatabaseConfig(enableSignedDatetimes = true)
)
```

В этом режиме стандартные Exposed-типы `date`, `datetime` и `timestamp` будут генерировать DDL с `Date32`, `Datetime64` и `Timestamp64`.

При этом колонка может быть типизирована и явно, через `ydbDate(...)`, `ydbDate32(...)`, `ydbDatetime(...)`, `ydbDatetime64(...)`, `ydbTimestamp(...)` и `ydbTimestamp64(...)`.

## Использование {#using}

### Определение таблиц {#tables}

В YDB `CREATE TABLE` должен содержать табличный `PRIMARY KEY (...)`. Стандартный DDL-путь Exposed 1.3.0 для таблиц с одноколоночным PK может сгенерировать inline-форму:

```sql
id Int32 PRIMARY KEY
```

YDB такую форму не принимает. Поэтому для таблиц, которые должны создаваться через Exposed DDL, необходимо переопределять `createStatement()` и вызывать `createYdbStatement()`:

```kotlin
import org.jetbrains.exposed.v1.core.PrimaryKey
import org.jetbrains.exposed.v1.core.Table
import tech.ydb.exposed.dialect.createYdbStatement
import tech.ydb.exposed.dialect.javatime.ydbTimestamp64
import tech.ydb.exposed.dialect.ydbDecimal

object Products : Table("products") {
    val id = integer("id")
    val sku = varchar("sku", 64)
    val name = varchar("name", 255)
    val category = varchar("category", 128)
    val price = ydbDecimal("price", precision = 10, scale = 2)
    val expiresAt = ydbTimestamp64("expires_at")

    override val primaryKey = PrimaryKey(id)

    init {
        index(false, sku)
    }

    override fun createStatement(): List<String> = createYdbStatement()
}
```

`createYdbStatement()`:

- рендерит все колонки без inline `PRIMARY KEY`;
- добавляет табличный `PRIMARY KEY (...)`;
- сохраняет `NOT NULL` и `DEFAULT`;
- сохраняет `storageParameters`, поэтому YDB-специфичные `WITH (...)` можно описывать через стандартный Exposed API.

Создание схемы выполняется обычными средствами Exposed:

```kotlin
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import tech.ydb.exposed.dialect.ydbTransaction

ydbTransaction(db) {
    SchemaUtils.create(Products)
}
```

### YDB-специфичные параметры таблицы {#table-options}

TTL и другие YDB-специфичные table options задаются через `storageParameters`:

```kotlin
import org.jetbrains.exposed.v1.core.PrimaryKey
import org.jetbrains.exposed.v1.core.RawTableStorageParameter
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.TableStorageParameter
import tech.ydb.exposed.dialect.createYdbStatement
import tech.ydb.exposed.dialect.javatime.ydbTimestamp64

object Sessions : Table("sessions") {
    val id = integer("id")
    val expireAt = ydbTimestamp64("expire_at")

    override val primaryKey = PrimaryKey(id)

    override val storageParameters: List<TableStorageParameter> =
        listOf(RawTableStorageParameter("TTL = Interval(\"PT1H\") ON expire_at"))

    override fun createStatement(): List<String> = createYdbStatement()
}
```

Secondary indexes, объявленные через стандартный `Table.index(...)`, создаются отдельно через:

```sql
ALTER TABLE ... ADD INDEX ... GLOBAL
```

## Операции чтения и записи {#dml}

Стандартные DSL-операции Exposed (`select`, `insert`, `update`, `deleteWhere`) работают без дополнительных изменений.

Диалект также использует native-операции YDB `UPSERT` и `REPLACE` через расширения `Table.upsert(...)` и `Table.replace(...)`:

```kotlin
import java.math.BigDecimal

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
```

Особенности семантики:

- `UPSERT` записывает только перечисленные в блоке колонки;
- при конфликте по первичному ключу остальные колонки строки не изменяются;
- при вставке новой строки все обязательные `NOT NULL` колонки без default-значения должны быть инициализированы;
- `REPLACE` перезаписывает строку по первичному ключу, поэтому пропущенные колонки трактуются как сбрасываемые к значениям по умолчанию;
- если у обязательной колонки нет default-значения, `REPLACE` с пропуском такой колонки завершится ошибкой;
- `upsert(where)` и PostgreSQL-подобная логика `ON CONFLICT DO UPDATE` не поддерживаются.

{% note warning %}

ANSI `MERGE` диалект намеренно не поддерживает. В сценариях YDB его роль выполняют `UPSERT` и `REPLACE`.

{% endnote %}

## Повторяемые транзакции {#retryable-transactions}

YDB использует optimistic concurrency control, поэтому транзакция может завершиться retryable-ошибкой и потребовать повторного выполнения. Для этого диалект предоставляет `ydbTransaction(...)`:

```kotlin
import tech.ydb.exposed.dialect.YdbRetryConfig
import tech.ydb.exposed.dialect.ydbTransaction

ydbTransaction(db) {
    // обычная read-write транзакция
}

ydbTransaction(db, retry = YdbRetryConfig.IDEMPOTENT) {
    // безопасно повторяемая операция
}

ydbTransaction(db, readOnly = true, retry = YdbRetryConfig.IDEMPOTENT) {
    // read-only сценарий
}
```

`YdbRetryConfig.IDEMPOTENT` следует использовать только тогда, когда тело транзакции можно безопасно выполнить повторно.

## Внешние миграции и проверка схемы {#migrations}

Для production-сценариев рекомендуется рассматривать DDL-генерацию Exposed как вспомогательный, а не основной путь. Обычно схема:

1. описывается отдельными SQL-скриптами;
2. мигрируется внешним инструментом с версионированием, например Flyway или Liquibase;
3. сверяется с моделью Exposed на стороне приложения.

Если приложение использует schema validation или migration diff generation через Exposed, добавьте:

{% list tabs %}

- Maven

  ```xml
  <dependency>
      <groupId>org.jetbrains.exposed</groupId>
      <artifactId>exposed-migration-core</artifactId>
      <version>${exposed.version}</version>
  </dependency>
  <dependency>
      <groupId>org.jetbrains.exposed</groupId>
      <artifactId>exposed-migration-jdbc</artifactId>
      <version>${exposed.version}</version>
  </dependency>
  ```

- Gradle

  ```groovy
  dependencies {
      implementation "org.jetbrains.exposed:exposed-migration-core:$exposedVersion"
      implementation "org.jetbrains.exposed:exposed-migration-jdbc:$exposedVersion"
  }
  ```

{% endlist %}

В Exposed 1.3.0 полный путь `MigrationUtils.statementsRequiredForDatabaseMigration(...)` безусловно читает metadata по `CHECK`-constraint'ам из `INFORMATION_SCHEMA.CHECK_CONSTRAINTS`. Текущий JDBC-драйвер YDB эту часть metadata не предоставляет, поэтому полный generic diff Exposed для YDB работает не полностью.

Практический YDB-совместимый путь:

- накатывать схему внешними миграциями;
- использовать Exposed table definitions как описание клиентской модели;
- валидировать drift через совместимые metadata-пути Exposed:
  - существующие колонки;
  - существующие secondary indexes.

В репозитории диалекта есть integration coverage для сценария, где схема создаётся raw SQL, а затем сверяется с Exposed-моделью без падения на unsupported metadata.

{% note info %}

Если схема была только что изменена raw SQL, выполняйте validation в новой транзакции, чтобы Exposed не работал со stale metadata cache.

{% endnote %}

## Типы данных {#types}

Таблица отображения стандартных Exposed-типов в [типы YDB](https://ydb.tech/docs/en/yql/reference/types/primitive):

| Exposed                | YDB                |
|------------------------|--------------------|
| `byte` / `ubyte`       | `Int8` / `Uint8`   |
| `short` / `ushort`     | `Int16` / `Uint16` |
| `integer` / `uinteger` | `Int32` / `Uint32` |
| `long`                 | `Int64`            |
| `float` / `double`     | `Float` / `Double` |
| `bool`                 | `Bool`             |
| `varchar` / `text`     | `Text`             |
| `binary` / `blob`      | `Bytes`            |
| `uuid`                 | `Uuid`             |
| `date`                 | `Date` или `Date32` |
| `datetime`             | `Datetime` или `Datetime64` |
| `timestamp`            | `Timestamp` или `Timestamp64` |
| `json`                 | `Json`             |
| `jsonb`                | `JsonDocument`     |

Особенности:

- `varchar(n)` отображается в `Text`; ограничение длины не кодируется в YDB DDL;
- `jsonb` соответствует `JsonDocument`;
- `autoIncrement()` отображается в `Serial` или `BigSerial`.

### YDB-специфичные типы

Диалект добавляет набор расширений для колонок:

```kotlin
import tech.ydb.exposed.dialect.ydbDecimal
import tech.ydb.exposed.dialect.ydbInterval
import tech.ydb.exposed.dialect.ydbInterval64
import tech.ydb.exposed.dialect.ydbJson
import tech.ydb.exposed.dialect.ydbJsonDocument
import tech.ydb.exposed.dialect.ydbUbyte
import tech.ydb.exposed.dialect.ydbUint32
import tech.ydb.exposed.dialect.ydbUint64
import tech.ydb.exposed.dialect.ydbUuid

val amount = ydbDecimal("amount", precision = 22, scale = 9)
val payload = ydbJson("payload")
val indexedPayload = ydbJsonDocument("indexed_payload")
val uid = ydbUuid("uid")
val flags = ydbUbyte("flags")
val counter = ydbUint32("counter")
val total = ydbUint64("total")
val duration = ydbInterval("duration")
val duration64 = ydbInterval64("duration64")
```

Для literal-значений `Decimal` в выражениях обновления доступно `ydbDecimalLiteral(...)`.

### Явный выбор temporal-типа на уровне колонки

```kotlin
import org.jetbrains.exposed.v1.core.PrimaryKey
import org.jetbrains.exposed.v1.core.Table
import tech.ydb.exposed.dialect.createYdbStatement
import tech.ydb.exposed.dialect.javatime.ydbDate
import tech.ydb.exposed.dialect.javatime.ydbDate32
import tech.ydb.exposed.dialect.javatime.ydbDatetime
import tech.ydb.exposed.dialect.javatime.ydbDatetime64
import tech.ydb.exposed.dialect.javatime.ydbTimestamp
import tech.ydb.exposed.dialect.javatime.ydbTimestamp64

object Events : Table("events") {
    val id = integer("id")
    val legacyDate = ydbDate("legacy_date")
    val signedDate = ydbDate32("signed_date")
    val legacyDatetime = ydbDatetime("legacy_datetime")
    val signedDatetime = ydbDatetime64("signed_datetime")
    val legacyTimestamp = ydbTimestamp("legacy_timestamp")
    val signedTimestamp = ydbTimestamp64("signed_timestamp")

    override val primaryKey = PrimaryKey(id)
    override fun createStatement(): List<String> = createYdbStatement()
}
```

{% note info %}

`registerYdbDialect(enableSignedDatetimes = true)` меняет DDL-названия только для стандартных Exposed `date`, `datetime` и `timestamp`. Явные `ydbDate(...)`, `ydbDate32(...)`, `ydbDatetime(...)`, `ydbDatetime64(...)`, `ydbTimestamp(...)` и `ydbTimestamp64(...)` всегда генерируют тот тип, который отражён в названии функции.

{% endnote %}

{% note warning %}

В текущей реализации binding для `Uint64` ограничен диапазоном `0..Long.MAX_VALUE`.

{% endnote %}

## Интеграция со Spring Boot 3 {#spring-boot-3}

Для Spring Boot 3 поддерживается отдельный модуль:

`tech.ydb.dialects:kotlin-exposed-ydb-dialect-spring-boot-starter`

Этот starter повторяет базовый сценарий из официальной документации Exposed по [интеграции со Spring Boot 3](https://www.jetbrains.com/help/exposed/spring-boot-integration.html#spring-boot-3), но добавляет поверх него YDB-специфичный слой:

- автоматическую регистрацию `registerYdbDialect(...)`;
- YDB-совместимый `DatabaseConfig` как primary Spring bean;
- бин `Database`, создаваемый из Spring-managed `DataSource`;
- нормализацию `spring.datasource.url` через `forceSignedDatetimes=...` до создания `DataSource`;
- автоматическую подстановку `tech.ydb.jdbc.YdbDriver`, если `driver-class-name` не задан;
- бин `YdbTransactionOperations` для retry-aware транзакций.

### Подключение spring-boot-starter

{% list tabs %}

- Maven

  ```xml
  <!-- Укажите актуальную версию -->
  <dependency>
      <groupId>tech.ydb.dialects</groupId>
      <artifactId>kotlin-exposed-ydb-dialect-spring-boot-starter</artifactId>
      <version>${exposed.ydb.dialect.version}</version>
  </dependency>
  ```

- Gradle

  ```groovy
  dependencies {
      // Укажите актуальную версию
      implementation "tech.ydb.dialects:kotlin-exposed-ydb-dialect-spring-boot-starter:$ydbDialectVersion"
  }
  ```

{% endlist %}

Starter сам подтягивает:

- `kotlin-exposed-ydb-dialect`;
- `org.jetbrains.exposed:exposed-spring-boot-starter`;
- `tech.ydb.jdbc:ydb-jdbc-driver`.

### Базовая конфигурация Spring Boot

```yaml
spring:
  datasource:
    url: jdbc:ydb:grpc://localhost:2136/local
  exposed:
    generate-ddl: false
    show-sql: false
    ydb:
      enable-signed-datetimes: false
```

Стандартные свойства official Exposed starter остаются доступными, в том числе:

- `spring.exposed.generate-ddl`
- `spring.exposed.excluded-packages`
- `spring.exposed.show-sql`

YDB-специфичное свойство starter:

- `spring.exposed.ydb.enable-signed-datetimes`

Если `spring.datasource.driver-class-name` не задан, starter автоматически подставляет:

`tech.ydb.jdbc.YdbDriver`

Если включить:

`spring.exposed.ydb.enable-signed-datetimes=true`

starter нормализует `spring.datasource.url` и добавляет или обновляет параметр:

`forceSignedDatetimes=true`

### Что именно делает starter

Автоконфигурация starter активируется только для `spring.datasource.url`, начинающегося с `jdbc:ydb:`.

После активации она:

- регистрирует YDB-диалект в Exposed через `registerYdbDialect(enableSignedDatetimes = ...)`;
- публикует YDB-ориентированный `DatabaseConfig` как primary bean; в нём используются `explicitDialect = YdbDialect(...)`, `defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE`, `defaultReadOnly = false` и `useNestedTransactions = false`;
- создаёт бин `Database`, если пользователь ещё не объявил его сам;
- создаёт бин `YdbTransactionOperations`, если в контексте уже доступен `Database`.

Таким образом, в типовом случае достаточно:

1. добавить зависимость starter;
2. указать `spring.datasource.url`, начинающийся с `jdbc:ydb:`;
3. при необходимости включить `spring.exposed.ydb.enable-signed-datetimes=true`.

Отдельно вызывать `registerYdbDialect()` и вручную собирать `DatabaseConfig` обычно не требуется.

Если приложению нужен собственный `Database` или собственная конфигурация Exposed, их можно объявить вручную в виде Spring bean'ов. В этом случае starter не мешает такому сценарию, а только добавляет YDB-специфичную обвязку вокруг стандартной интеграции Exposed.

### Автоматическая генерация схемы

`spring.exposed.generate-ddl=true` работает так же, как и в official Exposed starter: при старте приложения Exposed создаёт схему на основе обнаруженных классов `Table`.

Для YDB здесь есть важный нюанс: таблицы, которые должны создаваться через DDL, нужно объявлять через обычный `Table` с переопределением:

```kotlin
override fun createStatement(): List<String> = createYdbStatement()
```

{% note warning %}

Если включить `spring.exposed.generate-ddl=true` и оставить таблицы на plain `Table` или `IdTable` без этого override, Exposed сгенерирует inline `PRIMARY KEY`, который YDB не принимает.

{% endnote %}

### Транзакции в Spring

Для стандартной интеграции со Spring используйте `@Transactional`:

```kotlin
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class ProductService {
    @Transactional
    fun renameProduct(id: Int, newName: String) {
        // Exposed DSL
    }
}
```

`@Transactional` даёт обычную Spring-интеграцию Exposed, но не добавляет YDB retry policy для optimistic concurrency conflicts.

Если требуется retry-aware путь с автоматическими повторами на retryable-ошибках YDB, используйте бин `YdbTransactionOperations`:

```kotlin
import org.springframework.stereotype.Service
import tech.ydb.exposed.dialect.YdbRetryConfig
import tech.ydb.exposed.dialect.spring.boot.autoconfigure.YdbTransactionOperations

@Service
class RetryableProductService(
    private val ydbTx: YdbTransactionOperations
) {
    fun saveOrUpdate() = ydbTx.execute(retry = YdbRetryConfig.IDEMPOTENT) {
        // Exposed DSL
    }
}
```

Обычно для одного участка кода выбирают один из двух путей:

- либо `@Transactional`;
- либо `YdbTransactionOperations.execute(...)`, если нужны автоматические повторы.

## Ограничения и особенности {#limitations}

- Exposed 1.3.0 не предоставляет dialect hook для рендера single-column `PRIMARY KEY` внутри `CREATE TABLE`;
- поэтому DDL-путь для YDB реализован как workaround через `createStatement()` и `createYdbStatement()`;
- каждая таблица, создаваемая через Exposed DDL в YDB, должна иметь `PRIMARY KEY`;
- `SchemaUtils.createMissingTablesAndColumns(...)` и `spring.exposed.generate-ddl=true` требуют такого же override `createStatement()`;
- ANSI `MERGE` не поддерживается;
- `upsert(where)` и PostgreSQL-подобная логика `ON CONFLICT DO UPDATE` не поддерживаются;
- functional indexes не поддерживаются;
- `GLOBAL UNIQUE` через `ALTER TABLE ... ADD INDEX ...` зависит от ограничений конкретной версии YDB;
- полный generic diff через `MigrationUtils.statementsRequiredForDatabaseMigration(...)` в Exposed 1.3.0 для YDB работает не полностью из-за неподдерживаемой metadata по `CHECK`-ограничениям.
