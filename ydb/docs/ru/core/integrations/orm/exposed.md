# Диалект {{ ydb-short-name }} для Kotlin Exposed

## Введение {#overview}

Это руководство описывает использование [JetBrains Exposed](https://github.com/JetBrains/Exposed) с {{ ydb-short-name }}.

Exposed - это Kotlin-библиотека для работы с базой данных через SQL DSL и DAO API поверх JDBC. Диалект {{ ydb-short-name }} добавляет в Exposed поддержку {{ ydb-short-name }}-совместимого SQL, отображение типов данных {{ ydb-short-name }}, retry-aware транзакции для optimistic concurrency control и {{ ydb-short-name }}-специфичные особенности схемы.

Руководство охватывает два практических сценария:

- основной JDBC-диалект для Exposed;
- пример собственной конфигурации для приложений на Spring Boot 3.

## Установка диалекта {{ ydb-short-name }} {#install-dialect}

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

Актуальные версии артефактов:

- [`tech.ydb.dialects:kotlin-exposed-ydb-dialect`](https://central.sonatype.com/artifact/tech.ydb.dialects/kotlin-exposed-ydb-dialect) на Maven Central;
- [`tech.ydb.jdbc:ydb-jdbc-driver`](https://central.sonatype.com/artifact/tech.ydb.jdbc/ydb-jdbc-driver) — там же.

Если приложение использует DAO, JSON DSL или другие модули Exposed, подключайте их стандартным способом вместе с основным диалектом.

## Подключение и конфигурация {#configuration-dialect}

Сначала зарегистрируйте диалект и драйвер, затем откройте `Database` через обычный `Database.connect(...)` и передайте рекомендуемые значения `DatabaseConfig`:

```kotlin
import org.jetbrains.exposed.v1.core.DatabaseConfig
import org.jetbrains.exposed.v1.jdbc.Database
import tech.ydb.exposed.dialect.registerYdbDialect
import java.sql.Connection

registerYdbDialect()

val db = Database.connect(
    url = "jdbc:ydb:grpc://localhost:2136/local",
    driver = "tech.ydb.jdbc.YdbDriver",
    databaseConfig = DatabaseConfig {
        useNestedTransactions = false
    }
)
```

`registerYdbDialect()`:

- регистрирует JDBC-драйвер `tech.ydb.jdbc.YdbDriver` для URL с префиксом `jdbc:ydb:`;
- регистрирует `YdbDialect` в Exposed;
- регистрирует metadata bridge для чтения существующих secondary indexes через JDBC metadata.

Рекомендуемая конфигурация Exposed для {{ ydb-short-name }} включает:

- `useNestedTransactions = false` — это настройка на стороне Exposed, она запрещает Exposed эмулировать вложенные транзакции. {{ ydb-short-name }} вложенные транзакции не поддерживает, поэтому такая эмуляция приводила бы к ошибкам;
- регистрацию `YdbDialect` через `registerYdbDialect(...)` до открытия `Database`.

## Особенности DDL в {{ ydb-short-name }} {#ddl}

Базовое использование Exposed — определение таблиц, DSL/DAO-запросы, транзакции и т.д. — описано в [официальной документации JetBrains Exposed](https://www.jetbrains.com/help/exposed/home.html). В этом разделе собраны только {{ ydb-short-name }}-специфичные особенности генерации DDL диалектом.

### Создание таблиц {#tables}

В {{ ydb-short-name }} `CREATE TABLE` должен содержать табличный `PRIMARY KEY (...)`. Стандартный DDL-путь Exposed 1.3.0 для таблиц с одноколоночным PK может сгенерировать inline-форму:

```sql
id Int32 PRIMARY KEY
```

{{ ydb-short-name }} такую форму не принимает. Поэтому для таблиц, которые должны создаваться через Exposed DDL, необходимо переопределять `createStatement()` и вызывать `createYdbStatement()`:

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
- сохраняет `storageParameters`, поэтому {{ ydb-short-name }}-специфичные `WITH (...)` можно описывать через стандартный Exposed API.

Создание схемы выполняется обычными средствами Exposed:

```kotlin
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import tech.ydb.exposed.dialect.ydbTransaction

ydbTransaction(db) {
    SchemaUtils.create(Products)
}
```

### {{ ydb-short-name }}-специфичные параметры таблицы {#table-options}

TTL и другие {{ ydb-short-name }}-специфичные table options задаются через `storageParameters`:

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

#### Произвольный YQL через `createYdbStatement()` {#custom-yql}

`createYdbStatement()` возвращает `List<String>`, поэтому к сгенерированному DDL можно добавить произвольные YQL-операторы — например, `CREATE TABLE` колоночной таблицы, `ALTER TABLE ADD INDEX ... LOCAL` или любой другой DDL, который диалект не покрывает first-class.

```kotlin
import org.jetbrains.exposed.v1.core.PrimaryKey
import org.jetbrains.exposed.v1.core.Table
import tech.ydb.exposed.dialect.createYdbStatement

object EventsColumn : Table("events_column") {
    val id = integer("id")
    val name = varchar("name", 255)

    override val primaryKey = PrimaryKey(id)

    override fun createStatement(): List<String> = createYdbStatement() + listOf(
        """
        CREATE TABLE events_column_olap (
            id Int32 NOT NULL,
            payload Json,
            PRIMARY KEY (id)
        ) WITH (STORE = COLUMN)
        """.trimIndent()
    )
}
```

Это удобный способ описать таблицы, для которых Exposed DSL не покрывает нужный {{ ydb-short-name }}-синтаксис. Для управления схемой в целом рекомендуется использовать [Flyway](../migration/flyway.md) или [Liquibase](../migration/liquibase.md).

## Операции чтения и записи {#dml}

Стандартные DSL-операции Exposed (`select`, `insert`, `update`, `deleteWhere`) работают без дополнительных изменений.

Диалект также использует native-операции {{ ydb-short-name }} `UPSERT` и `REPLACE` через расширения `Table.upsert(...)`, `Table.batchUpsert(...)` и `Table.replace(...)`:

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

Семантика соответствует YQL-операторам [`UPSERT INTO`](../../yql/reference/syntax/upsert_into.md) и [`REPLACE INTO`](../../yql/reference/syntax/replace_into.md).
Exposed-специфичные параметры `onUpdate` и `keyColumns` для `upsert(...)` игнорируются, а `upsert(where)` и PostgreSQL-подобная логика `ON CONFLICT DO UPDATE` не поддерживаются.

{% note warning %}

ANSI `MERGE` диалект намеренно не поддерживает. В сценариях {{ ydb-short-name }} его роль выполняют `UPSERT` и `REPLACE`.

{% endnote %}

## Повторяемые транзакции {#retryable-transactions}

{{ ydb-short-name }} использует optimistic concurrency control, поэтому транзакция может завершиться retryable-ошибкой и потребовать повторного выполнения. Для этого диалект предоставляет `ydbTransaction(...)`:

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

## Типы данных {#types}

Таблица отображения стандартных Exposed-типов в [типы {{ ydb-short-name }}](../../yql/reference/types/primitive.md):

| Exposed                | {{ ydb-short-name }}                |
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

- `varchar(n)` отображается в `Text`; ограничение длины не кодируется в {{ ydb-short-name }} DDL;
- `jsonb` соответствует `JsonDocument` — это бинарное представление JSON, оптимизированное для эффективного извлечения через `JSON_VALUE` / `JSON_EXISTS` / `JSON_QUERY`. Сам `JsonDocument` нельзя использовать как ключ B-Tree-индекса (в {{ ydb-short-name }} JSON не является comparable-типом);
- `autoIncrement()` отображается в `Serial` или `BigSerial`.

### {{ ydb-short-name }}-специфичные типы

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
import tech.ydb.exposed.dialect.ydbUshort
import tech.ydb.exposed.dialect.ydbUlong
import tech.ydb.exposed.dialect.ydbUuid

val amount = ydbDecimal("amount", precision = 22, scale = 9)
val payload = ydbJson("payload")
val indexedPayload = ydbJsonDocument("indexed_payload")
val uid = ydbUuid("uid")
val flags = ydbUbyte("flags")
val counter = ydbUint32("counter")
val total = ydbUint64("total")
val someField = ydbUshort("some_field")
val totalFullRange = ydbUlong("total_full_range")
val duration = ydbInterval("duration")
val duration64 = ydbInterval64("duration64")
```

Соответствие {{ ydb-short-name }}-типов и Kotlin-типов рантайма:

| Функция                       | {{ ydb-short-name }}-тип        | Kotlin-тип                          |
|-------------------------------|----------------|-------------------------------------|
| `ydbDecimal(name, p, s)`      | `Decimal(p, s)`| `java.math.BigDecimal`              |
| `ydbInterval(name)`           | `Interval`     | `java.time.Duration`                |
| `ydbInterval64(name)`         | `Interval64`   | `java.time.Duration`                |
| `ydbJson(name)`               | `Json`         | `String`                            |
| `ydbJsonDocument(name)`       | `JsonDocument` | `String`                            |
| `ydbUuid(name)`               | `Uuid`         | `java.util.UUID`                    |
| `ydbUbyte(name)`              | `Uint8`        | `kotlin.UByte`                      |
| `ydbUshort(name)`             | `Uint16`       | `kotlin.UShort`                     |
| `ydbUint32(name)`             | `Uint32`       | `kotlin.UInt`                       |
| `ydbUint64(name)`             | `Uint64`       | `kotlin.Long` (`0..Long.MAX_VALUE`) |
| `ydbUlong(name)`              | `Uint64`       | `kotlin.ULong`                      |

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

`ydbUint64(...)` хранит значение в Kotlin `Long`, поэтому поддерживается только диапазон `0..Long.MAX_VALUE`. Для значений выше `Long.MAX_VALUE` используйте `ydbUlong(...)` — он представлен Kotlin-типом `ULong` и покрывает весь диапазон YQL `Uint64`.

{% endnote %}

## Интеграция со Spring Boot 3 {#spring-boot-3}

Для Spring Boot 3 рекомендуется использовать официальный сценарий Exposed из документации по [интеграции со Spring Boot 3](https://www.jetbrains.com/help/exposed/spring-boot-integration.html#spring-boot-3) и добавить в приложение небольшую {{ ydb-short-name }}-специфичную конфигурацию.

Логика ранее существовавшего starter-модуля может служить референсом для такой ручной настройки: в приложении нужно явно зарегистрировать диалект, явно указать {{ ydb-short-name }} JDBC driver, согласовать `forceSignedDatetimes=...` в URL и создать `Database` из Spring-managed `DataSource`.

### Зависимости

{% list tabs %}

- Maven

  ```xml
  <!-- Укажите актуальную версию -->
  <dependency>
      <groupId>org.jetbrains.exposed</groupId>
      <artifactId>exposed-spring-boot-starter</artifactId>
      <version>${exposed.version}</version>
  </dependency>
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
      // Укажите актуальную версию
      implementation "org.jetbrains.exposed:exposed-spring-boot-starter:$exposedVersion"
      implementation "tech.ydb.dialects:kotlin-exposed-ydb-dialect:$ydbDialectVersion"
      implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
  }
  ```

{% endlist %}

### Базовая конфигурация Spring Boot

```yaml
spring:
  datasource:
    url: jdbc:ydb:grpc://localhost:2136/local?forceSignedDatetimes=false
    driver-class-name: tech.ydb.jdbc.YdbDriver
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

В примере выше используется {{ ydb-short-name }}-специфичное свойство:

- `spring.exposed.ydb.enable-signed-datetimes`

В этом варианте имя свойства `spring.exposed.ydb.enable-signed-datetimes` выбирается самим приложением по соглашению с остальной конфигурацией. Его можно заменить на любое другое, если так удобнее. Если signed-режим включён, значение в этом свойстве нужно согласовать с JDBC URL, то есть использовать `forceSignedDatetimes=true`.

### Пример собственной конфигурации

Ниже показан минимальный пример `@Configuration`, который добавляет к стандартной Spring Boot интеграции Exposed {{ ydb-short-name }}-специфичные шаги:

- вызывает `registerYdbDialect(...)`;
- задаёт рекомендуемые значения `DatabaseConfig`;
- создаёт бин `Database` из Spring-managed `DataSource`.

```kotlin
import org.jetbrains.exposed.v1.core.DatabaseConfig
import org.jetbrains.exposed.v1.jdbc.Database
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import tech.ydb.exposed.dialect.registerYdbDialect
import java.sql.Connection
import javax.sql.DataSource

@Configuration
class YdbExposedConfiguration(
    @Value("\${spring.exposed.ydb.enable-signed-datetimes:false}")
    private val enableSignedDatetimes: Boolean
) {
    @Bean
    fun ydbDialectRegistration(): InitializingBean = InitializingBean {
        registerYdbDialect(enableSignedDatetimes = enableSignedDatetimes)
    }

    @Bean
    fun databaseConfig(): DatabaseConfig = DatabaseConfig {
        defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
        defaultReadOnly = false
        useNestedTransactions = false
    }

    @Bean
    @DependsOn("ydbDialectRegistration")
    fun database(dataSource: DataSource, databaseConfig: DatabaseConfig): Database =
        Database.connect(
            datasource = dataSource,
            databaseConfig = databaseConfig
        )
}
```

Если в приложении уже есть собственная логика создания `DataSource` или `Database`, те же шаги можно перенести и в неё. Ключевые {{ ydb-short-name }}-специфичные действия здесь остаются теми же: зарегистрировать диалект, вручную согласовать `forceSignedDatetimes` в JDBC URL, явно указать `tech.ydb.jdbc.YdbDriver` и передать Exposed рекомендуемый `DatabaseConfig`.

Таким образом, в типовом случае достаточно:

1. подключить `exposed-spring-boot-starter`, `kotlin-exposed-ydb-dialect` и `ydb-jdbc-driver`;
2. указать `spring.datasource.url`, начинающийся с `jdbc:ydb:` и содержащий `forceSignedDatetimes=...`;
3. добавить собственный `@Configuration`, аналогичный примеру выше;
4. при необходимости включить `spring.exposed.ydb.enable-signed-datetimes=true`.

То есть {{ ydb-short-name }}-интеграция для Spring Boot здесь описывается как явная конфигурация приложения, а не как отдельный starter-артефакт.

### Автоматическая генерация схемы

`spring.exposed.generate-ddl=true` работает так же, как и в official Exposed starter: при старте приложения Exposed создаёт схему на основе обнаруженных классов `Table`.

Для {{ ydb-short-name }} здесь есть важный нюанс: таблицы, которые должны создаваться через DDL, нужно объявлять через обычный `Table` с переопределением:

```kotlin
override fun createStatement(): List<String> = createYdbStatement()
```

{% note warning %}

Если включить `spring.exposed.generate-ddl=true` и оставить таблицы на plain `Table` или `IdTable` без этого override, Exposed сгенерирует inline `PRIMARY KEY`, который {{ ydb-short-name }} не принимает.

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

`@Transactional` даёт обычную Spring-интеграцию Exposed, но не добавляет {{ ydb-short-name }} retry policy для optimistic concurrency conflicts.

Если требуется retry-aware путь с автоматическими повторами на retryable-ошибках {{ ydb-short-name }}, в своём Spring-сервисе можно вызывать `ydbTransaction(...)` напрямую, передавая бин `Database`:

```kotlin
import org.springframework.stereotype.Service
import org.jetbrains.exposed.v1.jdbc.Database
import tech.ydb.exposed.dialect.YdbRetryConfig
import tech.ydb.exposed.dialect.ydbTransaction

@Service
class RetryableProductService(
    private val database: Database
) {
    fun saveOrUpdate() =
        ydbTransaction(database, retry = YdbRetryConfig.IDEMPOTENT) {
            // Exposed DSL
        }
}
```

Обычно для одного участка кода выбирают один из двух путей:

- либо `@Transactional`;
- либо `ydbTransaction(database, ...)`, если нужны автоматические повторы.

## Управление схемой {#schema-management}

Для production-сценариев рекомендуется рассматривать DDL-генерацию Exposed как вспомогательный, а не основной путь. Обычно схема:

1. описывается отдельными SQL-скриптами;
2. мигрируется внешним инструментом с версионированием — например, [Flyway](../migration/flyway.md) или [Liquibase](../migration/liquibase.md);
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

В Exposed 1.3.0 полный путь `MigrationUtils.statementsRequiredForDatabaseMigration(...)` безусловно читает metadata по `CHECK`-constraint'ам из `INFORMATION_SCHEMA.CHECK_CONSTRAINTS`. Текущий JDBC-драйвер {{ ydb-short-name }} эту часть metadata не предоставляет, поэтому полный generic diff Exposed для {{ ydb-short-name }} работает не полностью.

Практический {{ ydb-short-name }}-совместимый путь:

- накатывать схему внешними миграциями;
- использовать Exposed table definitions как описание клиентской модели;
- валидировать drift через совместимые metadata-пути Exposed:
  - существующие колонки;
  - существующие secondary indexes.

В репозитории диалекта есть integration coverage для сценария, где схема создаётся raw SQL, а затем сверяется с Exposed-моделью без падения на unsupported metadata.

{% note info %}

Если схема была только что изменена raw SQL, выполняйте validation в новой транзакции, чтобы Exposed не работал со stale metadata cache.

{% endnote %}

### Signed temporal types в DDL {#signed-temporal-types}

`enableSignedDatetimes` относится исключительно к генерации схемы через Exposed DDL: он меняет DDL-имена для стандартных Exposed-типов `date()`, `datetime()` и `timestamp()`. На уже существующие таблицы и на runtime-binding отдельных колонок этот флаг сам по себе ничего не меняет.

{% note info %}

Поскольку Exposed DDL — это test/dev-путь, флаг `enableSignedDatetimes` тоже относится в первую очередь к тестам и локальной разработке. В production temporal-типы фиксируются в версионируемых SQL-миграциях.

{% endnote %}

По умолчанию (`enableSignedDatetimes = false`) стандартные Exposed temporal-типы попадают в сгенерированный `CREATE TABLE` как:

- `Date`
- `Datetime`
- `Timestamp`

Если в Exposed DDL нужен signed-режим, согласуйте его в двух местах: включите `enableSignedDatetimes` у диалекта (это меняет DDL стандартных temporal-типов) и добавьте `forceSignedDatetimes=true` в JDBC URL (это меняет binding на стороне драйвера):

```kotlin
registerYdbDialect(enableSignedDatetimes = true)

val db = Database.connect(
    url = "jdbc:ydb:grpc://localhost:2136/local?forceSignedDatetimes=true",
    driver = "tech.ydb.jdbc.YdbDriver",
    databaseConfig = DatabaseConfig {
        defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
        defaultReadOnly = false
        useNestedTransactions = false
    }
)
```

В этом режиме стандартные Exposed-типы `date`, `datetime` и `timestamp` будут попадать в сгенерированный `CREATE TABLE` как `Date32`, `Datetime64` и `Timestamp64`.

Если нужен явный контроль типа отдельной колонки, используйте расширения `ydbDate(...)`, `ydbDate32(...)`, `ydbDatetime(...)`, `ydbDatetime64(...)`, `ydbTimestamp(...)` и `ydbTimestamp64(...)`. Они всегда генерируют DDL и привязывают параметр через JDBC vendor code именно того типа, который указан в имени функции, независимо от `enableSignedDatetimes`.

Если в JDBC URL уже есть другие параметры, флаг нужно добавлять через `&forceSignedDatetimes=true`.

## Ограничения и особенности {#limitations}

- Exposed 1.3.0 не предоставляет dialect hook для рендера single-column `PRIMARY KEY` внутри `CREATE TABLE`;
- поэтому DDL-путь для {{ ydb-short-name }} реализован как workaround через `createStatement()` и `createYdbStatement()`;
- каждая таблица, создаваемая через Exposed DDL в {{ ydb-short-name }}, должна иметь `PRIMARY KEY`;
- `SchemaUtils.createMissingTablesAndColumns(...)` и `spring.exposed.generate-ddl=true` требуют такого же override `createStatement()`;
- ANSI `MERGE` не поддерживается;
- `upsert(where)` и PostgreSQL-подобная логика `ON CONFLICT DO UPDATE` не поддерживаются;
- functional indexes не поддерживаются;
- `GLOBAL UNIQUE` через `ALTER TABLE ... ADD INDEX ...` зависит от ограничений конкретной версии {{ ydb-short-name }};
- `LOCAL` индексы и column-oriented таблицы (`STORE = COLUMN`) first-class в диалекте не поддерживаются; при необходимости их можно навесить через произвольный YQL в [`createYdbStatement()`](#custom-yql) (test-only) — диалект ориентирован на row-oriented OLTP-таблицы;
- полный generic diff через `MigrationUtils.statementsRequiredForDatabaseMigration(...)` в Exposed 1.3.0 для {{ ydb-short-name }} работает не полностью из-за неподдерживаемой metadata по `CHECK`-ограничениям.
