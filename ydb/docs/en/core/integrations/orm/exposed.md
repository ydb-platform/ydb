# Dialect {{ ydb-short-name }} for Kotlin Exposed

## Introduction {#overview}

This guide describes using [JetBrains Exposed](https://github.com/JetBrains/Exposed) with {{ ydb-short-name }}.

Exposed is a Kotlin library for working with a database via SQL DSL and DAO API on top of JDBC. The {{ ydb-short-name }} dialect adds support for {{ ydb-short-name }}-compatible SQL, {{ ydb-short-name }} data type mapping, retry-aware transactions for optimistic concurrency control, and {{ ydb-short-name }}-specific schema features.

The guide covers two practical scenarios:

- Main JDBC dialect for Exposed
- Example of custom configuration for Spring Boot 3 applications.

## Installing the {{ ydb-short-name }} dialect {#install-dialect}

Examples for various build systems:

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
      // Specify the current versions
      implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
      implementation "tech.ydb.dialects:kotlin-exposed-ydb-dialect:$ydbDialectVersion"
  }
  ```

{% endlist %}

Current versions of artifacts:

- [`tech.ydb.dialects:kotlin-exposed-ydb-dialect`](https://central.sonatype.com/artifact/tech.ydb.dialects/kotlin-exposed-ydb-dialect) on Maven Central
- [`tech.ydb.jdbc:ydb-jdbc-driver`](https://central.sonatype.com/artifact/tech.ydb.jdbc/ydb-jdbc-driver) — same as above.

If the application uses DAO, JSON DSL, or other Exposed modules, connect them in the standard way together with the main dialect.

## Connection and configuration {#configuration-dialect}

First, register the dialect and driver, then open `Database` through a regular `Database.connect(...)` and pass the recommended values `DatabaseConfig`:


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

- Registers the JDBC driver `tech.ydb.jdbc.YdbDriver` for URLs with the prefix `jdbc:ydb:`.
- registers `YdbDialect` in Exposed
- registers a metadata bridge for reading existing secondary indexes via JDBC metadata.

Recommended Exposed configuration for {{ ydb-short-name }} includes:

- `useNestedTransactions = false` — a setting on the Exposed side that prevents Exposed from emulating nested transactions. {{ ydb-short-name }} does not support nested transactions, so such emulation would lead to errors.
- registration of `YdbDialect` through `registerYdbDialect(...)` before opening `Database`.

## DDL features in {{ ydb-short-name }} {#ddl}

Basic usage of Exposed — defining tables, DSL/DAO queries, transactions, etc. — is described in the [official JetBrains Exposed documentation](https://www.jetbrains.com/help/exposed/home.html). This section only covers {{ ydb-short-name }}-specific features of DDL dialect generation.

### Creating tables {#tables}

In {{ ydb-short-name }} `CREATE TABLE` must contain a table `PRIMARY KEY (...)`. The standard Exposed 1.3.0 DDL path for tables with a single-column PK can generate an inline form:


```sql
id Int32 PRIMARY KEY
```


{{ ydb-short-name }} does not accept this form. Therefore, for tables that should be created via Exposed DDL, you need to override `createStatement()` and call `createYdbStatement()`:


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

- renders all columns without inline `PRIMARY KEY`
- Adds a table `PRIMARY KEY (...)`
- saves `NOT NULL` and `DEFAULT`
- preserves `storageParameters`, therefore {{ ydb-short-name }}-specific `WITH (...)` can be described through the standard Exposed API.

Schema creation is performed using standard Exposed tools:


```kotlin
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import tech.ydb.exposed.dialect.ydbTransaction

ydbTransaction(db) {
    SchemaUtils.create(Products)
}
```


### {{ ydb-short-name }}-specific table parameters {#table-options}

TTL and other {{ ydb-short-name }}-specific table options are set via `storageParameters`:


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


Secondary indexes declared via the standard `Table.index(...)` are created separately via:


```sql
ALTER TABLE ... ADD INDEX ... GLOBAL
```


#### Arbitrary YQL via `createYdbStatement()` {#custom-yql}

`createYdbStatement()` returns `List<String>`, so you can add arbitrary YQL statements to the generated DDL — for example, `CREATE TABLE` of a columnar table, `ALTER TABLE ADD INDEX ... LOCAL`, or any other DDL that the dialect does not cover first-class.


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


This is a convenient way to describe tables for which the Exposed DSL does not cover the required {{ ydb-short-name }} syntax. To manage the schema as a whole, it is recommended to use [Flyway](../migration/flyway.md) or [Liquibase](../migration/liquibase.md).

## Read and write operations {#dml}

Standard Exposed DSL operations (`select`, `insert`, `update`, `deleteWhere`) work without additional modifications.

The dialect also uses native operations {{ ydb-short-name }} `UPSERT` and `REPLACE` through extensions `Table.upsert(...)`, `Table.batchUpsert(...)`, and `Table.replace(...)`:


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


The semantics correspond to the YQL statements [`UPSERT INTO`](../../yql/reference/syntax/upsert_into.md) and [`REPLACE INTO`](../../yql/reference/syntax/replace_into.md).
Exposed-specific parameters `onUpdate` and `keyColumns` for `upsert(...)` are ignored, and `upsert(where)` and PostgreSQL-like logic `ON CONFLICT DO UPDATE` are not supported.

{% note warning %}

The ANSI `MERGE` dialect is intentionally not supported. In {{ ydb-short-name }} scenarios, its role is performed by `UPSERT` and `REPLACE`.

{% endnote %}

## Repeatable transactions {#retryable-transactions}

{{ ydb-short-name }} uses optimistic concurrency control, so a transaction may end with a retryable error and require re-execution. For this, the dialect provides `ydbTransaction(...)`:


```kotlin
import tech.ydb.exposed.dialect.YdbRetryConfig
import tech.ydb.exposed.dialect.ydbTransaction

ydbTransaction(db) {
    // regular read-write transaction
}

ydbTransaction(db, retry = YdbRetryConfig.IDEMPOTENT) {
    // safely repeatable operation
}

ydbTransaction(db, readOnly = true, retry = YdbRetryConfig.IDEMPOTENT) {
    // read-only scenario
}
```


`YdbRetryConfig.IDEMPOTENT` should only be used when the transaction body can be safely re-executed.

## Data types {#types}

Table mapping standard Exposed types to [{{ ydb-short-name }} types](../../yql/reference/types/primitive.md):

| Exposed | {{ ydb-short-name }} |
| --- | --- |
| `byte` / `ubyte` | `Int8` / `Uint8` |
| `short` / `ushort` | `Int16` / `Uint16` |
| `integer` / `uinteger` | `Int32` / `Uint32` |
| `long` | `Int64` |
| `float` / `double` | `Float` / `Double` |
| `bool` | `Bool` |
| `varchar` / `text` | `Text` |
| `binary` / `blob` | `Bytes` |
| `uuid` | `Uuid` |
| `date` | `Date` or `Date32` |
| `datetime` | `Datetime` or `Datetime64` |
| `timestamp` | `Timestamp` or `Timestamp64` |
| `json` | `Json` |
| `jsonb` | `JsonDocument` |

Features:

- `varchar(n)` maps to `Text`; the length constraint is not encoded in {{ ydb-short-name }} DDL;
- `jsonb` corresponds to `JsonDocument` — this is a binary JSON representation optimized for efficient retrieval via `JSON_VALUE` / `JSON_EXISTS` / `JSON_QUERY`. `JsonDocument` itself cannot be used as a B-tree index key (in {{ ydb-short-name }}, JSON is not a comparable type);
- `autoIncrement()` maps to `Serial` or `BigSerial`.

### {{ ydb-short-name }}-specific types

The dialect adds a set of extensions for columns:


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


Mapping of {{ ydb-short-name }} types and Kotlin runtime types:

| Function | {{ ydb-short-name }} type | Kotlin type |
| --- | --- | --- |
| `ydbDecimal(name, p, s)` | `Decimal(p, s)` | `java.math.BigDecimal` |
| `ydbInterval(name)` | `Interval` | `java.time.Duration` |
| `ydbInterval64(name)` | `Interval64` | `java.time.Duration` |
| `ydbJson(name)` | `Json` | `String` |
| `ydbJsonDocument(name)` | `JsonDocument` | `String` |
| `ydbUuid(name)` | `Uuid` | `java.util.UUID` |
| `ydbUbyte(name)` | `Uint8` | `kotlin.UByte` |
| `ydbUshort(name)` | `Uint16` | `kotlin.UShort` |
| `ydbUint32(name)` | `Uint32` | `kotlin.UInt` |
| `ydbUint64(name)` | `Uint64` | `kotlin.Long` (`0..Long.MAX_VALUE`) |
| `ydbUlong(name)` | `Uint64` | `kotlin.ULong` |

For literal values of `Decimal` in update expressions, `ydbDecimalLiteral(...)` is available.

### Explicit selection of temporal type at the column level


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

`registerYdbDialect(enableSignedDatetimes = true)` changes DDL names only for standard Exposed `date`, `datetime`, and `timestamp`. Explicit `ydbDate(...)`, `ydbDate32(...)`, `ydbDatetime(...)`, `ydbDatetime64(...)`, `ydbTimestamp(...)`, and `ydbTimestamp64(...)` always generate the type reflected in the function name.

{% endnote %}

{% note warning %}

`ydbUint64(...)` stores a value in Kotlin `Long`, so only the `0..Long.MAX_VALUE` range is supported. For values above `Long.MAX_VALUE`, use `ydbUlong(...)` — it is represented by the Kotlin type `ULong` and covers the entire YQL `Uint64` range.

{% endnote %}

## Integration with Spring Boot 3 {#spring-boot-3}

For Spring Boot 3, it is recommended to use the official Exposed scenario from the [Spring Boot 3 integration](https://www.jetbrains.com/help/exposed/spring-boot-integration.html#spring-boot-3) documentation and add a small {{ ydb-short-name }}-specific configuration to the application.

The logic of the previously existing starter module can serve as a reference for such manual configuration: in the application, you need to explicitly register the dialect, explicitly specify the {{ ydb-short-name }} JDBC driver, match `forceSignedDatetimes=...` in the URL, and create `Database` from Spring-managed `DataSource`.

### Dependencies

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
      // Specify the current version
      implementation "org.jetbrains.exposed:exposed-spring-boot-starter:$exposedVersion"
      implementation "tech.ydb.dialects:kotlin-exposed-ydb-dialect:$ydbDialectVersion"
      implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
  }
  ```

{% endlist %}

### Basic Spring Boot configuration


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


The standard properties of the official Exposed starter remain available, including:

- `spring.exposed.generate-ddl`
- `spring.exposed.excluded-packages`
- `spring.exposed.show-sql`

The example above uses a {{ ydb-short-name }}-specific property:

- `spring.exposed.ydb.enable-signed-datetimes`

In this variant, the property name `spring.exposed.ydb.enable-signed-datetimes` is chosen by the application itself by convention with the rest of the configuration. It can be replaced with any other name if more convenient. If signed mode is enabled, the value in this property must be aligned with the JDBC URL, i.e., use `forceSignedDatetimes=true`.

### Custom configuration example

Below is a minimal example of `@Configuration` that adds {{ ydb-short-name }}-specific steps to the standard Spring Boot Exposed integration:

- Calls `registerYdbDialect(...)`.
- Sets the recommended values of `DatabaseConfig`.
- Creates a bean `Database` from Spring-managed `DataSource`.


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


If the application already has its own logic for creating `DataSource` or `Database`, the same steps can be moved into it. The key {{ ydb-short-name }}-specific actions here remain the same: register the dialect, manually align `forceSignedDatetimes` in the JDBC URL, explicitly specify `tech.ydb.jdbc.YdbDriver`, and pass the recommended `DatabaseConfig` to Exposed.

Thus, in a typical case, it is sufficient to:

1. Add `exposed-spring-boot-starter`, `kotlin-exposed-ydb-dialect`, and `ydb-jdbc-driver`.
2. Specify `spring.datasource.url` starting with `jdbc:ydb:` and containing `forceSignedDatetimes=...`.
3. Add a custom `@Configuration` similar to the example above.
4. Enable `spring.exposed.ydb.enable-signed-datetimes=true` if necessary.

That is, the {{ ydb-short-name }} integration for Spring Boot is described here as an explicit application configuration, not as a separate starter artifact.

### Automatic schema generation

`spring.exposed.generate-ddl=true` works the same as in the official Exposed starter: on application startup, Exposed creates a schema based on the discovered `Table` classes.

For {{ ydb-short-name }}, there is an important nuance here: tables that should be created via DDL must be declared using a regular `Table` with an override:


```kotlin
override fun createStatement(): List<String> = createYdbStatement()
```


{% note warning %}

If you enable `spring.exposed.generate-ddl=true` and leave tables on plain `Table` or `IdTable` without this override, Exposed will generate an inline `PRIMARY KEY` that {{ ydb-short-name }} does not accept.

{% endnote %}

### Transactions in Spring

For standard Spring integration, use `@Transactional`:


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


`@Transactional` provides the usual Spring Exposed integration, but does not add a {{ ydb-short-name }} retry policy for optimistic concurrency conflicts.

If a retry-aware path with automatic retries on {{ ydb-short-name }} retryable errors is required, in your Spring service you can call `ydbTransaction(...)` directly, passing the bean `Database`:


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


Typically, for one piece of code, one of two paths is chosen:

- Either `@Transactional`.
- Or `ydbTransaction(database, ...)` if automatic retries are needed.

## Schema management {#schema-management}

For production scenarios, it is recommended to consider Exposed DDL generation as a supplementary, not primary, path. Typically, the schema:

1. Is described by separate SQL scripts.
2. Is migrated by an external versioning tool — for example, [Flyway](../migration/flyway.md) or [Liquibase](../migration/liquibase.md).
3. Is reconciled with the Exposed model on the application side.

If the application uses schema validation or migration diff generation via Exposed, add:

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

In Exposed 1.3.0, the full path `MigrationUtils.statementsRequiredForDatabaseMigration(...)` unconditionally reads metadata about `CHECK` constraints from `INFORMATION_SCHEMA.CHECK_CONSTRAINTS`. The current JDBC driver {{ ydb-short-name }} does not provide this part of the metadata, so the full generic Exposed diff for {{ ydb-short-name }} does not work completely.

A practical {{ ydb-short-name }}-compatible path:

- Apply the schema using external migrations.
- Use Exposed table definitions as a description of the client model.
- Validate drift through compatible Exposed metadata paths:

  - Existing columns.
  - Existing secondary indexes.

The dialect repository has integration coverage for a scenario where the schema is created with raw SQL and then reconciled with the Exposed model without failing on unsupported metadata.

{% note info %}

If the schema was just changed by raw SQL, perform validation in a new transaction so that Exposed does not work with stale metadata cache.

{% endnote %}

### Signed temporal types in DDL {#signed-temporal-types}

`enableSignedDatetimes` refers exclusively to schema generation via Exposed DDL: it changes DDL names for the standard Exposed types `date()`, `datetime()`, and `timestamp()`. This flag itself does not change anything for existing tables or for runtime binding of individual columns.

{% note info %}

Since Exposed DDL is a test/dev path, the `enableSignedDatetimes` flag also primarily relates to tests and local development. In production, temporal types are fixed in versioned SQL migrations.

{% endnote %}

By default (`enableSignedDatetimes = false`), the standard Exposed temporal types appear in the generated `CREATE TABLE` as:

- `Date`
- `Datetime`
- `Timestamp`

If signed mode is needed in Exposed DDL, configure it in two places: enable `enableSignedDatetimes` on the dialect (this changes the DDL of standard temporal types) and add `forceSignedDatetimes=true` to the JDBC URL (this changes the binding on the driver side):


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


In this mode, the standard Exposed types `date`, `datetime`, and `timestamp` will appear in the generated `CREATE TABLE` as `Date32`, `Datetime64`, and `Timestamp64`.

If you need explicit control of an individual column type, use the extensions `ydbDate(...)`, `ydbDate32(...)`, `ydbDatetime(...)`, `ydbDatetime64(...)`, `ydbTimestamp(...)`, and `ydbTimestamp64(...)`. They always generate DDL and bind the parameter via JDBC vendor code of exactly the type specified in the function name, regardless of `enableSignedDatetimes`.

If the JDBC URL already has other parameters, the flag must be added via `&forceSignedDatetimes=true`.

## Limitations and specifics {#limitations}

- Exposed 1.3.0 does not provide a dialect hook for rendering a single-column `PRIMARY KEY` inside `CREATE TABLE`.
- therefore, the DDL path for {{ ydb-short-name }} is implemented as a workaround via `createStatement()` and `createYdbStatement()`.
- each table created via Exposed DDL in {{ ydb-short-name }} must have `PRIMARY KEY`.
- `SchemaUtils.createMissingTablesAndColumns(...)` and `spring.exposed.generate-ddl=true` require the same override `createStatement()`.
- ANSI `MERGE` is not supported.
- `upsert(where)` and PostgreSQL-like logic `ON CONFLICT DO UPDATE` are not supported.
- functional indexes are not supported.
- `GLOBAL UNIQUE` via `ALTER TABLE ... ADD INDEX ...` depends on the limitations of the specific {{ ydb-short-name }} version.
- `LOCAL` indexes and column-oriented tables (`STORE = COLUMN`) are not supported first-class in the dialect; if necessary, they can be attached via arbitrary YQL in [`createYdbStatement()`](#custom-yql) (test-only) — the dialect is oriented towards row-oriented OLTP tables.
- full generic diff via `MigrationUtils.statementsRequiredForDatabaseMigration(...)` in Exposed 1.3.0 for {{ ydb-short-name }} does not work completely due to unsupported metadata on `CHECK` constraints.
