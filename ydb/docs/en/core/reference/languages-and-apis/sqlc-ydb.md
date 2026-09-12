# sqlc-ydb: typed code from YQL

[sqlc-ydb](https://github.com/ydb-platform/sqlc-ydb) generates typed code for {{ ydb-short-name }} from a schema and [YQL](../../yql/reference/index.md) queries. You write SQL; the tool creates methods that bind parameters and read results through the selected SDK or driver. It is an independent project following the familiar [sqlc](https://sqlc.dev/) workflow.

This guide takes two queries through generation and execution with version 0.1.0: nine languages and 18 SDK/framework profiles. Select your language and framework once: the selection is synchronized across all four tabbed sections.

## Install the tool {#install}

On Linux and macOS, run the commands below. Generation needs no Go installation, SDK or database connection. The installer selects the latest stable release, verifies SHA256 and places the executable in `~/.local/bin`. If PATH setup is needed, follow the installer’s instructions before running `sqlc-ydb version`.

```bash
curl -fsSL https://raw.githubusercontent.com/ydb-platform/sqlc-ydb/main/install.sh | bash
sqlc-ydb version
```

For Windows, use a [release archive](https://github.com/ydb-platform/sqlc-ydb/releases/tag/v0.1.0). To pin the version, pass `--version v0.1.0` to the installer with `bash -s -- --version v0.1.0`. See the [installation guide](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/docs/installation.md) for other options.

## Define the schema and queries {#inputs}

Create an empty project directory and save this schema, shared by all languages, as `schema.sql`:

```yql
CREATE TABLE authors (
    id Uint64 NOT NULL,
    name Utf8 NOT NULL,
    PRIMARY KEY (id)
);
```

Save these two queries as `queries.sql`. `UpsertAuthor` inserts an author or updates an existing author’s name; `GetAuthor` reads by primary key. Parameter names start with `$`; their types are inferred from the schema, so no `DECLARE` is needed here.

```yql
-- name: UpsertAuthor :exec
UPSERT INTO authors (id, name) VALUES ($author_id, $author_name);

-- name: GetAuthor :one
SELECT id, name FROM authors WHERE id = $author_id;
```

The `-- name:` comment sets the generated method name. `:exec` executes a statement without result rows; `:one` retrieves one row. Every example below uses these same input files.

## 1. Select a generator in sqlc.yaml {#configuration}

Save the configuration from the selected tab as `sqlc.yaml`. Input paths and the `out` directory are resolved relative to this file.

{% list tabs group=sqlc-language %}

- C++ {#lang-cpp}

  {% list tabs group=sqlc-cpp %}

  - YDB SDK {#cpp-ydb}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          cpp:
            out: "cpp/ydb"
            namespace: "authors"
            runtime: "ydb"
    ```

  - userver {#cpp-userver}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          cpp:
            out: "cpp/userver"
            namespace: "authors"
            runtime: "userver"
    ```

  {% endlist %}

- Go {#lang-go}

  {% list tabs group=sqlc-go %}

  - YDB SDK {#go-native}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          go:
            out: "go/native"
            package: "authors"
            sql_package: "ydb"
    ```

  - database/sql {#go-sql}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          go:
            out: "go/sql"
            package: "authors"
            sql_package: "database/sql"
    ```

  {% endlist %}

- Java {#lang-java}

  {% list tabs group=sqlc-java %}

  - YDB SDK {#java-ydb}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          java:
            out: "java/ydb"
            package: "authors"
            runtime: "ydb"
    ```

  - JDBC {#java-jdbc}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          java:
            out: "java/jdbc"
            package: "authors"
            runtime: "jdbc"
    ```

  - jOOQ {#java-jooq}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          java:
            out: "java/jooq"
            package: "authors"
            runtime: "jooq"
    ```

  {% endlist %}

- Python {#lang-python}

  {% list tabs group=sqlc-python %}

  - YDB SDK {#python-ydb}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          python:
            out: "python/ydb"
            runtime: "ydb"
    ```

  - DB-API {#python-dbapi}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          python:
            out: "python/dbapi"
            runtime: "dbapi"
    ```

  - SQLAlchemy {#python-sqlalchemy}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          python:
            out: "python/sqlalchemy"
            runtime: "sqlalchemy"
    ```

  {% endlist %}

- C# {#lang-csharp}

  {% list tabs group=sqlc-csharp %}

  - ADO.NET {#csharp-adonet}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          csharp:
            out: "csharp/adonet"
            namespace: "Authors"
            runtime: "adonet"
    ```

  - Dapper {#csharp-dapper}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          csharp:
            out: "csharp/dapper"
            namespace: "Authors"
            runtime: "dapper"
    ```

  {% endlist %}

- TypeScript {#lang-typescript}

  {% list tabs group=sqlc-typescript %}

  - YDB SDK {#typescript-ydb}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          typescript:
            out: "typescript"
            runtime: "ydb"
    ```

  {% endlist %}

- Rust {#lang-rust}

  {% list tabs group=sqlc-rust %}

  - YDB SDK {#rust-ydb}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          rust:
            out: "rust"
            runtime: "ydb"
    ```

  {% endlist %}

- PHP {#lang-php}

  {% list tabs group=sqlc-php %}

  - YDB SDK {#php-ydb}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          php:
            out: "php"
            namespace: "Authors"
            runtime: "ydb"
    ```

  {% endlist %}

- Kotlin {#lang-kotlin}

  {% list tabs group=sqlc-kotlin %}

  - YDB SDK {#kotlin-ydb}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          kotlin:
            out: "kotlin/ydb"
            package: "authors"
            runtime: "ydb"
    ```

  - JDBC {#kotlin-jdbc}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          kotlin:
            out: "kotlin/jdbc"
            package: "authors"
            runtime: "jdbc"
    ```

  - Exposed {#kotlin-exposed}

    ```yaml
    version: "2"
    sql:
      - engine: ydb
        schema: schema.sql
        queries: queries.sql
        gen:
          kotlin:
            out: "kotlin/exposed"
            package: "authors"
            runtime: "exposed"
    ```

  {% endlist %}

{% endlist %}

## 2. Generate code {#generate}

Run these commands next to `sqlc.yaml`. `compile` checks the inputs, `generate` writes code, and `diff` checks that saved files match generation. Add `diff` to CI: it exits with code 1 when files differ.

```bash
sqlc-ydb compile
sqlc-ydb generate
sqlc-ydb diff
```

The following excerpts show actual generated row types or read methods for the shared query. Imports and enclosing class declarations are omitted. Use the complete files in `out`; do not edit generated code manually.

{% list tabs group=sqlc-language %}

- C++ {#lang-cpp}

  {% list tabs group=sqlc-cpp %}

  - YDB SDK {#cpp-ydb}

    ```cpp
    struct GetAuthorRow final {
        std::uint64_t id;
        std::string name;
    };

        void UpsertAuthor(std::uint64_t author_id, const std::string& author_name) const;
        std::optional<GetAuthorRow> GetAuthor(std::uint64_t author_id) const;
    ```

  - userver {#cpp-userver}

    ```cpp
    struct GetAuthorRow final {
        std::uint64_t id;
        ::userver::ydb::Utf8 name;
    };

        void UpsertAuthor(std::uint64_t author_id, const ::userver::ydb::Utf8& author_name) const;
        std::optional<GetAuthorRow> GetAuthor(std::uint64_t author_id) const;
    ```

  {% endlist %}

- Go {#lang-go}

  {% list tabs group=sqlc-go %}

  - YDB SDK {#go-native}

    ```go
    // -- name: GetAuthor :one
    func (q *Queries) GetAuthor(ctx context.Context, arg uint64, opts ...query.ExecuteOption) (GetAuthorRow, error) {
        parameters := ydb.ParamsBuilder()
        parameters = parameters.Param("$author_id").Uint64(arg)

        callOptions := append([]query.ExecuteOption(nil), opts...)
        callOptions = append(callOptions, query.WithParameters(parameters.Build()))

        result, err := q.db.QueryRow(ctx, ""+
            "SELECT id, name FROM authors WHERE id = $author_id;",
            callOptions...,
        )
        if err != nil {
            return GetAuthorRow{}, err
        }

        var row GetAuthorRow
        if err := result.ScanNamed(
            query.Named("id", &row.ID),
            query.Named("name", &row.Name),
        ); err != nil {
            return GetAuthorRow{}, err
        }

        return row, nil
    }
    ```

  - database/sql {#go-sql}

    ```go
    // -- name: GetAuthor :one
    func (q *Queries) GetAuthor(ctx context.Context, arg uint64) (GetAuthorRow, error) {
        var row GetAuthorRow
        err := q.db.QueryRowContext(ctx, ""+
            "SELECT id, name FROM authors WHERE id = $author_id;",
            sql.Named("author_id", arg),
        ).Scan(
            &row.ID,
            &row.Name,
        )

        return row, err
    }
    ```

  {% endlist %}

- Java {#lang-java}

  {% list tabs group=sqlc-java %}

  - YDB SDK {#java-ydb}

    ```java
    public record GetAuthorRow(long id, String name) {}

        // -- name: GetAuthor :one
        public java.util.Optional<GetAuthorRow> getAuthor(long authorId) {
            var _params = Params.create();
            _params.put("$author_id", PrimitiveValue.newUint64(authorId));
            var _query = QueryReader.readFrom(
                    client.createQuery("""
                        SELECT id, name FROM authors WHERE id = $author_id;\
                        """, _params)).join().getValue();
            if (_query.getResultSetCount() != 1) throw new IllegalStateException("Expected one result set");
            var _rows = _query.getResultSet(0);
            if (!_rows.next()) return java.util.Optional.empty();
            long _value0 = _rows.getColumn(0).getUint64();
            String _value1 = _rows.getColumn(1).getText();
            return java.util.Optional.of(new GetAuthorRow(_value0, _value1));
        }
    ```

  - JDBC {#java-jdbc}

    ```java
    public record GetAuthorRow(long id, String name) {}

        // -- name: GetAuthor :one
        public java.util.Optional<GetAuthorRow> getAuthor(long authorId) throws java.sql.SQLException {
            try (var _prepared = client.prepareStatement("""
                SELECT id, name FROM authors WHERE id = ?;\
                """)) {
                _prepared.setObject(1, PrimitiveValue.newUint64(authorId));
                try (var _rows = _prepared.executeQuery()) {
                    if (!_rows.next()) return java.util.Optional.empty();
                    long _value0 = _rows.getLong(1);
                    String _value1 = _rows.getString(2);
                    return java.util.Optional.of(new GetAuthorRow(_value0, _value1));
                }
            }
        }
    ```

  - jOOQ {#java-jooq}

    ```java
    public record GetAuthorRow(ULong id, String name) {}

        // -- name: GetAuthor :one
        public Optional<GetAuthorRow> getAuthor(ULong authorId) {
            return dsl.select(AUTHORS.ID, AUTHORS.NAME)
                    .from(AUTHORS)
                    .where(AUTHORS.ID.eq(val(authorId, YdbTypes.UINT64)))
                    .fetchOptional(mapping(GetAuthorRow::new));
        }
    ```

  {% endlist %}

- Python {#lang-python}

  {% list tabs group=sqlc-python %}

  - YDB SDK {#python-ydb}

    ```python
    @dataclass
    class Authors:
        id: int
        name: str
    ```

  - DB-API {#python-dbapi}

    ```python
    @dataclass
    class Authors:
        id: int
        name: str
    ```

  - SQLAlchemy {#python-sqlalchemy}

    ```python
    @dataclass
    class Authors:
        id: int
        name: str
    ```

  {% endlist %}

- C# {#lang-csharp}

  {% list tabs group=sqlc-csharp %}

  - ADO.NET {#csharp-adonet}

    ```csharp
    public sealed record GetAuthorRow(
        ulong ID,
        string Name
    );

        // -- name: GetAuthor :one
        public async Task<GetAuthorRow> GetAuthorAsync(ulong authorId, CancellationToken cancellationToken = default)
        {
            await using var command = new YdbCommand(
                "SELECT id, name FROM authors WHERE id = $author_id;", _connection) { Transaction = _transaction };
            command.Parameters.Add(new YdbParameter("$author_id", DbType.UInt64, authorId));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException("query returned no rows");
            }
            return GetAuthorRowFrom(reader);
        }

        private static GetAuthorRow GetAuthorRowFrom(DbDataReader reader) => new(
            reader.GetFieldValue<ulong>(0),
            reader.GetFieldValue<string>(1)
        );
    ```

  - Dapper {#csharp-dapper}

    ```csharp
    public sealed record GetAuthorRow(
        ulong ID,
        string Name
    );

        // -- name: GetAuthor :one
        public async Task<GetAuthorRow> GetAuthorAsync(ulong authorId, CancellationToken cancellationToken = default, int? commandTimeout = null)
        {
            var parameters = new YdbParameters(
                new YdbParameter("$author_id", DbType.UInt64, authorId)
            );

            var command = new CommandDefinition(
                commandText: """
                SELECT id, name FROM authors WHERE id = $author_id;
                """,
                parameters: parameters,
                transaction: _transaction,
                commandTimeout: commandTimeout,
                cancellationToken: cancellationToken);

            return await _connection.QueryFirstAsync<GetAuthorRow>(command).ConfigureAwait(false);
        }
    ```

  {% endlist %}

- TypeScript {#lang-typescript}

  {% list tabs group=sqlc-typescript %}

  - YDB SDK {#typescript-ydb}

    ```typescript
    // -- name: GetAuthor :one
      async getAuthor(authorId: bigint, configure?: ConfigureQuery): Promise<GetAuthorRow | null> {
        const stmt = this.#sql<[GetAuthorRow]>`SELECT id, name FROM authors WHERE id = $author_id;`
          .parameter("author_id", new Uint64(authorId));
        configure?.(stmt);
        const [rows] = await stmt;

        return rows[0] ?? null;
      }
    ```

  {% endlist %}

- Rust {#lang-rust}

  {% list tabs group=sqlc-rust %}

  - YDB SDK {#rust-ydb}

    ```rust
    // -- name: GetAuthor :one
        #[builder(on(String, into))]
        pub async fn author(&mut self, author_id: u64) -> ydb::YdbResult<GetAuthorRow> {
            let mut row = self
                .client
                .query_row(r"SELECT id, name FROM authors WHERE id = $author_id;")
                .param("$author_id", author_id)
                .await?;
            Ok(GetAuthorRow {
                id: row.remove_field(0)?.try_into()?,
                name: row.remove_field(1)?.try_into()?,
            })
        }
    ```

  {% endlist %}

- PHP {#lang-php}

  {% list tabs group=sqlc-php %}

  - YDB SDK {#php-ydb}

    ```php
    final class GetAuthorRow
    {
        public function __construct(
            public readonly string $id,
            public readonly string $name
        ) {}
    }
    ```

  {% endlist %}

- Kotlin {#lang-kotlin}

  {% list tabs group=sqlc-kotlin %}

  - YDB SDK {#kotlin-ydb}

    ```kotlin
    data class GetAuthorRow(
        val id: Long,
        val name: String
    )

        // -- name: GetAuthor :one
        fun getAuthor(authorId: Long): GetAuthorRow? {
            val _params = Params.create()
            _params.put("\$author_id", PrimitiveValue.newUint64(authorId))
            val _query = if (transaction != null) {
                QueryReader.readFrom(transaction.createQuery(
                    "SELECT id, name FROM authors WHERE id = \$author_id;", _params)).join().getValue()
            } else {
                client!!.supplyResult { _session ->
                    QueryReader.readFrom(_session.createQuery(
                    "SELECT id, name FROM authors WHERE id = \$author_id;", TxMode.SERIALIZABLE_RW, _params))
                }.join().getValue()
            }
            kotlin.check(_query.getResultSetCount() == 1) { "Expected one result set" }
            val _rows = _query.getResultSet(0)
            if (!_rows.next()) return null
            val _value0: Long = _rows.getColumn(0).getUint64()
            val _value1: String = _rows.getColumn(1).getText()
            return GetAuthorRow(_value0, _value1)
        }
    ```

  - JDBC {#kotlin-jdbc}

    ```kotlin
    data class GetAuthorRow(
        val id: Long,
        val name: String
    )

        // -- name: GetAuthor :one
        fun getAuthor(authorId: Long): GetAuthorRow? {
            client.prepareStatement(
                "SELECT id, name FROM authors WHERE id = ?;").use { _prepared ->
                _prepared.setObject(1, PrimitiveValue.newUint64(authorId))
                _prepared.executeQuery().use { _rows ->
                    if (!_rows.next()) return null
                    val _value0: Long = _rows.getLong(1)
                    val _value1: String = _rows.getString(2)
                    return GetAuthorRow(_value0, _value1)
                }
            }
        }
    ```

  - Exposed {#kotlin-exposed}

    ```kotlin
    data class GetAuthorRow(
        val id: Long,
        val name: String
    )

        // -- name: GetAuthor :one
        fun getAuthor(authorId: Long): GetAuthorRow? {
            val _connection = client.connection.connection as java.sql.Connection
            _connection.prepareStatement(
                "SELECT id, name FROM authors WHERE id = ?;").use { _prepared ->
                _prepared.setObject(1, PrimitiveValue.newUint64(authorId))
                _prepared.executeQuery().use { _rows ->
                    if (!_rows.next()) return null
                    val _value0: Long = _rows.getLong(1)
                    val _value1: String = _rows.getString(2)
                    return GetAuthorRow(_value0, _value1)
                }
            }
        }
    ```

  {% endlist %}

{% endlist %}

## Prepare the application {#prepare}

The generator does not create database tables. Apply `schema.sql` using your migration tool or the [YDB CLI](../ydb-cli/sql.md), then add a row for the single-query example:

```yql
UPSERT INTO authors (id, name) VALUES (42, "Alice");
```

Add the selected SDK/driver dependencies and configure [authentication](../ydb-sdk/auth.md). The fragments below assume initialized connections or clients and imported generated code. See the [release examples](https://github.com/ydb-platform/sqlc-ydb/tree/v0.1.0/examples) for complete projects with dependencies and connection setup.

## 3. Execute a single query {#single-query}

Call the generated method for author 42:

{% list tabs group=sqlc-language %}

- C++ {#lang-cpp}

  {% list tabs group=sqlc-cpp %}

  - YDB SDK {#cpp-ydb}

    ```cpp
    authors::Queries queries{client};
    auto author = queries.GetAuthor(42);
    ```

    `client` is a configured `NYdb::NQuery::TQueryClient`; C++20 and the YDB C++ SDK are required. Standalone calls use SDK retries; `RetryQuerySync` retries the entire transaction. `Queries` does not finish a supplied transaction: the example commits or rolls it back explicitly. [Complete example](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/examples/authors/cpp/native/main.cpp).

  - userver {#cpp-userver}

    ```cpp
    authors::Queries queries{table_client};
    auto author = queries.GetAuthor(42);
    ```

    `table_client` is a `userver::ydb::TableClient` obtained from the YDB component; run this code in a userver coroutine. `RetryTx` retries the whole callback and finishes the transaction according to `TxAction`; return `kRollback` to discard changes. `Utf8` is userver's string type for YQL Utf8. [Complete example](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/examples/authors/cpp/userver/smoke_handler.cpp).

  {% endlist %}

- Go {#lang-go}

  {% list tabs group=sqlc-go %}

  - YDB SDK {#go-native}

    ```go
    q := authors.New(driver.Query())
    author, err := q.GetAuthor(ctx, 42)
    if err != nil {
        return err
    }
    fmt.Println(author.Name)
    ```

    `driver` is an initialized `*ydb.Driver`; `ctx` is a context. Import the generated package as `authors` and `github.com/ydb-platform/ydb-go-sdk/v3/query` for the transaction. These fragments belong in a function returning `error`.

  - database/sql {#go-sql}

    ```go
    q := authors.New(connection)
    author, err := q.GetAuthor(ctx, 42)
    if err != nil {
        return err
    }
    fmt.Println(author.Name)
    ```

    `connection` is an initialized `*sql.DB` using the YDB driver; `ctx` is a context. Import the generated package as `authors`. These fragments belong in a function returning `error`; retry the complete transaction block when needed.

  {% endlist %}

- Java {#lang-java}

  {% list tabs group=sqlc-java %}

  - YDB SDK {#java-ydb}

    ```java
    GetAuthorRow author = retry.supplyResult(session -> {
        var tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
        try {
            var row = new Queries(tx).getAuthor(42L).orElseThrow();
            return CompletableFuture.completedFuture(tx.commit().join().map(info -> row));
        } finally {
            if (tx.isActive()) {
                tx.rollback().join().expectSuccess();
            }
        }
    }).join().getValue();
    ```

    `retry` is an application-configured `SessionRetryContext`; import `TxMode`, `CompletableFuture`, and the generated `Queries` and `GetAuthorRow`. Even one Java native call uses an open transaction. `retry` reruns the whole function with a new session; the caller completes the transaction.

  - JDBC {#java-jdbc}

    ```java
    GetAuthorRow author = new Queries(connection).getAuthor(42L).orElseThrow();
    ```

    `connection` is a `java.sql.Connection` initially in autocommit mode, with no other active transaction. Import the generated `Queries` and `GetAuthorRow`. The transaction code restores autocommit after commit or rollback and propagates exceptions. The application can retry the whole transaction when appropriate.

  - jOOQ {#java-jooq}

    ```java
    GetAuthorRow author = new Queries(dsl).getAuthor(ULong.valueOf(42)).orElseThrow();
    ```

    `dsl` is a `YdbDSLContext`; import `YDB`, `ULong`, and the generated `Queries` and `GetAuthorRow`. The single call uses an autocommit connection. `transactionResult` commits on success and rolls back on an exception; `YDB.using(configuration)` uses the current transaction connection. The application handles retries around the whole transaction.

  {% endlist %}

- Python {#lang-python}

  {% list tabs group=sqlc-python %}

  - YDB SDK {#python-ydb}

    ```python
    import ydb
    from python.ydb.queries import Querier

    queries = Querier(pool, retry_settings=ydb.RetrySettings(idempotent=True))
    author = queries.get_author(42)
    ```

    `pool` is an initialized `ydb.QuerySessionPool`. `retry_tx_sync` retries the whole transaction and commits after the callback; the callback must have no external side effects. The repeated UPSERT in this example sets fixed values. [Runnable example](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/examples/authors/python/smoke.py).

  - DB-API {#python-dbapi}

    ```python
    from python.dbapi.queries import Querier

    author = Querier(connection).get_author(42)
    ```

    `connection` is an open `ydb_dbapi` connection with no active transaction. The driver defaults to AUTOCOMMIT: select SERIALIZABLE before `begin()` to share a transaction. The caller manages retries of the whole transaction. [Runnable example](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/examples/authors/python/smoke.py).

  - SQLAlchemy {#python-sqlalchemy}

    ```python
    from python.sqlalchemy.queries import Querier

    with engine.connect() as connection:
        author = Querier(connection).get_author(42)
    ```

    `engine` is an initialized SQLAlchemy Engine using the `yql+ydb` dialect. Set SERIALIZABLE before `begin()`: a transaction block alone does not override the driver's AUTOCOMMIT. The context commits or rolls back; the caller manages retries of the whole transaction. [Runnable example](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/examples/authors/python/smoke.py).

  {% endlist %}

- C# {#lang-csharp}

  {% list tabs group=sqlc-csharp %}

  - ADO.NET {#csharp-adonet}

    ```csharp
    var author = await dataSource.ExecuteAsync(
        (connection, ct) => new Authors.Queries(connection).GetAuthorAsync(42UL, ct),
        cancellationToken);
    ```

    Requires .NET 8, Ydb.Sdk 0.35.0, a configured `YdbDataSource dataSource`, and a `CancellationToken cancellationToken`. `ExecuteAsync` retries the whole callback according to the data source policy. Commit is explicit; `await using` rolls back an unfinished transaction on an exception. [Complete example](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/examples/authors/csharp/adonet/Smoke.cs). `GetAuthorAsync` throws `InvalidOperationException` when no row is found.

  - Dapper {#csharp-dapper}

    ```csharp
    var author = await dataSource.ExecuteAsync(
        (connection, ct) => new Authors.Queries(connection).GetAuthorAsync(42UL, ct),
        cancellationToken);
    ```

    Requires .NET 8, Ydb.Sdk 0.35.0 and Dapper 2.1.79, a configured `YdbDataSource dataSource`, and a `CancellationToken cancellationToken`. `ExecuteAsync` retries the whole callback according to the data source policy. Commit is explicit; `await using` rolls back an unfinished transaction on an exception. [Complete example](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/examples/csharp/Program.cs). `GetAuthorAsync` throws `InvalidOperationException` when no row is found.

  {% endlist %}

- TypeScript {#lang-typescript}

  {% list tabs group=sqlc-typescript %}

  - YDB SDK {#typescript-ydb}

    ```typescript
    import { Queries } from "./typescript/queries.js";

    const queries = new Queries(sql);
    const author = await queries.getAuthor(42n);
    console.log(author?.name);
    ```

    `sql` is the `query(driver)` function from `@ydbjs/query` for an initialized driver. `transaction` commits on callback success and rolls back on failure.

  {% endlist %}

- Rust {#lang-rust}

  {% list tabs group=sqlc-rust %}

  - YDB SDK {#rust-ydb}

    ```rust
    use generated::queries::Queries;

    let mut queries = Queries::new(&mut query_client);
    let author = queries.author().author_id(42).call().await?;
    println!("{}", author.name);
    ```

    `query_client` is a mutable `ydb::QueryClient`. Expose the files in `rust/` as the `generated` module and add the `ydb` and `bon` dependencies. The SDK commits on callback success and retries the whole callback.

  {% endlist %}

- PHP {#lang-php}

  {% list tabs group=sqlc-php %}

  - YDB SDK {#php-ydb}

    ```php
    use Authors\Queries;

    $queries = new Queries($table, idempotent: true);
    $author = $queries->getAuthor('42');
    ```

    `$table` is an initialized `YdbPlatform\Ydb\Table`; generated classes are available through autoload. Pass `Uint64` as a decimal string. For this SELECT, safe retries are explicitly enabled with `idempotent: true`. [Runnable example](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/examples/php/live.php).

  {% endlist %}

- Kotlin {#lang-kotlin}

  {% list tabs group=sqlc-kotlin %}

  - YDB SDK {#kotlin-ydb}

    ```kotlin
    val author = requireNotNull(Queries(retry).getAuthor(42L))
    ```

    `retry` is an application-configured `SessionRetryContext`; import `TxMode`, `CompletableFuture`, and the generated `Queries`. The single call uses the retry context; the composed operation passes one `QueryTransaction` to the helpers. The outer `retry` retries the whole transaction.

  - JDBC {#kotlin-jdbc}

    ```kotlin
    val author = requireNotNull(Queries(connection).getAuthor(42L))
    ```

    `connection` is a `java.sql.Connection` initially in autocommit mode, with no other active transaction; import the generated `Queries`. The transaction code restores autocommit after commit or rollback and propagates exceptions. The application can retry the whole transaction when appropriate.

  - Exposed {#kotlin-exposed}

    ```kotlin
    val author = ydbTransaction(database) {
        requireNotNull(Queries(this).getAuthor(42L))
    }
    ```

    `database` is a configured Exposed `Database` with the YDB dialect registered; import `tech.ydb.exposed.dialect.ydbTransaction` and the generated `Queries`. `ydbTransaction` supplies one `JdbcTransaction`, commits on success, and rolls back on an exception. Retry policy applies to the whole block.

  {% endlist %}

{% endlist %}

## 4. Combine calls in a transaction {#transactions}

Use the same transaction object for both calls: write the name, then read it. The application controls transaction boundaries and retries; retry the whole block when needed. Keep external side effects out of callbacks that the SDK may retry.

{% list tabs group=sqlc-language %}

- C++ {#lang-cpp}

  {% list tabs group=sqlc-cpp %}

  - YDB SDK {#cpp-ydb}

    ```cpp
    std::optional<authors::GetAuthorRow> author;
    const auto status = client.RetryQuerySync([&](NYdb::NQuery::TSession session) -> NYdb::TStatus {
        auto started = session.BeginTransaction(
            NYdb::NQuery::TTxSettings::SerializableRW()).GetValueSync();
        if (!started.IsSuccess()) {
            return started;
        }
        auto transaction = started.GetTransaction();
        try {
            authors::Queries queries{transaction};
            queries.UpsertAuthor(42, "Alice");
            author = queries.GetAuthor(42);
        } catch (const NYdb::NStatusHelpers::TYdbErrorException& error) {
            transaction.Rollback().GetValueSync();
            return error.GetStatus();
        } catch (...) {
            transaction.Rollback().GetValueSync();
            throw;
        }
        return transaction.Commit().GetValueSync();
    });
    NYdb::NStatusHelpers::ThrowOnError(status);
    ```

  - userver {#cpp-userver}

    ```cpp
    std::optional<authors::GetAuthorRow> author;
    table_client.RetryTx("upsert-and-read-author", {}, [&](::userver::ydb::TxActor& transaction) {
        authors::Queries queries{transaction};
        queries.UpsertAuthor(42, ::userver::ydb::Utf8{"Alice"});
        author = queries.GetAuthor(42);
        return ::userver::ydb::TxAction::kCommit;
    });
    ```

  {% endlist %}

- Go {#lang-go}

  {% list tabs group=sqlc-go %}

  - YDB SDK {#go-native}

    ```go
    return driver.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
        q := authors.New(tx)
        if err := q.UpsertAuthor(ctx, authors.UpsertAuthorParams{
            AuthorID: 42, AuthorName: "Alice",
        }); err != nil {
            return err
        }
        _, err := q.GetAuthor(ctx, 42)
        return err
    })
    ```

  - database/sql {#go-sql}

    ```go
    tx, err := connection.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    q := authors.New(tx)
    if err := q.UpsertAuthor(ctx, authors.UpsertAuthorParams{
        AuthorID: 42, AuthorName: "Alice",
    }); err != nil {
        return err
    }
    if _, err := q.GetAuthor(ctx, 42); err != nil {
        return err
    }
    return tx.Commit()
    ```

  {% endlist %}

- Java {#lang-java}

  {% list tabs group=sqlc-java %}

  - YDB SDK {#java-ydb}

    ```java
    GetAuthorRow author = retry.supplyResult(session -> {
        var tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
        try {
            var queries = new Queries(tx);
            queries.upsertAuthor(42L, "Alice");
            var row = queries.getAuthor(42L).orElseThrow();
            return CompletableFuture.completedFuture(tx.commit().join().map(info -> row));
        } finally {
            if (tx.isActive()) {
                tx.rollback().join().expectSuccess();
            }
        }
    }).join().getValue();
    ```

  - JDBC {#java-jdbc}

    ```java
    connection.setAutoCommit(false);
    try {
        var queries = new Queries(connection);
        queries.upsertAuthor(42L, "Alice");
        GetAuthorRow author = queries.getAuthor(42L).orElseThrow();
        connection.commit();
    } catch (Exception error) {
        connection.rollback();
        throw error;
    } finally {
        connection.setAutoCommit(true);
    }
    ```

  - jOOQ {#java-jooq}

    ```java
    GetAuthorRow author = dsl.transactionResult(configuration -> {
        var queries = new Queries(YDB.using(configuration));
        var id = ULong.valueOf(42);
        queries.upsertAuthor(id, "Alice");
        return queries.getAuthor(id).orElseThrow();
    });
    ```

  {% endlist %}

- Python {#lang-python}

  {% list tabs group=sqlc-python %}

  - YDB SDK {#python-ydb}

    ```python
    import ydb
    from python.ydb.queries import Querier


    def update_author(tx):
        queries = Querier(tx)
        queries.upsert_author(42, "Alice")
        return queries.get_author(42)


    author = pool.retry_tx_sync(
        update_author,
        tx_mode=ydb.QuerySerializableReadWrite(),
        retry_settings=ydb.RetrySettings(idempotent=True),
    )
    ```

  - DB-API {#python-dbapi}

    ```python
    import ydb_dbapi
    from python.dbapi.queries import Querier

    connection.set_isolation_level(ydb_dbapi.IsolationLevel.SERIALIZABLE)
    connection.begin()
    try:
        queries = Querier(connection)
        queries.upsert_author(42, "Alice")
        author = queries.get_author(42)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    ```

  - SQLAlchemy {#python-sqlalchemy}

    ```python
    from python.sqlalchemy.queries import Querier

    with engine.connect().execution_options(isolation_level="SERIALIZABLE") as connection:
        with connection.begin():
            queries = Querier(connection)
            queries.upsert_author(42, "Alice")
            author = queries.get_author(42)
    ```

  {% endlist %}

- C# {#lang-csharp}

  {% list tabs group=sqlc-csharp %}

  - ADO.NET {#csharp-adonet}

    ```csharp
    var author = await dataSource.ExecuteAsync(async (connection, ct) =>
    {
        await using var transaction = (YdbTransaction)await connection.BeginTransactionAsync(ct);
        var queries = new Authors.Queries(connection, transaction);
        await queries.UpsertAuthorAsync(new Authors.UpsertAuthorParams(42UL, "Alice"), ct);
        var row = await queries.GetAuthorAsync(42UL, ct);
        await transaction.CommitAsync(ct);
        return row;
    }, cancellationToken);
    ```

  - Dapper {#csharp-dapper}

    ```csharp
    var author = await dataSource.ExecuteAsync(async (connection, ct) =>
    {
        await using var transaction = (YdbTransaction)await connection.BeginTransactionAsync(ct);
        var queries = new Authors.Queries(connection, transaction);
        await queries.UpsertAuthorAsync(new Authors.UpsertAuthorParams(42UL, "Alice"), ct);
        var row = await queries.GetAuthorAsync(42UL, ct);
        await transaction.CommitAsync(ct);
        return row;
    }, cancellationToken);
    ```

  {% endlist %}

- TypeScript {#lang-typescript}

  {% list tabs group=sqlc-typescript %}

  - YDB SDK {#typescript-ydb}

    ```typescript
    import { Queries } from "./typescript/queries.js";
    import type { ConfigureQuery } from "./typescript/queries.js";

    const author = await sql.transaction(async (tx, signal) => {
      const queries = new Queries(tx);
      const configure: ConfigureQuery = (stmt) => { stmt.signal(signal); };
      await queries.upsertAuthor({ authorId: 42n, authorName: "Alice" }, configure);
      return queries.getAuthor(42n, configure);
    });
    console.log(author?.name);
    ```

  {% endlist %}

- Rust {#lang-rust}

  {% list tabs group=sqlc-rust %}

  - YDB SDK {#rust-ydb}

    ```rust
    use generated::queries::Queries;

    let author = query_client.retry_tx(ydb::closure!(async |tx| {
        let mut queries = Queries::new(tx);
        queries.upsert_author()
            .author_id(42)
            .author_name("Alice")
            .call().await?;
        Ok(queries.author().author_id(42).call().await?)
    })).await?;
    println!("{}", author.name);
    ```

  {% endlist %}

- PHP {#lang-php}

  {% list tabs group=sqlc-php %}

  - YDB SDK {#php-ydb}

    Unsupported: each helper opens and commits its own transaction. The SDK has no public API that both joins an existing transaction and returns the raw results needed for lossless decoding. Wrapping these calls in `retryTransaction()` does not combine them. [PHP contract](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/docs/php.md).

  {% endlist %}

- Kotlin {#lang-kotlin}

  {% list tabs group=sqlc-kotlin %}

  - YDB SDK {#kotlin-ydb}

    ```kotlin
    val author = retry.supplyResult { session ->
        val tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW)
        try {
            val queries = Queries(tx)
            queries.upsertAuthor(42L, "Alice")
            val row = requireNotNull(queries.getAuthor(42L))
            CompletableFuture.completedFuture(tx.commit().join().map { row })
        } finally {
            if (tx.isActive) {
                tx.rollback().join().expectSuccess()
            }
        }
    }.join().getValue()
    ```

  - JDBC {#kotlin-jdbc}

    ```kotlin
    connection.autoCommit = false
    try {
        val queries = Queries(connection)
        queries.upsertAuthor(42L, "Alice")
        val author = requireNotNull(queries.getAuthor(42L))
        connection.commit()
    } catch (error: Exception) {
        connection.rollback()
        throw error
    } finally {
        connection.autoCommit = true
    }
    ```

  - Exposed {#kotlin-exposed}

    ```kotlin
    val author = ydbTransaction(database) {
        val queries = Queries(this)
        queries.upsertAuthor(42L, "Alice")
        requireNotNull(queries.getAuthor(42L))
    }
    ```

  {% endlist %}

{% endlist %}

## Differences from upstream sqlc {#limitations}

sqlc-ydb preserves the SQL-first workflow and the `init`, `compile`, `generate`, `diff`, and `version` commands, but is not a drop-in replacement for upstream sqlc:

- It supports only {{ ydb-short-name }} and a subset of sqlc v2 configuration. There are no external plugins, sqlc macros or cloud commands; all nine generators are built into the binary.
- It analyzes a subset of YQL. Unsupported constructs and types produce errors; successful generation does not replace server-side query validation.
- Generated APIs follow each SDK’s contract: for example, `Uint64` maps to Go `uint64`, TypeScript `bigint` or a PHP string. Missing-row behavior for `:one` also depends on the profile.
- Besides `:one` and `:exec`, `:many` returns a collection. Bound large reads in SQL. Streaming APIs and `:execrows` are not generated.
- ORM entities and generic CRUD for Hibernate, Spring JPA and linq2db are outside the project’s contract.

See the [target reference](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/docs/targets.md) and [compatibility contract](https://github.com/ydb-platform/sqlc-ydb/blob/v0.1.0/docs/compatibility.md) for details. If a query is unsupported or generated code is awkward to use, open an [issue](https://github.com/ydb-platform/sqlc-ydb/issues) with a minimal schema, query, configuration and the output of `sqlc-ydb version --verbose`.
