# sqlc-ydb: типизированный код из YQL

[sqlc-ydb](https://github.com/ydb-platform/sqlc-ydb) генерирует типизированный код для {{ ydb-short-name }} по схеме и запросам на [YQL](../yql/reference/index.md). Вы пишете SQL, а утилита создаёт методы, передающие параметры и читающие результаты через выбранный SDK или драйвер. Это самостоятельный проект со знакомым подходом [sqlc](https://sqlc.dev/).

Ниже показан путь от схемы и двух запросов до сгенерированного кода и его вызовов в приложении. Выберите язык и фреймворк во вкладках один раз — выбор синхронизируется во всех четырёх разделах страницы.

## Установите утилиту {#install}

На Linux и macOS выполните команды ниже. Генерация не требует SDK или подключения к базе. Установщик выбирает последнюю стабильную версию, проверяет SHA256 и помещает бинарник в `~/.local/bin`. Если нужна настройка `PATH`, выполните напечатанную установщиком инструкцию перед `sqlc-ydb version`.

```bash
curl -fsSL https://raw.githubusercontent.com/ydb-platform/sqlc-ydb/main/install.sh | bash
sqlc-ydb version
```

Для Windows скачайте подходящий архив со [страницы последнего стабильного релиза](https://github.com/ydb-platform/sqlc-ydb/releases/latest). Другие способы установки и выбор конкретной версии описаны в [инструкции установки](https://github.com/ydb-platform/sqlc-ydb/blob/main/docs/installation.md).

## Опишите схему и запросы {#inputs}

Создайте пустой каталог проекта и сохраните общую для всех языков схему в `schema.sql`:

```yql
CREATE TABLE authors (
    id Uint64 NOT NULL,
    name Utf8 NOT NULL,
    PRIMARY KEY (id)
);
```

В `queries.sql` сохраните два запроса. `UpsertAuthor` добавляет автора или обновляет имя существующего, а `GetAuthor` читает запись по ключу. Имена параметров начинаются с `$`; их типы выводятся из схемы, поэтому `DECLARE` здесь не нужен.

```yql
-- name: UpsertAuthor :exec
UPSERT INTO authors (id, name) VALUES ($author_id, $author_name);

-- name: GetAuthor :one
SELECT id, name FROM authors WHERE id = $author_id;
```

Комментарий `-- name:` задаёт имя метода. `:exec` означает выполнение без строк результата, `:one` — получение одной строки. Все примеры ниже используют именно эти файлы.

## 1. Выберите генератор в sqlc.yaml {#configuration}

Сохраните конфигурацию из выбранной вкладки в `sqlc.yaml`. Пути входных файлов и каталога `out` вычисляются относительно этого файла.

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

## 2. Сгенерируйте код {#generate}

В каталоге с `sqlc.yaml` выполните команды. `compile` проверяет входные данные, `generate` записывает код, а `diff` проверяет, что сохранённый результат совпадает с генерацией. Последнюю команду можно включить в CI: при различиях она возвращает код 1.

```bash
sqlc-ydb compile
sqlc-ydb generate
sqlc-ydb diff
```

Ниже — фрагменты реального результата генерации из общего запроса: тип строки или метод чтения. Импорты и окружающие объявления классов опущены. Используйте полные файлы из каталога `out`; не редактируйте их вручную.

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

## Подготовьте приложение {#prepare}

Генератор не создаёт таблицы в базе. Примените `schema.sql` через принятый в проекте механизм миграций или [YDB CLI](../reference/ydb-cli/sql.md), а для примера одиночного чтения добавьте запись:

```yql
UPSERT INTO authors (id, name) VALUES (42, "Alice");
```

Подключите зависимости выбранного SDK/драйвера и настройте [аутентификацию](../reference/ydb-sdk/auth.md). Фрагменты ниже используют уже открытые соединения или клиенты и импортированный сгенерированный код. Полные проекты с зависимостями и настройкой подключения доступны в [примерах проекта](https://github.com/ydb-platform/sqlc-ydb/tree/main/examples).

## 3. Выполните отдельный запрос {#single-query}

Вызовите сгенерированный метод для автора с идентификатором 42:

{% list tabs group=sqlc-language %}

- C++ {#lang-cpp}

  {% list tabs group=sqlc-cpp %}

  - YDB SDK {#cpp-ydb}

    ```cpp
    authors::Queries queries{client};
    auto author = queries.GetAuthor(42);
    ```

    `client` — настроенный `NYdb::NQuery::TQueryClient`; нужны C++20 и YDB C++ SDK. Одиночный вызов использует SDK retry; `RetryQuerySync` повторяет всю транзакцию. `Queries` не завершает переданную транзакцию: пример явно делает commit или rollback. [Полный пример](https://github.com/ydb-platform/sqlc-ydb/blob/main/examples/authors/cpp/native/main.cpp).

  - userver {#cpp-userver}

    ```cpp
    authors::Queries queries{table_client};
    auto author = queries.GetAuthor(42);
    ```

    `table_client` — `userver::ydb::TableClient` из компонента YDB; код выполняется в корутине userver. `RetryTx` повторяет весь callback и завершает транзакцию по `TxAction`; `kRollback` позволяет отменить изменения. `Utf8` — строковый тип userver для YQL Utf8. [Полный пример](https://github.com/ydb-platform/sqlc-ydb/blob/main/examples/authors/cpp/userver/smoke_handler.cpp).

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

    `driver` — открытый `*ydb.Driver`, `ctx` — контекст. Импортируйте сгенерированный пакет как `authors`, а для транзакции — `github.com/ydb-platform/ydb-go-sdk/v3/query`. Фрагменты находятся в функции, возвращающей `error`.

  - database/sql {#go-sql}

    ```go
    q := authors.New(connection)
    author, err := q.GetAuthor(ctx, 42)
    if err != nil {
        return err
    }
    fmt.Println(author.Name)
    ```

    `connection` — открытый `*sql.DB` с драйвером YDB, `ctx` — контекст. Импортируйте сгенерированный пакет как `authors`. Фрагменты находятся в функции, возвращающей `error`; при повторе выполните весь транзакционный блок заново.

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

    `retry` имеет тип `SessionRetryContext` и настроен приложением; импорты: `TxMode`, `CompletableFuture`, сгенерированные `Queries` и `GetAuthorRow`. Даже одиночный вызов Java native использует открытую транзакцию. `retry` повторяет всю функцию с новой сессией; завершение транзакции остаётся в вызывающем коде.

  - JDBC {#java-jdbc}

    ```java
    GetAuthorRow author = new Queries(connection).getAuthor(42L).orElseThrow();
    ```

    `connection` имеет тип `java.sql.Connection`, исходно работает в autocommit и не содержит другой транзакции. Импортированы сгенерированные `Queries` и `GetAuthorRow`. Код транзакции возвращает autocommit после commit или rollback; исключения передаются вызывающему коду. Повтор всей транзакции при необходимости организует приложение.

  - jOOQ {#java-jooq}

    ```java
    GetAuthorRow author = new Queries(dsl).getAuthor(ULong.valueOf(42)).orElseThrow();
    ```

    `dsl` имеет тип `YdbDSLContext`; импорты: `YDB`, `ULong`, сгенерированные `Queries` и `GetAuthorRow`. Для отдельного вызова соединение работает в autocommit. `transactionResult` фиксирует изменения при успехе и откатывает при исключении; `YDB.using(configuration)` сохраняет соединение текущей транзакции. Повторы организует приложение вокруг всей транзакции.

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

    `pool` — готовый `ydb.QuerySessionPool`. `retry_tx_sync` повторяет всю транзакцию и выполняет commit после callback; callback не должен иметь внешних побочных эффектов. В примере повторяемый UPSERT задаёт фиксированные значения. [Запускаемый пример](https://github.com/ydb-platform/sqlc-ydb/blob/main/examples/authors/python/smoke.py).

  - DB-API {#python-dbapi}

    ```python
    from python.dbapi.queries import Querier

    author = Querier(connection).get_author(42)
    ```

    `connection` — открытое соединение `ydb_dbapi` без активной транзакции. По умолчанию драйвер использует AUTOCOMMIT: для общей транзакции нужно выбрать SERIALIZABLE до `begin()`. Повторы всей транзакции организует вызывающий код. [Запускаемый пример](https://github.com/ydb-platform/sqlc-ydb/blob/main/examples/authors/python/smoke.py).

  - SQLAlchemy {#python-sqlalchemy}

    ```python
    from python.sqlalchemy.queries import Querier

    with engine.connect() as connection:
        author = Querier(connection).get_author(42)
    ```

    `engine` — готовый SQLAlchemy Engine с диалектом `yql+ydb`. SERIALIZABLE задаётся до `begin()`: один transaction block не отменяет AUTOCOMMIT драйвера. Контекст выполняет commit или rollback; повторы всей транзакции организует вызывающий код. [Запускаемый пример](https://github.com/ydb-platform/sqlc-ydb/blob/main/examples/authors/python/smoke.py).

  {% endlist %}

- C# {#lang-csharp}

  {% list tabs group=sqlc-csharp %}

  - ADO.NET {#csharp-adonet}

    ```csharp
    var author = await dataSource.ExecuteAsync(
        (connection, ct) => new Authors.Queries(connection).GetAuthorAsync(42UL, ct),
        cancellationToken);
    ```

    Нужны .NET 8, Ydb.Sdk, настроенный `YdbDataSource dataSource` и `CancellationToken cancellationToken`. `ExecuteAsync` повторяет весь callback согласно политике data source. Транзакция коммитится явно; `await using` откатывает незавершённую транзакцию при исключении. [Полный пример](https://github.com/ydb-platform/sqlc-ydb/blob/main/examples/authors/csharp/adonet/Smoke.cs). Если строка не найдена, `GetAuthorAsync` выбрасывает `InvalidOperationException`.

  - Dapper {#csharp-dapper}

    ```csharp
    var author = await dataSource.ExecuteAsync(
        (connection, ct) => new Authors.Queries(connection).GetAuthorAsync(42UL, ct),
        cancellationToken);
    ```

    Нужны .NET 8, Ydb.Sdk и Dapper, настроенный `YdbDataSource dataSource` и `CancellationToken cancellationToken`. `ExecuteAsync` повторяет весь callback согласно политике data source. Транзакция коммитится явно; `await using` откатывает незавершённую транзакцию при исключении. [Полный пример](https://github.com/ydb-platform/sqlc-ydb/blob/main/tests/examples/csharp/Program.cs). Если строка не найдена, `GetAuthorAsync` выбрасывает `InvalidOperationException`.

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

    `sql` — функция `query(driver)` из `@ydbjs/query` для подготовленного драйвера. `transaction` фиксирует изменения после успешного callback и откатывает при ошибке.

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

    `query_client` — изменяемый `ydb::QueryClient`. Подключите файлы из `rust/` как модуль `generated` и зависимости `ydb` и `bon`. SDK фиксирует транзакцию после успешного callback; повторяется весь callback.

  {% endlist %}

- PHP {#lang-php}

  {% list tabs group=sqlc-php %}

  - YDB SDK {#php-ydb}

    ```php
    use Authors\Queries;

    $queries = new Queries($table, idempotent: true);
    $author = $queries->getAuthor('42');
    ```

    `$table` — готовый `YdbPlatform\Ydb\Table`; сгенерированные классы подключены через autoload. `Uint64` передаётся десятичной строкой. Для этого SELECT безопасные повторы явно включены через `idempotent: true`. [Запускаемый пример](https://github.com/ydb-platform/sqlc-ydb/blob/main/tests/examples/php/live.php).

  {% endlist %}

- Kotlin {#lang-kotlin}

  {% list tabs group=sqlc-kotlin %}

  - YDB SDK {#kotlin-ydb}

    ```kotlin
    val author = requireNotNull(Queries(retry).getAuthor(42L))
    ```

    `retry` имеет тип `SessionRetryContext` и настроен приложением; импорты: `TxMode`, `CompletableFuture` и сгенерированный `Queries`. Одиночный вызов использует retry-контекст, а составная операция передаёт хелперам одну `QueryTransaction`. Внешний `retry` повторяет всю транзакцию.

  - JDBC {#kotlin-jdbc}

    ```kotlin
    val author = requireNotNull(Queries(connection).getAuthor(42L))
    ```

    `connection` имеет тип `java.sql.Connection`, исходно работает в autocommit и не содержит другой транзакции; импортирован сгенерированный `Queries`. Код транзакции возвращает autocommit после commit или rollback; исключения передаются вызывающему коду. Повтор всей транзакции при необходимости организует приложение.

  - Exposed {#kotlin-exposed}

    ```kotlin
    val author = ydbTransaction(database) {
        requireNotNull(Queries(this).getAuthor(42L))
    }
    ```

    `database` представляет настроенный Exposed `Database` с зарегистрированным YDB-диалектом; импорты: `tech.ydb.exposed.dialect.ydbTransaction` и сгенерированный `Queries`. Блок `ydbTransaction` передаёт одну `JdbcTransaction`, фиксирует её при успехе и откатывает при исключении. Политика повторов применяется к блоку целиком.

  {% endlist %}

{% endlist %}

## 4. Объедините вызовы в транзакцию {#transactions}

Передайте обоим вызовам один объект транзакции: сначала запишите имя, затем прочитайте его. Приложение управляет границами транзакции и повторами; при повторе выполняйте весь блок. В callback, который SDK может повторить, не помещайте внешние побочные эффекты.

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

    Не поддерживается: каждый helper открывает и завершает собственную транзакцию. В SDK нет публичного API, который одновременно присоединяется к существующей транзакции и возвращает необработанные результаты, нужные для точного декодирования. Обёртка `retryTransaction()` не объединяет эти вызовы. [Контракт PHP](https://github.com/ydb-platform/sqlc-ydb/blob/main/docs/php.md).

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

## Отличия от upstream sqlc {#limitations}

sqlc-ydb сохраняет SQL-first подход и команды `init`, `compile`, `generate`, `diff`, `version`, но не является полной заменой upstream sqlc:

- Поддерживает только {{ ydb-short-name }} и подмножество конфигурации sqlc v2. Внешних плагинов, макросов sqlc и облачных команд нет; генераторы встроены в бинарник.
- Анализирует подмножество YQL. Неподдерживаемые конструкции и типы вызывают ошибку; успешная генерация не заменяет проверку запроса на сервере.
- Генерирует API под конкретный SDK: например, `Uint64` становится Go `uint64`, TypeScript `bigint` или PHP-строкой. Поведение `:one` при отсутствии строки также зависит от профиля.
- Помимо `:one` и `:exec`, есть `:many`, возвращающий коллекцию. Ограничивайте большие выборки в SQL. Потоковый API и `:execrows` не генерируются.
- ORM-сущности и универсальный CRUD для Hibernate, Spring JPA и linq2db не входят в контракт проекта.

Подробности и ограничения профилей приведены в [справочнике генераторов](https://github.com/ydb-platform/sqlc-ydb/blob/main/docs/targets.md) и [описании совместимости](https://github.com/ydb-platform/sqlc-ydb/blob/main/docs/compatibility.md). Если ваш запрос не поддерживается или сгенерированный код неудобен, создайте [issue](https://github.com/ydb-platform/sqlc-ydb/issues) с минимальными схемой, запросом, конфигурацией и выводом `sqlc-ydb version --verbose`.
