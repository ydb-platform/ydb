# Setting the transaction execution mode

To execute queries in the {{ ydb-short-name }} SDK, you must specify the [transaction execution mode](../../concepts/transactions.md#modes).

Below are code examples that use the {{ ydb-short-name }} SDK built-in tools for creating a *transaction execution mode* object.

## ImplicitTx {#implicittx}

The [ImplicitTx](../../concepts/transactions#implicit) mode allows you to execute a single query without explicit transaction management. The query is executed in its own implicit transaction, which is automatically committed on success.

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/query/client.h>

    void ImplicitTxExample(NYdb::NQuery::TSession session) {
      auto result = session.ExecuteQuery(
          "SELECT 1",
          NYdb::NQuery::TTxControl::NoTx()
      ).GetValueSync();

      // ...
    }
    ```

  - userver

    {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  {% endlist %}
- Go

  {% list tabs %}

  - Native SDK

    ```go
    package main

    import (
      "context"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/query"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      row, err := db.Query().QueryRow(ctx, "SELECT 1",
        query.WithTxControl(query.ImplicitTxControl()),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
      // working with row
      _ = row
    }
    ```

  - database/sql

    ```go
    package main

    import (
      "context"
      "database/sql"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer nativeDriver.Close(ctx)

      connector, err := ydb.Connector(nativeDriver)
      if err != nil {
        panic(err)
      }
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()

      // ImplicitTx - query without explicit transaction (auto-commit)
      row := db.QueryRowContext(ctx, "SELECT 1")
      var result int
      if err := row.Scan(&result); err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    // ...
    try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
        QueryReader reader = retryCtx.supplyResult(
            session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.NONE))
        );
        // working with reader
    }
    ```

  - JDBC

    On the JDBC side, the implicit mode is associated with auto-commit (`Connection.setAutoCommit(true)` by default): each individual query is treated as a separate transaction and committed automatically. When `setAutoCommit(false)`, boundaries are set by explicit `commit`/`rollback`.

    In the **current implementation** of the {{ ydb-short-name }} JDBC driver, for **single** **read** calls, the **snapshot** mode is enabled if **`setReadOnly(true)`** is called on `Connection`. For **write** queries, **Serializable Read/Write** is used (the connection must not have read-only mode enabled).

    In **Spring**, the **`@Transactional(readOnly = true)`** attribute at transaction start causes `Connection.setReadOnly(true)` to be called on the connection, so for read-only scenarios snapshot is selected automatically. Hibernate, JOOQ, and other wrappers over JDBC use the same connection — the read-only flag must be set explicitly as well (or through the framework's transaction settings if they propagate it to `Connection`).

  {% endlist %}
- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    def execute_query(pool: ydb.QuerySessionPool):
        pool.execute_with_retries("SELECT 1")
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    async def execute_query(pool: ydb.aio.QuerySessionPool):
        await pool.execute_with_retries("SELECT 1")
    ```

  - SQLAlchemy

    ```python
    import sqlalchemy as sa
    from ydb_sqlalchemy import IsolationLevel

    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    with engine.connect().execution_options(isolation_level=IsolationLevel.AUTOCOMMIT) as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}
- C#

  {% list tabs %}

  - ADO.NET

    ```csharp
    using Ydb.Sdk.Ado;

    await using var connection = await dataSource.OpenRetryableConnectionAsync();
    // Execution without explicit transaction (auto-commit)
    await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
    await command.ExecuteNonQueryAsync();
    ```

  - Entity Framework

    ```csharp
    using Microsoft.EntityFrameworkCore;

    await using var context = await dbContextFactory.CreateDbContextAsync();
    // Entity Framework auto-commit mode (without explicit transaction)
    var result = await context.SomeEntities.FirstOrDefaultAsync();
    ```

  - linq2db

    ```csharp
    using LinqToDB;
    using LinqToDB.Data;

    using var db = new DataConnection(
        new DataOptions().UseConnectionString(
            "YDB",
            "Host=localhost;Port=2136;Database=/local;UseTls=false"
        )
    );
    // linq2db auto-commit mode (without explicit transaction)
    var result = db.GetTable<Employee>().FirstOrDefault(e => e.Id == 1);
    ```

  {% endlist %}
- JavaScript


  ```javascript
  import { sql } from '@ydbjs/query';

  // ...

  // ImplicitTx - one query without explicit transaction
  const result = await sql`SELECT 1`;
  ```

- Rust

  ImplicitTx mode is not supported.
- PHP


  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;

  $config = [
      // YDB config
  ];

  $ydb = new Ydb($config);
  $result = $ydb->table()->query('SELECT 1;');
  ```

{% endlist %}

## Serializable {#serializable}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/query/client.h>

    void SerializableExample(NYdb::NQuery::TSession session) {
        auto settings = NYdb::NQuery::TTxSettings::SerializableRW();
        auto result = session.ExecuteQuery(
            "SELECT 1",
            NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
        ).GetValueSync();

        // ...
    }
    ```

  - userver

    ```cpp
    #include <userver/ydb/table.hpp>

    void SerializableExample(ydb::TableClient& client) {
        auto result = client.ExecuteQuery(
            ydb::OperationSettings{.tx_mode = ydb::TransactionMode::kSerializableRW},
            ydb::Query{"SELECT 1;"}
        );
        // ...
    }
    ```

  {% endlist %}
- Go

  {% list tabs %}

  - Native SDK

    ```go
    package main

    import (
      "context"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/query"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      row, err := db.Query().QueryRow(ctx, "SELECT 1",
        query.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
      // working with row
      _ = row
    }
    ```

  - database/sql

    ```go
    package main

    import (
      "context"
      "database/sql"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer nativeDriver.Close(ctx)

      connector, err := ydb.Connector(nativeDriver)
      if err != nil {
        panic(err)
      }
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()

      err = retry.DoTx(ctx, db,
        func(ctx context.Context, tx *sql.Tx) error {
          row := tx.QueryRowContext(ctx, "SELECT 1")
          var result int
          return row.Scan(&result)
        },
        retry.WithIdempotent(true),
        // Serializable Read-Write mode is used by default for transactions
        // Or it can be set explicitly as shown below
        retry.WithTxOptions(&sql.TxOptions{
          Isolation: sql.LevelSerializable,
          ReadOnly:  false,
        }),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    // ...
    try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
        QueryReader reader = retryCtx.supplyResult(
            session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.SERIALIZABLE_RW))
        );
        // Working with reader
    }
    ```

  - JDBC

    For **single** calls via JDBC, the current driver implementation uses **Serializable Read/Write** — including when working from Spring Boot, ORM, and other wrappers over JDBC. The **snapshot** mode for reads is set via **`setReadOnly(true)`** (see [ImplicitTx](#implicittx)). The semantics of auto-commit and explicit transactions are described there.

  {% endlist %}
- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    def execute_query(pool: ydb.QuerySessionPool):
        def callee(session: ydb.QuerySession):
            with session.transaction(ydb.QuerySerializableReadWrite()).execute(
                "SELECT 1",
                commit_tx=True,
            ) as result_sets:
                pass

        pool.retry_operation_sync(callee)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    async def execute_query(pool: ydb.aio.QuerySessionPool):
        async def callee(session):
            async with session.transaction(tx_mode=ydb.QuerySerializableReadWrite()) as tx:
                async with await tx.execute("SELECT 1", commit_tx=True) as result_sets:
                    pass

        await pool.retry_operation_async(callee)
    ```

  - SQLAlchemy

    ```python
    import sqlalchemy as sa
    from ydb_sqlalchemy import IsolationLevel

    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    with engine.connect().execution_options(isolation_level=IsolationLevel.SERIALIZABLE) as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}
- C#

  {% list tabs %}

  - ADO.NET

    ```csharp
    using Ydb.Sdk.Ado;

    // Serializable mode is used by default
    await _ydbDataSource.ExecuteInTransactionAsync(async ydbConnection =>
        {
            var ydbCommand = ydbConnection.CreateCommand();
            ydbCommand.CommandText = """
                                     UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
                                     VALUES (2, 5, 13, "Test Episode", Date("2018-08-27"))
                                     """;
            await ydbCommand.ExecuteNonQueryAsync();
            ydbCommand.CommandText = """
                                     INSERT INTO episodes(series_id, season_id, episode_id, title, air_date)
                                     VALUES
                                         (2, 5, 21, "Test 21", Date("2018-08-27")),
                                         (2, 5, 22, "Test 22", Date("2018-08-27"))
                                     """;
            await ydbCommand.ExecuteNonQueryAsync();
        }
    );
    ```

  - Entity Framework

    ```csharp
    var strategy = db.Database.CreateExecutionStrategy();

    // Entity Framework uses Serializable mode by default
    strategy.ExecuteInTransaction(
        db,
        ctx =>
        {
            ctx.Users.AddRange(
                new User { Name = "Alex", Email = "alex@example.com" },
                new User { Name = "Kirill", Email = "kirill@example.com" }
            );

            ctx.SaveChanges();

            var users = ctx.Users.OrderBy(u => u.Id).ToList();
            Console.WriteLine("Users in database:");
            foreach (var user in users)
                Console.WriteLine($"- {user.Id}: {user.Name} ({user.Email})");
        },
        ctx => ctx.Users.Any(u => u.Email == "alex@example.com")
            && ctx.Users.Any(u => u.Email == "kirill@example.com")
      );
    ```

  - linq2db

    ```csharp
    using LinqToDB;
    using LinqToDB.Data;

    // linq2db uses Serializable mode by default
    using var db = new DataConnection(
        new DataOptions().UseConnectionString(
            "YDB",
            "Host=localhost;Port=2136;Database=/local;UseTls=false"
        )
    );

    await using var tr = await db.BeginTransactionAsync();

    await db.InsertAsync(new Episode
    {
        SeriesId = 2, SeasonId = 5, EpisodeId = 13, Title = "Test Episode", AirDate = new DateTime(2018, 08, 27)
    });
    await db.InsertAsync(new Episode
        { SeriesId = 2, SeasonId = 5, EpisodeId = 21, Title = "Test 21", AirDate = new DateTime(2018, 08, 27) });
    await db.InsertAsync(new Episode
        { SeriesId = 2, SeasonId = 5, EpisodeId = 22, Title = "Test 22", AirDate = new DateTime(2018, 08, 27) });

    await tr.CommitAsync();
    ```

  {% endlist %}
- JavaScript


  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  // Serializable Read-Write mode is used by default
  await sql.begin({ idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });

  // Or explicitly specify the transaction mode
  await sql.begin({ isolation: 'serializableReadWrite', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust


  ```rust
  use ydb::{TransactionOptions};

  let tx_options = TransactionOptions::default().with_mode(
    ydb::Mode::SerializableReadWrite
  );
  let table_client = db.table_client().clone_with_transaction_options(tx_options);
  let result = table_client.retry_transaction(|mut tx| async move {
    let res = tx.query("SELECT 1".into()).await?;
    return Ok(res)
  }).await?;
  ```

- PHP


  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;

  $config = [
      // YDB config
  ];

  $ydb = new Ydb($config);
  $result = $ydb->table()->retryTransaction(
    function (Session $session) {
        return $session->query('SELECT 1 AS value;');
    },
    true,
    null,
    ['tx_mode' => 'serializable_read_write']
  );
  ```

{% endlist %}

## Online Read-Only {#online-read-only}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/query/client.h>

    void OnlineReadOnlyExample(NYdb::NQuery::TSession session) {
        auto settings = NYdb::NQuery::TTxSettings::OnlineRO();
        auto result = session.ExecuteQuery(
            "SELECT 1",
            NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
        ).GetValueSync();

        // ...
    }
    ```

  - userver

    ```cpp
    #include <userver/ydb/table.hpp>

    void OnlineReadOnlyExample(ydb::TableClient& client) {
        auto result = client.ExecuteQuery(
            ydb::OperationSettings{.tx_mode = ydb::TransactionMode::OnlineRO},
            ydb::Query{"SELECT 1;"}
        );
        // ...
    }
    ```

  {% endlist %}
- Go

  {% list tabs %}

  - Native SDK

    ```go
    package main

    import (
      "context"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/query"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      row, err := db.Query().QueryRow(ctx, "SELECT 1",
        query.WithTxControl(
          query.OnlineReadOnlyTxControl(query.WithInconsistentReads()),
        ),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
      // working with row
      _ = row
    }
    ```

  - database/sql

    ```go
    package main

    import (
      "context"
      "database/sql"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
      "github.com/ydb-platform/ydb-go-sdk/v3/table"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer nativeDriver.Close(ctx)

      connector, err := ydb.Connector(nativeDriver)
      if err != nil {
        panic(err)
      }
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()

      err = retry.Do(
        ydb.WithTxControl(ctx, table.OnlineReadOnlyTxControl(table.WithInconsistentReads())),
        db,
        func(ctx context.Context, conn *sql.Conn) error {
          row := conn.QueryRowContext(ctx, "SELECT 1")
          var result int
          return row.Scan(&result)
        },
        retry.WithIdempotent(true),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    // ...
    try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
        QueryReader reader = retryCtx.supplyResult(
            session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.ONLINE_RO))
        );
        // Working with reader
    }
    ```

  - JDBC

    The Online Read-Only mode from the Query API is **not set separately** for **single** calls via JDBC. For **snapshot** reads, call **`Connection.setReadOnly(true)`** (in Spring — `@Transactional(readOnly = true)`). For writes, see [ImplicitTx](#implicittx).

  {% endlist %}
- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    def execute_query(pool: ydb.QuerySessionPool):
        def callee(session: ydb.QuerySession):
            with session.transaction(ydb.QueryOnlineReadOnly()).execute(
                "SELECT 1",
                commit_tx=True,
            ) as result_sets:
                pass

        pool.retry_operation_sync(callee)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    async def execute_query(pool: ydb.aio.QuerySessionPool):
        async def callee(session):
            async with session.transaction(tx_mode=ydb.QueryOnlineReadOnly()) as tx:
                async with await tx.execute("SELECT 1", commit_tx=True) as result_sets:
                    pass

        await pool.retry_operation_async(callee)
    ```

  - SQLAlchemy

    ```python
    import sqlalchemy as sa
    from ydb_sqlalchemy import IsolationLevel

    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    with engine.connect().execution_options(isolation_level=IsolationLevel.ONLINE_READONLY) as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}
- C#

  {% list tabs %}

  - ADO.NET

    ```csharp
    using Ydb.Sdk.Ado;

    // OnlineRo — data is as up-to-date as possible at the time of each read operation;
    // data within a single transaction is consistent
    await using var connection = await dataSource.OpenConnectionAsync();
    await using var transaction = await connection.BeginTransactionAsync(TransactionMode.OnlineRo);
    await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
    await using var reader = await command.ExecuteReaderAsync();
    await transaction.CommitAsync();

    // OnlineInconsistentRo — maximum performance, minimal consistency:
    // data may be inconsistent even within a single read operation
    await using var connection2 = await dataSource.OpenConnectionAsync();
    await using var transaction2 = await connection2.BeginTransactionAsync(TransactionMode.OnlineInconsistentRo);
    await using var command2 = new YdbCommand(connection2) { CommandText = "SELECT 1" };
    await using var reader2 = await command2.ExecuteReaderAsync();
    await transaction2.CommitAsync();
    ```

  - Entity Framework

    Entity Framework does not support the OnlineRo mode directly.
    Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  - linq2db

    linq2db does not support the OnlineRo mode directly.
    Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endlist %}
- JavaScript


  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'onlineReadOnly', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust


  ```rust
  let tx_options = TransactionOptions::default().with_mode(
    ydb::Mode::OnlineReadonly,
  ).with_autocommit(true);
  let table_client = db.table_client().clone_with_transaction_options(tx_options);
  let result = table_client.retry_transaction(|mut tx| async move {
    let res = tx.query("SELECT 1".into()).await?;
    return Ok(res)
  }).await?;
  ```

- PHP


  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;

  $config = [
      // YDB config
  ];

  $ydb = new Ydb($config);
  $result = $ydb->table()->retrySession(function (Session $session) {
    $query = $session->newQuery('SELECT 1 AS value;');
    $query->beginTx('online_read_only');
    return $query->execute();
  }, true);
  ```

{% endlist %}

## Stale Read-Only {#stale-read-only}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/query/client.h>

    void StaleReadOnlyExample(NYdb::NQuery::TSession session) {
        auto settings = NYdb::NQuery::TTxSettings::StaleRO();
        auto result = session.ExecuteQuery(
            "SELECT 1",
            NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
        ).GetValueSync();

        // ...
    }
    ```

  - userver

    ```cpp
    #include <userver/ydb/table.hpp>

    void StaleReadOnlyExample(ydb::TableClient& client) {
        auto result = client.ExecuteQuery(
            ydb::OperationSettings{.tx_mode = ydb::TransactionMode::kStaleRO},
            ydb::Query{"SELECT 1;"}
        );
        // ...
    }
    ```

  {% endlist %}
- Go

  {% list tabs %}

  - Native SDK

    ```go
    package main

    import (
      "context"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/query"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      row, err := db.Query().QueryRow(ctx, "SELECT 1",
        query.WithTxControl(query.StaleReadOnlyTxControl()),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
      // working with row
      _ = row
    }
    ```

  - database/sql

    ```go
    package main

    import (
      "context"
      "database/sql"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
      "github.com/ydb-platform/ydb-go-sdk/v3/table"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer nativeDriver.Close(ctx)

      connector, err := ydb.Connector(nativeDriver)
      if err != nil {
        panic(err)
      }
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()

      err = retry.Do(
        ydb.WithTxControl(ctx, table.StaleReadOnlyTxControl()),
        db,
        func(ctx context.Context, conn *sql.Conn) error {
          row := conn.QueryRowContext(ctx, "SELECT 1")
          var result int
          return row.Scan(&result)
        },
        retry.WithIdempotent(true),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    // ...
    try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
        QueryReader reader = retryCtx.supplyResult(
            session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.STALE_RO))
        );
        // Working with reader
    }
    ```

  - JDBC

    The Stale Read-Only mode from the Query API is **not set separately** for **single** calls via JDBC. For **snapshot** reads, call **`Connection.setReadOnly(true)`** (in Spring — `@Transactional(readOnly = true)`). For writes, see [ImplicitTx](#implicittx).

  {% endlist %}
- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    def execute_query(pool: ydb.QuerySessionPool):
        def callee(session: ydb.QuerySession):
            with session.transaction(ydb.QueryStaleReadOnly()).execute(
                "SELECT 1",
                commit_tx=True,
            ) as result_sets:
                pass

        pool.retry_operation_sync(callee)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    async def execute_query(pool: ydb.aio.QuerySessionPool):
        async def callee(session):
            async with session.transaction(tx_mode=ydb.QueryStaleReadOnly()) as tx:
                async with await tx.execute("SELECT 1", commit_tx=True) as result_sets:
                    pass

        await pool.retry_operation_async(callee)
    ```

  - SQLAlchemy

    ```python
    import sqlalchemy as sa
    from ydb_sqlalchemy import IsolationLevel

    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    with engine.connect().execution_options(isolation_level=IsolationLevel.STALE_READONLY) as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}
- C#

  {% list tabs %}

  - ADO.NET

    ```csharp
    using Ydb.Sdk.Ado;

    // StaleRo — data may be slightly outdated relative to the current state;
    // provides maximum read speed due to relaxed consistency
    await using var connection = await dataSource.OpenConnectionAsync();
    await using var transaction = await connection.BeginTransactionAsync(TransactionMode.StaleRo);
    await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
    await using var reader = await command.ExecuteReaderAsync();
    await transaction.CommitAsync();
    ```

  - Entity Framework

    Entity Framework does not support the StaleRo mode directly.
    Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  - linq2db

    linq2db does not support the StaleRo mode directly.
    Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endlist %}
- JavaScript


  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'staleReadOnly', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust

  Stale Read-Only mode is not supported in the Rust SDK.
- PHP


  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;

  $config = [
      // YDB config
  ];

  $ydb = new Ydb($config);
  $result = $ydb->table()->retrySession(function (Session $session) {
    $query = $session->newQuery('SELECT 1 AS value;');
    $query->beginTx('stale_read_only');
    return $query->execute();
  }, true);
  ```

{% endlist %}

## Snapshot Read-Only {#snapshot-read-only}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/query/client.h>

    void SnapshotReadOnlyExample(NYdb::NQuery::TSession session) {
        auto settings = NYdb::NQuery::TTxSettings::SnapshotRO();
        auto result = session.ExecuteQuery(
            "SELECT 1",
            NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
        ).GetValueSync();

        // ...
    }
    ```

  - userver

    ```cpp
    #include <userver/ydb/table.hpp>

    void SnapshotReadOnlyExample(ydb::TableClient& client) {
        auto result = client.ExecuteQuery(
            ydb::OperationSettings{.tx_mode = ydb::TransactionMode::kSnapshotRO},
            ydb::Query{"SELECT 1;"}
        );
        // ...
    }
    ```

  {% endlist %}
- Go

  {% list tabs %}

  - Native SDK

    ```go
    package main

    import (
      "context"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/query"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      row, err := db.Query().QueryRow(ctx, "SELECT 1",
        query.WithTxControl(query.SnapshotReadOnlyTxControl()),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
      // working with row
      _ = row
    }
    ```

  - database/sql

    ```go
    package main

    import (
      "context"
      "database/sql"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer nativeDriver.Close(ctx)

      connector, err := ydb.Connector(nativeDriver)
      if err != nil {
        panic(err)
      }
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()

      // Snapshot Read-Only — provides consistent data reading at a specific point in time
      err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
        row := tx.QueryRowContext(ctx, "SELECT 1")
        var result int
        return row.Scan(&result)
      }, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
        Isolation: sql.LevelSnapshot,
        ReadOnly:  true,
      }))
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    // ...
    try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
        QueryReader reader = retryCtx.supplyResult(
            session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.SNAPSHOT_RO))
        );
        // Working with reader
    }
    ```

  - JDBC

    For **single** **read-only** calls via JDBC, the **snapshot** mode is enabled when **`Connection.setReadOnly(true)`** is set on the connection (in Spring — **`@Transactional(readOnly = true)`** propagates this to the driver when opening a transaction). For details, see the [ImplicitTx](#implicittx) section.

  {% endlist %}
- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    def execute_query(pool: ydb.QuerySessionPool):
        def callee(session: ydb.QuerySession):
            with session.transaction(ydb.QuerySnapshotReadOnly()).execute(
                "SELECT 1",
                commit_tx=True,
            ) as result_sets:
                pass

        pool.retry_operation_sync(callee)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    async def execute_query(pool: ydb.aio.QuerySessionPool):
        async def callee(session):
            async with session.transaction(tx_mode=ydb.QuerySnapshotReadOnly()) as tx:
                async with await tx.execute("SELECT 1", commit_tx=True) as result_sets:
                    pass

        await pool.retry_operation_async(callee)
    ```

  - SQLAlchemy

    ```python
    import sqlalchemy as sa
    from ydb_sqlalchemy import IsolationLevel

    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    with engine.connect().execution_options(isolation_level=IsolationLevel.SNAPSHOT_READONLY) as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}
- C#

  {% list tabs %}

  - ADO.NET

    ```csharp
    using Ydb.Sdk.Ado;

    await using var connection = await dataSource.OpenConnectionAsync();
    await using var transaction = await connection.BeginTransactionAsync(TransactionMode.SnapshotRo);
    await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
    await using var reader = await command.ExecuteReaderAsync();
    await transaction.CommitAsync();
    ```

  - Entity Framework

    Entity Framework does not support the Snapshot Read-Only mode directly.
    Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  - linq2db

    linq2db does not support Snapshot Read-Only mode directly. Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endlist %}
- JavaScript


  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'snapshotReadOnly', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust

  Snapshot Read-Only mode is not supported in the Rust SDK.
- PHP


  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;

  $config = [
      // YDB config
  ];

  $ydb = new Ydb($config);
  $result = $ydb->table()->retryTransaction(
    function (Session $session) {
        return $session->query('SELECT 1 AS value;');
    },
    true,
    null,
    ['tx_mode' => 'snapshot_read_only']
  );
  ```

{% endlist %}

## Snapshot Read-Write {#snapshot-read-write}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/query/client.h>

    void SnapshotReadWriteExample(NYdb::NQuery::TSession session) {
        auto settings = NYdb::NQuery::TTxSettings::SnapshotRW();
        auto result = session.ExecuteQuery(
            "SELECT 1",
            NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
        ).GetValueSync();

        // ...
    }
    ```

  - userver

    ```cpp
    #include <userver/ydb/table.hpp>

    void SnapshotReadWriteExample(ydb::TableClient& client) {
        auto result = client.ExecuteQuery(
            ydb::OperationSettings{.tx_mode = ydb::TransactionMode::kSnapshotRW},
            ydb::Query{"SELECT 1;"}
        );
        // ...
    }
    ```

  {% endlist %}
- Go

  {% list tabs %}

  - Native SDK

    ```go
    package main

    import (
      "context"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/query"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      row, err := db.Query().QueryRow(ctx, "SELECT 1",
        query.WithTxControl(query.SnapshotReadWriteTxControl(query.CommitTx())),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
      // working with row
      _ = row
    }
    ```

  - database/sql

    ```go
    package main

    import (
      "context"
      "database/sql"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer nativeDriver.Close(ctx)

      connector, err := ydb.Connector(nativeDriver)
      if err != nil {
        panic(err)
      }
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()

      // Snapshot Read-Write — provides consistent data reading at a specific point in time
      // with write capability
      err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
        row := tx.QueryRowContext(ctx, "SELECT 1")
        var result int
        return row.Scan(&result)
      }, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
        Isolation: sql.LevelSnapshot,
        ReadOnly:  false,
      }))
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    // ...
    try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
        QueryReader reader = retryCtx.supplyResult(
            session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.SNAPSHOT_RW))
        );
        // Working with reader
    }
    ```

  - JDBC

    Snapshot Read-Write mode from the Query API is **not set separately** for **single** calls via JDBC; for write queries, the current driver implementation uses **Serializable Read/Write** (see [ImplicitTx](#implicittx) and [Serializable](#serializable)).

  {% endlist %}
- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    def execute_query(pool: ydb.QuerySessionPool):
        def callee(session: ydb.QuerySession):
            with session.transaction(ydb.QuerySnapshotReadWrite()).execute(
                "SELECT 1",
                commit_tx=True,
            ) as result_sets:
                pass

        pool.retry_operation_sync(callee)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    async def execute_query(pool: ydb.aio.QuerySessionPool):
        async def callee(session):
            async with session.transaction(tx_mode=ydb.QuerySnapshotReadWrite()) as tx:
                async with await tx.execute("SELECT 1", commit_tx=True) as result_sets:
                    pass

        await pool.retry_operation_async(callee)
    ```

  - SQLAlchemy

    ```python
    import sqlalchemy as sa
    from ydb_sqlalchemy import IsolationLevel

    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    with engine.connect().execution_options(isolation_level=IsolationLevel.SNAPSHOT_READWRITE) as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}
- C#

  {% list tabs %}

  - ADO.NET

    ```csharp
    await _ydbDataSource.ExecuteInTransactionAsync(async ydbConnection =>
        {
            var ydbCommand = ydbConnection.CreateCommand();
            ydbCommand.CommandText = """
                                     UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
                                     VALUES (2, 5, 13, "Test Episode", Date("2018-08-27"))
                                     """;
            await ydbCommand.ExecuteNonQueryAsync();
            ydbCommand.CommandText = """
                                     INSERT INTO episodes(series_id, season_id, episode_id, title, air_date)
                                     VALUES
                                         (2, 5, 21, "Test 21", Date("2018-08-27")),
                                         (2, 5, 22, "Test 22", Date("2018-08-27"))
                                     """;
            await ydbCommand.ExecuteNonQueryAsync();
        }, TransactionMode.SnapshotRw
    );
    ```

  - Entity Framework

    ```csharp
    var strategy = db.Database.CreateExecutionStrategy();

    strategy.Execute(() =>
        {
            using var ctx = new AppDbContext(options);
            using var tr = ctx.Database.BeginTransaction(IsolationLevel.Snapshot);

            ctx.Users.AddRange(
                new User { Name = "Alex", Email = "alex@example.com" },
                new User { Name = "Kirill", Email = "kirill@example.com" }
            );

            ctx.SaveChanges();

            var users = ctx.Users.OrderBy(u => u.Id).ToList();
            Console.WriteLine("Users in database:");
            foreach (var user in users)
                Console.WriteLine($"- {user.Id}: {user.Name} ({user.Email})");
          }
    );
    ```

  - linq2db

    ```csharp
    await using var db = new MyYdb(BuildOptions());
    await using var tr = await db.BeginTransactionAsync(IsolationLevel.Snapshot);

    await db.InsertAsync(new Episode
    {
        SeriesId = 2, SeasonId = 5, EpisodeId = 13, Title = "Test Episode", AirDate = new DateTime(2018, 08, 27)
    });
    await db.InsertAsync(new Episode
        { SeriesId = 2, SeasonId = 5, EpisodeId = 21, Title = "Test 21", AirDate = new DateTime(2018, 08, 27) });
    await db.InsertAsync(new Episode
        { SeriesId = 2, SeasonId = 5, EpisodeId = 22, Title = "Test 22", AirDate = new DateTime(2018, 08, 27) });

    await tr.CommitAsync();
    ```

  {% endlist %}
- JavaScript


  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'snapshotReadWrite', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust

  Snapshot Read-Write mode is not supported in the Rust SDK.
- PHP

  Snapshot Read-Write mode is not supported in the PHP SDK.

{% endlist %}
