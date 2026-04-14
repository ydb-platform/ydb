# Установка режима выполнения транзакции

Для выполнения запросов в {{ ydb-short-name }} SDK необходимо указывать [режим выполнения транзакции](../../concepts/transactions.md#modes).

Ниже приведены примеры кода, которые используют встроенные в {{ ydb-short-name }} SDK средства создания объекта *режим выполнения транзакции*.

## ImplicitTx {#implicittx}

[ImplicitTx](../../concepts/transactions#implicit) режим позволяет выполнить один запрос без явного управления транзакцией. Запрос выполняется в своей собственной неявной транзакции, которая автоматически фиксируется в случае успеха.

{% list tabs group=lang %}

- C++

  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>

  void ExecuteQuery(NYdb::NQuery::TSession session) {
    auto result = session.ExecuteQuery(
        "SELECT 1",
        NYdb::NQuery::TTxControl::NoTx()
    ).GetValueSync();

    // ...
  }
  ```

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
      // работа с row
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

      // ImplicitTx - запрос без явной транзакции (авто-коммит)
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
        // работа с reader
    }
    ```

  - JDBC

    На стороне JDBC неявный режим связан с авто-коммитом (`Connection.setAutoCommit(true)` по умолчанию): каждый отдельный запрос оформляется как отдельная транзакция и фиксируется автоматически. При `setAutoCommit(false)` границы задаются явными `commit`/`rollback`.

    В **текущей реализации** {{ ydb-short-name }} JDBC-драйвера для **одиночных** вызовов при **чтении** режим **snapshot** включается, если на `Connection` вызван **`setReadOnly(true)`**. Для запросов **с записью** применяется **Serializable Read/Write** (на соединении не должен быть включён режим только чтения).

    В **Spring** атрибут **`@Transactional(readOnly = true)`** при старте транзакции приводит к вызову `Connection.setReadOnly(true)` на соединении, поэтому для read-only-сценариев snapshot выбирается автоматически. Hibernate, JOOQ и другие обёртки над JDBC используют то же соединение — флаг read-only нужно выставлять так же явно (или через транзакционные настройки фреймворка, если они прокидывают его в `Connection`).

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
    // Выполнение без явной транзакции (авто-коммит)
    await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
    await command.ExecuteNonQueryAsync();
    ```

  - Entity Framework

    ```csharp
    using Microsoft.EntityFrameworkCore;

    await using var context = await dbContextFactory.CreateDbContextAsync();
    // Режим авто-коммита Entity Framework (без явной транзакции)
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
    // Режим авто-коммита linq2db (без явной транзакции)
    var result = db.GetTable<Employee>().FirstOrDefault(e => e.Id == 1);
    ```

  {% endlist %}

- JavaScript

  ```javascript
  import { sql } from '@ydbjs/query';

  // ...

  // ImplicitTx - один запрос без явной транзакции
  const result = await sql`SELECT 1`;
  ```

- Rust

  Режим ImplicitTx не поддерживается

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

  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>

  void ExecuteQuery(NYdb::NQuery::TSession session) {
      auto settings = NYdb::NQuery::TTxSettings::SerializableRW();
      auto result = session.ExecuteQuery(
          "SELECT 1",
          NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
      ).GetValueSync();

      // ...
  }
  ```

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
      // работа с row
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
        // Режим Serializable Read-Write используется по умолчанию для транзакций
        // Либо его можно установить явно как приведено ниже
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
        // Работа с reader
    }
    ```

  - JDBC

    Для **одиночных** вызовов через JDBC в текущей реализации драйвера используется **Serializable Read/Write** — в том числе при работе из Spring Boot, ORM и других обёрток над JDBC. Режим **snapshot** для чтений задаётся через **`setReadOnly(true)`** (см. [ImplicitTx](#implicittx)). Семантика авто-коммита и явных транзакций описана там же.

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

    // Режим Serializable используется по умолчанию
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

    // Entity Framework использует режим Serializable по умолчанию
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

    // linq2db использует режим Serializable по умолчанию
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

  // Режим Serializable Read-Write используется по умолчанию
  await sql.begin({ idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });

  // Или явно указать режим транзакции
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

  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>

  void ExecuteQuery(NYdb::NQuery::TSession session) {
      auto settings = NYdb::NQuery::TTxSettings::OnlineRO();
      auto result = session.ExecuteQuery(
          "SELECT 1",
          NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
      ).GetValueSync();

      // ...
  }
  ```

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
      // работа с row
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
        // Работа с reader
    }
    ```

  - JDBC

    Режим Online Read-Only из Query API для **одиночных** вызовов через JDBC **отдельно не задаётся**. Для чтения в **snapshot** вызовите **`Connection.setReadOnly(true)`** (в Spring — `@Transactional(readOnly = true)`). Запись — см. [ImplicitTx](#implicittx).

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

    // OnlineRo — данные максимально актуальны на момент каждой операции чтения;
    // данные в рамках одной транзакции согласованы
    await using var connection = await dataSource.OpenConnectionAsync();
    await using var transaction = await connection.BeginTransactionAsync(TransactionMode.OnlineRo);
    await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
    await using var reader = await command.ExecuteReaderAsync();
    await transaction.CommitAsync();

    // OnlineInconsistentRo — максимальная производительность, минимальная согласованность:
    // данные могут быть несогласованы даже в рамках одной операции чтения
    await using var connection2 = await dataSource.OpenConnectionAsync();
    await using var transaction2 = await connection2.BeginTransactionAsync(TransactionMode.OnlineInconsistentRo);
    await using var command2 = new YdbCommand(connection2) { CommandText = "SELECT 1" };
    await using var reader2 = await command2.ExecuteReaderAsync();
    await transaction2.CommitAsync();
    ```

  - Entity Framework

    Entity Framework не поддерживает режим OnlineRo напрямую.
    Используйте ydb-dotnet-sdk или ADO.NET для этого уровня изоляции.

  - linq2db

    linq2db не поддерживает режим OnlineRo напрямую.
    Используйте ydb-dotnet-sdk или ADO.NET для этого уровня изоляции.

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

  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>

  void ExecuteQuery(NYdb::NQuery::TSession session) {
      auto settings = NYdb::NQuery::TTxSettings::StaleRO();
      auto result = session.ExecuteQuery(
          "SELECT 1",
          NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
      ).GetValueSync();

      // ...
  }
  ```

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
      // работа с row
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
        // Работа с reader
    }
    ```

  - JDBC

    Режим Stale Read-Only из Query API для **одиночных** вызовов через JDBC **отдельно не задаётся**. Для чтения в **snapshot** вызовите **`Connection.setReadOnly(true)`** (в Spring — `@Transactional(readOnly = true)`). Запись — см. [ImplicitTx](#implicittx).

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

    // StaleRo — данные могут быть незначительно устаревшими относительно актуального состояния;
    // обеспечивает максимальную скорость чтения за счёт ослабленной согласованности
    await using var connection = await dataSource.OpenConnectionAsync();
    await using var transaction = await connection.BeginTransactionAsync(TransactionMode.StaleRo);
    await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
    await using var reader = await command.ExecuteReaderAsync();
    await transaction.CommitAsync();
    ```

  - Entity Framework

    Entity Framework не поддерживает режим StaleRo напрямую.
    Используйте ydb-dotnet-sdk или ADO.NET для этого уровня изоляции.

  - linq2db

    linq2db не поддерживает режим StaleRo напрямую.
    Используйте ydb-dotnet-sdk или ADO.NET для этого уровня изоляции.

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

  Режим Stale Read-Only не поддерживается в rust sdk.

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

  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>

  void ExecuteQuery(NYdb::NQuery::TSession session) {
      auto settings = NYdb::NQuery::TTxSettings::SnapshotRO();
      auto result = session.ExecuteQuery(
          "SELECT 1",
          NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
      ).GetValueSync();

      // ...
  }
  ```

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
      // работа с row
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

      // Snapshot Read-Only — обеспечивает согласованное чтение данных на определённый момент времени
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
        // Работа с reader
    }
    ```

  - JDBC

    Для **одиночных** вызовов **только на чтение** через JDBC режим **snapshot** включается, когда на соединении выставлен **`Connection.setReadOnly(true)`** (в Spring — **`@Transactional(readOnly = true)`** прокидывает это в драйвер при открытии транзакции). Подробнее — в разделе [ImplicitTx](#implicittx).

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

    Entity Framework не поддерживает режим Snapshot Read-Only напрямую.
    Используйте ydb-dotnet-sdk или ADO.NET для этого уровня изоляции.

  - linq2db

    linq2db не поддерживает режим Snapshot Read-Only напрямую.
    Используйте ydb-dotnet-sdk или ADO.NET для этого уровня изоляции.

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

  Режим Snapshot Read-Only не поддерживается в rust sdk.

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

  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>

  void ExecuteQuery(NYdb::NQuery::TSession session) {
      auto settings = NYdb::NQuery::TTxSettings::SnapshotRW();
      auto result = session.ExecuteQuery(
          "SELECT 1",
          NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
      ).GetValueSync();

      // ...
  }
  ```

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
      // работа с row
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

      // Snapshot Read-Write — обеспечивает согласованное чтение данных на определённый момент времени
      // с возможностью записи
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
        // Работа с reader
    }
    ```

  - JDBC

    Режим Snapshot Read-Write из Query API для **одиночных** вызовов через JDBC **отдельно не задаётся**; для запросов с записью в текущей реализации драйвера используется **Serializable Read/Write** (см. [ImplicitTx](#implicittx) и [Serializable](#serializable)).

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

  Режим Snapshot Read-Write не поддерживается в rust sdk.

- PHP

  Режим Snapshot Read-Write не поддерживается в PHP SDK.

{% endlist %}
