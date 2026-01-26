# Setting up the transaction execution mode

To run your queries, first you need to specify the [transaction execution mode](../../concepts/transactions.md#modes) in the {{ ydb-short-name }} SDK.

Below are code examples showing the {{ ydb-short-name }} SDK built-in tools to create an object for the *transaction execution mode*.

## ImplicitTx {#implicittx}

[ImplicitTx](../../concepts/transactions#implicit) mode allows executing a single query without explicit transaction control. The query is executed in its own implicit transaction that automatically commits if successful.

{% list tabs group=lang %}

- Go

  {% cut "database/sql" %}

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

  {% endcut %}

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
    // work with row
    _ = row
  }
  ```

- Java

  {% cut "JDBC" %}

  Functionality is under development.

  {% endcut %}

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
      // work with reader
  }
  ```

- Python

  {% cut "dbapi" %}

  ```python
  import ydb_dbapi as dbapi

  with dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      connection.set_isolation_level(dbapi.IsolationLevel.AUTOCOMMIT)
  ```

  {% endcut %}

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      pool.execute_with_retries("SELECT 1")
  ```

- C++

  ```cpp
  auto result = session.ExecuteQuery(
      "SELECT 1",
      NYdb::NQuery::TTxControl::NoTx()
  ).GetValueSync();
  ```

- C# (.NET)

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;

  await using var connection = await dataSource.OpenRetryableConnectionAsync();

  // Execute without explicit transaction (auto-commit)
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
  await command.ExecuteNonQueryAsync();
  ```

  {% endcut %}

  {% cut "Entity Framework" %}

  ```csharp
  using Microsoft.EntityFrameworkCore;

  await using var context = await dbContextFactory.CreateDbContextAsync();

  // Entity Framework auto-commit mode (no explicit transaction)
  var result = await context.SomeEntities.FirstOrDefaultAsync();
  ```

  {% endcut %}

  {% cut "linq2db" %}

  ```csharp
  using LinqToDB;
  using LinqToDB.Data;

  using var db = new DataConnection(
      new DataOptions().UseConnectionString(
          "YDB",
          "Host=localhost;Port=2136;Database=/local;UseTls=false"
      )
  );

  // linq2db auto-commit mode (no explicit transaction)
  var result = db.GetTable<Employee>().FirstOrDefault(e => e.Id == 1);
  ```

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  // ImplicitTx - single query without explicit transaction
  var response = await queryClient.Exec("SELECT 1");
  ```

- Js/Ts

  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  // ImplicitTx - single query without explicit transaction
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

- Go

  {% cut "database/sql" %}

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
      // The Serializable Read-Write mode is used by default for transactions.
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

  {% endcut %}

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
      /* without explicit tx control option used serializable read-write tx control by default */
      query.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
    )
    if err != nil {
      fmt.Printf("unexpected error: %v", err)
    }
    // work with row
    _ = row
  }
  ```

- Java

  {% cut "JDBC" %}

  Functionality is under development.

  {% endcut %}

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
      // work with reader
  }
  ```

- Python

  {% cut "dbapi" %}

  ```python
  import ydb_dbapi as dbapi

  with dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      connection.set_isolation_level(dbapi.IsolationLevel.SERIALIZABLE)
  ```

  {% endcut %}

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      # Serializable Read-Write mode is used by default
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QuerySerializableReadWrite()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # work with result_sets

      pool.retry_operation_sync(callee)
  ```

- C++

  ```cpp
  auto settings = NYdb::NQuery::TTxSettings::SerializableRW();
  auto result = session.ExecuteQuery(
      "SELECT 1",
      NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
  ).GetValueSync();
  ```

- C# (.NET)

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;

  // Serializable Read-Write mode is used by default
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

  {% endcut %}

  {% cut "Entity Framework" %}

  ```csharp
  var strategy = db.Database.CreateExecutionStrategy();

  // Serializable Read-Write mode is used by default
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

  {% endcut %}

  {% cut "linq2db" %}

  ```csharp
  using LinqToDB;
  using LinqToDB.Data;

  // linq2db uses Serializable isolation by default
  using var db = new DataConnection(
      new DataOptions().UseConnectionString(
          "YDB",
          "Host=localhost;Port=2136;Database=/local;UseTls=false"
      )
  );

  // Serializable Read-Write mode is used by default
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

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Services.Query;

  // Serializable Read-Write mode is used by default
  var response = await queryClient.Exec("SELECT 1");
  ```

- Js/Ts

  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  // Serializable Read-Write mode is used by default
  await sql.begin({ idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });

  // Or explicitly specify transaction mode
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

- Go

  {% cut "database/sql" %}

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

  {% endcut %}

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
    // work with row
    _ = row
  }
  ```

- Java

  {% cut "JDBC" %}

  Functionality is under development.

  {% endcut %}

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
      // work with reader
  }
  ```

- Python

  {% cut "dbapi" %}

  ```python
  import ydb_dbapi as dbapi

  with dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      connection.set_isolation_level(dbapi.IsolationLevel.ONLINE_READONLY)
  ```

  {% endcut %}

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QueryOnlineReadOnly()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # work with result_sets

      pool.retry_operation_sync(callee)
  ```

- C++

  ```cpp
  auto settings = NYdb::NQuery::TTxSettings::OnlineRO();
  auto result = session.ExecuteQuery(
      "SELECT 1",
      NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
  ).GetValueSync();
  ```

- C# (.NET)

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;

  await using var connection = await dataSource.OpenConnectionAsync();
  await using var transaction = await connection.BeginTransactionAsync(TransactionMode.OnlineRo);
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
  await using var reader = await command.ExecuteReaderAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  {% cut "Entity Framework" %}

  Entity Framework does not expose Snapshot Read-Only mode directly.
  Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endcut %}

  {% cut "linq2db" %}

  linq2db does not expose Snapshot Read-Only mode directly.
  Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  var response = await queryClient.ReadAllRows("SELECT 1", txMode: TransactionMode.OnlineRo);
  ```

- Js/Ts

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

- Go

  {% cut "database/sql" %}

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

  {% endcut %}

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
    // work with row
    _ = row
  }
  ```

- Java

  {% cut "JDBC" %}

  Functionality is under development.

  {% endcut %}

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
      // work with reader
  }
  ```

- Python

  {% cut "dbapi" %}

  ```python
  import ydb_dbapi as dbapi

  with dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      connection.set_isolation_level(dbapi.IsolationLevel.STALE_READONLY)
  ```

  {% endcut %}

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QueryStaleReadOnly()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # work with result_sets

      pool.retry_operation_sync(callee)
  ```

- C++

  ```cpp
  auto settings = NYdb::NQuery::TTxSettings::StaleRO();
  auto result = session.ExecuteQuery(
      "SELECT 1",
      NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
  ).GetValueSync();
  ```

- C# (.NET)

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;

  await using var connection = await dataSource.OpenConnectionAsync();
  await using var transaction = await connection.BeginTransactionAsync(TransactionMode.StaleRo);
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
  await using var reader = await command.ExecuteReaderAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  {% cut "Entity Framework" %}

  Entity Framework does not expose StaleRo mode directly.
  Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endcut %}

  {% cut "linq2db" %}

  linq2db does not expose StaleRo mode directly.
  Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  var response = await queryClient.ReadAllRows("SELECT 1", txMode: TransactionMode.StaleRo);
  ```

- Js/Ts

  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'staleReadOnly', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust

  Stale Read-Only mode is not supported.

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

- Go

  {% cut "database/sql" %}

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

    // Snapshot Read-Only - provides consistent reading of data at a specific point in time
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

  {% endcut %}

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
    // work with row
    _ = row
  }
  ```

- Java

  {% cut "JDBC" %}

  ```java
  import java.sql.Connection;
  import java.sql.DriverManager;
  import java.sql.ResultSet;
  import java.sql.Statement;

  // ...

  try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local")) {
      connection.setAutoCommit(false);
      // SNAPSHOT_RO is used by default for read-only connections
      connection.setReadOnly(true);

      try (Statement statement = connection.createStatement()) {
          ResultSet rs = statement.executeQuery("SELECT 1");
          // work with rs
      }

      connection.commit();
  }
  ```

  {% endcut %}

  {% cut "JDBC" %}

  Functionality is under development.

  {% endcut %}

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
      // work with reader
  }
  ```

- Python

  {% cut "dbapi" %}

  ```python
  import ydb_dbapi as dbapi

  with dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      connection.set_isolation_level(dbapi.IsolationLevel.SNAPSHOT_READONLY)
  ```

  {% endcut %}

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QuerySnapshotReadOnly()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # work with result_sets

      pool.retry_operation_sync(callee)
  ```

- C++

  ```cpp
  auto settings = NYdb::NQuery::TTxSettings::SnapshotRO();
  auto result = session.ExecuteQuery(
      "SELECT 1",
      NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
  ).GetValueSync();
  ```

- C# (.NET)

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;

  await using var connection = await dataSource.OpenConnectionAsync();
  await using var transaction = await connection.BeginTransactionAsync(TransactionMode.SnapshotRo);
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
  await using var reader = await command.ExecuteReaderAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  {% cut "Entity Framework" %}

  Entity Framework does not expose SnapshotRo mode directly.
  Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endcut %}

  {% cut "linq2db" %}

  linq2db does not expose SnapshotRo mode directly.
  Use ydb-dotnet-sdk or ADO.NET for this isolation level.

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  var response = await queryClient.ReadAllRows("SELECT 1", TransactionMode.SnapshotRo);
  ```

- Js/Ts

  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'snapshotReadOnly', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust

  Snapshot Read-Only mode is not supported.

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

- Rust

  Snapshot Read-Only mode is not supported.

{% endlist %}

## Snapshot Read-Write {#snapshot-read-write}

{% list tabs group=lang %}

- Go

  {% cut "database/sql" %}

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

    // Snapshot Read-Write - provides consistent reading of data at a specific point in time
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

  {% endcut %}

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
    // work with row
    _ = row
  }
  ```

- Java

  {% cut "JDBC" %}

  Functionality is under development.

  {% endcut %}

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
      // work with reader
  }
  ```

- Python

  {% cut "dbapi" %}

  ```python
  import ydb_dbapi as dbapi

  with dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      connection.set_isolation_level(dbapi.IsolationLevel.SNAPSHOT_READWRITE)
  ```

  {% endcut %}

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QuerySnapshotReadWrite()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # work with result_sets

      pool.retry_operation_sync(callee)
  ```

- C++

  ```cpp
  auto settings = NYdb::NQuery::TTxSettings::SnapshotRW();
  auto result = session.ExecuteQuery(
      "SELECT 1",
      NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
  ).GetValueSync();
  ```

- C# (.NET)

  {% cut "ADO.NET" %}

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
  {% endcut %}

  {% cut "EF" %}

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

  {% endcut %}

  {% cut "linq2db" %}

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

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  var response = await queryClient.ReadAllRows("SELECT 1", TransactionMode.SnapshotRw);
  ```

- Js/Ts

  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'snapshotReadWrite', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust

  Snapshot Read-Write mode is not supported.

- PHP

  Snapshot Read-Write mode is not supported in PHP SDK.

{% endlist %}
