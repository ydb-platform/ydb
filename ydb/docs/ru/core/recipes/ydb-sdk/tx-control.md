# Установка режима выполнения транзакции

Для выполнения запросов в {{ ydb-short-name }} SDK необходимо указывать [режим выполнения транзакции](../../concepts/transactions.md#modes).

Ниже приведены примеры кода, которые используют встроенные в {{ ydb-short-name }} SDK средства создания объекта *режим выполнения транзакции*.

## Serializable {#serializable}

{% list tabs group=lang %}

- Go (native)

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

- Go (database/sql)

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

    // Режим Serializable Read-Write используется по умолчанию для транзакций
    err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
      row := tx.QueryRowContext(ctx, "SELECT 1")
      var result int
      return row.Scan(&result)
    }, retry.WithIdempotent(true))
    if err != nil {
      fmt.Printf("unexpected error: %v", err)
    }
  }
  ```

- Java

  ```java
  import tech.ydb.query.QueryClient;
  import tech.ydb.query.tools.QueryReader;
  import tech.ydb.query.tools.SessionRetryContext;

  // ...

  QueryClient queryClient = QueryClient.newClient(transport).build();
  SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

  retryCtx.supplyResult(session -> {
      QueryReader reader = QueryReader.readFrom(
          session.createQuery("SELECT 1", TxMode.SERIALIZABLE_RW)
      ).join().getValue();
      return CompletableFuture.completedFuture(Result.success(reader));
  }).join().getValue();
  ```

- JDBC

  ```java
  import java.sql.Connection;
  import java.sql.DriverManager;
  import java.sql.ResultSet;
  import java.sql.Statement;

  // ...

  try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local")) {
      // Режим Serializable используется по умолчанию
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

      try (Statement statement = connection.createStatement()) {
          ResultSet rs = statement.executeQuery("SELECT 1");
          // работа с rs
      }

      connection.commit();
  }
  ```

- Python

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      # Режим Serializable Read-Write используется по умолчанию
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QuerySerializableReadWrite()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # работа с result_sets

      pool.retry_operation_sync(callee)
  ```

- Python (dbapi)

  ```python
  import ydb.dbapi

  with ydb.dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      # Serializable режим используется по умолчанию
      with connection.cursor() as cursor:
          cursor.execute("SELECT 1")
          row = cursor.fetchone()
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

  ```csharp
  using Ydb.Sdk.Services.Query;

  // Режим Serializable Read-Write используется по умолчанию
  var response = await queryClient.Exec("SELECT 1");
  ```

- Node.js

  ```typescript
  import { Driver, QuerySession } from 'ydb-sdk';

  // ...

  await driver.queryClient.do({
      fn: async (session: QuerySession) => {
          const result = await session.execute({
              text: 'SELECT 1',
              txControl: { beginTx: { serializableReadWrite: {} }, commitTx: true },
          });
          // работа с result
      },
  });
  ```

- Rust

  ```rust
  use ydb::{Query, TransactionOptions};

  let query = Query::new("SELECT 1");
  let tx_options = TransactionOptions::new()
      .with_serializable_read_write();
  let result = client.query(query, tx_options).await?;
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;

  $config = [
      // Database path
      'database'    => '/ru-central1/b1glxxxxxxxxxxxxxxxx/etn0xxxxxxxxxxxxxxxx',

      // Database endpoint
      'endpoint'    => 'ydb.serverless.yandexcloud.net:2135',

      // Auto discovery (dedicated server only)
      'discovery'   => false,

      // IAM config
      'iam_config'  => [
          // 'root_cert_file' => './CA.pem',  Root CA file (uncomment for dedicated server only)
      ],

      'credentials' => new AccessTokenAuthentication('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') // use from reference/ydb-sdk/auth
  ];

  $ydb = new Ydb($config);
  $ydb->table()->retryTransaction(function(Session $session){
    $session->query('SELECT 1;');
  })
  ```

{% endlist %}

## Online Read-Only {#online-read-only}

{% list tabs group=lang %}

- Go (native)

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

- Go (database/sql)

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

    // Online Read-Only — режим чтения, дающий доступ к актуальным данным
    // без строгих гарантий согласованности
    err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
      row := tx.QueryRowContext(ctx, "SELECT 1")
      var result int
      return row.Scan(&result)
    }, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
      Isolation: sql.LevelReadCommitted,
      ReadOnly:  true,
    }))
    if err != nil {
      fmt.Printf("unexpected error: %v", err)
    }
  }
  ```

- Java

  ```java
  import tech.ydb.query.QueryClient;
  import tech.ydb.query.tools.QueryReader;
  import tech.ydb.query.tools.SessionRetryContext;

  // ...

  QueryClient queryClient = QueryClient.newClient(transport).build();
  SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

  retryCtx.supplyResult(session -> {
      QueryReader reader = QueryReader.readFrom(
          session.createQuery("SELECT 1", TxMode.ONLINE_RO)
      ).join().getValue();
      return CompletableFuture.completedFuture(Result.success(reader));
  }).join().getValue();
  ```

- Python

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QueryOnlineReadOnly()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # работа с result_sets

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

{% endlist %}

## Stale Read-Only {#stale-read-only}

{% list tabs group=lang %}

- Go (native)

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

- Go (database/sql)

  Режим Stale Read-Only не поддерживается напрямую в стандартном интерфейсе `database/sql`. Рекомендуется использовать нативный Go SDK для этого режима транзакций.

- Java

  ```java
  import tech.ydb.query.QueryClient;
  import tech.ydb.query.tools.QueryReader;
  import tech.ydb.query.tools.SessionRetryContext;

  // ...

  QueryClient queryClient = QueryClient.newClient(transport).build();
  SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

  retryCtx.supplyResult(session -> {
      QueryReader reader = QueryReader.readFrom(
          session.createQuery("SELECT 1", TxMode.STALE_RO)
      ).join().getValue();
      return CompletableFuture.completedFuture(Result.success(reader));
  }).join().getValue();
  ```

- Python

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QueryStaleReadOnly()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # работа с result_sets

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

{% endlist %}

## Snapshot Read-Only {#snapshot-read-only}

{% list tabs group=lang %}

- Go (native)

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

- Go (database/sql)

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

- Java

  ```java
  import tech.ydb.query.QueryClient;
  import tech.ydb.query.tools.QueryReader;
  import tech.ydb.query.tools.SessionRetryContext;

  // ...

  QueryClient queryClient = QueryClient.newClient(transport).build();
  SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

  retryCtx.supplyResult(session -> {
      QueryReader reader = QueryReader.readFrom(
          session.createQuery("SELECT 1", TxMode.SNAPSHOT_RO)
      ).join().getValue();
      return CompletableFuture.completedFuture(Result.success(reader));
  }).join().getValue();
  ```

- JDBC

  ```java
  import java.sql.Connection;
  import java.sql.DriverManager;
  import java.sql.ResultSet;
  import java.sql.Statement;

  // ...

  try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local")) {
      connection.setAutoCommit(false);
      connection.setReadOnly(true);

      try (Statement statement = connection.createStatement()) {
          ResultSet rs = statement.executeQuery("SELECT 1");
          // работа с rs
      }

      connection.commit();
  }
  ```

- Python

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QuerySnapshotReadOnly()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # работа с result_sets

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

  ```csharp
  using Ydb.Sdk.Services.Query;

  var response = await queryClient.ReadAllRows(
      "SELECT 1",
      txMode: TxMode.SnapshotRo
  );
  ```

- Node.js

  ```typescript
  import { Driver, QuerySession } from 'ydb-sdk';

  // ...

  await driver.queryClient.do({
      fn: async (session: QuerySession) => {
          const result = await session.execute({
              text: 'SELECT 1',
              txControl: { beginTx: { snapshotReadOnly: {} }, commitTx: true },
          });
          // работа с result
      },
  });
  ```

{% endlist %}

## Snapshot Read-Write {#snapshot-read-write}

{% list tabs group=lang %}

- Go (native)

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

- Go (database/sql)

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

- Java

  ```java
  import tech.ydb.query.QueryClient;
  import tech.ydb.query.tools.QueryReader;
  import tech.ydb.query.tools.SessionRetryContext;

  // ...

  QueryClient queryClient = QueryClient.newClient(transport).build();
  SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

  retryCtx.supplyResult(session -> {
      QueryReader reader = QueryReader.readFrom(
          session.createQuery("SELECT 1", TxMode.SNAPSHOT_RW)
      ).join().getValue();
      return CompletableFuture.completedFuture(Result.success(reader));
  }).join().getValue();
  ```

- Python

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      def callee(session: ydb.QuerySession):
          with session.transaction(ydb.QuerySnapshotReadWrite()).execute(
              "SELECT 1",
              commit_tx=True,
          ) as result_sets:
              pass  # работа с result_sets

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

{% endlist %}
