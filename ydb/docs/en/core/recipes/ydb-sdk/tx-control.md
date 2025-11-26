# Setting up the transaction execution mode

To run your queries, first you need to specify the [transaction execution mode](../../concepts/transactions.md#modes) in the {{ ydb-short-name }} SDK.

Below are code examples showing the {{ ydb-short-name }} SDK built-in tools to create an object for the *transaction execution mode*.

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

- Java

   ```java
   import tech.ydb.query.QueryClient;
   import tech.ydb.query.TxMode;
   import tech.ydb.query.tools.QueryReader;
   import tech.ydb.query.tools.SessionRetryContext;
   import tech.ydb.core.Result;
   import java.util.concurrent.CompletableFuture;

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
       // Serializable mode is used by default
       connection.setAutoCommit(false);
       connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

       try (Statement statement = connection.createStatement()) {
           ResultSet rs = statement.executeQuery("SELECT 1");
           // work with rs
       }

       connection.commit();
   }
   ```

- Python

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

- Python (dbapi)

   ```python
   import ydb.dbapi

   with ydb.dbapi.connect(host="localhost", port="2136", database="/local") as connection:
       # Serializable mode is used by default
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

   // Serializable Read-Write mode is used by default
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
           // work with result
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

- C# (ADO.NET)

   ```csharp
   using Ydb.Sdk.Ado;
   using Ydb.Sdk.Services.Query;

   await using var connection = await dataSource.OpenConnectionAsync();
   // Serializable is the default transaction mode
   await using var transaction = await connection.BeginTransactionAsync(TxMode.SerializableRw);
   await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
   await command.ExecuteNonQueryAsync();
   await transaction.CommitAsync();
   ```

- C# (Entity Framework)

   ```csharp
   using Microsoft.EntityFrameworkCore;

   // Entity Framework uses Serializable isolation by default
   await using var context = await dbContextFactory.CreateDbContextAsync();
   await using var transaction = await context.Database.BeginTransactionAsync();
   var result = await context.SomeEntities.FirstOrDefaultAsync();
   await transaction.CommitAsync();
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
     // work with row
     _ = row
   }
   ```

- Java

   ```java
   import tech.ydb.query.QueryClient;
   import tech.ydb.query.TxMode;
   import tech.ydb.query.tools.QueryReader;
   import tech.ydb.query.tools.SessionRetryContext;
   import tech.ydb.core.Result;
   import java.util.concurrent.CompletableFuture;

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
     // work with row
     _ = row
   }
   ```

- Java

   ```java
   import tech.ydb.query.QueryClient;
   import tech.ydb.query.TxMode;
   import tech.ydb.query.tools.QueryReader;
   import tech.ydb.query.tools.SessionRetryContext;
   import tech.ydb.core.Result;
   import java.util.concurrent.CompletableFuture;

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
     // work with row
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

- Java

   ```java
   import tech.ydb.query.QueryClient;
   import tech.ydb.query.TxMode;
   import tech.ydb.query.tools.QueryReader;
   import tech.ydb.query.tools.SessionRetryContext;
   import tech.ydb.core.Result;
   import java.util.concurrent.CompletableFuture;

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
           // work with rs
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

   ```csharp
   using Ydb.Sdk.Services.Query;

   var response = await queryClient.ReadAllRows(
       "SELECT 1",
       txMode: TxMode.SnapshotRo
   );
   ```

- C# (ADO.NET)

   ```csharp
   using Ydb.Sdk.Ado;
   using Ydb.Sdk.Services.Query;

   await using var connection = await dataSource.OpenConnectionAsync();
   await using var transaction = await connection.BeginTransactionAsync(TxMode.SnapshotRo);
   await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
   await using var reader = await command.ExecuteReaderAsync();
   await transaction.CommitAsync();
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
           // work with result
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
     // work with row
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

- Java

   ```java
   import tech.ydb.query.QueryClient;
   import tech.ydb.query.TxMode;
   import tech.ydb.query.tools.QueryReader;
   import tech.ydb.query.tools.SessionRetryContext;
   import tech.ydb.core.Result;
   import java.util.concurrent.CompletableFuture;

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

- C# (ADO.NET)

   ```csharp
   using Ydb.Sdk.Ado;
   using Ydb.Sdk.Services.Query;

   await using var connection = await dataSource.OpenConnectionAsync();
   await using var transaction = await connection.BeginTransactionAsync(TxMode.SnapshotRw);
   await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
   await command.ExecuteNonQueryAsync();
   await transaction.CommitAsync();
   ```

{% endlist %}
