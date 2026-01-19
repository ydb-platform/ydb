# Установка режима выполнения транзакции

Для выполнения запросов в {{ ydb-short-name }} SDK необходимо указывать [режим выполнения транзакции](../../concepts/transactions.md#modes).

Ниже приведены примеры кода, которые используют встроенные в {{ ydb-short-name }} SDK средства создания объекта *режим выполнения транзакции*.

## ImplicitTx {#implicittx}

[ImplicitTx](../../concepts/transactions#implicit) режим позволяет выполнить один запрос без явного управления транзакцией. Запрос выполняется в своей собственной неявной транзакции, которая автоматически фиксируется в случае успеха.

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

    // ImplicitTx - запрос без явной транзакции (авто-коммит)
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
      query.WithTxControl(query.NoTx()),
    )
    if err != nil {
      fmt.Printf("unexpected error: %v", err)
    }
    // работа с row
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
      // Режим авто-коммита (по умолчанию)
      connection.setAutoCommit(true);

      try (Statement statement = connection.createStatement()) {
          ResultSet rs = statement.executeQuery("SELECT 1");
          // работа с rs
      }
  }
  ```

  {% endcut %}

  ```java
  import tech.ydb.query.QueryClient;
  import tech.ydb.query.tools.QueryReader;
  import tech.ydb.query.tools.SessionRetryContext;

  // ...
  try (QueryClient queryClient = QueryClient.newClient(transport).build()) {
      SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
      QueryReader reader = retryCtx.supplyResult(
          session -> QueryReader.readFrom(session.createQuery("SELECT 1"))
      );
      // работа с reader
  }
  ```

- Python

  {% cut "dbapi" %}

  ```python
  import ydb_dbapi

  with ydb_dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      # Режим авто-коммита
      connection.set_autocommit(True)
      with connection.cursor() as cursor:
          cursor.execute("SELECT 1")
          row = cursor.fetchone()
  ```

  {% endcut %}

  ```python
  import ydb

  def execute_query(pool: ydb.QuerySessionPool):
      # ImplicitTx - запрос без явной транзакции
      def callee(session: ydb.QuerySession):
          with session.execute(
              "SELECT 1",
          ) as result_sets:
              pass  # работа с result_sets

      pool.retry_operation_sync(callee)
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

  await using var connection = await dataSource.OpenConnectionAsync();
  // Выполнение без явной транзакции (авто-коммит)
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1" };
  await command.ExecuteNonQueryAsync();
  ```

  {% endcut %}

  {% cut "Entity Framework" %}

  ```csharp
  using Microsoft.EntityFrameworkCore;

  // Режим авто-коммита Entity Framework (без явной транзакции)
  await using var context = await dbContextFactory.CreateDbContextAsync();
  var result = await context.SomeEntities.FirstOrDefaultAsync();
  ```

  {% endcut %}

  {% cut "linq2db" %}

  ```csharp
  using LinqToDB;
  using LinqToDB.Data;

  // Режим авто-коммита linq2db (без явной транзакции)
  using var db = new DataConnection(
      new DataOptions().UseConnectionString(
          "YDB",
          "Host=localhost;Port=2136;Database=/local;UseTls=false"
      )
  );

  var result = db.GetTable<Employee>().FirstOrDefault(e => e.Id == 1);
  ```

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Services.Query;

  // ImplicitTx - один запрос без явной транзакции
  var response = await queryClient.Exec(
      "SELECT 1",
      txMode: TxMode.None
  );
  ```

- Js/Ts

  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  // ImplicitTx - один запрос без явной транзакции
  const result = await sql`SELECT 1`;
  ```

- Rust

  ```rust
  use ydb::Query;

  // ImplicitTx - запрос без явной транзакции
  let query = Query::new("SELECT 1");
  let result = client.query(query, None).await?;
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\AccessTokenAuthentication;

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
  // ImplicitTx - один запрос без явной транзакции
  $result = $ydb->table()->query('SELECT 1;');
  ```

{% endlist %}

## Serializable {#serializable}

{% list tabs group=lang %}

- Go

  {% cut "ydb-go-sdk" %}

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

  {% endcut %}

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

  {% endcut %}

- Java

  {% cut "ydb-java-sdk" %}

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

  {% endcut %}

  {% cut "JDBC" %}

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

  {% endcut %}

- Python

  {% cut "ydb-python-sdk" %}

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

  {% endcut %}

  {% cut "dbapi" %}

  ```python
  import ydb_dbapi

  with ydb_dbapi.connect(host="localhost", port="2136", database="/local") as connection:
      # Serializable режим используется по умолчанию
      with connection.cursor() as cursor:
          cursor.execute("SELECT 1")
          row = cursor.fetchone()
  ```

  {% endcut %}

- C++

  ```cpp
  auto settings = NYdb::NQuery::TTxSettings::SerializableRW();
  auto result = session.ExecuteQuery(
      "SELECT 1",
      NYdb::NQuery::TTxControl::BeginTx(settings).CommitTx()
  ).GetValueSync();
  ```

- C# (.NET)

  {% cut "ydb-dotnet-sdk" %}

  ```csharp
  using Ydb.Sdk.Services.Query;

  // Режим Serializable Read-Write используется по умолчанию
  var response = await queryClient.Exec("SELECT 1");
  ```

  {% endcut %}

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  await using var connection = await dataSource.OpenConnectionAsync();
  // Режим Serializable используется по умолчанию
  await using var transaction = await connection.BeginTransactionAsync(TxMode.SerializableRw);
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
  await command.ExecuteNonQueryAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  {% cut "Entity Framework" %}

  ```csharp
  using Microsoft.EntityFrameworkCore;

  // Entity Framework использует режим Serializable по умолчанию
  await using var context = await dbContextFactory.CreateDbContextAsync();
  await using var transaction = await context.Database.BeginTransactionAsync();
  var result = await context.SomeEntities.FirstOrDefaultAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  {% cut "linq2db" %}

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

  // Выполнение запроса (транзакции управляются автоматически)
  var result = db.GetTable<Employee>().FirstOrDefault(e => e.Id == 1);
  ```

  {% endcut %}

- Js/Ts

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

- Go

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

- Java

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

- C# (.NET)

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  await using var connection = await dataSource.OpenConnectionAsync();
  await using var transaction = await connection.BeginTransactionAsync(TxMode.OnlineRo);
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
  await using var reader = await command.ExecuteReaderAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Services.Query;

  var response = await queryClient.ReadAllRows(
      "SELECT 1",
      txMode: TxMode.OnlineRo
  );
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
  use ydb::{Query, TransactionOptions};

  let query = Query::new("SELECT 1");
  let tx_options = TransactionOptions::new()
      .with_online_read_only();
  let result = client.query(query, tx_options).await?;
  ```

- PHP

  Online Read-Only режим не поддерживается напрямую.
  Используйте Serializable или Snapshot Read-Only для операций чтения.

{% endlist %}

## Stale Read-Only {#stale-read-only}

{% list tabs group=lang %}

- Go

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

- Java

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

- C# (.NET)

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  await using var connection = await dataSource.OpenConnectionAsync();
  await using var transaction = await connection.BeginTransactionAsync(TxMode.StaleRo);
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
  await using var reader = await command.ExecuteReaderAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  ```csharp
  using Ydb.Sdk.Services.Query;

  var response = await queryClient.ReadAllRows(
      "SELECT 1",
      txMode: TxMode.StaleRo
  );
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

  ```rust
  use ydb::{Query, TransactionOptions};

  let query = Query::new("SELECT 1");
  let tx_options = TransactionOptions::new()
      .with_stale_read_only();
  let result = client.query(query, tx_options).await?;
  ```

- PHP

  Stale Read-Only режим не поддерживается напрямую.
  Используйте Serializable или Snapshot Read-Only для операций чтения.

{% endlist %}

## Snapshot Read-Only {#snapshot-read-only}

{% list tabs group=lang %}

- Go

  {% cut "ydb-go-sdk" %}

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

  {% endcut %}

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

  {% endcut %}

- Java

  {% cut "ydb-java-sdk" %}

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

  {% endcut %}

  {% cut "JDBC" %}

  ```java
  import java.sql.Connection;
  import java.sql.DriverManager;
  import java.sql.ResultSet;
  import java.sql.Statement;

  // ...

  try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local")) {
      connection.setAutoCommit(false);
      // Режим SNAPSHOT_RO используется по умолчанию для read-only подключений
      connection.setReadOnly(true);

      try (Statement statement = connection.createStatement()) {
          ResultSet rs = statement.executeQuery("SELECT 1");
          // работа с rs
      }

      connection.commit();
  }
  ```

  {% endcut %}

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

  {% cut "ydb-dotnet-sdk" %}

  ```csharp
  using Ydb.Sdk.Services.Query;

  var response = await queryClient.ReadAllRows(
      "SELECT 1",
      txMode: TxMode.SnapshotRo
  );
  ```

  {% endcut %}

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  await using var connection = await dataSource.OpenConnectionAsync();
  await using var transaction = await connection.BeginTransactionAsync(TxMode.SnapshotRo);
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
  await using var reader = await command.ExecuteReaderAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  {% cut "linq2db" %}

  ```csharp
  using LinqToDB;
  using LinqToDB.Data;

  // linq2db не поддерживает режим Snapshot Read-Only напрямую.
  // Используйте ydb-dotnet-sdk или ADO.NET для этого уровня изоляции.
  ```

  {% endcut %}

- Js/Ts

  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'snapshotReadOnly', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

{% endlist %}

## Snapshot Read-Write {#snapshot-read-write}

{% list tabs group=lang %}

- Go

  {% cut "ydb-go-sdk" %}

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

  {% endcut %}

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

  {% endcut %}

- Java

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

- C# (.NET)

  {% cut "ADO.NET" %}

  ```csharp
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Services.Query;

  await using var connection = await dataSource.OpenConnectionAsync();
  await using var transaction = await connection.BeginTransactionAsync(TxMode.SnapshotRw);
  await using var command = new YdbCommand(connection) { CommandText = "SELECT 1", Transaction = transaction };
  await command.ExecuteNonQueryAsync();
  await transaction.CommitAsync();
  ```

  {% endcut %}

  {% cut "linq2db" %}

  ```csharp
  using LinqToDB;
  using LinqToDB.Data;

  // linq2db не поддерживает режим Snapshot Read-Write напрямую.
  // Используйте ydb-dotnet-sdk или ADO.NET для этого уровня изоляции.
  ```

  {% endcut %}

- Js/Ts

  ```typescript
  import { sql } from '@ydbjs/query';

  // ...

  await sql.begin({ isolation: 'snapshotReadWrite', idempotent: true }, async (tx) => {
      return await tx`SELECT 1`;
  });
  ```

- Rust

  ```rust
  use ydb::{Query, TransactionOptions};

  let query = Query::new("SELECT 1");
  let tx_options = TransactionOptions::new()
      .with_snapshot_read_write();
  let result = client.query(query, tx_options).await?;
  ```

- PHP

  Режим Snapshot Read-Write не поддерживается напрямую в PHP SDK.

{% endlist %}
