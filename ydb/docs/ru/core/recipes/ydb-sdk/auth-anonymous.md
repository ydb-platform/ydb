# Анонимная аутентификация

<!-- markdownlint-disable blanks-around-fences -->

Ниже приведены примеры кода анонимной аутентификации в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

  Анонимная аутентификация является аутентификацией по умолчанию.
  Явным образом анонимную аутентификацию можно включить так:

  ```go
  package main

  import (
    "context"

    "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    db, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      ydb.WithAnonymousCredentials(),
    )
    if err != nil {
      panic(err)
    }
    defer db.Close(ctx)
    ...
  }
  ```

- Go (database/sql)

  Анонимная аутентификация является аутентификацией по умолчанию.
  Явным образом анонимную аутентификацию можно включить так:

  ```go
  package main

  import (
    "context"
    "database/sql"
    "os"

    "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    nativeDriver, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      ydb.WithAnonymousCredentials(),
    )
    if err != nil {
      panic(err)
    }
    defer nativeDriver.Close(ctx)
    connector, err := ydb.Connector(nativeDriver)
    if err != nil {
      panic(err)
    }
    db := sql.OpenDB(connector)
    defer db.Close()
    ...
  }
  ```

- Java

  ```java
  public void work(String connectionString) {
      AuthProvider authProvider = NopAuthProvider.INSTANCE;

      GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
              .withAuthProvider(authProvider)
              .build());

      QueryClient queryClient = QueryClient.newClient(transport).build();

      doWork(queryClient);

      queryClient.close();
      transport.close();
  }
  ```

- JDBC

  ```java
  public void work() {
      // Подключение без дополнительных опций будет осуществляться с анонимной аутентификацией
      try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local")) {
        doWork(connection);
      }
  }
  ```

- Node.js

  {% include [auth-anonymous](../../_includes/nodejs/auth-anonymous.md) %}

- Python

  {% include [auth-anonymous](../../_includes/python/auth-anonymous.md) %}

- Python (asyncio)

  {% include [auth-anonymous](../../_includes/python/async/auth-anonymous.md) %}

- C#

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```

<<<<<<< HEAD
=======
  Для Entity Framework и linq2db используйте тот же connectionString.

- Rust

  ```rust
  use ydb::{AnonymousCredentials, ClientBuilder, YdbResult};

  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .with_credentials(AnonymousCredentials::new())
      .client()?;
  ```

>>>>>>> 317adb799 (dev: update dotnet snippets (#38018))
- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\Implement\AnonymousAuthentication;

  $config = [

      // Database path
      'database'    => '/local',

      // Database endpoint
      'endpoint'    => 'localhost:2136',

      // Auto discovery (dedicated server only)
      'discovery'   => false,

      // IAM config
      'iam_config'  => [
          'insecure' => true,
          // 'root_cert_file' => './CA.pem', // Root CA file (uncomment for dedicated server)
      ],

      'credentials' => new AnonymousAuthentication()
  ];

  $ydb = new Ydb($config);
  ```

- {% endlist %}
