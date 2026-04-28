# Аутентификация при помощи сервиса метаданных

<!-- markdownlint-disable blanks-around-fences -->

Ниже приведены примеры кода аутентификации при помощи переменных окружения в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

  ```go
  package main

  import (
    "context"
    "os"

    "github.com/ydb-platform/ydb-go-sdk/v3"
    yc "github.com/ydb-platform/ydb-go-yc-metadata"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    db, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      yc.WithCredentials(),
      yc.WithInternalCA(), // append Yandex Cloud certificates
    )
    if err != nil {
      panic(err)
    }
    defer db.Close(ctx)
    ...
  }
  ```

- Go (database/sql)

  ```go
  package main

  import (
    "context"
    "database/sql"
    "os"

    "github.com/ydb-platform/ydb-go-sdk/v3"
    yc "github.com/ydb-platform/ydb-go-yc-metadata"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    nativeDriver, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      yc.WithCredentials(),
      yc.WithInternalCA(), // append Yandex Cloud certificates
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
      AuthProvider authProvider = CloudAuthHelper.getMetadataAuthProvider();

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
      Properties props = new Properties();
      props.setProperty("useMetadata", "true");
      try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", props)) {
        doWork(connection);
      }

      // Опцию useMetadata также можно указать прямо в JDBC URL
      try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local?useMetadata=true")) {
        doWork(connection);
      }
  }
  ```

- Node.js

  {% include [auth-metadata](../../_includes/nodejs/auth-metadata.md) %}

- Python

  {% include [auth-metadata](../../_includes/python/auth-metadata.md) %}

- Python (asyncio)

  {% include [auth-metadata](../../_includes/python/async/auth-metadata.md) %}

- C#

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource(
      "Host=ydb.serverless.yandexcloud.net;Port=2135;Database=/ru-central1/<folder-id>/<database-id>;EnableMetadataCredentials=True");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```

<<<<<<< HEAD
=======
  Для Entity Framework и linq2db используйте тот же connectionString.

- Rust

  ```rust
  use ydb::{ClientBuilder, MetadataUrlCredentials, YdbResult};

  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .with_credentials(MetadataUrlCredentials::new())
      .client()?;
  ```

>>>>>>> 317adb799 (dev: update dotnet snippets (#38018))
- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\Implement\MetadataAuthentication;

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

      'credentials' => new MetadataAuthentication()
  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
