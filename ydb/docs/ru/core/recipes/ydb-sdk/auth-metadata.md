# Аутентификация при помощи сервиса метаданных

{% include [work in progress message](_includes/addition.md) %}

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

      TableClient tableClient = TableClient.newClient(transport).build();

      doWork(tableClient);

      tableClient.close();
      transport.close();
  }
  ```

- Node.js

  {% include [auth-metadata](../../_includes/nodejs/auth-metadata.md) %}

- Python

  {% include [auth-metadata](../../_includes/python/auth-metadata.md) %}

- Python (asyncio)

  {% include [auth-metadata](../../_includes/python/async/auth-metadata.md) %}

- C# (.NET)

  ```C#
  using Ydb.Sdk;
  using Ydb.Sdk.Yc;

  var metadataProvider = new MetadataProvider();

  // Await initial IAM token.
  await metadataProvider.Initialize();

  var config = new DriverConfig(
      endpoint: endpoint, // Database endpoint, "grpcs://host:port"
      database: database, // Full database path
      credentials: metadataProvider
  );

  await using var driver = await Driver.CreateInitialized(config);
  ```

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
