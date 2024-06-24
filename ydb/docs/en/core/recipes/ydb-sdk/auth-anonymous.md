# Anonymous authentication

{% include [work in progress message](_includes/addition.md) %}

Below are examples of the code for anonymous authentication in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

  By default, anonymous authentication is used.
  You can explicitly enable anonymous authentication as follows:
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

  By default, anonymous authentication is used.
  You can explicitly enable anonymous authentication as follows:
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

      TableClient tableClient = TableClient.newClient(transport).build();

      doWork(tableClient);

      tableClient.close();
      transport.close();
  }
  ```

- Node.js

  {% include [auth-anonymous](../../_includes/nodejs/auth-anonymous.md) %}

- Python

  {% include [auth-anonymous](../../_includes/python/auth-anonymous.md) %}

- Python (asyncio)

  {% include [auth-anonymous](../../_includes/python/async/auth-anonymous.md) %}

- C# (.NET)

  ```C#
  using Ydb.Sdk;
  using Ydb.Sdk.Auth;

  const string endpoint = "grpc://localhost:2136";
  const string database = "/local";

  var config = new DriverConfig(
      endpoint: endpoint,
      database: database,
      credentials: new AnonymousProvider()
  );

  await using var driver = await Driver.CreateInitialized(config);
  ```

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

{% endlist %}
