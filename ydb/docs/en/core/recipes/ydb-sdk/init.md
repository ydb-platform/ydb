---
title: "Instructions for initializing the driver in {{ ydb-short-name }}"
description: "The article describes the examples of the code for connecting to {{ ydb-short-name }} (driver creation) in different {{ ydb-short-name }} SDKs."
---

# Initialize the driver

{% include [work in progress message](_includes/addition.md) %}

To connect to {{ ydb-short-name }}, you need to specify the required and additional parameters that define the driver's behavior (learn more in [Connecting to the {{ ydb-short-name }} server](../../concepts/connect.md)).

Below are examples of the code for connecting to {{ ydb-short-name }} (driver creation) in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

  ```golang
  package main

  import (
    "context"

    "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // ...
  }
  ```

- Go (database/sql)

  {% cut "Using a connector (recommended)" %}

    ```golang
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

      // ...
    }
    ```

  {% endcut %}

  {% cut "Using a connection string" %}

    The `database/sql` driver is registered when importing the package of a specific driver separated by an underscore:
    ```golang
    package main

    import (
      "database/sql"

      _ "github.com/ydb-platform/ydb-go-sdk/v3"
    )

    func main() {
      db, err := sql.Open("ydb", "grpc://localhost:2136/local")
      if err != nil {
        panic(err)
      }
      defer db.Close()

      // ...
    }
    ```

  {% endcut %}

- Java


  {% include [work in progress message](_includes/addition.md) %}

- C# (.NET)

  ```C#
  using Ydb.Sdk;

  var config = new DriverConfig(
      endpoint: "grpc://localhost:2136",
      database: "/local"
  );

  await using var driver = await Driver.CreateInitialized(config);
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
          // 'root_cert_file' => './CA.pem', // Root CA file (uncomment for dedicated server)
      ],
      
      'credentials' => new \YdbPlatform\Ydb\Auth\Implement\AccessTokenAuthentication('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') // use from reference/ydb-sdk/auth
  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
