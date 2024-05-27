---
title: "Instructions for authenticating using environment variables in {{ ydb-short-name }}"
description: "The section describes examples of the authentication code using environment variables in different {{ ydb-short-name }} SDKs."
---

# Authentication using environment variables

{% include [work in progress message](_includes/addition.md) %}

When using this method, the authentication mode and its parameters are defined by the environment that an application is run in, [as described here](../../reference/ydb-sdk/auth.md#env).

By setting one of the following environment variables, you can control the authentication method:

* `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=<path/to/sa_key_file>`: Use a service account file in Yandex Cloud.
* `YDB_ANONYMOUS_CREDENTIALS="1"`: Use anonymous authentication. Relevant for testing against a Docker container with {{ ydb-short-name }}.
* `YDB_METADATA_CREDENTIALS="1"`: Use the metadata service inside Yandex Cloud (a Yandex function or a VM).
* `YDB_ACCESS_TOKEN_CREDENTIALS=<access_token>`: Use token-based authentication.

Below are examples of the code for authentication using environment variables in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

  ```go
  package main

  import (
    "context"
    "os"
    environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
    "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    db, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      environ.WithEnvironCredentials(ctx),
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
    environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
    "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    nativeDriver, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      environ.WithEnvironCredentials(ctx),
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
      AuthProvider authProvider = CloudAuthHelper.getAuthProviderFromEnviron();

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

  ```typescript
    import { Driver, getCredentialsFromEnv } from 'ydb-sdk';

    export async function connect(endpoint: string, database: string) {
        const authService = getCredentialsFromEnv();
        const driver = new Driver({endpoint, database, authService});
        const timeout = 10000;
        if (!await driver.ready(timeout)) {
            console.log(`Driver has not become ready in ${timeout}ms!`);
            process.exit(1);
        }
        console.log('Driver connected')
        return driver
    }
  ```

- Python

  ```python
    import os
    import ydb

    with ydb.Driver(
        connection_string=os.environ["YDB_CONNECTION_STRING"],
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=5)
        ...
  ```

- Python (asyncio)

  ```python
    import os
    import ydb
    import asyncio

    async def ydb_init():
        async with ydb.aio.Driver(
            endpoint=os.environ["YDB_ENDPOINT"],
            database=os.environ["YDB_DATABASE"],
            credentials=ydb.credentials_from_env_variables(),
        ) as driver:
            await driver.wait()
            ...

    asyncio.run(ydb_init())
  ```

- C#

  {% include [work in progress message](_includes/addition.md) %}

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\EnvironCredentials;

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
      
      'credentials' => new EnvironCredentials()
  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
