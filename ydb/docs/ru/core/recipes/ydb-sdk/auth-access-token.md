# Аутентификация при помощи токена

<!-- markdownlint-disable blanks-around-fences -->

Ниже приведены примеры кода аутентификации при помощи токена в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go

  {% list tabs %}

  - Native SDK

    ```go
    package main

    import (
      "context"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
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
      ...
    }
    ```

  - database/sql

    {% cut "Если используется коннектор для создания подключения к {{ ydb-short-name }}" %}

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
        db := sql.OpenDB(connector)
        defer db.Close()
        ...
      }
      ```

    {% endcut %}

    {% cut "Если используется строка подключения" %}

      ```go
      package main

      import (
        "context"
        "database/sql"
        "os"

        _ "github.com/ydb-platform/ydb-go-sdk/v3"
      )

      func main() {
        db, err := sql.Open("ydb", "grpcs://localhost:2135/local?token="+os.Getenv("YDB_TOKEN"))
        if err != nil {
          panic(err)
        }
        defer db.Close()
        ...
      }
      ```

    {% endcut %}

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    public void work(String accessToken) {
        AuthProvider authProvider = new TokenAuthProvider(accessToken);

        try (GrpcTransport transport = GrpcTransport.forConnectionString("grpcs://localhost:2135/local")
                .withAuthProvider(authProvider)
                .build();
             QueryClient queryClient = QueryClient.newClient(transport).build()) {

            doWork(queryClient);
        }
    }
    ```

  - JDBC

    ```java
    public void work() throws SQLException {
        // Подключение с указанием значения токена аутентификации
        Properties props1 = new Properties();
        props1.setProperty("token", "AQAD-XXXXXXXXXXXXXXXXXXXX");
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", props1)) {
            doWork(connection);
        }

        // Подключение с чтением токена аутентификации из указанного файла
        Properties props2 = new Properties();
        props2.setProperty("tokenFile", "~/.ydb_token");
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", props2)) {
            doWork(connection);
        }
    }
    ```

    В Spring Boot, ORM и прочих сторонних фреймворках вокруг JDBC используйте ту же JDBC-строку подключения и те же параметры аутентификации, что и выше (например, `spring.datasource.url` с query-параметрами или `spring.datasource.*` для токена и файла токена).

  {% endlist %}

- JavaScript

  {% include [auth-access-token](../../_includes/nodejs/auth-access-token.md) %}

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-access-token](../../_includes/python/auth-access-token.md) %}

  - Native SDK (Asyncio)

    {% include [auth-access-token](../../_includes/python/async/auth-access-token.md) %}

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa

    engine = sa.create_engine(
        "yql+ydb://localhost:2136/local",
        connect_args={
            "credentials": {"token": os.environ["YDB_TOKEN"]}
        }
    )
    with engine.connect() as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}

- C# (.NET)

  ```C#
  using Ydb.Sdk;
  using Ydb.Sdk.Auth;

  const string endpoint = "grpc://localhost:2136";
  const string database = "/local";
  const string token = "MY_VERY_SECURE_TOKEN";

  var config = new DriverConfig(
      endpoint: endpoint,
      database: database,
      credentials: new TokenProvider(token)
  );

  await using var driver = await Driver.CreateInitialized(config);
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\Implement\AccessTokenAuthentication;

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

      'credentials' => new AccessTokenAuthentication('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')
  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
