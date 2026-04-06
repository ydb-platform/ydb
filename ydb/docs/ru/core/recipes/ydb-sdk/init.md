# Инициализация драйвера

Для подключения к {{ ydb-short-name }} требуется указать обязательные параметры (подробнее читайте в разделе [Подключение к серверу {{ ydb-short-name }}](../../concepts/connect.md)) и дополнительные, которые определяют поведение драйвера при работе.

Ниже приведены примеры кода подключения к {{ ydb-short-name }} (создания драйвера) в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go

  {% list tabs %}

  - Native SDK

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

  - database/sql

    {% cut "С помощью коннектора (рекомендуемый способ)" %}

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

    {% cut "С помощью строки подключения" %}

    Регистрация драйвера `database/sql` реализуется в момент импорта пакета конкретного драйвера через символ подчеркивания:

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

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    public void work() {
        try (GrpcTransport transport = GrpcTransport.forConnectionString("grpc://localhost:2136/local")
                .build()) {
            // Работа с transport
            doWork(transport);
        }
    }
    ```

  - JDBC

    ```java
    public void work() throws SQLException {
        // Драйвер tech.ydb.jdbc.YdbDriver должен быть в classpath для автозагрузки через DriverManager
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local")) {
            // Работа с connection
            doWork(connection);
        }
    }
    ```

    Для Spring Boot укажите URL и класс драйвера в `application.properties` или `application.yml` (`spring.datasource.url`, `spring.datasource.driver-class-name`).

    Spring Boot, а также ORM и прочие инструменты вокруг JDBC (Hibernate, JOOQ, MyBatis и т.п.) инициализируют транспорт к {{ ydb-short-name }} так же, как обычный JDBC: достаточно подключить зависимость с {{ ydb-short-name }} JDBC-драйвером и задать URL подключения — отдельная настройка нативного `GrpcTransport` не требуется.

  {% endlist %}

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    with ydb.Driver(connection_string="grpc://localhost:2136?database=/local") as driver:
      driver.wait(timeout=5)
      ...
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb
    import asyncio

    async def ydb_init():
      async with ydb.aio.Driver(endpoint="grpc://localhost:2136", database="/local") as driver:
        await driver.wait()
        ...

    asyncio.run(ydb_init())
    ```

  {% endlist %}

- C# (.NET)

  ```C#
  using Ydb.Sdk;

  var config = new DriverConfig(
      endpoint: "grpc://localhost:2136",
      database: "/local"
  );

  await using var driver = await Driver.CreateInitialized(config);
  ```

- JavaScript

  ```javascript
  import { Driver } from '@ydbjs/core'

  const driver = new Driver('grpc://localhost:2136/local')
  await driver.ready()
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

- Rust

  ```rust
  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .with_credentials(AccessTokenCredentials::from("..."))
        .client()?
  ```

{% endlist %}
