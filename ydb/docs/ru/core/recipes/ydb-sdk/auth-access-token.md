# Аутентификация при помощи токена

<!-- markdownlint-disable blanks-around-fences -->

Аутентификация с токеном доступа передаёт заранее полученный IAM-токен в каждый запрос к {{ ydb-short-name }}. Этот способ подходит, когда токен уже есть в приложении — например, получен через OAuth или утилиту `yc`. Типичные шаги: получить токен, создать провайдер аутентификации, открыть транспорт и выполнить запрос. Подробности о режимах аутентификации — в разделе [Аутентификация](../../reference/ydb-sdk/auth.md); базовое подключение — в рецепте [инициализации драйвера](./init.md). Альтернативы: [анонимная](./auth-anonymous.md), [через переменные окружения](./auth-env.md), [метаданные](./auth-metadata.md), [сервисный аккаунт](./auth-service-account.md), [логин и пароль](./auth-static.md).

Ниже приведены примеры кода аутентификации при помощи токена в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>

    NYdb::TDriver CreateDriverWithAccessToken(const std::string& accessToken) {
        auto config = NYdb::TDriverConfig("grpcs://localhost:2135/?database=/local")
            .SetAuthToken(accessToken);

        return NYdb::TDriver(config);
    }
    ```

  - userver

    {% cut "secdist" %}

    ```json
    {
      "ydb_settings": {
        "db": {
          "token": "<access token>"
        }
      }
    }
    ```

    {% endcut %}

    Код инициализации `ydb::YdbComponent`, получения `ydb::TableClient` и запуска `components::MinimalServerComponentList` — как в примере из [init.md](./init.md).

  {% endlist %}

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
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.auth.TokenAuthProvider;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    public class AccessTokenExample {
        public static void main(String[] args) throws Exception {
            // Строка подключения из переменной окружения или локальный {{ ydb-short-name }} по умолчанию
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            // IAM-токен из переменной окружения YDB_ACCESS_TOKEN_CREDENTIALS
            String token = System.getenv("YDB_ACCESS_TOKEN_CREDENTIALS");
            if (token == null || token.isEmpty()) {
                throw new IllegalStateException("Задайте переменную окружения YDB_ACCESS_TOKEN_CREDENTIALS");
            }

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(new TokenAuthProvider(token))
                    .build();
                 QueryClient queryClient = QueryClient.newClient(transport).build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
                QueryReader reader = retryCtx.supplyResult(
                        session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.NONE))
                ).join().getValue();

                // Проверка подключения: выводим результат SELECT 1
                ResultSetReader rs = reader.getResultSet(0);
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getColumn(0).getInt32());
                }
            }
        }
    }
    ```

  - JDBC

    ```java
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.Properties;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    public class AccessTokenJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            // Вариант 1: токен задаётся свойством token
            Properties propsWithToken = new Properties();
            String token = System.getenv("YDB_ACCESS_TOKEN_CREDENTIALS");
            if (token == null || token.isEmpty()) {
                throw new IllegalStateException("Задайте переменную окружения YDB_ACCESS_TOKEN_CREDENTIALS");
            }
            propsWithToken.setProperty("token", token);

            try (Connection connection = DriverManager.getConnection(jdbcUrl, propsWithToken);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }

            // Вариант 2: токен читается из файла — свойство tokenFile
            Properties propsWithTokenFile = new Properties();
            propsWithTokenFile.setProperty("tokenFile", System.getenv().getOrDefault("YDB_TOKEN_FILE", "~/.ydb_token"));

            try (Connection connection = DriverManager.getConnection(jdbcUrl, propsWithTokenFile);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }
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

- C#

  ```C#
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Auth;

  var builder = new YdbConnectionStringBuilder("Host=localhost;Port=2136;Database=/local")
  {
      CredentialsProvider = new TokenProvider("MY_VERY_SECURE_TOKEN")
  };

  await using var dataSource = new YdbDataSource(builder);
  await using var connection = await dataSource.OpenConnectionAsync();
  ```
  
  Для Entity Framework и linq2db не поддерживается.

- Rust

  ```rust
  use ydb::{AccessTokenCredentials, ClientBuilder, YdbResult};

  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .with_credentials(AccessTokenCredentials::from(std::env::var("YDB_TOKEN")?))
      .client()?;
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
