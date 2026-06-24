# Анонимная аутентификация

<!-- markdownlint-disable blanks-around-fences -->

Анонимная аутентификация не передаёт учётные данные — {{ ydb-short-name }} SDK подключается к базе без токена, логина или пароля. Это режим по умолчанию для локальной разработки и тестирования против Docker-контейнера с {{ ydb-short-name }}. Типичные шаги: указать строку подключения, явно или неявно выбрать анонимный провайдер и выполнить запрос. Подробности — в разделе [Аутентификация](../../reference/ydb-sdk/auth.md); базовое подключение — в рецепте [инициализации драйвера](./init.md). Другие способы: [токен](./auth-access-token.md), [переменные окружения](./auth-env.md), [метаданные](./auth-metadata.md), [сервисный аккаунт](./auth-service-account.md), [логин и пароль](./auth-static.md).

Ниже приведены примеры кода анонимной аутентификации в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    Анонимная аутентификация является аутентификацией по умолчанию.
    Явным образом анонимную аутентификацию можно включить так:

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/types/credentials/credentials.h>

    NYdb::TDriver CreateDriverAnonymous() {
        auto config = NYdb::TDriverConfig("grpc://localhost:2136/local")
            .SetCredentialsProviderFactory(NYdb::CreateInsecureCredentialsProviderFactory());

        return NYdb::TDriver(config);
    }
    ```

  - userver

    Если в статическом конфиге не задавать `credentials-provider`, не указывать `databases.*.credentials` и не класть в secdist для этой базы `token`, `iam_jwt_params` и пару `user`/`password`, драйвер будет использовать анонимный режим по умолчанию.

    Код инициализации `ydb::YdbComponent`, получения `ydb::TableClient` и запуска `components::MinimalServerComponentList` — как в примере из [init.md](./init.md).

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

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

  - database/sql

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

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.auth.NopAuthProvider;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    public class AnonymousAuthExample {
        public static void main(String[] args) throws Exception {
            // Строка подключения из переменной окружения или локальный {{ ydb-short-name }} по умолчанию
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    // Явно указываем анонимную аутентификацию (режим по умолчанию)
                    .withAuthProvider(NopAuthProvider.INSTANCE)
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
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    public class AnonymousAuthJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            // Подключение без дополнительных свойств — анонимная аутентификация
            try (Connection connection = DriverManager.getConnection(jdbcUrl);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }
        }
    }
    ```

    В Spring Boot, ORM и прочих сторонних фреймворках вокруг JDBC подключение задаётся той же JDBC-строкой подключения, что и выше (например, `spring.datasource.url`).

  {% endlist %}

- JavaScript

  {% include [auth-anonymous](../../_includes/nodejs/auth-anonymous.md) %}

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-anonymous](../../_includes/python/auth-anonymous.md) %}

  - Native SDK (Asyncio)

    {% include [auth-anonymous](../../_includes/python/async/auth-anonymous.md) %}

  - SQLAlchemy

    ```python
    import sqlalchemy as sa

    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    with engine.connect() as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}

- C#

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```

  Для Entity Framework и linq2db используйте тот же connectionString.

- Rust

  ```rust
  use ydb::{AnonymousCredentials, ClientBuilder, YdbResult};

  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .with_credentials(AnonymousCredentials::new())
      .client()?;
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
