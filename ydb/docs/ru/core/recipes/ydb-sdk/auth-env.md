# Аутентификация при помощи переменных окружения

Аутентификация через переменные окружения позволяет не зашивать учётные данные в код: SDK или JDBC-драйвер сам определяет режим по переменным `YDB_*` в окружении процесса. Этот способ удобен для контейнеров, CI/CD и деплоя в облако, где секреты передаются через окружение. Типичные шаги: задать нужные переменные окружения, создать провайдер аутентификации (или подключиться через JDBC без явных свойств) и выполнить запрос. Подробности о порядке выбора режима — в разделе [Аутентификация](../../reference/ydb-sdk/auth.md#env); базовое подключение — в рецепте [инициализации драйвера](./init.md). Другие способы: [токен](./auth-access-token.md), [анонимная](./auth-anonymous.md), [метаданные](./auth-metadata.md), [сервисный аккаунт](./auth-service-account.md), [логин и пароль](./auth-static.md).

При использовании данного метода режим аутентификации и его параметры будут определены окружением, в котором запускается приложение, в [описанном здесь порядке](../../reference/ydb-sdk/auth.md#env).

Установив одну из следующих переменных окружения, можно управлять способом аутентификации:

* `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=<path/to/sa_key_file>` — использовать файл сервисного аккаунта в Яндекс Облаке.
* `YDB_ANONYMOUS_CREDENTIALS="1"` — Использовать анонимную аутентификацию. Актуально для тестирования против докер-контейнера с {{ ydb-short-name }}.
* `YDB_METADATA_CREDENTIALS="1"` — использовать сервис метаданных внутри Яндекс Облака (Яндекс функция или виртуальная машина).
* `YDB_ACCESS_TOKEN_CREDENTIALS=<access_token>` — использовать аутентификацию с токеном.

Ниже приведены примеры кода аутентификации при помощи переменных окружения в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/helpers/helpers.h>

    NYdb::TDriver CreateDriverFromEnvironment(const std::string& connectionString) {
        return NYdb::TDriver(NYdb::CreateFromEnvironment(connectionString));
    }
    ```

  - userver

    {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

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

  - database/sql

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

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.auth.iam.CloudAuthHelper;
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    public class EnvironAuthExample {
        public static void main(String[] args) throws Exception {
            // Строка подключения из переменной окружения или локальный {{ ydb-short-name }} по умолчанию
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            // Режим аутентификации определяется переменными окружения YDB_*
            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
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

    JDBC-драйвер читает переменные окружения `YDB_*` в порядке, описанном в разделе [Аутентификация](../../reference/ydb-sdk/auth.md#env). Явно передавать учётные данные не нужно — достаточно пустого объекта `Properties`.

    ```java
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.Properties;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    public class EnvironAuthJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            // Пустые свойства: драйвер сам выберет способ аутентификации по переменным окружения
            try (Connection connection = DriverManager.getConnection(jdbcUrl, new Properties());
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }
        }
    }
    ```

    В Spring Boot, ORM и прочих сторонних фреймворках вокруг JDBC укажите ту же JDBC-строку подключения; учётные данные из переменных окружения подхватываются драйвером так же, как в примере выше (например, через `spring.datasource.url`).

  {% endlist %}

- JavaScript

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

  {% list tabs %}

  - Native SDK

    {% include [auth-env](../../_includes/python/auth-env.md) %}

  - Native SDK (Asyncio)

    {% include [auth-env](../../_includes/python/async/auth-env.md) %}

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa
    import ydb

    engine = sa.create_engine(
        "yql+ydb://localhost:2136/local",
        connect_args={
            "credentials": ydb.credentials_from_env_variables()
        }
    )
    with engine.connect() as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  ```rust
  use ydb::{ClientBuilder, FromEnvCredentials, YdbResult};

  let client = ClientBuilder::new_from_connection_string(std::env::var("YDB_CONNECTION_STRING")?)?
      .with_credentials(FromEnvCredentials::new()?)
      .client()?;
  ```

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
