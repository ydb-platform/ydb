# Аутентификация при помощи файла сервисного аккаунта

<!-- markdownlint-disable blanks-around-fences -->

Аутентификация с файлом сервисного аккаунта использует JSON-ключ для формирования JWT и получения IAM-токена. Этот способ подходит для серверных приложений вне облака, CI/CD и локальной разработки с доступом к {{ ydb-short-name }} в Яндекс Облаке. Типичные шаги: сохранить ключ сервисного аккаунта в файл, передать путь к нему в провайдер аутентификации, открыть транспорт и выполнить запрос. Подробности — в разделе [Аутентификация](../../reference/ydb-sdk/auth.md); базовое подключение — в рецепте [инициализации драйвера](./init.md). Другие способы: [токен](./auth-access-token.md), [анонимная](./auth-anonymous.md), [переменные окружения](./auth-env.md), [метаданные](./auth-metadata.md), [логин и пароль](./auth-static.md).

Ниже приведены примеры кода аутентификации при помощи файла сервисного аккаунта в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/iam/iam.h>

    NYdb::TDriver CreateDriverWithServiceAccountKeyFile(
        const std::string& connectionString,
        const std::string& saKeyFilePath,
        const std::string& internalCA)
    {
        auto config = NYdb::TDriverConfig(connectionString)
            .UseSecureConnection(internalCA)
            .SetCredentialsProviderFactory(NYdb::CreateIamJwtFileCredentialsProviderFactory({
                .JwtFilename = saKeyFilePath,
            }));

        return NYdb::TDriver(config);
    }
    ```

  - userver

    {% cut "secdist" %}

    `<PEM>` - сертификаты Yandex Cloud.

    ```json
    {
      "ydb_settings": {
        "db": {
          "iam_jwt_params": {
            "id": "...",
            "service_account_id": "...",
            "private_key": "..."
          },
          "secure_connection_cert": "<PEM>"
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
      yc "github.com/ydb-platform/ydb-go-yc"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        yc.WithServiceAccountKeyFileCredentials(
          os.Getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"),
        ),
        yc.WithInternalCA(), // append Yandex Cloud certificates
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

      "github.com/ydb-platform/ydb-go-sdk/v3"
      yc "github.com/ydb-platform/ydb-go-yc"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        yc.WithServiceAccountKeyFileCredentials(
          os.Getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"),
        ),
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

    public class ServiceAccountAuthExample {
        public static void main(String[] args) throws Exception {
            // Строка подключения из переменной окружения или локальный {{ ydb-short-name }} по умолчанию
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            // Путь к JSON-ключу сервисного аккаунта
            String saKeyFile = System.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            if (saKeyFile == null || saKeyFile.isEmpty()) {
                throw new IllegalStateException(
                        "Задайте переменную окружения YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            }

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile))
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

    public class ServiceAccountAuthJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            String saKeyFile = System.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            if (saKeyFile == null || saKeyFile.isEmpty()) {
                throw new IllegalStateException(
                        "Задайте переменную окружения YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            }

            Properties props = new Properties();
            props.setProperty("saKeyFile", saKeyFile);

            try (Connection connection = DriverManager.getConnection(jdbcUrl, props);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }
        }
    }
    ```

    Опцию `saKeyFile` также можно указать прямо в JDBC URL: `jdbc:ydb:grpc://localhost:2136/local?saKeyFile=~/keys/sa_key.json`.

    В Spring Boot, ORM и прочих сторонних фреймворках вокруг JDBC укажите ту же JDBC-строку подключения и параметр `saKeyFile` (в URL или в свойствах `DataSource`), что и в примере выше.

  {% endlist %}

- JavaScript

  Загрузка данных сервисного аккаунта из файла:

  {% include [auth-sa-file](../../_includes/nodejs/auth-sa-file.md) %}

  Загрузка данных сервисного аккаунта из стороннего источника (например, из хранилища секретов):

  {% include [auth-sa-data](../../_includes/nodejs/auth-sa-data.md) %}

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-sa-data](../../_includes/python/auth-service-account.md) %}

  - Native SDK (Asyncio)

    {% include [auth-sa-data](../../_includes/python/async/auth-service-account.md) %}

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa
    import ydb.iam

    engine = sa.create_engine(
        "yql+ydb://localhost:2136/local",
        connect_args={
            "credentials": ydb.iam.ServiceAccountCredentials.from_file(
                os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]
            )
        }
    )
    with engine.connect() as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}

- C#

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource(
      "Host=ydb.serverless.yandexcloud.net;Port=2135;Database=/ru-central1/<folder-id>/<database-id>;ServiceAccountKeyFilePath=path/to/sa_file.json");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```

  Для Entity Framework и linq2db используйте тот же connectionString.

- Rust

  ```rust
  use ydb::{ClientBuilder, ServiceAccountCredentials, YdbResult};

  let client = ClientBuilder::new_from_connection_string(std::env::var("YDB_CONNECTION_STRING")?)?
      .with_credentials(ServiceAccountCredentials::from_env()?)
      .client()?;
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\JwtWithJsonAuthentication;

  $config = [
      'database'    => '/ru-central1/b1glxxxxxxxxxxxxxxxx/etn0xxxxxxxxxxxxxxxx',
      'endpoint'    => 'ydb.serverless.yandexcloud.net:2135',
      'discovery'   => false,
      'iam_config'  => [
          'temp_dir'       => './tmp', // Temp directory
          // 'root_cert_file' => './CA.pem', // Root CA file (uncomment for dedicated server)ы
      ],

      'credentials' => new JwtWithJsonAuthentication('./jwtjson.json')
  ];

  $ydb = new Ydb($config);
  ```

  или

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\JwtWithPrivateKeyAuthentication;

  $config = [
      'database'    => '/ru-central1/b1glxxxxxxxxxxxxxxxx/etn0xxxxxxxxxxxxxxxx',
      'endpoint'    => 'ydb.serverless.yandexcloud.net:2135',
      'discovery'   => false,
      'iam_config'  => [
          'temp_dir'           => './tmp', // Temp directory
          // 'root_cert_file' => './CA.pem', // Root CA file (uncomment for dedicated server)

      ],

      'credentials' => new JwtWithPrivateKeyAuthentication(
          "ajexxxxxxxxx","ajeyyyyyyyyy",'./private.key')

  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
