# Аутентификация при помощи сервиса метаданных

<!-- markdownlint-disable blanks-around-fences -->

Аутентификация через сервис метаданных получает IAM-токен из HTTP-эндпоинта виртуальной машины или облачной функции Яндекс Облака. Этот способ предназначен для приложений, которые работают внутри Яндекс Облака и не хранят ключи в коде. Типичные шаги: создать провайдер метаданных, открыть транспорт с защищённым соединением к {{ ydb-short-name }} в облаке и выполнить запрос. Подробности — в разделе [Аутентификация](../../reference/ydb-sdk/auth.md); базовое подключение — в рецепте [инициализации драйвера](./init.md). Другие способы: [токен](./auth-access-token.md), [анонимная](./auth-anonymous.md), [переменные окружения](./auth-env.md), [сервисный аккаунт](./auth-service-account.md), [логин и пароль](./auth-static.md).

Ниже приведены примеры кода аутентификации при помощи сервиса метаданных в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/iam/iam.h>

    NYdb::TDriver CreateDriverWithMetadataCredentials(
        const std::string& connectionString,
        const std::string& internalCA)
    {
        auto config = NYdb::TDriverConfig(connectionString)
            .UseSecureConnection(internalCA)
            .SetCredentialsProviderFactory(NYdb::CreateIamCredentialsProviderFactory());

        return NYdb::TDriver(config);
    }
    ```

  - userver

    Готовой настройки для сервиса метаданных в `ydb::YdbComponent` нет — нужен свой `ydb::CredentialsProviderComponent` с `NYdb::CreateIamCredentialsProviderFactory()`.

    {% cut "static config" %}

    ```yaml
    ydb:
        credentials-provider: ydb-metadata-credentials
        databases:
            db:
                endpoint: grpcs://localhost:2135
                database: /local
                credentials: {}
    ```

    {% endcut %}

    {% cut "secdist" %}

    `<PEM>` - сертификаты Yandex Cloud.

    ```json
    {
      "ydb_settings": {
        "db": {
          "secure_connection_cert": "<PEM>"
        }
      }
    }
    ```

    {% endcut %}

    ```cpp
    #include <userver/components/component_base.hpp>
    #include <userver/components/minimal_server_component_list.hpp>
    #include <userver/storages/secdist/component.hpp>
    #include <userver/storages/secdist/provider_component.hpp>
    #include <userver/utils/daemon_run.hpp>
    #include <userver/ydb/component.hpp>
    #include <userver/ydb/credentials.hpp>
    #include <userver/ydb/table.hpp>

    #include <ydb-cpp-sdk/client/iam/iam.h>

    class YdbMetadataCredentials final : public ydb::CredentialsProviderComponent {
    public:
        static constexpr std::string_view kName = "ydb-metadata-credentials";

        using ydb::CredentialsProviderComponent::CredentialsProviderComponent;

        std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateCredentialsProviderFactory(
            const yaml_config::YamlConfig&) const override
        {
            return NYdb::CreateIamCredentialsProviderFactory();
        }
    };

    class MyYdbWorker final : public components::ComponentBase {
    public:
        static constexpr std::string_view kName = "my-ydb-worker";

        MyYdbWorker(const components::ComponentConfig& config, const components::ComponentContext& context)
            : components::ComponentBase(config, context),
              table_client_(context.FindComponent<ydb::YdbComponent>().GetTableClient("db"))
        {
            // ...
        }

    private:
        std::shared_ptr<ydb::TableClient> table_client_;
    };

    int main(int argc, char* argv[]) {
        auto component_list = components::MinimalServerComponentList()
            .Append<components::DefaultSecdistProvider>()
            .Append<components::Secdist>()
            .Append<YdbMetadataCredentials>()
            .Append<ydb::YdbComponent>()
            .Append<MyYdbWorker>();
        return utils::DaemonMain(argc, argv, component_list);
    }
    ```

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

  - database/sql

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

    public class MetadataAuthExample {
        public static void main(String[] args) throws Exception {
            // Строка подключения из переменной окружения или локальный {{ ydb-short-name }} по умолчанию
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            // Токен запрашивается у сервиса метаданных виртуальной машины или функции в Яндекс Облаке
            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(CloudAuthHelper.getMetadataAuthProvider())
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

    public class MetadataAuthJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            // Аутентификация через сервис метаданных Яндекс Облака
            Properties props = new Properties();
            props.setProperty("useMetadata", "true");

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

    Опцию `useMetadata` также можно указать прямо в JDBC URL: `jdbc:ydb:grpc://localhost:2136/local?useMetadata=true`.

    В Spring Boot, ORM и прочих сторонних фреймворках вокруг JDBC передайте те же JDBC URL и параметры (`useMetadata` в URL или в свойствах источника данных), что и в примере выше.

  {% endlist %}

- JavaScript

  {% include [auth-metadata](../../_includes/nodejs/auth-metadata.md) %}

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-metadata](../../_includes/python/auth-metadata.md) %}

  - Native SDK (Asyncio)

    {% include [auth-metadata](../../_includes/python/async/auth-metadata.md) %}

  - SQLAlchemy

    ```python
    import sqlalchemy as sa
    import ydb.iam

    engine = sa.create_engine(
        "yql+ydb://localhost:2136/local",
        connect_args={
            "credentials": ydb.iam.MetadataUrlCredentials()
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
      "Host=ydb.serverless.yandexcloud.net;Port=2135;Database=/ru-central1/<folder-id>/<database-id>;EnableMetadataCredentials=True");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```

  Для Entity Framework и linq2db используйте тот же connectionString.

- Rust

  ```rust
  use ydb::{ClientBuilder, MetadataUrlCredentials, YdbResult};

  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .with_credentials(MetadataUrlCredentials::new())
      .client()?;
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
