# Аутентификация при помощи сервиса метаданных

<!-- markdownlint-disable blanks-around-fences -->

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
    public void work(String connectionString) {
        AuthProvider authProvider = CloudAuthHelper.getMetadataAuthProvider();

        try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
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
        Properties props = new Properties();
        props.setProperty("useMetadata", "true");
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", props)) {
            doWork(connection);
        }

        // Опцию useMetadata также можно указать прямо в JDBC URL
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local?useMetadata=true")) {
            doWork(connection);
        }
    }
    ```

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
