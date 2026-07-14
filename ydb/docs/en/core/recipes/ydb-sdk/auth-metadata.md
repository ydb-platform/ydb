# Authentication using the metadata service

<!-- markdownlint-disable blanks-around-fences -->

Authentication via the metadata service obtains an IAM token from the HTTP endpoint of a virtual machine or cloud function in Yandex Cloud. This method is intended for applications that run inside Yandex Cloud and do not store keys in code. Typical steps: create a metadata provider, open a transport with a secure connection to {{ ydb-short-name }} in the cloud, and execute a query. For details, see the [Authentication](../../reference/ydb-sdk/auth.md) section; for basic connection, see the [driver initialization](./init.md) recipe. Other methods: [token](./auth-access-token.md), [anonymous](./auth-anonymous.md), [environment variables](./auth-env.md), [service account](./auth-service-account.md), [login and password](./auth-static.md).

Below are code examples for authentication using the metadata service in different {{ ydb-short-name }} SDKs.

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

    There is no ready-made configuration for the metadata service in `ydb::YdbComponent` — you need your own `ydb::CredentialsProviderComponent` with `NYdb::CreateIamCredentialsProviderFactory()`.

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

    `<PEM>` - Yandex Cloud certificates.


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
            // Connection string from environment variable or local {{ ydb-short-name }} by default
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            // Token is requested from the metadata service of a virtual machine or function in Yandex Cloud
            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(CloudAuthHelper.getMetadataAuthProvider())
                    .build();
                 QueryClient queryClient = QueryClient.newClient(transport).build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
                QueryReader reader = retryCtx.supplyResult(
                        session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.NONE))
                ).join().getValue();

                // Connection check: output the result of SELECT 1
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

            // Authentication via Yandex Cloud metadata service
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


    The `useMetadata` option can also be specified directly in the JDBC URL: `jdbc:ydb:grpc://localhost:2136/local?useMetadata=true`.

    In Spring Boot, ORM, and other third-party frameworks around JDBC, pass the same JDBC URL and parameters (`useMetadata` in the URL or in the data source properties) as in the example above.

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


  For Entity Framework and linq2db, use the same connectionString.

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
