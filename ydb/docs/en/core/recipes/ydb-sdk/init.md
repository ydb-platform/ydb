# Driver initialization

To connect to {{ ydb-short-name }}, you must specify the required parameters (read more in the [Connecting to the {{ ydb-short-name }} server](../../concepts/connect.md) section) and optional ones that determine the driver's behavior during operation.

Below are code examples for connecting to {{ ydb-short-name }} (creating a driver) in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>

    int main() {
      auto driverConfig = NYdb::TDriverConfig("grpc://localhost:2136/local");

      NYdb::TDriver driver(driverConfig);

      // ...

      driver.Stop();

      return 0;
    }
    ```

  - userver

    {% cut "static config" %}

    ```yaml
    ydb:
        databases:
            db:
                endpoint: grpc://localhost:2136
                database: /local
    ```

    {% endcut %}


    ```cpp
    #include <userver/components/component_base.hpp>
    #include <userver/components/minimal_server_component_list.hpp>
    #include <userver/storages/secdist/component.hpp>
    #include <userver/storages/secdist/provider_component.hpp>
    #include <userver/utils/daemon_run.hpp>
    #include <userver/ydb/component.hpp>
    #include <userver/ydb/table.hpp>

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
            .Append<ydb::YdbComponent>()
            .Append<MyYdbWorker>();
        return utils::DaemonMain(argc, argv, component_list);
    }
    ```

  {% endlist %}

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

    {% cut "Using помощью connector (recommended method)" %}

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

    {% cut "Using помощью string connection" %}

    The `database/sql` driver is registered when importing the specific driver package using an underscore:


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

  To connect, specify the [connection string](../../concepts/connect.md) and, if necessary, configure [authentication](../../reference/ydb-sdk/auth.md). It is recommended to execute queries using `QueryClient` and `SessionRetryContext` (see [retries](./retry.md)).

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;

    public class InitExample {
        public static void main(String[] args) {
            // Connection string: endpoint and database path
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
                 QueryClient queryClient = QueryClient.newClient(transport).build()) {

                // Retry context for reliable query execution
                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

                // Checking connection with a simple query
                QueryReader reader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                        session.createQuery("SELECT 1 AS value", TxMode.NONE, Params.empty())
                )).join().getValue();

                ResultSetReader rs = reader.getResultSet(0);
                if (rs.next()) {
                    System.out.println("Подключение успешно, SELECT 1 = " + rs.getColumn("value").getInt32());
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

    public class JdbcInitExample {
        public static void main(String[] args) throws SQLException {
            // The tech.ydb.jdbc.YdbDriver driver must be in the classpath for auto-loading via DriverManager
            String url = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            try (Connection connection = DriverManager.getConnection(url);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1 AS value")) {

                if (rs.next()) {
                    System.out.println("Подключение успешно, SELECT 1 = " + rs.getInt("value"));
                }
            }
        }
    }
    ```


    For Spring Boot, specify the URL and driver class in `application.properties` or `application.yml` (`spring.datasource.url`, `spring.datasource.driver-class-name`).

    Spring Boot, as well as ORMs and other tools around JDBC (Hibernate, JOOQ, MyBatis, etc.), initialize the transport to {{ ydb-short-name }} in the same way as regular JDBC: just add a dependency with the {{ ydb-short-name }} JDBC driver and set the connection URL — no separate configuration of the native `GrpcTransport` is required.

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

- C#

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
  await using var connection = await dataSource.OpenConnectionAsync();
  // ...
  ```

- JavaScript

  ```javascript
  import { Driver } from '@ydbjs/core'

  const driver = new Driver('grpc://localhost:2136/local')
  await driver.ready()
  ```

- Rust

  ```rust
  use ydb::{AccessTokenCredentials, ClientBuilder, YdbResult};

  #[tokio::main]
  async fn main() -> YdbResult<()> {
      let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
          .with_credentials(AccessTokenCredentials::from("..."))
          .client()?;
      client.wait().await?;
      Ok(())
  }
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

{% endlist %}
