# Initialize the driver

To connect to {{ ydb-short-name }}, you must specify the required parameters (see [Connecting to a {{ ydb-short-name }} server](../../concepts/connect.md)) and optional parameters that control driver behavior.

Below are examples of connecting to {{ ydb-short-name }} (creating a driver) in different {{ ydb-short-name }} SDKs.

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

    {% cut "Using a connector (recommended)" %}

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

    {% cut "Using a connection string" %}

    The `database/sql` driver is registered when you import the driver package with a blank import:

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
            // Work with transport
            doWork(transport);
        }
    }
    ```

  - JDBC

    ```java
    public void work() throws SQLException {
        // tech.ydb.jdbc.YdbDriver must be on the classpath for DriverManager auto-loading
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local")) {
            // Work with connection
            doWork(connection);
        }
    }
    ```

    For Spring Boot, set the URL and driver class in `application.properties` or `application.yml` (`spring.datasource.url`, `spring.datasource.driver-class-name`).

    Spring Boot, ORMs, and other JDBC stacks (Hibernate, JOOQ, MyBatis, and so on) talk to {{ ydb-short-name }} like any JDBC source: add the {{ ydb-short-name }} JDBC dependency and configure the URL — you do not need to configure native `GrpcTransport` separately.

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
