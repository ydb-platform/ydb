# Random choice

<<<<<<< HEAD
The {{ ydb-short-name }} SDK uses the `random_choice` algorithm by default.
=======
The {{ ydb-short-name }} SDK uses the `random_choice` (uniform random) balancing algorithm by default, except the C++ SDK, which defaults to ["prefer the nearest data center"](./balancing-prefer-local.md).
>>>>>>> 26186944f5a (DOCSUP-127029: [YDBDOCS-1972] docs: align RU YDB SDK docs with nested tab structure. Организация процесса перевода (1 архив) (1 шт.) (#37826))

Below are examples of explicitly setting the "random choice" balancing algorithm in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go

  {% list tabs %}

  - Native SDK

    ```go
    package main

<<<<<<< HEAD
   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     db, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithBalancer(
         balancers.RandomChoice(),
       ),
     )
     if err != nil {
       panic(err)
     }
     defer db.Close(ctx)
     ...
   }
   ```
=======
    import (
      "context"
      "os"
>>>>>>> 26186944f5a (DOCSUP-127029: [YDBDOCS-1972] docs: align RU YDB SDK docs with nested tab structure. Организация процесса перевода (1 архив) (1 шт.) (#37826))

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithBalancer(
          balancers.RandomChoice(),
        ),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      // ...
    }
    ```

  - database/sql

    Client-side balancing in the {{ ydb-short-name }} `database/sql` driver happens only when opening a new connection (in `database/sql` terms), which maps to a {{ ydb-short-name }} session on a specific node. After the session is created, all queries on that session go to that node. Queries on the same {{ ydb-short-name }} session are not balanced across nodes.

    Example for "random choice" balancing:

    ```go
    package main

    import (
      "context"
      "database/sql"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
    )

<<<<<<< HEAD
     db := sql.OpenDB(connector)
     defer db.Close()
     ...
   }
   ```

=======
    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithBalancer(
          balancers.RandomChoice(),
        ),
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
      // ...
    }
    ```

  {% endlist %}

- C++

  ```cpp
  #include <ydb-cpp-sdk/client/driver/driver.h>

  int main() {
    auto connectionString = std::string(std::getenv("YDB_CONNECTION_STRING"));

    auto driverConfig = NYdb::TDriverConfig(connectionString)
      .SetBalancingPolicy(NYdb::TBalancingPolicy::UseAllNodes());

    NYdb::TDriver driver(driverConfig);
    // ...
    driver.Stop(true);
    return 0;
  }
  ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import os
    import ydb

    driver_config = ydb.DriverConfig(
        endpoint=os.environ["YDB_ENDPOINT"],
        database=os.environ["YDB_DATABASE"],
        credentials=ydb.credentials_from_env_variables(),
        use_all_nodes=True,  # uniform random choice
    )

    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=5)
        # ...
    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import ydb
    import asyncio

    async def ydb_init():
        driver_config = ydb.DriverConfig(
            endpoint=os.environ["YDB_ENDPOINT"],
            database=os.environ["YDB_DATABASE"],
            credentials=ydb.credentials_from_env_variables(),
            use_all_nodes=True,  # uniform random choice
        )
        async with ydb.aio.Driver(driver_config) as driver:
            await driver.wait()
            # ...

    asyncio.run(ydb_init())
    ```

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa

    engine = sa.create_engine(
        os.environ["YDB_SQLALCHEMY_URL"],
        connect_args={
            "driver_config_kwargs": {
                "use_all_nodes": True,  # uniform random choice
            }
        },
    )
    ```

  {% endlist %}

- JavaScript

  {% include [work-in-progress](../../_includes/work-in-progress.md) %}

- Java

  {% list tabs %}

  - Native SDK

    "Random choice" in the Java SDK corresponds to the `USE_ALL_NODES` policy in `BalancingSettings` (this is also the default if you do not override settings).

    ```java
    import tech.ydb.core.grpc.BalancingSettings;
    import tech.ydb.core.grpc.GrpcTransport;

    try (GrpcTransport transport = GrpcTransport.forConnectionString("grpc://localhost:2136/local")
            .withBalancingSettings(BalancingSettings.fromPolicy(BalancingSettings.Policy.USE_ALL_NODES))
            .build()) {
        // ...
    }
    ```

  - JDBC

    Balancing when selecting a new session is handled by the native transport inside the driver; if needed, use the same parameters as in the native SDK via [JDBC connection settings](../../reference/languages-and-apis/jdbc-driver/properties.md).

    In Spring Boot, ORMs, and other JDBC wrappers, use the same JDBC URL and balancing parameters as with the driver directly (for example `spring.datasource.url` with query parameters or `DataSource` properties).

  {% endlist %}

>>>>>>> 26186944f5a (DOCSUP-127029: [YDBDOCS-1972] docs: align RU YDB SDK docs with nested tab structure. Организация процесса перевода (1 архив) (1 шт.) (#37826))
{% endlist %}
