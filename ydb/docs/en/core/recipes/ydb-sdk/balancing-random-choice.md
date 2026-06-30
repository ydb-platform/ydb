# Uniform random choice

{{ ydb-short-name }} SDK uses the `random_choice` algorithm (uniform random balancing) by default, except for the C++ SDK, which uses the ["prefer nearest datacenter"](./balancing-prefer-local.md) algorithm by default.

Below are code examples for forcibly setting the "uniform random choice" balancing algorithm in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

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

  - userver

    {% cut "static config" %}

    ```yaml
    ydb:
        databases:
            db:
                endpoint: grpc://localhost:2136
                database: /local
                prefer_local_dc: false
    ```

    {% endcut %}

    The code for initializing `ydb::YdbComponent`, obtaining `ydb::TableClient`, and starting `components::MinimalServerComponentList` is as in the example from [init.md](./init.md).

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

    Client-side balancing in the `database/sql` driver for {{ ydb-short-name }} is performed only when a new connection is established (in `database/sql` terms), which is a {{ ydb-short-name }} session on a specific node. After the session is created, all queries on that session are directed to the node where the session was created. Query balancing on the same {{ ydb-short-name }} session between different {{ ydb-short-name }} nodes does not occur.

    Example code for setting the "uniform random choice" balancing algorithm:


    ```go
    package main

    import (
      "context"
      "database/sql"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
    )

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
        use_all_nodes=True,  # равномерный случайный выбор
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
            use_all_nodes=True,  # равномерный случайный выбор
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
                "use_all_nodes": True,  # равномерный случайный выбор
            }
        },
    )
    ```

  {% endlist %}

- C#

  This algorithm is used by default.

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  {% list tabs %}

  - Native SDK

    The "uniform random choice" algorithm in the Java SDK is set by the `USE_ALL_NODES` policy in `BalancingSettings` (this is the default behavior if settings are not overridden).


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

    Balancing when selecting a new session is set on the native transport side inside the driver; if necessary, use the same parameters as in the native SDK via [JDBC connection settings](../../reference/languages-and-apis/jdbc-driver/properties.md).

    In Spring Boot, ORM, and other third-party frameworks around JDBC, specify the same JDBC connection string and balancing parameters as when using the driver directly (for example, `spring.datasource.url` with the required query parameters or `DataSource` properties).

  {% endlist %}

- Rust

  The RandomChoice policy (random selection of an endpoint among discovery nodes) is used **by default** — no additional configuration is required.

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
