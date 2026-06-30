# Prefer the nearest data center

Below are code examples for setting the balancing algorithm option "prefer the nearest data center" in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    The {{ ydb-short-name }} C++ SDK uses the `prefer_local_dc` algorithm (prefer the nearest data center) by default.


    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>

    int main() {
      auto connectionString = std::string(std::getenv("YDB_CONNECTION_STRING"));

      auto driverConfig = NYdb::TDriverConfig(connectionString)
        .SetBalancingPolicy(NYdb::TBalancingPolicy::UsePreferableLocation());

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
                prefer_local_dc: true
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
          balancers.PreferLocalDC(
            balancers.RandomChoice(),
          ),
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

    Example code for setting the balancing algorithm "prefer the nearest data center":


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
          balancers.PreferLocalDC(
            balancers.RandomChoice(),
          ),
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
        use_all_nodes=False,  # предпочитать ближайший дата-центр
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
            use_all_nodes=False,  # предпочитать ближайший дата-центр
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
                "use_all_nodes": False,  # предпочитать ближайший дата-центр
            }
        },
    )
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.core.grpc.BalancingSettings;
    import tech.ydb.core.grpc.GrpcTransport;

    try (GrpcTransport transport = GrpcTransport.forConnectionString("grpc://localhost:2136/local")
            .withBalancingSettings(BalancingSettings.detectLocalDs())
            .build()) {
        // ...
    }
    ```

  - JDBC

    See [JDBC driver properties](../../reference/languages-and-apis/jdbc-driver/properties.md); if necessary, set the balancing policy via the native transport.

    In Spring Boot, ORM, and other third-party frameworks around JDBC, specify the same JDBC connection string and balancing parameters as when using the driver directly (for example, `spring.datasource.url` or `DataSource` properties).

  {% endlist %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#239](https://github.com/ydb-platform/ydb-rs-sdk/issues/239)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
