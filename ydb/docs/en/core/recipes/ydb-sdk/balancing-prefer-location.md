# Prefer a specific availability zone

Below are code examples for setting the balancing algorithm option "prefer availability zone" in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    In the C++ SDK, you can select only one availability zone as the preferred one.


    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>

    int main() {
      auto connectionString = std::string(std::getenv("YDB_CONNECTION_STRING"));

      auto driverConfig = NYdb::TDriverConfig(connectionString)
        .SetBalancingPolicy(NYdb::TBalancingPolicy::UsePreferableLocation("datacenter1"));

      NYdb::TDriver driver(driverConfig);
      // ...
      driver.Stop(true);
      return 0;
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

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithBalancer(
          balancers.PreferLocations(
            balancers.RandomChoice(),
            "a",
            "b",
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

    Example code for setting the balancing algorithm "prefer availability zone":


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
          balancers.PreferLocations(
            balancers.RandomChoice(),
            "a",
            "b",
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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  In **Java SDK**, the availability zone preference is set in the gRPC transport settings.

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.core.grpc.BalancingSettings;
    import tech.ydb.core.grpc.GrpcTransport;

    try (GrpcTransport transport = GrpcTransport.forConnectionString("grpc://localhost:2136/local")
            .withBalancingSettings(BalancingSettings.fromLocation("a")) // предпочитаемая зона доступности
            .build()) {
        // ...
    }
    ```

  - JDBC

    Check the supported availability zone parameters in the [JDBC driver properties](../../reference/languages-and-apis/jdbc-driver/properties.md) or set the balancing via the native API when embedding the driver.

    In Spring Boot, ORM, and other third-party frameworks around JDBC, pass the same JDBC URL and availability zone parameters as in direct connection (for example, in `spring.datasource.url` or pool properties).

  {% endlist %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#238](https://github.com/ydb-platform/ydb-rs-sdk/issues/238)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
