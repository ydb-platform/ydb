# Random choice

The {{ ydb-short-name }} SDK uses the `random_choice` algorithm by default, except for the C++ SDK, which uses the ["prefer the nearest data center"](./balancing-prefer-local.md) algorithm by default.

Below are examples of the code for forced setting of the "random choice" balancing algorithm in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

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

- Go (database/sql)

   Client load balancing in the {{ ydb-short-name }} `database/sql` driver is performed only when establishing a new connection (in `database/sql` terms), which is a {{ ydb-short-name }} session on a specific node. Once the session is created, all queries in this session are passed to the node where the session was created. Queries in the same {{ ydb-short-name }} session are not balanced between different {{ ydb-short-name }} nodes.

   Example of the code for setting the "random choice" balancing algorithm:

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

{% endlist %}
