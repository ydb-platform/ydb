# Set the session pool size

{{ ydb-short-name }} creates an [actor](../../concepts/glossary.md#actor) for each session. As a result, the session pool size on the client affects resource consumption (memory, CPU) on the server side of {{ ydb-short-name }}.

For example, if 1000 clients of a single database each open 1000 sessions, then 1,000,000 actors are created on the server side. Such a number of actors consumes significant amounts of memory and CPU resources. Without a limit on the number of sessions on the client, this can lead to slow cluster operation and a degraded state.

By default, the {{ ydb-short-name }} SDK has a limit of 50 sessions when using native drivers. When using third-party libraries, such as Go `database/sql`, no limit is set.

It is recommended to set the limit on the number of sessions on the client to the minimum required for normal operation of the client application. Note that a session is single-threaded on both the server and client sides. Accordingly, if the application needs to execute 1000 concurrent queries (inflight) to {{ ydb-short-name }} for the estimated load, the limit should be set to 1000 sessions.

It is important to distinguish between the estimated RPS (requests per second) and inflight. In the first case, we are talking about the total number of queries executed against {{ ydb-short-name }} in 1 second. For example, with RPS = 10000 and an average query execution latency of 100 ms, it is sufficient to set the limit to 1000 sessions. This means that each session will execute an average of 10 sequential queries per estimated second.

Below are code examples for setting the session pool limit in different {{ ydb-short-name }} SDKs.

{% list tabs %}

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
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithSessionPoolSizeLimit(500),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      ...
    }
    ```

  - database/sql

    The `database/sql` library has its own connection pool. Each connection in `database/sql` corresponds to a specific {{ ydb-short-name }} session. The connection pool in `database/sql` is managed using the `sql.DB.SetMaxOpenConns` and `sql.DB.SetMaxIdleConns` functions. For more information, see the [documentation](https://pkg.go.dev/database/sql#DB.SetMaxOpenConns) `database/sql`.

    Example code using the connection pool size `database/sql`:


    ```golang
    package main

    import (
      "context"
      "database/sql"

      _ "github.com/ydb-platform/ydb-go-sdk/v3"
    )

    func main() {
      db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
      if err != nil {
        panic(err)
      }
      defer db.Close()
      db.SetMaxOpenConns(100)
      db.SetMaxIdleConns(100)
      db.SetConnMaxIdleTime(time.Second) // workaround for background keep-aliving of YDB sessions
      ...
    }
    ```

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    this.queryClient = QueryClient.newClient(transport)
            // 10 — minimum number of active sessions held in the pool during cleanup
            // 500 — maximum size of the session pool
            .sessionPoolMinSize(10)
            .sessionPoolMaxSize(500)
            .build();
    ```

  - JDBC

    When working with JDBC, external connection pools such as [HikariCP](https://github.com/brettwooldridge/HikariCP) or [C3p0](https://github.com/swaldman/c3p0) are typically used. In the default operation mode, the {{ ydb-short-name }} JDBC driver determines the number of connections opened by the external pool and adjusts the session pool size accordingly. Therefore, it is sufficient to configure `HikariCP` or `C3p0` correctly to set up the session pool.

    Example of configuring the HikariCP pool in the Spring configuration:


    ```properties
    spring.datasource.url=jdbc:ydb:grpc://localhost:2136/local
    spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
    spring.datasource.hikari.maximum-pool-size=100 # maximum JDBC connections
    ```

  {% endlist %}

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import os
    import ydb

    with ydb.Driver(
        connection_string=os.environ["YDB_CONNECTION_STRING"],
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=5)
        with ydb.QuerySessionPool(driver, size=500) as pool:
            # ...
    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import ydb
    import asyncio

    async def ydb_init():
        async with ydb.aio.Driver(
            connection_string=os.environ["YDB_CONNECTION_STRING"],
            credentials=ydb.credentials_from_env_variables(),
        ) as driver:
            await driver.wait()
            async with ydb.aio.QuerySessionPool(driver, size=500) as pool:
                # ...

    asyncio.run(ydb_init())
    ```

  - SQLAlchemy

    Setting the pool size is not currently supported.

  {% endlist %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
