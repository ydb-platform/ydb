# Setting the session pool size

{{ ydb-short-name }} creates an [actor](../../concepts/glossary.md#actor) for each session. Consequently, the session pool size of a client affects resource consumption (RAM, CPU) on the server side of {{ ydb-short-name }}.

For example, if 1000 clients of the same database have 1000 sessions each, then 1000000 [actors](../../concepts/glossary.md#actor) are created on the server side, consuming a significant amount of memory and CPU. If you do not limit the number of sessions on the client side, this may result in a slow cluster that is at risk of failure.

By default, the {{ ydb-short-name }} SDK sets a limit of 50 sessions when using native drivers. However, no session limit is enforced for third-party libraries, such as Go `database/sql`.

It’s recommended to set the client session limit to the minimum required for the normal operation of the client application. Keep in mind that sessions are single-threaded on both the server and client sides. For instance, if the application needs to handle 1000 simultaneous (in-flight) requests to {{ ydb-short-name }} based on its estimated load, the session limit should be set to 1000.

It is important to distinguish between estimated RPS (requests per second) and in-flight requests. RPS refers to the total number of requests completed by {{ ydb-short-name }} within one second. For example, if RPS = 10000 and the average latency is 100 ms, a session limit of 1000 is sufficient. This configuration allows each session to perform an average of 10 consecutive requests within the estimated second.

Below are examples of the code for setting the session pool limit in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

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

- Go (database/sql)

  The `database/sql` library has its own connection pool. Each `database/sql`connection corresponds to a specific {{ ydb-short-name }} session. A `database/sql` connection pool is managed by the `sql.DB.SetMaxOpenConns` and `sql.DB.SetMaxIdleConns` functions. Learn more in the `database/sql` [documentation](https://pkg.go.dev/database/sql#DB.SetMaxOpenConns).

  Example of the code that uses the size of `database/sql` connection pool:

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

- Java

  ```java
  this.queryClient = QueryClient.newClient(transport)
          // 10 - minimum number of active sessions to keep in the pool during the cleanup
          // 500 - maximum number of sessions in the pool
          .sessionPoolMinSize(10)
          .sessionPoolMaxSize(500)
          .build();
  ```

- JDBC Driver

  Usually working with JDBC applications use the different connections pools, such as [HikariCP](https://github.com/brettwooldridge/HikariCP) or [C3p0](https://github.com/swaldman/c3p0). By default the {{ ydb-short-name }} JDBC driver detects current count of opened connections and tunes the session pool size itself. So if the application has correct configured `HikariCP` или `C3p0`, it may not configure the session pool.

  Example of HikariCP configuration in `String` application.properties:

  ```properties
    spring.datasource.url=jdbc:ydb:grpc://localhost:2136/local
    spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
    spring.datasource.hikari.maximum-pool-size=100 # maximum size of JDBC connections
  ```

{% endlist %}
