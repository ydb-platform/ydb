# Set session pool size

{{ ydb-short-name }} creates an [actor](../../concepts/glossary.md#actor) for each session. As a result, the session pool size on the client affects resource consumption (memory, CPU) on the server side {{ ydb-short-name }}.

For example, if 1,000 clients of a single database open 1,000 sessions each, 1,000,000 actors are created on the server side. This number of actors consumes significant amounts of memory and CPU resources. Without a limit on the number of client sessions, this can lead to slow cluster performance and a semi‑failure state.

By default, the {{ ydb-short-name }} SDK sets a limit of 50 sessions when using native drivers. When using third‑party libraries, such as Go `database/sql`, the limit is not set.

You should set the client‑side session limit to the minimum required for normal operation of the client application. Keep in mind that a session is single‑threaded on both the server and client sides. Accordingly, if the application needs to perform 1,000 concurrent (inflight) requests in {{ ydb-short-name }}, the limit should be set to 1,000 sessions.

It is important to distinguish between calculated RPS (requests per second) and inflight. In the former case, we refer to the total number of requests sent to {{ ydb-short-name }} per second. For example, with RPS = 10,000 and an average request latency of 100 ms, a limit of 1,000 sessions is sufficient. This means that each session will execute, on average, 10 sequential requests per second.

Below are code examples for setting the session pool limit in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    - `MaxActiveSessions` — maximum pool size (default 50).
    - `MinPoolSize` — minimum number of sessions (default 10). The SDK will stop closing sessions on timeout once the limit is reached, so the number is not guaranteed.


    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/query/client.h>

    NYdb::NQuery::TQueryClient CreateQueryClient(const NYdb::TDriver& driver) {
        NYdb::NQuery::TClientSettings settings;
        settings.SessionPoolSettings(
            NYdb::NQuery::TSessionPoolSettings()
                .MaxActiveSessions(500)
                .MinPoolSize(10));
        return NYdb::NQuery::TQueryClient(driver, settings);
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
                max_pool_size: 500
                min_pool_size: 10
    ```

    {% endcut %}

    Initialization code `ydb::YdbComponent`, obtaining `ydb::TableClient` and starting `components::MinimalServerComponentList` — as in the example from [init.md](./init.md).

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

    The `database/sql` library has its own connection pool. Each connection in `database/sql` corresponds to a specific {{ ydb-short-name }} session. The connection pool in `database/sql` is managed using the `sql.DB.SetMaxOpenConns` and `sql.DB.SetMaxIdleConns` functions. More details are provided in the [documentation](https://pkg.go.dev/database/sql#DB.SetMaxOpenConns) `database/sql`.

    Code example using the connection pool size `database/sql`:


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

    The pool size is set when creating `TableClient` or `QueryClient`. Each session in the pool is a separate [actor](../../concepts/glossary.md#actor) on the server, so you should choose the limit based on the calculated inflight rather than only on RPS (see the introductory section above).


    ```java
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;

    public class SessionPoolLimitExample {

        public static void main(String[] args) {
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
                 QueryClient queryClient = QueryClient.newClient(transport)
                         // 10 — minimum number of active sessions retained in the pool during cleanup
                         // 500 — maximum size of the session pool
                         .sessionPoolMinSize(10)
                         .sessionPoolMaxSize(500)
                         .build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

                QueryReader reader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                        session.createQuery("SELECT 1 AS value", TxMode.NONE, Params.empty())
                )).join().getValue();

                ResultSetReader rs = reader.getResultSet(0);
                if (rs.next()) {
                    System.out.println("Пул настроен, SELECT 1 = " + rs.getColumn("value").getInt32());
                }
            }
        }
    }
    ```

  - JDBC

    When working with JDBC, external connection pools such as [HikariCP](https://github.com/brettwooldridge/HikariCP) are typically used. {{ ydb-short-name }} The JDBC driver adapts the internal session pool to the number of open JDBC connections, so the limit is set on the external pool side. Connection properties are in the [JDBC driver documentation](../../reference/languages-and-apis/jdbc-driver/properties.md).


    ```java
    import java.sql.Connection;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    import com.zaxxer.hikari.HikariConfig;
    import com.zaxxer.hikari.HikariDataSource;

    public class JdbcSessionPoolLimitExample {

        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(jdbcUrl);
            config.setDriverClassName("tech.ydb.jdbc.YdbDriver");
            config.setMaximumPoolSize(100); // maximum JDBC connections (= YDB sessions)

            try (HikariDataSource dataSource = new HikariDataSource(config);
                 Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1 AS value")) {

                rs.next();
                System.out.println("HikariCP pool size = 100, SELECT 1 = " + rs.getInt("value"));
            }
        }
    }
    ```


    In Spring Boot, the same parameters are set via `spring.datasource.hikari.maximum-pool-size` (see the example in the introductory section).

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

    Setting the pool size is currently not supported.

  {% endlist %}

- C#

  In the {{ ydb-short-name }} C# SDK, session pool parameters are set via the connection string:


  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource(
      "Host=localhost;Port=2136;Database=/local;MaxPoolSize=500;MinPoolSize=10;SessionIdleTimeout=60");
  ```


  * `MaxPoolSize` — maximum session pool size (default 100)
  * `MinPoolSize` — minimum number of sessions retained in the pool (default 0)
  * `SessionIdleTimeout` — session idle time in seconds before it is closed (default 300)

  For Entity Framework and linq2db, use the same connectionString.

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  The session pool size is set on the driver via [`Client::with_session_pool`](https://docs.rs/ydb/latest/ydb/struct.Client.html#method.with_session_pool) and [`SessionPoolSettings::with_limit`](https://docs.rs/ydb/latest/ydb/struct.SessionPoolSettings.html#method.with_limit). The pool is shared for table and query clients:


  ```rust
  use ydb::{ClientBuilder, SessionPoolSettings, YdbResult};

  #[tokio::main]
  async fn main() -> YdbResult<()> {
      let client = ClientBuilder::new_from_connection_string(
          std::env::var("YDB_CONNECTION_STRING")?,
      )?
      .client()?;
      client.wait().await?;

      let client = client
          .with_session_pool(SessionPoolSettings::new().with_limit(500))
          .await?;

      let mut qc = client.query_client();
      let mut row = qc.query_row("SELECT 1 AS one").await?;
      Ok(())
  }
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
