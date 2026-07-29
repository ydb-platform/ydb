# Enabling logging

Below are code examples of enabling logging in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  The functionality is not supported at the moment.

- Go

  {% list tabs %}

  - Native SDK

    There are several ways to enable logs in an application using `ydb-go-sdk`:

    {% cut "Through variable environment `YDB_LOG_SEVERITY_LEVEL`" %}

    This environment variable enables the built-in `ydb-go-sdk` logger (synchronous, non-blocking) with output to the standard output stream.
    You can set the environment variable as follows:


    ```shell
    export YDB_LOG_SEVERITY_LEVEL=info
    ```


    (available values `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`, default `quiet`).

    {% endcut %}

    {% cut "Connect third‑party logger `go.uber.org/zap`" %}

    ```go
    package main

    import (
      "context"
      "os"

      "go.uber.org/zap"

      ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      var log *zap.Logger // zap-logger with init out of this scope
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydbZap.WithTraces(
          log,
          trace.DetailsAll,
        ),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      ...
    }
    ```

    {% endcut %}

    {% cut "Connect third‑party logger `github.com/rs/zerolog`" %}

    ```go
    package main

    import (
      "context"
      "os"

      "github.com/rs/zerolog"

      ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      var log zerolog.Logger // zap-logger with init out of this scope
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydbZerolog.WithTraces(
          &log,
          trace.DetailsAll,
        ),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      ...
    }
    ```

    {% endcut %}

    {% include [overlay](_includes/debug-logs-go-appendix.md) %}

    {% cut "Connect custom implementation logger `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}

    ```go
    package main

    import (
      "context"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/log"
      "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      var logger log.Logger // logger implementation with init out of this scope
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithLogger(
          logger,
          trace.DetailsAll,
        ),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      ...
    }
    ```

    {% endcut %}

    {% cut "Implement custom package logging" %}

    You can implement a custom logging package based on driver events in the tracing package `github.com/ydb-platform/ydb-go-sdk/v3/trace`. The tracing package `github.com/ydb-platform/ydb-go-sdk/v3/trace` contains descriptions of all logged driver events.

    {% endcut %}

    {% cut "Implement retrieving information about server errors `IterateByIssues`" %}

    When working with {{ ydb-short-name }} via the Go SDK you can not only enable logging of requests and responses, but also programmatically obtain detailed information about server errors (issues) — additional messages that YDB returns in the response when operation execution fails. To iterate over the list of issues contained in the server response, use the [IterateByIssues](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3#IterateByIssues) method.

    {% endcut %}

  - database/sql

    There are several ways to enable logging in an application that uses `ydb-go-sdk`:

    {% cut "Through variable environment `YDB_LOG_SEVERITY_LEVEL`" %}

    This environment variable enables the built-in `ydb-go-sdk` logger (synchronous, non-blocking) with output to the standard output stream.
    You can set the environment variable as follows:


    ```shell
    export YDB_LOG_SEVERITY_LEVEL=info
    ```


    (available values `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`, default `quiet`).

    {% endcut %}

    {% cut "Connect third‑party logger `go.uber.org/zap`" %}

    ```go
    package main

    import (
      "context"
      "database/sql"
      "os"

      "go.uber.org/zap"

      ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      var log *zap.Logger // zap-logger with init out of this scope
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydbZap.WithTraces(
          log,
          trace.DetailsAll,
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
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()
      ...
    }
    ```

    {% endcut %}

    {% cut "Connect third‑party logger `github.com/rs/zerolog`" %}

    ```go
    package main

    import (
      "context"
      "database/sql"
      "os"

      "github.com/rs/zerolog"

      ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      var log zerolog.Logger // zap-logger with init out of this scope
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydbZerolog.WithTraces(
          &log,
          trace.DetailsAll,
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
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()
      ...
    }
    ```

    {% endcut %}

    {% include [overlay](_includes/debug-logs-go-sql-appendix.md) %}

    {% cut "Connect custom implementation logger `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}

    ```go
    package main

    import (
      "context"
      "database/sql"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/log"
      "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      var logger log.Logger // logger implementation with init out of this scope
      nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithLogger(
          logger,
          trace.DetailsAll,
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
      defer connector.Close()

      db := sql.OpenDB(connector)
      defer db.Close()
      ...
    }
    ```

    {% endcut %}

    {% cut "Implement custom package logging" %}

    You can implement a custom logging package based on driver events in the tracing package `github.com/ydb-platform/ydb-go-sdk/v3/trace`. The tracing package `github.com/ydb-platform/ydb-go-sdk/v3/trace` contains descriptions of all logged driver events.

    {% endcut %}

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    {{ ydb-short-name }} Java SDK uses [SLF4J](https://www.slf4j.org/) as a logging facade: the SDK writes messages via the SLF4J API, and the actual output (log4j2, logback, etc.) is attached through application dependencies. Place the log4j2 configuration in `src/main/resources/log4j2.xml`.

    Maven dependencies (Native SDK + log4j2):


    ```xml
    <dependencies>
        <dependency>
            <groupId>tech.ydb</groupId>
            <artifactId>ydb-sdk-core</artifactId>
            <version><!-- актуальная версия --></version>
        </dependency>
        <dependency>
            <groupId>tech.ydb</groupId>
            <artifactId>ydb-sdk-query</artifactId>
            <version><!-- актуальная версия --></version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-slf4j2-impl</artifactId>
            <version>2.24.3</version>
        </dependency>
    </dependencies>
    ```


    The following loggers are available in {{ ydb-short-name }} Java SDK:

    * `tech.ydb.core.grpc` — gRPC transport (`info` by default, `debug` — all RPCs)
    * `tech.ydb.table.impl` — internal driver state, including the session pool
    * `tech.ydb.table.SessionRetryContext` — retries, attempt duration
    * `tech.ydb.table.Session` — query text, status, and execution time

    Example `src/main/resources/log4j2.xml`:


    ```xml
    <Configuration status="WARN">
      <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
          <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
      </Appenders>

      <Loggers>
        <Logger name="io.netty" level="warn" additivity="false">
          <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="io.grpc.netty" level="warn" additivity="false">
          <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="tech.ydb.core.grpc" level="info" additivity="false">
          <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="tech.ydb.table.impl" level="info" additivity="false">
          <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="tech.ydb.table.SessionRetryContext" level="debug" additivity="false">
          <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="tech.ydb.table.Session" level="debug" additivity="false">
          <AppenderRef ref="Console"/>
        </Logger>

        <Root level="debug">
          <AppenderRef ref="Console"/>
        </Root>
      </Loggers>
    </Configuration>
    ```


    The runnable example — connection and `SELECT 1` with logging enabled:


    ```java
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;

    public class DebugLogsNativeExample {

        public static void main(String[] args) {
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
                 QueryClient queryClient = QueryClient.newClient(transport).build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

                QueryReader reader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                        session.createQuery("SELECT 1 AS value", TxMode.NONE, Params.empty())
                )).join().getValue();

                ResultSetReader rs = reader.getResultSet(0);
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getColumn("value").getInt32());
                }
            }
        }
    }
    ```

  - JDBC

    The JDBC driver uses the same SLF4J stack; configure the `tech.ydb.*` loggers the same way as in the native SDK. Place the log4j2 configuration in `src/main/resources/log4j2.xml` (see the XML above). Maven dependencies:


    ```xml
    <dependencies>
        <dependency>
            <groupId>tech.ydb.jdbc</groupId>
            <artifactId>ydb-jdbc-driver</artifactId>
            <version><!-- актуальная версия --></version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-slf4j2-impl</artifactId>
            <version>2.24.3</version>
        </dependency>
    </dependencies>
    ```


    ```java
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    public class DebugLogsJdbcExample {

        public static void main(String[] args) throws SQLException {
            String url = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            try (Connection connection = DriverManager.getConnection(url);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1 AS value")) {

                rs.next();
                System.out.println("SELECT 1 = " + rs.getInt("value"));
            }
        }
    }
    ```


    The same debug logs are available in all other frameworks around JDBC (Spring Boot, ORM, connection pools, etc.): they flow to {{ ydb-short-name }} through this driver, and you only need to attach the same `slf4j` / log4j2 / logback configuration in the application.

  {% endlist %}

- Python

  The Python SDK uses the standard logging library — `logging`. To enable a specific logging mode:


  ```python
  import logging

  logging.getLogger('ydb').setLevel(logging.DEBUG)
  ```

- JavaScript

  The [debug](https://www.npmjs.com/package/debug) library is used for logging events inside the SDK.
  To enable logs, set the environment variable `DEBUG` to the SDK event filter value — `DEBUG=ydbjs:*`.

- Rust

  Inside the crate `ydb`, messages are sent through the standard library for the Rust ecosystem, [`tracing`](https://docs.rs/tracing) (this is the crate name; it also includes regular text logs at the debug/trace level, not only “distributed tracing”). To see the output in the console, attach a subscriber before creating the client, for example [`⟦C3⟧_subscriber::fmt`](https://docs.rs/tracing-subscriber) with the desired level (`TRACE` for maximum detail). Example: [`basic-logs.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/basic-logs.rs).


  ```rust
  tracing_subscriber::fmt()
      .with_max_level(tracing::Level::TRACE)
      .init();

  let client = ydb::ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .client()?;
  ```

- C#

  In the {{ ydb-short-name }} C# SDK, logging is connected via the standard `ILoggerFactory` interface from `Microsoft.Extensions.Logging`. You can pass any implementation — a console logger, Serilog, NLog, and others:


  ```C#
  using Microsoft.Extensions.Logging;
  using Ydb.Sdk.Ado;

  var loggerFactory = LoggerFactory.Create(builder =>
      builder.AddConsole().SetMinimumLevel(LogLevel.Debug));

  var ydbBuilder = new YdbConnectionStringBuilder
  {
      Host = "localhost",
      Port = 2136,
      Database = "/local",
      LoggerFactory = loggerFactory
  };

  await using var dataSource = new YdbDataSource(ydbBuilder);
  await using var connection = await dataSource.OpenConnectionAsync();
  ```


  Since `LoggerFactory` accepts the standard `ILoggerFactory`, you can connect Serilog or NLog without additional adapters:


  ```C#
  // Serilog
  var loggerFactory = new SerilogLoggerFactory(Log.Logger);

  // NLog
  var loggerFactory = LoggerFactory.Create(builder => builder.AddNLog());
  ```

- PHP

  In the YDB PHP SDK, for logging you need to use a class that implements `\Psr\Log\LoggerInterface`.
  The YDB-PHP-SDK has built-in loggers in the `YdbPlatform\Ydb\Logger` namespace:

  * `NullLogger` — the default one that outputs nothing
  * `SimpleStdLogger($level)` — a logger that outputs logs to stderr.

  Usage example:


  ```php
  $config = [
    'logger' => new \YdbPlatform\Ydb\Logger\SimpleStdLogger(\YdbPlatform\Ydb\Logger\SimpleStdLogger::INFO)
  ]
  $ydb = new \YdbPlatform\Ydb\Ydb($config);
  ```

{% endlist %}
