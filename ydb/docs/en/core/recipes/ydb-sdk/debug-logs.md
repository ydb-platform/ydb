# Enabling logging

Below are code examples for enabling logging in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go

  {% list tabs %}

  - Native SDK

    There are several ways to enable logs in an application that uses `ydb-go-sdk`:

    {% cut "Via variable environment `YDB_LOG_SEVERITY_LEVEL`" %}

    This environment variable enables the logger built into `ydb-go-sdk` (synchronous, non-blocking) with output to the standard output stream.
    You can set the environment variable as follows:


    ```shell
    export YDB_LOG_SEVERITY_LEVEL=info
    ```


    (available values `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`, by default `quiet`).

    {% endcut %}

    {% cut "Connect third-party logger `go.uber.org/zap`" %}

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

    {% cut "Connect third-party logger `github.com/rs/zerolog`" %}

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

    {% cut "Connect own implementation logger `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}

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

    {% cut "Implement own package logging" %}

    You can implement your own logging package based on driver events in the `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package. The `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package contains descriptions of all logged driver events.

    {% endcut %}

    {% cut "Implement obtaining information about server errors `IterateByIssues`" %}

    When working with {{ ydb-short-name }} through the Go SDK, you can not only enable logging of requests and responses, but also programmatically obtain detailed information about server errors (issues) — additional messages that YDB returns as part of the response when operations fail. To iterate over the list of issues contained in the server response, use the [IterateByIssues](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3#IterateByIssues) method.

    {% endcut %}

  - database/sql

    There are several ways to enable logs in an application that uses `ydb-go-sdk`:

    {% cut "Via variable environment `YDB_LOG_SEVERITY_LEVEL`" %}

    This environment variable enables the logger built into `ydb-go-sdk` (synchronous, non-blocking) with output to the standard output stream.
    You can set the environment variable as follows:


    ```shell
    export YDB_LOG_SEVERITY_LEVEL=info
    ```


    (available values `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`, by default `quiet`).

    {% endcut %}

    {% cut "Connect third-party logger `go.uber.org/zap`" %}

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

    {% cut "Connect third-party logger `github.com/rs/zerolog`" %}

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

    {% cut "Connect own implementation logger `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}

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

    {% cut "Implement own package logging" %}

    You can implement your own logging package based on driver events in the `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package. The `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package contains descriptions of all logged driver events.

    {% endcut %}

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    The {{ ydb-short-name }} Java SDK uses the slf4j library for logging, which allows using different logging levels (`error`, `warn`, `info`, `debug`, `trace`) for one or more loggers. The following loggers are available in the current implementation:

    * The `tech.ydb.core.grpc` logger provides information about the internal implementation of the gRPC protocol.
    * The `debug` level logs all operations over the gRPC protocol; it is recommended to use it only for debugging.
    * The `info` level is recommended for use by default.
    * The `tech.ydb.table.impl` logger at the `debug` level allows you to track the internal state of the YDB driver, in particular the operation of the session pool.
    * The `tech.ydb.table.SessionRetryContext` logger at the `debug` level will inform you about the number of retries, the results of executed queries, the execution time of individual retries, and the total execution time of the entire operation.
    * The `tech.ydb.table.Session` logger at the `debug` level provides information about the query text, response status, and execution time for various session operations.

    Enabling and configuring Java SDK loggers depends on the `slf4j-api` implementation used.
    Here is an example of `log4j2` configuration for the `log4j-slf4j-impl` library.


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

        <Root level="debug" >
          <AppenderRef ref="Console"/>
        </Root>
      </Loggers>
    </Configuration>
    ```

  - JDBC

    The JDBC driver uses the same logging stack via `slf4j`; configure the `tech.ydb.*` loggers the same way as in the native SDK.

    The same debug logs are available in all other frameworks around JDBC (Spring Boot, ORM, connection pools, etc.): they access {{ ydb-short-name }} through this driver, so you just need to include the same `slf4j` / log4j2 / logback configuration in your application.

  {% endlist %}

- Python

  The Python SDK uses the standard logging library - `logging`. To enable a specific logging mode:


  ```python
  import logging

  logging.getLogger('ydb').setLevel(logging.DEBUG)
  ```

- JavaScript

  The [debug](https://www.npmjs.com/package/debug) library is used to log events inside the SDK.
  To enable logs, set the `DEBUG` environment variable to the SDK event filter value - `DEBUG=ydbjs:*`.

- Rust

  Inside the `ydb` crate, messages go through the standard Rust ecosystem library [`tracing`](https://docs.rs/tracing) (this is the crate name; it also includes regular text logs at debug/trace level, not just 'distributed tracing'). To see console output, before creating the client, attach a subscriber, for example [`tracing_subscriber::fmt`](https://docs.rs/tracing-subscriber) with the required level (`TRACE` for maximum detail). Example: [`basic-logs.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/basic-logs.rs).


  ```rust
  tracing_subscriber::fmt()
      .with_max_level(tracing::Level::TRACE)
      .init();

  let client = ydb::ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .client()?;
  ```

- PHP

  In the YDB PHP SDK, for logging you need to use a class that implements `\Psr\Log\LoggerInterface`.
  The YDB-PHP-SDK includes built-in loggers in the `YdbPlatform\Ydb\Logger` namespace:

  * `NullLogger` - the default one that outputs nothing
  * `SimpleStdLogger($level)` - a logger that outputs logs to stderr.

  Usage example:


  ```php
  $config = [
    'logger' => new \YdbPlatform\Ydb\Logger\SimpleStdLogger(\YdbPlatform\Ydb\Logger\SimpleStdLogger::INFO)
  ]
  $ydb = new \YdbPlatform\Ydb\Ydb($config);
  ```

{% endlist %}
