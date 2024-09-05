# Enabling logging

{% include [work in progress message](_includes/addition.md) %}

Below are examples of code that enables logging in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

  There are several ways to enable logs in an application that uses `ydb-go-sdk`:

  {% cut "Using the `YDB_LOG_SEVERITY_LEVEL` environment variable" %}

  This environment variable enables the built-in `ydb-go-sdk` logger (synchronous, non-block) and prints to the standard output stream.
  You can set the environment variable as follows:
  ```shell
  export YDB_LOG_SEVERITY_LEVEL=info
  ```
  (possible values: `trace`, `debug`, `info`, `warn`, `error`, `fatal`, and `quiet`, defaults to `quiet`).

  {% endcut %}

  {% cut "Enable a third-party logger `go.uber.org/zap`" %}
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

  {% cut "Enable a third-party logger `github.com/rs/zerolog`" %}
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

  {% cut "Enable a custom logger implementation `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}
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

  {% cut "Implement your own logging package" %}

  You can implement your own logging package based on the driver events in the `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package. The `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package describes all logged driver events.

  {% endcut %}

- Go (database/sql)

  There are several ways to enable logs in an application that uses `ydb-go-sdk`:

  {% cut "Using the `YDB_LOG_SEVERITY_LEVEL` environment variable" %}

  This environment variable enables the built-in `ydb-go-sdk` logger (synchronous, non-block) and prints to the standard output stream.
  You can set the environment variable as follows:
  ```shell
  export YDB_LOG_SEVERITY_LEVEL=info
  ```
  (possible values: `trace`, `debug`, `info`, `warn`, `error`, `fatal`, and `quiet`, defaults to `quiet`).

  {% endcut %}

  {% cut "Enable a third-party logger `go.uber.org/zap`" %}
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

  {% cut "Enable a third-party logger `github.com/rs/zerolog`" %}
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

  {% cut "Enable a custom logger implementation `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}
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

  {% cut "Implement your own logging package" %}

  You can implement your own logging package based on the driver events in the `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package. The `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package describes all logged driver events.

  {% endcut %}

- Java

  For logging purposes, the {{ ydb-short-name }} Java SDK uses the slf4j library, which supports multiple logging levels (`error`, `warn`, `info`, `debug`, `trace`) for one or many loggers. The current implementation supports the following loggers:

  * The `com.yandex.ydb.core.grpc` logger provides information about the internal implementation of the gRPC protocol
  * The `debug` level logs all operations run over gRPC, so we recommend using it only for debugging purposes.
  * The `info` level is recommended by default

  * On the `debug` level, the `com.yandex.ydb.table.impl` logger enables you to track the internal status of the YDB driver, including session pool health.

  * On the `debug` level, the `com.yandex.ydb.table.SessionRetryContext` logger will inform you of the number of retries, results of executed queries, execution time of specific retries, and the total operation execution time

  * On the `debug` level, the `com.yandex.ydb.table.Session` logger gives you details about the query text, response status, and execution time of specific operations within the session


  Enabling and configuration the Java SDK loggers depends on the `slf4j-api` implementation used.
  Here's an example of a `log4j2` configuration for the `log4j-slf4j-impl` library

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
      <Logger name="com.yandex.ydb.core.grpc" level="info" additivity="false">
        <AppenderRef ref="Console"/>
      </Logger>
      <Logger name="com.yandex.ydb.table.impl" level="info" additivity="false">
        <AppenderRef ref="Console"/>
      </Logger>
      <Logger name="com.yandex.ydb.table.SessionRetryContext" level="debug" additivity="false">
        <AppenderRef ref="Console"/>
      </Logger>
      <Logger name="com.yandex.ydb.table.Session" level="debug" additivity="false">
        <AppenderRef ref="Console"/>
      </Logger>

      <Root level="debug" >
        <AppenderRef ref="Console"/>
      </Root>
    </Loggers>
  </Configuration>
  ```

- PHP

  For logging purposes, you need to use a class, that implements `\Psr\Log\LoggerInterface`.
  YDB-PHP-SDK has build-in loggers in `YdbPlatform\Ydb\Logger` namespace:
  * `NullLogger` - default logger, which writes nothing
  * `SimpleStdLogger($level)` - logger, which writes to logs in stderr.

  Usage example:
  ```php
  $config = [
    'logger' => new \YdbPlatform\Ydb\Logger\SimpleStdLogger(\YdbPlatform\Ydb\Logger\SimpleStdLogger::INFO)
  ]
  $ydb = new \YdbPlatform\Ydb\Ydb($config);
  ```

{% endlist %}