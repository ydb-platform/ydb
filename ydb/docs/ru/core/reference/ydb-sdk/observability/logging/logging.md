# Включение логирования

Ниже приведены примеры кода включения логирования в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- C++

  Функциональность на данный момент не поддерживается.

- Go

  {% list tabs %}

  - Native SDK

    Есть несколько способов включить логи в приложении, использующем `ydb-go-sdk`:

    {% cut "Через переменную окружения `YDB_LOG_SEVERITY_LEVEL`" %}

    Данная переменная окружения включает встроенный в `ydb-go-sdk` логгер (синхронный, неблочный) с выводом в стандартный поток вывода.
    Выставить переменную окружения можно так:

    ```shell
    export YDB_LOG_SEVERITY_LEVEL=info
    ```

    (доступные значения `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`, по умолчанию `quiet`).

    {% endcut %}

    {% cut "Подключить сторонний логгер `go.uber.org/zap`" %}

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

    {% cut "Подключить сторонний логгер `github.com/rs/zerolog`" %}

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

    {% cut "Подключить собственную имплементацию логгера `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}

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

    {% cut "Реализовать собственный пакет логирования" %}

    Реализовать собственный пакет логирования можно на основе событий драйвера в пакете трассировки `github.com/ydb-platform/ydb-go-sdk/v3/trace`. Пакет трассировки `github.com/ydb-platform/ydb-go-sdk/v3/trace` содержит описание всех протоколируемых событий драйвера.

    {% endcut %}

    {% cut "Реализовать получение информации о серверных ошибках `IterateByIssues`" %}

    При работе с {{ ydb-short-name }} через Go SDK вы можете не только включать логирование запросов и ответов, но и программно получать детализированные сведения о серверных ошибках (issues) — дополнительных сообщениях, которые YDB возвращает в составе ответа при ошибках выполнения операций. Для итерации по списку issues, содержащихся в ответе от сервера, используйте метод [IterateByIssues](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3#IterateByIssues).

    {% endcut %}

  - database/sql

    Есть несколько способов включить логи в приложении, использующем `ydb-go-sdk`:

    {% cut "Через переменную окружения `YDB_LOG_SEVERITY_LEVEL`" %}

    Данная переменная окружения включает встроенный в `ydb-go-sdk` логгер (синхронный, неблочный) с выводом в стандартный поток вывода.
    Выставить переменную окружения можно так:

    ```shell
    export YDB_LOG_SEVERITY_LEVEL=info
    ```

    (доступные значения `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`, по умолчанию `quiet`).

    {% endcut %}

    {% cut "Подключить сторонний логгер `go.uber.org/zap`" %}

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

    {% cut "Подключить сторонний логгер `github.com/rs/zerolog`" %}

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

    {% cut "Подключить собственную имплементацию логгера `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}

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

    {% cut "Реализовать собственный пакет логирования" %}

    Реализовать собственный пакет логирования можно на основе событий драйвера в пакете трассировки `github.com/ydb-platform/ydb-go-sdk/v3/trace`. Пакет трассировки `github.com/ydb-platform/ydb-go-sdk/v3/trace` содержит описание всех протоколируемых событий драйвера.

    {% endcut %}

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    {{ ydb-short-name }} Java SDK использует [SLF4J](https://www.slf4j.org/) как фасад логирования: SDK пишет сообщения через API SLF4J, а конкретный вывод (log4j2, logback и т.д.) подключается зависимостями приложения. Конфигурацию log4j2 разместите в `src/main/resources/log4j2.xml`.

    Зависимости Maven (Native SDK + log4j2):

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

    В {{ ydb-short-name }} Java SDK доступны следующие логгеры:

    * `tech.ydb.core.grpc` — gRPC-транспорт (`info` по умолчанию, `debug` — все RPC)
    * `tech.ydb.table.impl` — внутреннее состояние драйвера, в том числе пул сессий
    * `tech.ydb.table.SessionRetryContext` — повторы, длительность попыток
    * `tech.ydb.table.Session` — текст запроса, статус и время выполнения

    Пример `src/main/resources/log4j2.xml`:

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

    Запускаемый пример — подключение и `SELECT 1` при включённом логировании:

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

    JDBC-драйвер использует тот же стек SLF4J; настройте логгеры `tech.ydb.*` так же, как в нативном SDK. Конфигурацию log4j2 разместите в `src/main/resources/log4j2.xml` (см. XML выше). Зависимости Maven:

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

    Те же дебаг-логи доступны во всех остальных фреймворках вокруг JDBC (Spring Boot, ORM, пулы соединений и т.д.): они ходят в {{ ydb-short-name }} через этот драйвер, достаточно подключить ту же конфигурацию `slf4j` / log4j2 / logback в приложении.

  {% endlist %}

- Python

  Python SDK использует стандартную библиотеку для логирования - `logging`. Для включения определенного режима логирования:

  ```python
  import logging

  logging.getLogger('ydb').setLevel(logging.DEBUG)
  ```

- JavaScript

  Для логирования событий внутри sdk используется библиотека [debug](https://www.npmjs.com/package/debug).
  Для включения логов необходимо задать переменную окружения `DEBUG` со значением фильтра по событиям sdk - `DEBUG=ydbjs:*`.

- Rust

  Внутри крейта `ydb` сообщения идут через стандартную для Rust экосистемы библиотеку [`tracing`](https://docs.rs/tracing) (это имя крейта; сюда же относятся обычные текстовые логи уровня debug/trace, не только «распределённая трассировка»). Чтобы видеть вывод в консоль, до создания клиента подключите подписчика, например [`tracing_subscriber::fmt`](https://docs.rs/tracing-subscriber) с нужным уровнем (`TRACE` для максимальной детализации). Пример: [`basic-logs.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/basic-logs.rs).

  ```rust
  tracing_subscriber::fmt()
      .with_max_level(tracing::Level::TRACE)
      .init();

  let client = ydb::ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .client()?;
  ```

- C#

  В {{ ydb-short-name }} C# SDK логирование подключается через стандартный интерфейс `ILoggerFactory` из `Microsoft.Extensions.Logging`. Можно передать любую реализацию — консольный логгер, Serilog, NLog и другие:

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

  Так как `LoggerFactory` принимает стандартный `ILoggerFactory`, подключить Serilog или NLog можно без дополнительных адаптеров:

  ```C#
  // Serilog
  var loggerFactory = new SerilogLoggerFactory(Log.Logger);

  // NLog
  var loggerFactory = LoggerFactory.Create(builder => builder.AddNLog());
  ```

- PHP

  В YDB PHP SDK для логирования вам нужно использовать класс, который реализует `\Psr\Log\LoggerInterface`.
  В YDB-PHP-SDK встроены логгеры в пространстве имен `YdbPlatform\Ydb\Logger`:

  * `NullLogger` - по умолчанию, который ничего не выводит
  * `SimpleStdLogger($level)` - логгер, который выводит логи в stderr.

  Пример использования:

  ```php
  $config = [
    'logger' => new \YdbPlatform\Ydb\Logger\SimpleStdLogger(\YdbPlatform\Ydb\Logger\SimpleStdLogger::INFO)
  ]
  $ydb = new \YdbPlatform\Ydb\Ydb($config);
  ```

{% endlist %}
