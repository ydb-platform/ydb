# Включение логирования

{% include [work in progress message](_includes/addition.md) %}

Ниже приведены примеры кода включения логирования в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

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

- Go (database/sql)

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

- Java

    В {{ ydb-short-name }} Java SDK для логирования используется библиотека slf4j, которая позволяет использовать различные уровни логирования (`error`, `warn`, `info`, `debug`, `trace`) для одного или нескольких логгеров. В текущей реализации доступны следующие логгеры:

    * Логгер `tech.ydb.core.grpc` предоставляет информацию о внутренней реализации grpc протокола
    * уровень `debug` логирует все операции по протоколу grpc, рекомедуется использовать только для отладки
    * уровень `info` рекомендуется использовать по умолчанию

    * Логгер `tech.ydb.table.impl` на уровне `debug` позволяет отслеживать внутреннее состояние драйвера ydb, в частности работу пула сессий.

    * Логгер `tech.ydb.table.SessionRetryContext` на уровне `debug` будет информировать о количестве ретраев, результатах выполненных запросов, времени выполнения отдельных ретраев и общем времени выполнения всей операции

    * Логгер `tech.ydb.table.Session` на уровне `debug` предоставляет информацию о тексте запроса, статусе ответа и времени выполнения для различных операций сессии


    Включение и настройка логгеров Java SDK зависит от используемой реализации `slf4j-api`.
    Здесь приведен пример конфигурации `log4j2` для библиотеки `log4j-slf4j-impl`

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
