Есть несколько способов включить логи в приложении, использующем `ydb-go-sdk`:
* Выставить переменную окружения `YDB_LOG_SEVERITY_LEVEL=info` (доступные значения `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`, по умолчанию `quiet`).
    Данная переменная окружения включает встроенный в `ydb-go-sdk` логгер (синхронный, неблочный) с выводом в стандартный поток вывода.
* {% cut "Подключить сторонний логгер `go.uber.org/zap`" %}
    ```go
    package main

    import (
        "context"

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
                ydbZap.WithDetails(trace.DetailsAll),
            ),
        )
        if err != nil {
            panic(err)
        }
        defer func() {
            _ = db.Close(ctx)
        }()
    }
    ```
    {% endcut %}
* {% cut "Подключить сторонний логгер `github.com/rs/zerolog`" %}
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
                log,
                ydbZerolog.WithDetails(trace.DetailsAll),
            ),
        )
        if err != nil {
            panic(err)
        }
        defer func() {
            _ = db.Close(ctx)
        }()
    }
    ```
    {% endcut %}
* {% cut "Подключить собственную имплементацию логгера `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}
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
                trace.DetailsAll,
                ydb.WithExternalLogger(logger),
            ),
        )
        if err != nil {
            panic(err)
        }
        defer func() {
            _ = db.Close(ctx)
        }()
    }
    ```
    {% endcut %}

{% include [overlay](go_appendix.md) %}

* Реализовать собственный пакет логирования на основе пакета трассировки `github.com/ydb-platform/ydb-go-sdk/v3/trace`
