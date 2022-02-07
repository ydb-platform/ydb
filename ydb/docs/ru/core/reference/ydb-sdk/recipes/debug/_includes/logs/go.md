Есть несколько способов включить логи в приложении, использующем `ydb-go-sdk`:
1. Выставить переменную окружения `YDB_LOG_SEVERITY_LEVEL=info` (доступные значения `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`, по умолчанию `quiet`). 
    Данная переменная окружения включает встроенный в `ydb-go-sdk` логгер (синхронный, неблочный) с выводом в стандартный поток вывода. 
2. {% cut "Подключить сторонний логгер `go.uber.org/zap`" %}
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
        db, err := ydb.New(
            ctx,
            ...
            ydb.WithTraceDriver(ydbZap.Driver(
                log,
                trace.DetailsAll,
            )),
            ydb.WithTraceTable(ydbZap.Table(
                log,
                trace.DetailsAll,
            )),
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
3. {% cut "Подключить сторонний логгер `github.com/rs/zerolog`" %} 
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
        db, err := ydb.New(
            ctx,
            ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
            ydb.WithTraceDriver(ydbZerolog.Driver(
                log,
                trace.DetailsAll,
            )),
            ydb.WithTraceTable(ydbZerolog.Table(
                log,
                trace.DetailsAll,
            )),
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
4. Реализовать собственный пакет логгирования на основе пакета трассировки `github.com/ydb-platform/ydb-go-sdk/v3/trace`
