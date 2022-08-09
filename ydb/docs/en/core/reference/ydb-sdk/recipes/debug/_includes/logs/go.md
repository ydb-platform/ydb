There are several ways to enable logs in an application that uses `ydb-go-sdk`:

* Set the environment variable `YDB_LOG_SEVERITY_LEVEL=info` (possible values: `trace`, `debug`, `info`, `warn`, `error`, `fatal`, and `quiet`, defaults to `quiet`).
  This environment variable enables the built-in `ydb-go-sdk` logger (synchronous, non-block) with output to the standard output stream.
* {% cut "Connect a third-party logger `go.uber.org/zap`" %}

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
* {% cut "Connect a third-party logger `go.uber.org/zap`" %}

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
* {% cut "Connect your own logger implementation `github.com/ydb-platform/ydb-go-sdk/v3/log.Logger`" %}

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

* Implement your own logging package based on the `github.com/ydb-platform/ydb-go-sdk/v3/trace` tracing package.

