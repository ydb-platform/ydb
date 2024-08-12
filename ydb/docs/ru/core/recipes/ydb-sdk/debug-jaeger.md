# Включение трассировки в Jaeger

{% include [work in progress message](_includes/addition.md) %}

Ниже приведены примеры кода включения трассировки в Jaeger в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

    ```go
    package main

    import (
        "context"
        "time"

        "github.com/opentracing/opentracing-go"
        jaegerConfig "github.com/uber/jaeger-client-go/config"

        "github.com/ydb-platform/ydb-go-sdk/v3"
        "github.com/ydb-platform/ydb-go-sdk/v3/trace"

        tracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
    )

    const (
        tracerURL   = "localhost:5775"
        serviceName = "ydb-go-sdk"
    )

    func main() {
        tracer, closer, err := jaegerConfig.Configuration{
            ServiceName: serviceName,
            Sampler: &jaegerConfig.SamplerConfig{
                Type:  "const",
                Param: 1,
            },
            Reporter: &jaegerConfig.ReporterConfig{
                LogSpans:            true,
                BufferFlushInterval: 1 * time.Second,
                LocalAgentHostPort:  tracerURL,
            },
        }.NewTracer()
        if err != nil {
            panic(err)
        }

        defer closer.Close()

        // set global tracer of this application
        opentracing.SetGlobalTracer(tracer)

        span, ctx := opentracing.StartSpanFromContext(context.Background(), "client")
        defer span.Finish()

        db, err := ydb.Open(ctx,
            os.Getenv("YDB_CONNECTION_STRING"),
            tracing.WithTraces(tracing.WithDetails(trace.DetailsAll)),
        )
        if err != nil {
            panic(err)
        }
        defer db.Close(ctx)
        ...
    }
    ```

- Go (database/sql)

    ```go
    package main

    import (
        "context"
        "database/sql"
        "time"

        "github.com/opentracing/opentracing-go"
        jaegerConfig "github.com/uber/jaeger-client-go/config"

        "github.com/ydb-platform/ydb-go-sdk/v3"
        "github.com/ydb-platform/ydb-go-sdk/v3/trace"

        tracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
    )

    const (
        tracerURL   = "localhost:5775"
        serviceName = "ydb-go-sdk"
    )

    func main() {
        tracer, closer, err := jaegerConfig.Configuration{
            ServiceName: serviceName,
                Sampler: &jaegerConfig.SamplerConfig{
                Type:  "const",
                Param: 1,
            },
            Reporter: &jaegerConfig.ReporterConfig{
                LogSpans:            true,
                BufferFlushInterval: 1 * time.Second,
                LocalAgentHostPort:  tracerURL,
            },
        }.NewTracer()
        if err != nil {
            panic(err)
        }

        defer closer.Close()

        // set global tracer of this application
        opentracing.SetGlobalTracer(tracer)

        span, ctx := opentracing.StartSpanFromContext(context.Background(), "client")
        defer span.Finish()

        nativeDriver, err := ydb.Open(ctx,
            os.Getenv("YDB_CONNECTION_STRING"),
            tracing.WithTraces(tracing.WithDetails(trace.DetailsAll)),
        )
        if err != nil {
            panic(err)
        }
        defer nativeDriver.Close(ctx)

        connector, err := ydb.Connector(nativeDriver)
        if err != nil {
            panic(err)
        }
        
        db := sql.OpnDB(connector)
        defer db.Close()
        ...
    }
    ```


{% endlist %}
