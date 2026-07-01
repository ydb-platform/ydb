# Включение трассировки в Jaeger

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

        "github.com/ydb-platform/ydb-go-sdk/v3"
        "github.com/ydb-platform/ydb-go-sdk/v3/trace"

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
<<<<<<< HEAD:ydb/docs/ru/core/recipes/ydb-sdk/debug-jaeger.md
=======

>>>>>>> 7835ec47514 (docs: Rust basic query example in example-app + other Rust code snippets + Vector search article refactoring + removed OpenTracing from feature-parity table (#43637)):ydb/docs/ru/core/reference/ydb-sdk/observability/tracing/jaeger.md
          connector, err := ydb.Connector(nativeDriver)
          if err != nil {
              panic(err)
          }

          db := sql.OpenDB(connector)
          defer db.Close()
          ...
      }
      ```

    {% endlist %}

- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Python

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Для распределённой трассировки используйте [`tracing`](https://docs.rs/tracing) и экспорт через OpenTelemetry ([#268](https://github.com/ydb-platform/ydb-rs-sdk/issues/268)).

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}