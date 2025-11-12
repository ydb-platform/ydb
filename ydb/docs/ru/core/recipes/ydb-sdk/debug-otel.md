# Включение трассировки в OpenTelemetry

Ниже приведены примеры кода включения трассировки в OpenTelemetry в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

   ```go
   package main

   import (
       "context"

       "github.com/ydb-platform/ydb-go-sdk/v3"
       "github.com/ydb-platform/ydb-go-sdk/v3/trace"

       ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
       ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
       otelTrace "go.opentelemetry.io/otel/sdk/trace"
   )

   func main() {
       ...
       tracerProvider := otelTrace.NewTracerProvider(
           // WithBatcher registers the exporter with the TracerProvider.
           // The exporter must implement the SpanExporter interface.
           // You can use any OpenTelemetry exporter (Jaeger, Zipkin, etc).
           otelTrace.WithBatcher(exp),
           // WithResource attaches metadata (like service name and environment) to all spans
           // created by this TracerProvider. Use resource.NewWithAttributes with standard
           // semantic keys such as semconv.ServiceNameKey.
           otelTrace.WithResource(resource.NewWithAttributes(res)),
       )   
       defer tracerProvider.Shutdown(ctx)

       // Set global tracer of this application.
       otel.SetTracerProvider(tracerProvider)

       // Create a root span.
       ctx, span := tracerProvider.Tracer("ydb-go-sdk-example").Start(context.Background(), "client")
       defer span.End()
       
       // If you want to see otel-trace-id in the logs, 
       // it’s important to connect the adapters in a specific order — first otel, then logger.
       db, err := ydb.Open(ctx,
           os.Getenv("YDB_CONNECTION_STRING"),
           ydbOtel.WithTraces(ydbOtel.WithDetails(trace.DetailsAll)),
           ydbZap.WithTraces(logger, trace.DetailsAll),
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

       "github.com/ydb-platform/ydb-go-sdk/v3"
       "github.com/ydb-platform/ydb-go-sdk/v3/trace"

       ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
       ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
       otelTrace "go.opentelemetry.io/otel/sdk/trace"
   )

   func main() {
       ...
       tracerProvider := otelTrace.NewTracerProvider(
           // WithBatcher registers the exporter with the TracerProvider.
           // The exporter must implement the SpanExporter interface.
           // You can use any OpenTelemetry exporter (Jaeger, Zipkin, etc).
           otelTrace.WithBatcher(exp),
           // WithResource attaches metadata (like service name and environment) to all spans
           // created by this TracerProvider. Use resource.NewWithAttributes with standard
           // semantic keys such as semconv.ServiceNameKey.
           otelTrace.WithResource(resource.NewWithAttributes(res)),
       )   
       defer tracerProvider.Shutdown(ctx)

       // Set global tracer of this application.
       otel.SetTracerProvider(tracerProvider)

       // Create a root span.
       ctx, span := traceProvider.Tracer("ydb-go-sdk-example").Start(context.Background(), "client")
       defer span.End()   

       // If you want to see otel-trace-id in the logs, 
       // it’s important to connect the adapters in a specific order — first otel, then logger.
       nativeDriver, err := ydb.Open(ctx,
           os.Getenv("YDB_CONNECTION_STRING"),
           ydbOtel.WithTraces(ydbOtel.WithDetails(trace.DetailsAll)),
           ydbZap.WithTraces(logger, trace.DetailsAll),
       )
       if err != nil {
           panic(err)
       }
       defer nativeDriver.Close(ctx)   
       
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