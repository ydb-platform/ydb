# Tracing with OpenTelemetry

{{ ydb-short-name }} SDK instrument Query Service operations with OpenTelemetry spans, providing distributed tracing from the application code to each gRPC call to YDB. The spans are exported via the standard OTLP protocol and are compatible with Jaeger, Grafana Tempo, Zipkin, and any other backend that supports OpenTelemetry.

## Created Spans {#spans}

Currently, spans are supported for Query Service operations. Support for topics and other services is planned for future versions of the SDK.

The following spans are created:

| Span | Type | Description |
|---|---|---|
| `ydb.RunWithRetry` | `Internal` | Covers the entire cycle of retries for a single operation |
| `ydb.Try` | `Internal` | One span for each attempt, including the first one; child RPC spans are attached to it |
| `ydb.CreateSession` | `Client` | Creating a session via gRPC `CreateSession` and `AttachStream` |
| `ydb.ExecuteQuery` | `Client` | Executing a single YQL query |
| `ydb.BeginTransaction` | `Client` | Explicitly starting a transaction |
| `ydb.Commit` | `Client` | Committing a transaction |
| `ydb.Rollback` | `Client` | Rolling back a transaction |
| `ydb.Driver.Initialize` | `Internal` | Initial driver initialization: cluster discovery and authentication |

A typical span tree for a transactional operation with a retry attempt looks like this:

```text
ydb.RunWithRetry  (Internal)
├─ ydb.Try        (Internal)   ← 1st attempt: ERROR
│  ├─ ydb.ExecuteQuery (Client)
│  ├─ ydb.ExecuteQuery (Client)
│  └─ ydb.Commit       (Client) ← ERROR: Transaction Lock Invalidated
└─ ydb.Try        (Internal)   ← 2nd attempt: SUCCESS, ydb.retry.backoff_ms=50
   ├─ ydb.ExecuteQuery (Client)
   ├─ ydb.ExecuteQuery (Client)
   └─ ydb.Commit       (Client)
```

`ydb.RunWithRetry` is the parent span for the entire operation with retries. A separate child span `ydb.Try` is created for each attempt: the first `ydb.Try` corresponds to the first attempt, the second — to the first **retry** attempt, and so on. RPC spans for a specific attempt, such as `ydb.ExecuteQuery` and `ydb.Commit`, are created within the corresponding `ydb.Try`.

{% note info %}

If an attempt ends with an error, its `ydb.Try` span ends with an error status. A new `ydb.Try` is created for a retry attempt; starting from the second attempt, it includes the `ydb.retry.backoff_ms` attribute — the wait time before this attempt in milliseconds. This wait is included in the duration of the next `ydb.Try` span: the span starts before the backoff pause, and then RPC calls for this attempt are executed after the pause.

{% endnote %}

## Span Attributes {#attributes}

The SDK uses both standard OpenTelemetry semantic conventions and YDB-specific extensions.

### Standard OpenTelemetry Attributes

The following attributes belong to the stable [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/) and can be processed by tracing backends as standard ones:

| Attribute | Where set | Description |
|---|---|---|
| `db.system.name` | RPC spans | Always `"ydb"` |
| `db.namespace` | RPC spans | Path to the YDB database |
| `server.address` | RPC spans | Main host from the connection string |
| `server.port` | RPC spans | Main port from the connection string |
| `network.peer.address` | RPC spans | Actual gRPC endpoint used for the call |
| `network.peer.port` | RPC spans | Actual port of the gRPC endpoint used for the call |
| `error.type` | Spans that ended with an error | Error type. For example: `"transport_error"`, `"ydb_error"`, or the full name of the exception class |
| `db.response.status_code` | RPC spans with `YdbException` | Textual name of the YDB status from the error, for example `ABORTED`, `UNAVAILABLE`, `OVERLOADED` |

### YDB-Specific Attributes

The following attributes are YDB extensions on top of the standard semantic conventions:

| Attribute | Where set | Description |
|---|---|---|
| `ydb.node.id` | RPC spans | ID of the YDB node that processed the request |
| `ydb.node.dc` | RPC spans | Data center of the YDB node that processed the request |
| `ydb.retry.backoff_ms` | `ydb.Try` spans starting from the second attempt | Wait time before the retry attempt in milliseconds |

## W3C trace context {#w3c}

The SDK automatically passes the W3C `traceparent` header in each outgoing gRPC call. This allows the YDB server to trace internal operations within the same trace without additional configuration. More about server-side tracing is in the section [Passing an external trace-id to {{ ydb-short-name }}](../../reference/observability/tracing/external-traces.md).

## Connecting to the SDK {#integration}

{% list tabs %}

- Go

  Install the OpenTelemetry adapter for the {{ ydb-short-name }} Go SDK:

  ```bash
  go get github.com/ydb-platform/ydb-go-sdk-otel
  ```

  Configure `TracerProvider` and pass the adapter to `ydb.Open`:

  ```go
  package main

  import (
      "context"
      "os"

      "go.opentelemetry.io/otel"
      "go.opentelemetry.io/otel/attribute"
      "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
      "go.opentelemetry.io/otel/sdk/resource"
      sdktrace "go.opentelemetry.io/otel/sdk/trace"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
  )

  func main() {
      ctx := context.Background()

      exporter, err := otlptracegrpc.New(ctx,
          otlptracegrpc.WithEndpoint("localhost:4317"),
          otlptracegrpc.WithInsecure(),
      )
      if err != nil {
          panic(err)
      }
      res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(
          attribute.String("service.name", "my-service"),
      ))
      tp := sdktrace.NewTracerProvider(
          sdktrace.WithBatcher(exporter),
          sdktrace.WithResource(res),
      )
      defer tp.Shutdown(ctx)
      otel.SetTracerProvider(tp)

      db, err := ydb.Open(ctx,
          os.Getenv("YDB_CONNECTION_STRING"),
          ydbOtel.WithTraces(
              ydbOtel.WithTracer(tp.Tracer("ydb-go-sdk")),
          ),
      )
      if err != nil {
          panic(err)
      }
      defer db.Close(ctx)
  }
  ```

- Python

  Install additional dependencies `opentelemetry` and the OTLP exporter:

  ```bash
  pip install ydb[opentelemetry]
  pip install opentelemetry-exporter-otlp-proto-grpc
  ```

  Call `enable_tracing()` after setting up the global `TracerProvider`:

  ```python
  from opentelemetry import trace
  from opentelemetry.sdk.trace import TracerProvider
  from opentelemetry.sdk.trace.export import BatchSpanProcessor
  from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
  from opentelemetry.sdk.resources import Resource

  import ydb
  from ydb.opentelemetry import enable_tracing

  resource = Resource(attributes={"service.name": "my-service"})
  provider = TracerProvider(resource=resource)
  provider.add_span_processor(
      BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
  )
  trace.set_tracer_provider(provider)

  enable_tracing()

  with ydb.Driver(endpoint="grpc://localhost:2136", database="/local") as driver:
      driver.wait(timeout=5)
      with ydb.QuerySessionPool(driver) as pool:
          pool.execute_with_retries("SELECT 1")

  provider.shutdown()
  ```

- C#

  Add the NuGet package:

  ```bash
  dotnet add package Ydb.Sdk.OpenTelemetry
  ```

  Register {{ ydb-short-name }} instrumentation when setting up OpenTelemetry in your service:

  ```csharp
  services.AddOpenTelemetry()
      .WithTracing(builder => builder
          .AddYdb()
          .AddOtlpExporter());
  ```

- Java

  Add YDB SDK and OpenTelemetry dependencies (example for Maven):

  ```xml
  <dependency>
      <groupId>tech.ydb</groupId>
      <artifactId>ydb-sdk-core</artifactId>
      <version>${ydb.sdk.version}</version>
  </dependency>
  <dependency>
      <groupId>io.opentelemetry</groupId>
      <artifactId>opentelemetry-sdk</artifactId>
      <version>${otel.version}</version>
  </dependency>
  <dependency>
      <groupId>io.opentelemetry</groupId>
      <artifactId>opentelemetry-exporter-otlp</artifactId>
      <version>${otel.version}</version>
  </dependency>
  ```

  Create an instance of the OpenTelemetry SDK and pass it to the transport via `OpenTelemetryTracer`:

  ```java
  import io.opentelemetry.api.OpenTelemetry;
  import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
  import io.opentelemetry.sdk.OpenTelemetrySdk;
  import io.opentelemetry.sdk.resources.Resource;
  import io.opentelemetry.sdk.trace.SdkTracerProvider;
  import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
  import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
  import tech.ydb.core.auth.CloudAuthHelper;
  import tech.ydb.core.grpc.GrpcTransport;
  import tech.ydb.core.opentelemetry.OpenTelemetryTracer;
  import tech.ydb.query.QueryClient;

  Resource resource = Resource.getDefault().toBuilder()
      .put(ResourceAttributes.SERVICE_NAME, "my-service")
      .build();

  SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
      .setResource(resource)
      .addSpanProcessor(BatchSpanProcessor.builder(
          OtlpGrpcSpanExporter.builder()
              .setEndpoint("http://localhost:4317")
              .build()
      ).build())
      .build();

  OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
      .setTracerProvider(tracerProvider)
      .build();

  try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
          .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
          .withTracer(OpenTelemetryTracer.fromOpenTelemetry(openTelemetry))
          .build();
       QueryClient queryClient = QueryClient.newClient(transport).build()) {
      // Use queryClient here
  }
  ```

  When using the JDBC driver, it is enough to add the `enableOpenTelemetryTracer=true` parameter to the connection string — the driver will automatically pick up the global OTel provider:

  ```text
  jdbc:ydb://<host>:<port>/<database>?enableOpenTelemetryTracer=true
  ```

- C++

  Include the OpenTelemetry tracing header from the {{ ydb-short-name }} C++ SDK and add a dependency on the OTel C++ SDK:

  ```cpp
  #include <ydb-cpp-sdk/client/driver/driver.h>
  #include <ydb-cpp-sdk/open_telemetry/trace.h>

  #include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
  #include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
  #include <opentelemetry/sdk/trace/tracer_provider.h>
  #include <opentelemetry/sdk/trace/simple_processor_factory.h>
  #include <opentelemetry/sdk/resource/resource.h>
  #include <opentelemetry/trace/provider.h>

  namespace sdktrace = opentelemetry::sdk::trace;
  namespace otlp     = opentelemetry::exporter::otlp;
  namespace resource = opentelemetry::sdk::resource;
  using namespace NYdb;

  // 1. Initialize the OTel tracing provider
  otlp::OtlpHttpExporterOptions opts;
  opts.url = "http://localhost:4318/v1/traces";
  auto exporter  = otlp::OtlpHttpExporterFactory::Create(opts);
  auto processor = sdktrace::SimpleSpanProcessorFactory::Create(std::move(exporter));
  auto res       = resource::Resource::Create({{"service.name", "my-service"}});
  auto otelProvider = std::make_shared<sdktrace::TracerProvider>(
      std::move(processor), res);
  opentelemetry::trace::Provider::SetTracerProvider(otelProvider);

  // 2. Wrap in the YDB tracing provider
  auto ydbTraceProvider = NTrace::CreateOtelTraceProvider(otelProvider);

  // 3. Create a YDB driver with tracing enabled
  auto driverConfig = TDriverConfig()
      .SetEndpoint("localhost:2136")
      .SetDatabase("/local")
      .SetTraceProvider(ydbTraceProvider);

  TDriver driver(driverConfig);
  ```

- JavaScript

  Install `@ydbjs/telemetry` along with the OpenTelemetry Node SDK and the OTLP exporter:

  ```bash
  npm install @ydbjs/telemetry @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
  ```

  Initialize `NodeSDK` before creating the driver and call `register()` from `@ydbjs/telemetry`:

  ```js
  import { NodeSDK } from '@opentelemetry/sdk-node'
  import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http'
  import { Driver } from '@ydbjs/core'
  import { query } from '@ydbjs/query'
  import { register } from '@ydbjs/telemetry'

  const sdk = new NodeSDK({
      serviceName: 'my-service',
      traceExporter: new OTLPTraceExporter({ url: 'http://localhost:4318/v1/traces' }),
  })
  sdk.start()

  // This must be called BEFORE creating the Driver — the W3C trace context propagation middleware is set up once during driver construction.
  const instrumentation = register()

  using driver = new Driver(process.env.YDB_CONNECTION_STRING)
  await driver.ready()
  await using sql = query(driver)
  // ...

  instrumentation.disable()
  await sdk.shutdown()
  ```

  Alternatively, using `--import` for autoloading before the application starts:

  ```bash
  node --import @opentelemetry/sdk-node/register --import @ydbjs/telemetry/register your-app.js
  ```

{% endlist %}