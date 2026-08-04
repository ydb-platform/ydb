# Metrics with OpenTelemetry

{{ ydb-short-name }} SDK instruments Query Service operations with [OpenTelemetry](https://opentelemetry.io/) metrics, allowing you to observe the client state — operation duration and count, session pool state — from application code to gRPC calls to YDB. Metrics are exported via the standard OTLP protocol and are compatible with Prometheus, Grafana, VictoriaMetrics, and any other backend that supports OpenTelemetry.

## List of metrics {#metrics-list}

### Operation metrics {#operation-metrics}

| Name | Type | Unit | Description |
| --- | --- | --- | --- |
| `ydb.client.operation.duration` | Histogram | `s` | Duration of a single client operation attempt (`ExecuteQuery`, `Commit`, `Rollback`, `CreateSession`). |
| `ydb.client.operation.failed` | Counter | `{operation}` | Number of unsuccessful client operations. |

### Session pool metrics {#session-pool-metrics}

| Name | Type | Unit | Description |
| --- | --- | --- | --- |
| `ydb.query.session.create_time` | Histogram | `s` | Duration of creating a new session. |
| `ydb.query.session.pending_requests` | Counter | `{request}` | Monotonic counter of requests for session acquisition that entered the wait queue since the pool was created. |
| `ydb.query.session.timeouts` | Counter | `{timeout}` | Monotonic counter of timeouts when waiting for a free session, since the pool was created. |
| `ydb.query.session.count` | Gauge | `{session}` | Current number of sessions in the pool, broken down by state. |
| `ydb.query.session.min` | Gauge | `{session}` | Configured minimum session pool size. |
| `ydb.query.session.max` | Gauge | `{session}` | Configured maximum size of the session pool. |

### Retry metrics {#retry-metrics}

| Name | Type | Unit | Description |
| --- | --- | --- | --- |
| `ydb.client.retry.duration` | Histogram | `s` | Total client-visible duration of a logical operation performed through a retry policy, including all attempts and backoff delays. |
| `ydb.client.retry.attempts` | Histogram | `{attempt}` | Distribution of the number of attempts per logical operation. The value `1` means success on the first attempt. |

## Attributes {#attributes}

| Name | Applies to | Value |
| --- | --- | --- |
| `database` | `ydb.client.operation.duration`, `ydb.client.operation.failed` | Database name {{ ydb-short-name }}. |
| `endpoint` | `ydb.client.operation.duration`, `ydb.client.operation.failed` | Discovery-endpoint in the format `host:port`. |
| `operation.name` | `ydb.client.operation.duration`, `ydb.client.operation.failed`, `ydb.client.retry.duration`, `ydb.client.retry.attempts` | Client operation name: `ExecuteQuery`, `Commit`, `Rollback`, `CreateSession`. |
| `status_code` | `ydb.client.operation.failed` | Status code {{ ydb-short-name }} (for example, `BAD_REQUEST`, `SCHEME_ERROR`). |
| `ydb.query.session.pool.name` | All `ydb.query.session.*` metrics | Session pool name. By default, it is formed as `<endpoint>/<database>` and configured through the API of a specific SDK. |
| `ydb.query.session.state` | `ydb.query.session.count` | Session state: `idle` or `used`. |

## Connecting to the SDK {#integration}

{% list tabs %}

- C++

  Include the OpenTelemetry metrics header from {{ ydb-short-name }} C++ SDK and register `MetricRegistry` in `TDriverConfig`:


  ```cpp
  #include <ydb-cpp-sdk/client/driver/driver.h>
  #include <ydb-cpp-sdk/open_telemetry/metrics.h>

  #include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>
  #include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h>
  #include <opentelemetry/sdk/metrics/meter_provider.h>
  #include <opentelemetry/sdk/metrics/view/view_registry.h>
  #include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
  #include <opentelemetry/sdk/resource/resource.h>
  #include <opentelemetry/metrics/provider.h>

  namespace sdkmetrics = opentelemetry::sdk::metrics;
  namespace otlp      = opentelemetry::exporter::otlp;
  namespace resource  = opentelemetry::sdk::resource;
  using namespace NYdb;

  // 1. Initialize the OTel metrics provider
  otlp::OtlpHttpMetricExporterOptions opts;
  opts.url = "http://localhost:4318/v1/metrics";

  auto exporter = otlp::OtlpHttpMetricExporterFactory::Create(opts);

  sdkmetrics::PeriodicExportingMetricReaderOptions readerOpts;
  readerOpts.export_interval_millis = std::chrono::milliseconds(5000);
  auto reader = sdkmetrics::PeriodicExportingMetricReaderFactory::Create(
      std::move(exporter), readerOpts);

  auto res = resource::Resource::Create({{"service.name", "my-service"}});
  auto rawProvider = std::make_shared<sdkmetrics::MeterProvider>(
      std::unique_ptr<sdkmetrics::ViewRegistry>(new sdkmetrics::ViewRegistry()), res);
  rawProvider->AddMetricReader(std::move(reader));

  std::shared_ptr<opentelemetry::metrics::MeterProvider> meterProvider = rawProvider;
  opentelemetry::metrics::Provider::SetMeterProvider(meterProvider);

  // 2. Wrap in the YDB metrics registry
  auto ydbMetricRegistry = NMetrics::CreateOtelMetricRegistry(meterProvider);

  // 3. Create a YDB driver with metrics enabled
  auto driverConfig = TDriverConfig()
      .SetEndpoint("localhost:2136")
      .SetDatabase("/local")
      .SetMetricRegistry(ydbMetricRegistry);

  TDriver driver(driverConfig);
  ```


  Metrics and tracing can be enabled together by registering both `MetricRegistry` and `TraceProvider` in `TDriverConfig`.

- Go

  Install the OpenTelemetry adapter for the {{ ydb-short-name }} Go SDK:


  ```bash
  go get github.com/ydb-platform/ydb-go-sdk-otel
  ```


  Configure `MeterProvider`, get `Meter` from it, and pass it to the `ydbOtel.WithMetrics` adapter, which connects to the driver via the `ydb.Open` option. The granularity of collected metrics can be configured via `ydbOtel.WithDetailer`:


  ```go
  package main

  import (
      "context"
      "os"

      "go.opentelemetry.io/otel"
      "go.opentelemetry.io/otel/attribute"
      "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
      "go.opentelemetry.io/otel/sdk/metric"
      "go.opentelemetry.io/otel/sdk/resource"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/trace"
      ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
  )

  func main() {
      ctx := context.Background()

      exporter, err := otlpmetricgrpc.New(ctx,
          otlpmetricgrpc.WithEndpoint("localhost:4317"),
          otlpmetricgrpc.WithInsecure(),
      )
      if err != nil {
          panic(err)
      }
      res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(
          attribute.String("service.name", "my-service"),
      ))
      mp := metric.NewMeterProvider(
          metric.WithReader(metric.NewPeriodicReader(exporter)),
          metric.WithResource(res),
      )
      defer mp.Shutdown(ctx)
      otel.SetMeterProvider(mp)

      db, err := ydb.Open(ctx,
          os.Getenv("YDB_CONNECTION_STRING"),
          ydbOtel.WithMetrics(
              mp.Meter("ydb-go-sdk"),
              ydbOtel.WithDetailer(trace.DetailsAll),
          ),
      )
      if err != nil {
          panic(err)
      }
      defer db.Close(ctx)
  }
  ```

- Java

  {% note info %}

  In the Java SDK, only [session pool metrics](#session-pool-metrics) are currently supported.

  {% endnote %}

  Pass your `OpenTelemetry` to the SDK via the `OpenTelemetryMeter` adapter and the `QueryClient.Builder#withMeter` method:


  ```java
  import io.opentelemetry.api.OpenTelemetry;
  import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
  import io.opentelemetry.sdk.OpenTelemetrySdk;
  import io.opentelemetry.sdk.metrics.SdkMeterProvider;
  import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;

  import tech.ydb.core.grpc.GrpcTransport;
  import tech.ydb.core.metrics.OpenTelemetryMeter;
  import tech.ydb.query.QueryClient;

  SdkMeterProvider meterProvider = SdkMeterProvider.builder()
      .registerMetricReader(PeriodicMetricReader
          .builder(OtlpGrpcMetricExporter.getDefault())
          .build())
      .build();

  OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
      .setMeterProvider(meterProvider)
      .build();

  try (GrpcTransport transport = GrpcTransport
          .forConnectionString(System.getenv("YDB_CONNECTION_STRING"))
          .build();
       QueryClient queryClient = QueryClient.newClient(transport)
          .withMeter(OpenTelemetryMeter.fromOpenTelemetry(openTelemetry))
          .sessionPoolName("my-app")
          .build()) {
      // Use queryClient
  }
  meterProvider.close();
  ```


  If you have already configured a global `GlobalOpenTelemetry`, you can use `OpenTelemetryMeter.createGlobal()`. `TableClient` also supports `withMeter(...)`.

- Python

  {% include [feature-not-supported](../../../../_includes/feature-not-supported.md) %}

- C#

  Add the NuGet package:


  ```bash
  dotnet add package Ydb.Sdk.OpenTelemetry
  ```


  Register the {{ ydb-short-name }} instrumentation in the OpenTelemetry metrics pipeline:


  ```csharp
  services.AddOpenTelemetry()
      .WithMetrics(builder => builder
          .AddYdb()
          .AddOtlpExporter());
  ```


  Or using a standalone `MeterProvider`:


  ```csharp
  using var meterProvider = Sdk.CreateMeterProviderBuilder()
      .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("my-service"))
      .AddYdb()
      .AddOtlpExporter()
      .Build();
  ```


  The session pool name is set by the `PoolName=` parameter in the `YdbDataSource` connection string. A full example with load and Grafana is in the SDK repository ( [Ydb.Sdk.AdoNet.OpenTelemetry/Metrics](https://github.com/ydb-platform/ydb-dotnet-sdk/blob/main/examples/Ydb.Sdk.AdoNet.OpenTelemetry/Metrics/Program.cs)).

- JavaScript

  Install the {{ ydb-short-name }} JavaScript SDK telemetry package and OpenTelemetry SDK:


  ```bash
  npm install @ydbjs/telemetry @opentelemetry/sdk-node @opentelemetry/exporter-metrics-otlp-http
  ```


  Initialize `NodeSDK` before creating `Driver` and register the instrumentation `@ydbjs/telemetry`:


  ```ts
  import { NodeSDK } from '@opentelemetry/sdk-node'
  import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http'
  import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'
  import { Driver } from '@ydbjs/core'
  import { query } from '@ydbjs/query'
  import { register } from '@ydbjs/telemetry'

  const sdk = new NodeSDK({
      serviceName: 'my-service',
      metricReader: new PeriodicExportingMetricReader({
          exporter: new OTLPMetricExporter({
              url: 'http://localhost:4318/v1/metrics',
          }),
      }),
  })
  sdk.start()

  // Must be called BEFORE creating the Driver: the package subscribes to
  // diagnostics_channel SDK events and converts them to OTel metrics.
  const instrumentation = register()

  const driver = new Driver(process.env.YDB_CONNECTION_STRING)
  await driver.ready()

  const sql = query(driver)
  await sql`SELECT 1`

  instrumentation.disable()
  await driver.close()
  await sdk.shutdown()
  ```


  Package `@ydbjs/telemetry` subscribes to events `node:diagnostics_channel` from `@ydbjs/core`, `@ydbjs/query`, `@ydbjs/auth`, and `@ydbjs/retry` and publishes metrics from [the list above](#metrics-list). Operation duration is exported as `db.client.operation.duration` (OpenTelemetry semantic conventions), not `ydb.client.operation.duration`. The current number of requests waiting for a session is gauge `ydb.query.session.acquire.pending`, not counter `ydb.query.session.pending_requests`. For more details, see the [JavaScript SDK repository](https://github.com/ydb-platform/ydb-js-sdk).

  In addition, the JavaScript SDK publishes its own retry metrics with names and semantics different from `ydb.client.retry.*`:

  | Name | Type | Unit | Description |
  | --- | --- | --- | --- |
  | `ydb.retry.attempts` | Counter | `{attempt}` | Monotonic counter of retry attempts with outcome tag `ydb.retry.outcome` (e.g., `success`, `retried`). Unlike the `ydb.client.retry.attempts` histogram, it does not show the distribution of the number of attempts per query. |
  | `ydb.retry.duration` | Histogram | `s` | Total duration of the retry cycle, including backoff (analogous to `ydb.client.retry.duration`). |

- Rust

  {% include [feature-not-supported](../../../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#268](https://github.com/ydb-platform/ydb-rs-sdk/issues/268)

- PHP

  {% include [feature-not-supported](../../../../_includes/feature-not-supported.md) %}

{% endlist %}
