# Метрики с OpenTelemetry

{{ ydb-short-name }} SDK инструментируют операции Query Service метриками [OpenTelemetry](https://opentelemetry.io/), позволяя наблюдать состояние клиента — длительность и количество операций, состояние пула сессий — от кода приложения до gRPC-вызовов к YDB. Метрики экспортируются по стандартному протоколу OTLP и совместимы с Prometheus, Grafana, VictoriaMetrics и любым другим бэкендом, поддерживающим OpenTelemetry.

## Список метрик {#metrics-list}

### Метрики операций {#operation-metrics}

| Имя                             | Тип       | Единица       | Описание                                                                                                |
|---------------------------------|-----------|---------------|---------------------------------------------------------------------------------------------------------|
| `ydb.client.operation.duration` | Histogram | `s`           | Длительность одной попытки клиентской операции (`ExecuteQuery`, `Commit`, `Rollback`, `CreateSession`). |
| `ydb.client.operation.failed`   | Counter   | `{operation}` | Количество неуспешных клиентских операций.                                                              |

### Метрики пула сессий {#session-pool-metrics}

| Имя                                  | Тип         | Единица     | Описание                                                              |
|--------------------------------------|-------------|-------------|-----------------------------------------------------------------------|
| `ydb.query.session.create_time`      | Histogram   | `s`         | Длительность создания новой сессии.                                   |
| `ydb.query.session.pending_requests` | Counter     | `{request}` | Монотонный счётчик запросов на получение сессии, попавших в очередь ожидания, с момента создания пула. |
| `ydb.query.session.timeouts`         | Counter     | `{timeout}` | Монотонный счётчик таймаутов при ожидании свободной сессии, с момента создания пула.                   |
| `ydb.query.session.count`            | Gauge       | `{session}` | Текущее количество сессий в пуле, разделённое по состояниям.          |
| `ydb.query.session.min`              | Gauge       | `{session}` | Настроенный минимальный размер пула сессий.                           |
| `ydb.query.session.max`              | Gauge       | `{session}` | Настроенный максимальный размер пула сессий.                          |

### Метрики повторных попыток {#retry-metrics}

| Имя                          | Тип       | Единица     | Описание                                                                                                                            |
|------------------------------|-----------|-------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `ydb.client.retry.duration`  | Histogram | `s`         | Полная видимая клиенту длительность логической операции, выполненной через политику повторов, включая все попытки и задержки backoff. |
| `ydb.client.retry.attempts`  | Histogram | `{attempt}` | Распределение числа попыток на одну логическую операцию. Значение `1` означает успех с первой попытки.                               |

## Атрибуты {#attributes}

| Имя                           | Применяется к                                                  | Значение                                                                                                        |
|-------------------------------|----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `database`                    | `ydb.client.operation.duration`, `ydb.client.operation.failed` | Имя базы данных {{ ydb-short-name }}.                                                                           |
| `endpoint`                    | `ydb.client.operation.duration`, `ydb.client.operation.failed` | Discovery-endpoint в формате `host:port`.                                                                       |
| `operation.name`              | `ydb.client.operation.duration`, `ydb.client.operation.failed`, `ydb.client.retry.duration`, `ydb.client.retry.attempts` | Имя клиентской операции: `ExecuteQuery`, `Commit`, `Rollback`, `CreateSession`.                                 |
| `status_code`                 | `ydb.client.operation.failed`                                  | Код статуса {{ ydb-short-name }} (например, `BAD_REQUEST`, `SCHEME_ERROR`).                                     |
| `ydb.query.session.pool.name` | Все метрики `ydb.query.session.*`                              | Имя пула сессий. По умолчанию формируется как `<endpoint>/<database>`; настраивается через API конкретного SDK. |
| `ydb.query.session.state`     | `ydb.query.session.count`                                      | Состояние сессии: `idle` или `used`.                                                                            |

## Подключение к SDK {#integration}

{% list tabs %}

- Go

  Установите адаптер OpenTelemetry для {{ ydb-short-name }} Go SDK:

  ```bash
  go get github.com/ydb-platform/ydb-go-sdk-otel
  ```

  Настройте `MeterProvider`, получите из него `Meter` и передайте его в адаптер `ydbOtel.WithMetrics`, который подключается к драйверу через опцию `ydb.Open`. Детализацию собираемых метрик можно настроить через `ydbOtel.WithDetailer`:

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

- Python

  {% include [feature-not-supported](../../../../_includes/feature-not-supported.md) %}

- C#

  Добавьте NuGet-пакет:

  ```bash
  dotnet add package Ydb.Sdk.OpenTelemetry
  ```

  Зарегистрируйте инструментацию {{ ydb-short-name }} в конвейере OpenTelemetry-метрик:

  ```csharp
  services.AddOpenTelemetry()
      .WithMetrics(builder => builder
          .AddYdb()
          .AddOtlpExporter());
  ```

  Или с использованием standalone `MeterProvider`:

  ```csharp
  using var meterProvider = Sdk.CreateMeterProviderBuilder()
      .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("my-service"))
      .AddYdb()
      .AddOtlpExporter()
      .Build();
  ```

  Имя пула сессий задаётся параметром `PoolName=` в строке подключения `YdbDataSource`. Полный пример с нагрузкой и Grafana — в репозитории SDK ([Ydb.Sdk.AdoNet.OpenTelemetry/Metrics](https://github.com/ydb-platform/ydb-dotnet-sdk/blob/main/examples/Ydb.Sdk.AdoNet.OpenTelemetry/Metrics/Program.cs)).

- Java

  {% note info %}

  В Java SDK на данный момент поддерживаются только [метрики пула сессий](#session-pool-metrics).

  {% endnote %}

  Передайте свой `OpenTelemetry` в SDK через адаптер `OpenTelemetryMeter` и метод `QueryClient.Builder#withMeter`:

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
      // Используйте queryClient
  }
  meterProvider.close();
  ```

  Если уже настроен глобальный `GlobalOpenTelemetry`, можно использовать `OpenTelemetryMeter.createGlobal()`. `TableClient` также поддерживает `withMeter(...)`.

- C++

  Подключите заголовок метрик OpenTelemetry из {{ ydb-short-name }} C++ SDK и зарегистрируйте `MetricRegistry` в `TDriverConfig`:

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

  // 1. Инициализируем провайдер метрик OTel
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

  // 2. Оборачиваем в регистратор метрик YDB
  auto ydbMetricRegistry = NMetrics::CreateOtelMetricRegistry(meterProvider);

  // 3. Создаём драйвер YDB с включёнными метриками
  auto driverConfig = TDriverConfig()
      .SetEndpoint("localhost:2136")
      .SetDatabase("/local")
      .SetMetricRegistry(ydbMetricRegistry);

  TDriver driver(driverConfig);
  ```

  Метрики и трассировку можно подключить вместе, зарегистрировав в `TDriverConfig` одновременно `MetricRegistry` и `TraceProvider`.

- JavaScript

  Установите пакет телеметрии {{ ydb-short-name }} JavaScript SDK и OpenTelemetry SDK:

  ```bash
  npm install @ydbjs/telemetry @opentelemetry/sdk-node @opentelemetry/exporter-metrics-otlp-http
  ```

  Инициализируйте `NodeSDK` до создания `Driver` и зарегистрируйте инструментацию `@ydbjs/telemetry`:

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

  // Должно быть вызвано ДО создания Driver: пакет подписывается на
  // diagnostics_channel события SDK и преобразует их в OTel metrics.
  const instrumentation = register()

  const driver = new Driver(process.env.YDB_CONNECTION_STRING)
  await driver.ready()

  const sql = query(driver)
  await sql`SELECT 1`

  instrumentation.disable()
  await driver.close()
  await sdk.shutdown()
  ```

  Пакет `@ydbjs/telemetry` подписывается на события `node:diagnostics_channel` из `@ydbjs/core`, `@ydbjs/query`, `@ydbjs/auth` и `@ydbjs/retry` и публикует метрики из [списка выше](#metrics-list). Длительность операций экспортируется как `db.client.operation.duration` (семантические конвенции OpenTelemetry), а не `ydb.client.operation.duration`. Текущее число ожидающих сессию запросов — gauge `ydb.query.session.acquire.pending`, а не counter `ydb.query.session.pending_requests`. Подробнее см. [репозиторий JavaScript SDK](https://github.com/ydb-platform/ydb-js-sdk).

  Кроме того, JavaScript SDK публикует собственные метрики повторных попыток с именами и семантикой, отличными от `ydb.client.retry.*`:

  | Имя                   | Тип       | Единица     | Описание                                                                                                                                              |
  |-----------------------|-----------|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
  | `ydb.retry.attempts`  | Counter   | `{attempt}` | Монотонный счётчик попыток повторного выполнения с тегом исхода `ydb.retry.outcome` (например, `success`, `retried`). В отличие от гистограммы `ydb.client.retry.attempts`, не показывает распределение числа попыток на один запрос. |
  | `ydb.retry.duration`  | Histogram | `s`         | Суммарная длительность цикла повторных попыток, включая backoff (аналог `ydb.client.retry.duration`).                                                  |

- Rust

  {% include [feature-not-supported](../../../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../../../_includes/feature-not-supported.md) %}

{% endlist %}
