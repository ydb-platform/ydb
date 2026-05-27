# Трассировка с OpenTelemetry

{{ ydb-short-name }} SDK инструментируют операции Query Service спанами [OpenTelemetry](https://opentelemetry.io/), обеспечивая распределённую трассировку от кода приложения до каждого gRPC-вызова к YDB. Спаны экспортируются по стандартному протоколу OTLP и совместимы с Jaeger, Grafana Tempo, Zipkin и любым другим бэкендом, поддерживающим OpenTelemetry.

## Создаваемые спаны {#spans}

В настоящее время спаны поддерживаются для операций Query Service. Поддержка топиков и других сервисов планируется в будущих версиях SDK.

Создаются следующие спаны:

| Спан | Тип | Описание |
|---|---|---|
| `ydb.RunWithRetry` | `Internal` | Охватывает весь цикл повторных попыток для одной операции |
| `ydb.Try` | `Internal` | Один спан на каждую попытку, включая первую; дочерние RPC-спаны прикрепляются к нему |
| `ydb.CreateSession` | `Client` | Создание сессии через gRPC `CreateSession` и `AttachStream` |
| `ydb.ExecuteQuery` | `Client` | Выполнение одного YQL-запроса |
| `ydb.BeginTransaction` | `Client` | Явный вызов начала транзакции |
| `ydb.Commit` | `Client` | Фиксация транзакции |
| `ydb.Rollback` | `Client` | Откат транзакции |
| `ydb.Driver.Initialize` | `Internal` | Первичная инициализация драйвера: обнаружение кластера и аутентификация |

Типичное дерево спанов для транзакционной операции с повторной попыткой выглядит следующим образом:

```text
ydb.RunWithRetry  (Internal)
├─ ydb.Try        (Internal)   ← 1-я попытка: ERROR
│  ├─ ydb.ExecuteQuery (Client)
│  ├─ ydb.ExecuteQuery (Client)
│  └─ ydb.Commit       (Client) ← ERROR: Transaction Lock Invalidated
└─ ydb.Try        (Internal)   ← 2-я попытка: SUCCESS, ydb.retry.backoff_ms=50
   ├─ ydb.ExecuteQuery (Client)
   ├─ ydb.ExecuteQuery (Client)
   └─ ydb.Commit       (Client)
```

`ydb.RunWithRetry` является родительским спаном для всей операции с ретраями. На каждую попытку выполнения создаётся отдельный дочерний спан `ydb.Try`: первый `ydb.Try` соответствует первой попытке, второй — первой **повторной** попытке и так далее. RPC-спаны конкретной попытки, например `ydb.ExecuteQuery` и `ydb.Commit`, создаются внутри соответствующего `ydb.Try`.

{% note info %}

Если попытка завершилась ошибкой, её спан `ydb.Try` завершается со статусом ошибки. При повторной попытке создаётся новый `ydb.Try`; начиная со второй попытки на нём указывается атрибут `ydb.retry.backoff_ms` — время ожидания перед этой попыткой в миллисекундах. Это ожидание входит в длительность следующего спана `ydb.Try`: спан начинается перед backoff-паузой, затем после паузы выполняются RPC-вызовы этой попытки.

{% endnote %}

## Атрибуты спанов {#attributes}

SDK использует как стандартные атрибуты OpenTelemetry semantic conventions, так и YDB-специфичные расширения.

### Стандартные атрибуты OpenTelemetry

Следующие атрибуты относятся к стабильным [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/) и могут обрабатываться бэкендами трассировки как стандартные:

| Атрибут | Где устанавливается | Описание                                                                                  |
|---|---|-------------------------------------------------------------------------------------------|
| `db.system.name` | RPC-спаны | Всегда `"ydb"`                                                                            |
| `db.namespace` | RPC-спаны | Путь к базе данных YDB                                                                    |
| `server.address` | RPC-спаны | Основной хост из строки подключения                                                       |
| `server.port` | RPC-спаны | Основной порт из строки подключения                                                       |
| `network.peer.address` | RPC-спаны | Фактический gRPC-эндпоинт, использованный для вызова                                      |
| `network.peer.port` | RPC-спаны | Фактический порт gRPC-эндпоинта, использованный для вызова                                |
| `error.type` | Спаны, завершившиеся ошибкой | Тип ошибки. Например: `"transport_error"`, `"ydb_error"` или полное имя класса исключения |
| `db.response.status_code` | RPC-спаны при `YdbException` | Текстовое название статуса YDB из ошибки, например `ABORTED`, `UNAVAILABLE`, `OVERLOADED` |


### YDB-специфичные атрибуты

Следующие атрибуты являются расширениями YDB поверх стандартных semantic conventions:

| Атрибут | Где устанавливается | Описание |
|---|---|---|
| `ydb.node.id` | RPC-спаны | Идентификатор узла YDB, обработавшего запрос |
| `ydb.node.dc` | RPC-спаны | Датацентр узла YDB, обработавшего запрос |
| `ydb.retry.backoff_ms` | Спаны `ydb.Try`, начиная со второй попытки | Время ожидания перед повторной попыткой в миллисекундах |

## Контекст трассировки W3C {#w3c}

SDK автоматически прокидывает заголовок W3C `traceparent` в каждый исходящий gRPC-вызов. Это позволяет серверу YDB трассировать внутренние операции в рамках той же трассы — без дополнительной настройки. Подробнее о серверной трассировке — в разделе [Передача внешнего trace-id в {{ ydb-short-name }}](../../reference/observability/tracing/external-traces.md).

## Подключение к SDK {#integration}

{% list tabs %}

- Go

  Установите адаптер OpenTelemetry для {{ ydb-short-name }} Go SDK:

  ```bash
  go get github.com/ydb-platform/ydb-go-sdk-otel
  ```

  Настройте `TracerProvider` и передайте адаптер в `ydb.Open`:

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

  Установите дополнительные зависимости `opentelemetry` и экспортёр OTLP:

  ```bash
  pip install ydb[opentelemetry]
  pip install opentelemetry-exporter-otlp-proto-grpc
  ```

  Вызовите `enable_tracing()` после настройки глобального `TracerProvider`:

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

  Добавьте NuGet-пакет:

  ```bash
  dotnet add package Ydb.Sdk.OpenTelemetry
  ```

  Зарегистрируйте инструментацию {{ ydb-short-name }} при настройке OpenTelemetry в вашем сервисе:

  ```csharp
  services.AddOpenTelemetry()
      .WithTracing(builder => builder
          .AddYdb()
          .AddOtlpExporter());
  ```

- Java

  Добавьте зависимости YDB SDK и OpenTelemetry (пример для Maven):

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

  Создайте экземпляр OpenTelemetry SDK и передайте его в транспорт через `OpenTelemetryTracer`:

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
      // Используйте queryClient здесь
  }
  ```

  При использовании JDBC-драйвера достаточно добавить параметр `enableOpenTelemetryTracer=true` в строку подключения — драйвер подхватит глобальный OTel-провайдер автоматически:

  ```text
  jdbc:ydb://<host>:<port>/<database>?enableOpenTelemetryTracer=true
  ```

- C++

  Подключите заголовок трассировки OpenTelemetry из {{ ydb-short-name }} C++ SDK и добавьте зависимость на OTel C++ SDK:

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

  // 1. Инициализируем провайдер трассировки OTel
  otlp::OtlpHttpExporterOptions opts;
  opts.url = "http://localhost:4318/v1/traces";
  auto exporter  = otlp::OtlpHttpExporterFactory::Create(opts);
  auto processor = sdktrace::SimpleSpanProcessorFactory::Create(std::move(exporter));
  auto res       = resource::Resource::Create({{"service.name", "my-service"}});
  auto otelProvider = std::make_shared<sdktrace::TracerProvider>(
      std::move(processor), res);
  opentelemetry::trace::Provider::SetTracerProvider(otelProvider);

  // 2. Оборачиваем в провайдер трассировки YDB
  auto ydbTraceProvider = NTrace::CreateOtelTraceProvider(otelProvider);

  // 3. Создаём драйвер YDB с включённой трассировкой
  auto driverConfig = TDriverConfig()
      .SetEndpoint("localhost:2136")
      .SetDatabase("/local")
      .SetTraceProvider(ydbTraceProvider);

  TDriver driver(driverConfig);
  ```

- JavaScript

  Установите `@ydbjs/telemetry` вместе с OpenTelemetry Node SDK и OTLP-экспортёром:

  ```bash
  npm install @ydbjs/telemetry @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
  ```

  Инициализируйте `NodeSDK` до создания драйвера и вызовите `register()` из `@ydbjs/telemetry`:

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

  // Должно быть вызвано ДО создания Driver — middleware пропагации W3C
  // trace context устанавливается один раз при construction'е драйвера.
  const instrumentation = register()

  using driver = new Driver(process.env.YDB_CONNECTION_STRING)
  await driver.ready()
  await using sql = query(driver)
  // ...

  instrumentation.disable()
  await sdk.shutdown()
  ```

  Альтернативно, через `--import` для автозагрузки до старта приложения:

  ```bash
  node --import @opentelemetry/sdk-node/register --import @ydbjs/telemetry/register your-app.js
  ```

{% endlist %}
