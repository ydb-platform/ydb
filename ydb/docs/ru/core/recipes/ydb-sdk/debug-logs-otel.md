# Экспорт логов в OpenTelemetry

Каждый {{ ydb-short-name }} SDK пишет свои внутренние логи (инициализация драйвера, пул сессий, выполнение запросов, повторные попытки и т.д.) через стандартное средство логирования своего языка. Вместо вывода логов только в консоль (см. [Включить логирование](debug-logs.md)) их можно перенаправить в [OpenTelemetry](https://opentelemetry.io/) Logs SDK и экспортировать по стандартному протоколу OTLP в коллектор. Коллектор затем пересылает записи в выбранный бэкенд для хранения и просмотра логов.

Принцип одинаков во всех SDK:

1. Создать OpenTelemetry `LoggerProvider` с OTLP-экспортёром логов и атрибутом ресурса `service.name`.
2. Перенаправить логгер SDK в этот провайдер через адаптер (log appender / bridge), превращающий каждую запись лога SDK в OTel log record. Состав адаптеров и статус поддержки логов в разных языках см. в [документации OpenTelemetry по логам](https://opentelemetry.io/docs/concepts/signals/logs/).
3. Запустить рабочую нагрузку — все внутренние логи SDK теперь отправляются в коллектор как OTLP log records.

{% note info %}

## Принципы {#principles}

Способ сбора логов зависит от языка и инфраструктуры — единого формата нет:

* **Программные мосты (log appender / bridge).** Приложение само отправляет логи в коллектор по OTLP прямо из процесса (workflow «direct-to-Collector»). Именно этот подход показан в примерах ниже.
* **Агенты-коллекторы.** Приложение пишет логи в файл или `stdout`, а отдельный агент (например, [filelog receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/filelogreceiver) в [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)) читает, парсит и пересылает их в бэкенд — без изменения кода приложения.

{% endnote %}

## Подключение к SDK {#integration}

{% list tabs %}

- Go

  Для {{ ydb-short-name }} Go SDK есть готовый адаптер [ydb-go-sdk-otel](https://github.com/ydb-platform/ydb-go-sdk-otel), который превращает события SDK в сигналы OpenTelemetry: трассы (`WithTracer`), метрики (`WithMetrics`) и логи (`WithLogger`). Адаптер не настраивает экспортёры сам — вы создаёте `LoggerProvider` с OTLP-экспортёром логов, получаете из него логгер и передаёте его в опцию `ydbOtel.WithLogger` при вызове `ydb.Open`. Каждая внутренняя запись лога SDK (инициализация драйвера, пул сессий, выполнение запросов, ретраи и т.д.) при этом отправляется в коллектор как OTLP log record.

  ```bash
  go get github.com/ydb-platform/ydb-go-sdk-otel
  go get go.opentelemetry.io/otel/sdk/log
  go get go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc
  ```

  ```go
  package main

  import (
      "context"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/trace"

      "go.opentelemetry.io/otel/attribute"
      "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
      sdklog "go.opentelemetry.io/otel/sdk/log"
      "go.opentelemetry.io/otel/sdk/resource"

      ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
  )

  func main() {
      ctx := context.Background()

      // 1. Настраиваем провайдер логов OTel с OTLP-экспортёром.
      exporter, err := otlploggrpc.New(ctx,
          otlploggrpc.WithEndpoint("localhost:4317"),
          otlploggrpc.WithInsecure(),
      )
      if err != nil {
          panic(err)
      }
      res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(
          attribute.String("service.name", "ydb-go-sdk-otel-logs-sample"),
      ))
      lp := sdklog.NewLoggerProvider(
          sdklog.WithResource(res),
          sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
      )
      defer lp.Shutdown(ctx)

      // 2. Открываем драйвер YDB с адаптером ydb-go-sdk-otel.
      // WithLogger пересылает события логов SDK в OTel log records.
      logger := lp.Logger("ydb-go-sdk")
      db, err := ydb.Open(ctx,
          os.Getenv("YDB_CONNECTION_STRING"),
          ydbOtel.WithLogger(logger, ydbOtel.WithDetailer(trace.DetailsAll)),
      )
      if err != nil {
          panic(err)
      }
      defer db.Close(ctx)
      // ... используйте db ...
  }
  ```

- Python

  {{ ydb-short-name }} Python SDK пишет логи через стандартный модуль `logging` (логгеры с именами `ydb.*`). Используйте встроенный в OpenTelemetry `LoggingHandler`, чтобы перенаправить эти записи в `LoggerProvider` и экспортировать по OTLP:

  ```bash
  pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc
  ```

  ```python
  import logging

  import ydb
  from opentelemetry._logs import set_logger_provider
  from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
  from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
  from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
  from opentelemetry.sdk.resources import Resource

  resource = Resource(attributes={"service.name": "ydb-otel-logs-example"})
  logger_provider = LoggerProvider(resource=resource)
  logger_provider.add_log_record_processor(
      BatchLogRecordProcessor(OTLPLogExporter(endpoint="http://localhost:4317"))
  )
  set_logger_provider(logger_provider)

  # Мост stdlib logging -> OpenTelemetry. Привязка обработчика к корневому логгеру
  # перехватывает всё, что SDK пишет через logging.getLogger("ydb...").
  otel_handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
  logging.basicConfig(level=logging.INFO, handlers=[otel_handler])
  logging.getLogger("ydb").setLevel(logging.INFO)

  with ydb.Driver(endpoint="grpc://localhost:2136", database="/local") as driver:
      driver.wait(timeout=5)
      with ydb.QuerySessionPool(driver) as pool:
          pool.execute_with_retries("SELECT 1")

  logger_provider.shutdown()
  ```

- C#

  {{ ydb-short-name }} C# SDK пишет логи через переданный ему `ILoggerFactory`. Создайте фабрику на основе провайдера логирования OpenTelemetry с OTLP-экспортёром и передайте её в источник данных:

  ```bash
  dotnet add package OpenTelemetry
  dotnet add package OpenTelemetry.Exporter.OpenTelemetryProtocol
  ```

  ```csharp
  using Microsoft.Extensions.Logging;
  using OpenTelemetry.Logs;
  using OpenTelemetry.Resources;
  using Ydb.Sdk.Ado;

  var resourceBuilder = ResourceBuilder.CreateDefault()
      .AddService("ydb-sdk-otel-logs-sample");

  using var loggerFactory = LoggerFactory.Create(builder =>
  {
      builder.AddOpenTelemetry(options =>
      {
          options.SetResourceBuilder(resourceBuilder);
          options.AddOtlpExporter(o => o.Endpoint = new Uri("http://localhost:4317"));
      });
  });

  // Передаём фабрику в SDK: каждый внутренний лог (инициализация драйвера, пул сессий,
  // выполнение запросов, ретраи, ...) экспортируется в коллектор как OTLP log record.
  await using var dataSource = new YdbDataSource(
      new YdbConnectionStringBuilder("Host=localhost;Port=2136;Database=/local")
      {
          LoggerFactory = loggerFactory
      });

  await using var connection = await dataSource.OpenConnectionAsync();
  await new YdbCommand("SELECT 1", connection).ExecuteNonQueryAsync();
  ```

- Java

  {{ ydb-short-name }} Java SDK пишет логи через `slf4j`. Если в качестве реализации `slf4j` используется logback, подключите готовый аппендер `opentelemetry-logback-appender-1.0`, который пересылает каждое событие в OpenTelemetry Logs SDK:

  ```xml
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
  <dependency>
      <groupId>io.opentelemetry.instrumentation</groupId>
      <artifactId>opentelemetry-logback-appender-1.0</artifactId>
      <version>${otel.instrumentation.version}</version>
  </dependency>
  <dependency>
      <groupId>ch.qos.logback</groupId>
      <artifactId>logback-classic</artifactId>
      <version>${logback.version}</version>
  </dependency>
  ```

  Подключите аппендер в `logback.xml`:

  ```xml
  <configuration>
      <appender name="OTEL" class="io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender">
      </appender>

      <root level="INFO">
          <appender-ref ref="OTEL"/>
      </root>
  </configuration>
  ```

  Соберите экземпляр OpenTelemetry SDK с OTLP-экспортёром логов и установите его в аппендер вызовом `OpenTelemetryAppender.install(...)` перед запуском рабочей нагрузки:

  ```java
  import java.util.concurrent.TimeUnit;

  import io.opentelemetry.api.OpenTelemetry;
  import io.opentelemetry.api.common.Attributes;
  import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
  import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
  import io.opentelemetry.sdk.OpenTelemetrySdk;
  import io.opentelemetry.sdk.logs.SdkLoggerProvider;
  import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
  import io.opentelemetry.sdk.resources.Resource;
  import tech.ydb.core.grpc.GrpcTransport;
  import tech.ydb.query.QueryClient;

  String serviceName = "ydb-java-sdk-otel-logs-sample";
  Resource resource = Resource.getDefault().merge(
          Resource.create(Attributes.builder().put("service.name", serviceName).build()));

  SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
          .setResource(resource)
          .addLogRecordProcessor(BatchLogRecordProcessor.builder(
                  OtlpGrpcLogRecordExporter.builder().setEndpoint("http://localhost:4317").build()
          ).build())
          .build();

  OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
          .setLoggerProvider(loggerProvider)
          .build();

  // Передаём OpenTelemetry SDK в аппендер, объявленный в logback.xml.
  OpenTelemetryAppender.install(openTelemetry);

  try (GrpcTransport transport = GrpcTransport.forConnectionString("grpc://localhost:2136/local").build();
       QueryClient queryClient = QueryClient.newClient(transport).build()) {
      // ... используйте queryClient ...
  } finally {
      loggerProvider.forceFlush().join(10, TimeUnit.SECONDS);
      loggerProvider.shutdown().join(10, TimeUnit.SECONDS);
  }
  ```

- C++

  {{ ydb-short-name }} C++ SDK пишет логи через `TLogBackend` из `util`. Реализуйте бэкенд, который превращает каждую запись в OTel log record, и передайте его в драйвер через `TDriverConfig::SetLog`. Сборку OpenTelemetry C++ SDK см. в [Getting Started](https://opentelemetry.io/docs/languages/cpp/getting-started/).

  ```cpp
  #include <ydb-cpp-sdk/client/driver/driver.h>
  #include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_factory.h>
  #include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_options.h>
  #include <opentelemetry/logs/provider.h>
  #include <opentelemetry/sdk/logs/batch_log_record_processor_factory.h>
  #include <opentelemetry/sdk/logs/logger_provider_factory.h>
  #include <opentelemetry/sdk/resource/resource.h>
  #include <library/cpp/logger/backend.h>
  #include <library/cpp/logger/record.h>
  #include <memory>
  
  namespace otlp     = opentelemetry::exporter::otlp;
  namespace logs_sdk = opentelemetry::sdk::logs;
  namespace logs_api = opentelemetry::logs;
  namespace resource = opentelemetry::sdk::resource;
  
  namespace {
  
  class TOtelLogBackend final : public TLogBackend {
  public:
    explicit TOtelLogBackend(opentelemetry::nostd::shared_ptr<logs_api::Logger> logger)
        : Logger_(std::move(logger)) {}

    void WriteData(const TLogRecord& rec) override {
        Logger_->EmitLogRecord(MapSeverity(rec.Priority),
                               opentelemetry::nostd::string_view(rec.Data, rec.Len));
    }

    void ReopenLog() override {}

  private:
    static logs_api::Severity MapSeverity(ELogPriority p) {
        switch (p) {
            case TLOG_EMERG:
            case TLOG_ALERT:
            case TLOG_CRIT:    return logs_api::Severity::kFatal;
            case TLOG_ERR:     return logs_api::Severity::kError;
            case TLOG_WARNING: return logs_api::Severity::kWarn;
            case TLOG_NOTICE:
            case TLOG_INFO:    return logs_api::Severity::kInfo;
            case TLOG_DEBUG:   return logs_api::Severity::kDebug;
            default:           return logs_api::Severity::kTrace;
        }
    }

    opentelemetry::nostd::shared_ptr<logs_api::Logger> Logger_;
  };
  } // namespace
  
  int main() {
    // 1. Настраиваем провайдер логов OTel с OTLP-экспортёром
    otlp::OtlpGrpcLogRecordExporterOptions exporterOpts;
    exporterOpts.endpoint = "localhost:4317";
    auto exporter  = otlp::OtlpGrpcLogRecordExporterFactory::Create(exporterOpts);
    auto processor = logs_sdk::BatchLogRecordProcessorFactory::Create(std::move(exporter));
    auto res       = resource::Resource::Create({{"service.name", "ydb-cpp-sdk-otel-logs-sample"}});

    std::shared_ptr<logs_api::LoggerProvider> provider(
        logs_sdk::LoggerProviderFactory::Create(std::move(processor), res));
    logs_api::Provider::SetLoggerProvider(provider);

    auto logger = provider->GetLogger("ydb-cpp-sdk");

    // 2. Передаём мост в драйвер YDB
    auto config = NYdb::TDriverConfig()
        .SetEndpoint("localhost:2136")
        .SetDatabase("/local")
        .SetLog(std::make_unique<TOtelLogBackend>(logger));

    NYdb::TDriver driver(config);
    // ... используйте driver ...
    driver.Stop(true);
  }
  ```

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
