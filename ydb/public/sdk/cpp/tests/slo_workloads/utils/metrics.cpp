#include "metrics.h"

#include "utils.h"

#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>

#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

using namespace std::chrono_literals;

class TOtelMetricsPusher : public IMetricsPusher {
public:
    TOtelMetricsPusher(const std::string& metricsPushUrl, const std::string& operationType)
        : OperationType_(operationType)
    {
        auto exporterOptions = opentelemetry::exporter::otlp::OtlpHttpMetricExporterOptions();
        exporterOptions.url = metricsPushUrl;

        auto exporter = opentelemetry::exporter::otlp::OtlpHttpMetricExporterFactory::Create(exporterOptions);

        opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions readerOptions;
        readerOptions.export_interval_millis = 1000ms;
        readerOptions.export_timeout_millis  = 500ms;

        auto metricReader = opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), readerOptions);

        MeterProvider_ = opentelemetry::sdk::metrics::MeterProviderFactory::Create();
        MeterProvider_->AddMetricReader(std::move(metricReader));

        Meter_ = MeterProvider_->GetMeter("slo_workloads", NYdb::GetSdkSemver());

        InitMetrics();
    }

    void PushRequestData(const TRequestData& requestData) override {
        if (requestData.Status == NYdb::EStatus::SUCCESS) {
            OperationsSuccessTotal_->Add(1, {{"operation_type", OperationType_}});
        } else {
            ErrorsTotal_->Add(1, {{"status", YdbStatusToString(requestData.Status)}});
            OperationsFailureTotal_->Add(1, {{"operation_type", OperationType_}});
        }
        OperationsTotal_->Add(1, {{"operation_type", OperationType_}});
        OperationLatencySeconds_->Record(requestData.Delay.SecondsFloat(), {{"operation_type", OperationType_}, {"status", YdbStatusToString(requestData.Status)}});
        RetryAttempts_->Record(requestData.RetryAttempts, {{"operation_type", OperationType_}});
    }

private:
    void InitMetrics() {
        ErrorsTotal_ = Meter_->CreateUInt64Counter("sdk_errors_total",
            "Total number of errors encountered, categorized by error type."
        );

        OperationsTotal_ = Meter_->CreateUInt64Counter("sdk_operations_total",
            "Total number of operations, categorized by type attempted by the SDK."
        );
    
        OperationsSuccessTotal_ = Meter_->CreateUInt64Counter("sdk_operations_success_total",
            "Total number of successful operations, categorized by type."
        );

        OperationsFailureTotal_ = Meter_->CreateUInt64Counter("sdk_operations_failure_total",
            "Total number of failed operations, categorized by type."
        );

        OperationLatencySeconds_ = CreateDoubleHistogram("sdk_operation_latency_seconds",
            "Latency of operations performed by the SDK in seconds, categorized by type and status.",
            {
				0.001,  // 1 ms
				0.002,  // 2 ms
				0.003,  // 3 ms
				0.004,  // 4 ms
				0.005,  // 5 ms
				0.0075, // 7.5 ms
				0.010,  // 10 ms
				0.020,  // 20 ms
				0.050,  // 50 ms
				0.100,  // 100 ms
				0.200,  // 200 ms
				0.500,  // 500 ms
				1.000,  // 1 s
			},
            "s"
        );

        RetryAttempts_ = Meter_->CreateInt64Gauge("sdk_retry_attempts",
            "Current retry attempts, categorized by operation type."
        );
    }

    std::unique_ptr<opentelemetry::metrics::Histogram<double>> CreateDoubleHistogram(
        const std::string& name,
        const std::string& description,
        const std::vector<double>& buckets,
        const std::string& unit = {})
    {
        auto selector = std::make_unique<opentelemetry::sdk::metrics::InstrumentSelector>(
            opentelemetry::sdk::metrics::InstrumentType::kHistogram,
            name,
            unit
        );

        auto meterSelector = std::make_unique<opentelemetry::sdk::metrics::MeterSelector>(
            "slo_workloads",
            NYdb::GetSdkSemver(),
            ""
        );

        auto histogramConfig = std::make_shared<opentelemetry::sdk::metrics::HistogramAggregationConfig>();
        histogramConfig->boundaries_ = buckets;

        auto view = std::make_unique<opentelemetry::sdk::metrics::View>(
            "",
            "",
            opentelemetry::sdk::metrics::AggregationType::kHistogram,
            histogramConfig
        );

        MeterProvider_->AddView(std::move(selector), std::move(meterSelector), std::move(view));

        return Meter_->CreateDoubleHistogram(name, description, unit);
    }

    std::string OperationType_;

    std::unique_ptr<opentelemetry::sdk::metrics::MeterProvider> MeterProvider_;
    std::shared_ptr<opentelemetry::metrics::Meter> Meter_;

    std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ErrorsTotal_;
    std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> OperationsTotal_;
    std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> OperationsSuccessTotal_;
    std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> OperationsFailureTotal_;
    std::unique_ptr<opentelemetry::metrics::Histogram<double>> OperationLatencySeconds_;
    std::unique_ptr<opentelemetry::metrics::Gauge<int64_t>> RetryAttempts_;
};

class TNoopMetricsPusher : public IMetricsPusher {
public:
    void PushRequestData([[maybe_unused]] const TRequestData& requestData) override {}
};

std::unique_ptr<IMetricsPusher> CreateOtelMetricsPusher(const std::string& metricsPushUrl, const std::string& operationType) {
    return std::make_unique<TOtelMetricsPusher>(metricsPushUrl, operationType);
}

std::unique_ptr<IMetricsPusher> CreateNoopMetricsPusher() {
    return std::make_unique<TNoopMetricsPusher>();
}
