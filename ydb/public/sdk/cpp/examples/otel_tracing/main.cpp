#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/tx.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/open_telemetry/trace.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/open_telemetry/metrics.h>

#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/view/view_registry.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/resource/resource.h>

#include <library/cpp/getopt/last_getopt.h>

#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/scope.h>

#include <util/string/cast.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

namespace nostd = opentelemetry::nostd;
namespace sdktrace = opentelemetry::sdk::trace;
namespace sdkmetrics = opentelemetry::sdk::metrics;
namespace otlp = opentelemetry::exporter::otlp;
namespace resource = opentelemetry::sdk::resource;

using namespace NYdb;
using namespace NYdb::NStatusHelpers;

struct TConfig {
    std::string Endpoint = "localhost:2136";
    std::string Database = "/local";
    std::string OtlpEndpoint = "http://localhost:4328";
    int Iterations = 20;
    int RetryWorkers = 6;
    int RetryOps = 30;
};

nostd::shared_ptr<opentelemetry::trace::TracerProvider> InitTracing(const TConfig& cfg) {
    otlp::OtlpHttpExporterOptions opts;
    opts.url = cfg.OtlpEndpoint + "/v1/traces";

    auto exporter = otlp::OtlpHttpExporterFactory::Create(opts);
    auto processor = sdktrace::SimpleSpanProcessorFactory::Create(std::move(exporter));

    auto res = resource::Resource::Create({
        {"service.name", "ydb-cpp-sdk-demo"},
        {"service.version", "1.0.0"},
    });

    std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
        std::make_shared<sdktrace::TracerProvider>(std::move(processor), res);
    return nostd::shared_ptr<opentelemetry::trace::TracerProvider>(provider);
}

nostd::shared_ptr<opentelemetry::metrics::MeterProvider> InitMetrics(const TConfig& cfg) {
    otlp::OtlpHttpMetricExporterOptions opts;
    opts.url = cfg.OtlpEndpoint + "/v1/metrics";

    auto exporter = otlp::OtlpHttpMetricExporterFactory::Create(opts);

    sdkmetrics::PeriodicExportingMetricReaderOptions readerOpts;
    readerOpts.export_interval_millis = std::chrono::milliseconds(5000);
    readerOpts.export_timeout_millis = std::chrono::milliseconds(3000);

    auto reader = sdkmetrics::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), readerOpts);

    auto res = resource::Resource::Create({
        {"service.name", "ydb-cpp-sdk-demo"},
        {"service.version", "1.0.0"},
    });

    auto rawProvider = std::make_shared<sdkmetrics::MeterProvider>(
        std::unique_ptr<sdkmetrics::ViewRegistry>(new sdkmetrics::ViewRegistry()), res);
    rawProvider->AddMetricReader(std::move(reader));

    std::shared_ptr<opentelemetry::metrics::MeterProvider> provider = rawProvider;
    return nostd::shared_ptr<opentelemetry::metrics::MeterProvider>(provider);
}

nostd::shared_ptr<opentelemetry::trace::Tracer> GetAppTracer() {
    return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("ydb-demo-app", "1.0.0");
}

void RunQueryWorkload(NQuery::TQueryClient& client, int iterations) {
    std::cout << "\n=== Query Service workload ===" << std::endl;

    auto tracer = GetAppTracer();

    {
        auto ddlSpan = tracer->StartSpan("QueryService.DDL");
        auto scope = opentelemetry::trace::Scope(ddlSpan);

        ThrowOnError(client.RetryQuerySync([](NQuery::TSession session) {
            return session.ExecuteQuery(R"(
                CREATE TABLE IF NOT EXISTS otel_demo (
                    id Uint64,
                    value Utf8,
                    PRIMARY KEY (id)
                )
            )", NQuery::TTxControl::NoTx()).GetValueSync();
        }));

        ddlSpan->SetStatus(opentelemetry::trace::StatusCode::kOk);
    }

    for (int i = 0; i < iterations; ++i) {
        auto iterSpan = tracer->StartSpan("QueryService.Iteration");
        auto scope = opentelemetry::trace::Scope(iterSpan);
        iterSpan->SetAttribute("iteration", static_cast<int64_t>(i + 1));

        std::cout << "  [Query] Iteration " << (i + 1) << "/" << iterations << std::endl;

        ThrowOnError(client.RetryQuerySync([i](NQuery::TSession session) {
            auto params = TParamsBuilder()
                .AddParam("$id").Uint64(i).Build()
                .AddParam("$val").Utf8("query_" + std::to_string(i)).Build()
                .Build();

            return session.ExecuteQuery(R"(
                DECLARE $id AS Uint64;
                DECLARE $val AS Utf8;
                UPSERT INTO otel_demo (id, value) VALUES ($id, $val)
            )", NQuery::TTxControl::BeginTx(NQuery::TTxSettings::SerializableRW()).CommitTx(),
                params).GetValueSync();
        }));

        ThrowOnError(client.RetryQuerySync([i](NQuery::TSession session) {
            auto params = TParamsBuilder()
                .AddParam("$id").Uint64(i).Build()
                .Build();

            return session.ExecuteQuery(R"(
                DECLARE $id AS Uint64;
                SELECT id, value FROM otel_demo WHERE id = $id
            )", NQuery::TTxControl::BeginTx(NQuery::TTxSettings::SerializableRW()).CommitTx(),
                params).GetValueSync();
        }));

        if (i % 5 == 4) {
            ThrowOnError(client.RetryQuerySync([](NQuery::TQueryClient client) -> TStatus {
                auto session = client.GetSession().GetValueSync().GetSession();
                auto beginResult = session.BeginTransaction(NQuery::TTxSettings::SerializableRW()).GetValueSync();
                if (!beginResult.IsSuccess()) {
                    return beginResult;
                }
                auto tx = beginResult.GetTransaction();

                auto result = session.ExecuteQuery(R"(
                    SELECT COUNT(*) AS cnt FROM otel_demo
                )", NQuery::TTxControl::Tx(tx)).GetValueSync();

                if (!result.IsSuccess()) {
                    return result;
                }

                return tx.Commit().GetValueSync();
            }));
        }

        iterSpan->SetStatus(opentelemetry::trace::StatusCode::kOk);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void RunTableWorkload(NTable::TTableClient& client, int iterations) {
    std::cout << "\n=== Table Service workload ===" << std::endl;

    auto tracer = GetAppTracer();

    for (int i = 0; i < iterations; ++i) {
        int id = 1000 + i;

        auto iterSpan = tracer->StartSpan("TableService.Iteration");
        auto scope = opentelemetry::trace::Scope(iterSpan);
        iterSpan->SetAttribute("iteration", static_cast<int64_t>(i + 1));

        std::cout << "  [Table] Iteration " << (i + 1) << "/" << iterations << std::endl;

        ThrowOnError(client.RetryOperationSync([id](NTable::TSession session) {
            auto params = session.GetParamsBuilder()
                .AddParam("$id").Uint64(id).Build()
                .AddParam("$val").Utf8("table_" + std::to_string(id)).Build()
                .Build();

            return session.ExecuteDataQuery(R"(
                DECLARE $id AS Uint64;
                DECLARE $val AS Utf8;
                UPSERT INTO otel_demo (id, value) VALUES ($id, $val)
            )", NTable::TTxControl::BeginTx(NTable::TTxSettings::SerializableRW()).CommitTx(),
                std::move(params)).GetValueSync();
        }));

        ThrowOnError(client.RetryOperationSync([id](NTable::TSession session) {
            auto params = session.GetParamsBuilder()
                .AddParam("$id").Uint64(id).Build()
                .Build();

            return session.ExecuteDataQuery(R"(
                DECLARE $id AS Uint64;
                SELECT id, value FROM otel_demo WHERE id = $id
            )", NTable::TTxControl::BeginTx(NTable::TTxSettings::SerializableRW()).CommitTx(),
                std::move(params)).GetValueSync();
        }));

        ThrowOnError(client.RetryOperationSync([](NTable::TSession session) -> TStatus {
            auto beginResult = session.BeginTransaction(NTable::TTxSettings::SerializableRW()).GetValueSync();
            if (!beginResult.IsSuccess()) {
                return beginResult;
            }
            auto tx = beginResult.GetTransaction();

            auto result = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) AS cnt FROM otel_demo
            )", NTable::TTxControl::Tx(tx)).GetValueSync();

            if (!result.IsSuccess()) {
                return result;
            }

            return tx.Commit().GetValueSync();
        }));

        if (i % 5 == 4) {
            auto rollbackResult = client.RetryOperationSync([](NTable::TSession session) -> TStatus {
                auto beginResult = session.BeginTransaction(NTable::TTxSettings::SerializableRW()).GetValueSync();
                if (!beginResult.IsSuccess()) {
                    return beginResult;
                }
                auto tx = beginResult.GetTransaction();
                return tx.Rollback().GetValueSync();
            });
            if (!rollbackResult.IsSuccess()) {
                std::cerr << "  Rollback status: " << static_cast<int>(rollbackResult.GetStatus()) << std::endl;
            }
        }

        iterSpan->SetStatus(opentelemetry::trace::StatusCode::kOk);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void RunRetryWorkload(NQuery::TQueryClient& client, int workers, int opsPerWorker) {
    std::cout << "\n=== Retry workload (SERIALIZABLE conflicts) ==="
              << " workers=" << workers << " ops=" << opsPerWorker << std::endl;

    auto tracer = GetAppTracer();

    {
        auto seedSpan = tracer->StartSpan("RetryWorkload.Seed");
        auto scope = opentelemetry::trace::Scope(seedSpan);

        ThrowOnError(client.RetryQuerySync([](NQuery::TSession session) {
            return session.ExecuteQuery(R"(
                UPSERT INTO otel_demo (id, value) VALUES (9999u, "seed")
            )", NQuery::TTxControl::BeginTx(NQuery::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        }));
    }

    std::atomic<int> conflicts{0};
    std::atomic<int> successes{0};
    std::vector<std::thread> threads;
    threads.reserve(workers);

    for (int w = 0; w < workers; ++w) {
        threads.emplace_back([&, w]() {
            auto workerTracer = GetAppTracer();
            for (int i = 0; i < opsPerWorker; ++i) {
                auto iterSpan = workerTracer->StartSpan("RetryWorkload.Op");
                auto scope = opentelemetry::trace::Scope(iterSpan);
                iterSpan->SetAttribute("worker", static_cast<int64_t>(w));
                iterSpan->SetAttribute("op", static_cast<int64_t>(i));

                auto status = client.RetryQuerySync(
                    [w, i, &conflicts](NQuery::TQueryClient client) -> TStatus {
                        auto sessionRes = client.GetSession().GetValueSync();
                        if (!sessionRes.IsSuccess()) {
                            return sessionRes;
                        }
                        auto session = sessionRes.GetSession();

                        auto beginRes = session.BeginTransaction(
                            NQuery::TTxSettings::SerializableRW()).GetValueSync();
                        if (!beginRes.IsSuccess()) {
                            return beginRes;
                        }
                        auto tx = beginRes.GetTransaction();

                        auto readRes = session.ExecuteQuery(R"(
                            SELECT value FROM otel_demo WHERE id = 9999u
                        )", NQuery::TTxControl::Tx(tx)).GetValueSync();
                        if (!readRes.IsSuccess()) {
                            if (readRes.GetStatus() == EStatus::ABORTED) {
                                conflicts.fetch_add(1);
                            }
                            return readRes;
                        }

                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(5 + (w * 7 + i * 3) % 20));

                        auto params = TParamsBuilder()
                            .AddParam("$v").Utf8("w" + std::to_string(w)
                                                  + "_i" + std::to_string(i)).Build()
                            .Build();

                        auto writeRes = session.ExecuteQuery(R"(
                            DECLARE $v AS Utf8;
                            UPSERT INTO otel_demo (id, value) VALUES (9999u, $v)
                        )", NQuery::TTxControl::Tx(tx), params).GetValueSync();
                        if (!writeRes.IsSuccess()) {
                            if (writeRes.GetStatus() == EStatus::ABORTED) {
                                conflicts.fetch_add(1);
                            }
                            return writeRes;
                        }

                        auto commitRes = tx.Commit().GetValueSync();
                        if (!commitRes.IsSuccess()
                            && commitRes.GetStatus() == EStatus::ABORTED) {
                            conflicts.fetch_add(1);
                        }
                        return commitRes;
                    });

                if (status.IsSuccess()) {
                    successes.fetch_add(1);
                    iterSpan->SetStatus(opentelemetry::trace::StatusCode::kOk);
                } else {
                    iterSpan->SetStatus(opentelemetry::trace::StatusCode::kError,
                                        std::string(ToString(status.GetStatus())));
                    std::cerr << "  [retry-wl] worker=" << w << " op=" << i
                              << " final_status=" << static_cast<int>(status.GetStatus())
                              << std::endl;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "  Retry workload done."
              << " successes=" << successes.load()
              << " observed_aborts=" << conflicts.load()
              << " (each abort triggers one SDK retry attempt)" << std::endl;
}

int main(int argc, char** argv) {
    TConfig cfg;

    NLastGetopt::TOpts opts;
    opts.AddLongOption('e', "endpoint", "YDB endpoint")
        .DefaultValue(cfg.Endpoint).StoreResult(&cfg.Endpoint);
    opts.AddLongOption('d', "database", "YDB database")
        .DefaultValue(cfg.Database).StoreResult(&cfg.Database);
    opts.AddLongOption("otlp", "OTLP HTTP endpoint")
        .DefaultValue(cfg.OtlpEndpoint).StoreResult(&cfg.OtlpEndpoint);
    opts.AddLongOption('n', "iterations", "Number of iterations")
        .DefaultValue(std::to_string(cfg.Iterations)).StoreResult(&cfg.Iterations);
    opts.AddLongOption("retry-workers", "Concurrent workers for retry workload (0 to skip)")
        .DefaultValue(std::to_string(cfg.RetryWorkers)).StoreResult(&cfg.RetryWorkers);
    opts.AddLongOption("retry-ops", "Operations per retry worker")
        .DefaultValue(std::to_string(cfg.RetryOps)).StoreResult(&cfg.RetryOps);

    NLastGetopt::TOptsParseResult parsedOpts(&opts, argc, argv);

    if (cfg.Endpoint.rfind("grpc://", 0) == 0) {
        cfg.Endpoint.erase(0, 7);
    } else if (cfg.Endpoint.rfind("grpcs://", 0) == 0) {
        cfg.Endpoint.erase(0, 8);
    }

    std::cout << "Initializing OpenTelemetry..." << std::endl;
    std::cout << "  OTLP endpoint: " << cfg.OtlpEndpoint << std::endl;

    auto tracerProvider = InitTracing(cfg);
    auto meterProvider = InitMetrics(cfg);

    opentelemetry::trace::Provider::SetTracerProvider(tracerProvider);
    opentelemetry::metrics::Provider::SetMeterProvider(meterProvider);

    auto ydbTraceProvider = NTrace::CreateOtelTraceProvider(tracerProvider);
    auto ydbMetricRegistry = NMetrics::CreateOtelMetricRegistry(meterProvider);

    std::cout << "Connecting to YDB at " << cfg.Endpoint << cfg.Database << std::endl;

    auto driverConfig = TDriverConfig()
        .SetEndpoint(cfg.Endpoint)
        .SetDatabase(cfg.Database)
        .SetDiscoveryMode(EDiscoveryMode::Off)
        .SetTraceProvider(ydbTraceProvider)
        .SetMetricRegistry(ydbMetricRegistry);

    TDriver driver(driverConfig);
    NQuery::TQueryClient queryClient(driver);
    NTable::TTableClient tableClient(driver);

    try {
        RunQueryWorkload(queryClient, cfg.Iterations);
        RunTableWorkload(tableClient, cfg.Iterations);

        if (cfg.RetryWorkers > 0 && cfg.RetryOps > 0) {
            RunRetryWorkload(queryClient, cfg.RetryWorkers, cfg.RetryOps);
        }

        std::cout << "\n=== Cleanup ===" << std::endl;
        ThrowOnError(queryClient.RetryQuerySync([](NQuery::TSession session) {
            return session.ExecuteQuery(
                "DROP TABLE otel_demo", NQuery::TTxControl::NoTx()).GetValueSync();
        }));
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << "Flushing telemetry..." << std::endl;

    driver.Stop(true);

    if (auto* sdkTracerProvider = dynamic_cast<sdktrace::TracerProvider*>(tracerProvider.get())) {
        sdkTracerProvider->ForceFlush();
    }
    if (auto* sdkMeterProvider = dynamic_cast<sdkmetrics::MeterProvider*>(meterProvider.get())) {
        sdkMeterProvider->ForceFlush();
    }

    std::this_thread::sleep_for(std::chrono::seconds(3));

    opentelemetry::trace::Provider::SetTracerProvider(
        nostd::shared_ptr<opentelemetry::trace::TracerProvider>{});
    opentelemetry::metrics::Provider::SetMeterProvider(
        nostd::shared_ptr<opentelemetry::metrics::MeterProvider>{});

    std::cout << "Done. Open Grafana at http://localhost:3000" << std::endl;
    std::cout << "  Jaeger UI at http://localhost:16686" << std::endl;
    std::cout << "  Prometheus at http://localhost:9090" << std::endl;

    return 0;
}
