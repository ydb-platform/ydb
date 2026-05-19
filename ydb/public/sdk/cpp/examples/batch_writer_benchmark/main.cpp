// Benchmark for TBatchWriter and a fair comparison against:
//   1) per-row UPSERT via NQuery::TQueryClient (no client-side batching)
//   2) manual fixed-size BulkUpsert calls (the existing
//      examples/bulk_upsert_simple model: build a List<Struct>, call
//      BulkUpsert synchronously)
//   3) TBatchWriter (NYdb::NBatch) with configurable parameters
//
// All three modes write the same total number of rows into the same
// schema. The benchmark reports total wall-clock time, throughput
// (rows/s), and per-row submit-to-resolution latency p50/p95/p99 for
// modes that expose per-row futures (1 and 3). Mode 2 only exposes
// per-batch latency, which we also report.
//
// Usage:
//   ./batch_writer_benchmark \
//        --endpoint localhost:2136 \
//        --database /local \
//        --total-rows 200000 \
//        --producers 8 \
//        --max-batch-rows 1000 \
//        --flush-interval-ms 200 \
//        --concurrency 4 \
//        --modes per_row,manual_bulk,batch_writer

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/batch/batch_writer.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/tx.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/getopt/last_getopt.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

using namespace NYdb;
using namespace NYdb::NStatusHelpers;

namespace {

struct TConfig {
    std::string Endpoint = "localhost:2136";
    std::string Database = "/local";
    std::string Table    = "batch_writer_bench";
    int TotalRows        = 200'000;
    int Producers        = 8;
    int MaxBatchRows     = 1000;
    int FlushIntervalMs  = 200;
    int Concurrency      = 4;
    std::string Modes    = "per_row,manual_bulk,batch_writer";
};

struct TLatencyStats {
    double P50 = 0;
    double P95 = 0;
    double P99 = 0;
    double Avg = 0;
    std::size_t Count = 0;
};

TLatencyStats ComputeStats(std::vector<double>& samples) {
    TLatencyStats s;
    s.Count = samples.size();
    if (samples.empty()) return s;
    std::sort(samples.begin(), samples.end());
    auto pick = [&](double q) {
        const std::size_t idx = std::min<std::size_t>(
            static_cast<std::size_t>(q * samples.size()),
            samples.size() - 1);
        return samples[idx];
    };
    s.P50 = pick(0.50);
    s.P95 = pick(0.95);
    s.P99 = pick(0.99);
    double sum = 0;
    for (double v : samples) sum += v;
    s.Avg = sum / samples.size();
    return s;
}

void EnsureTable(NTable::TTableClient& client, std::string_view fullPath) {
    ThrowOnError(client.RetryOperationSync([&](NTable::TSession session) {
        auto desc = NTable::TTableBuilder()
            .AddNullableColumn("producer", EPrimitiveType::Uint32)
            .AddNullableColumn("seq",      EPrimitiveType::Uint64)
            .AddNullableColumn("payload",  EPrimitiveType::Utf8)
            .SetPrimaryKeyColumns({"producer", "seq"})
            .Build();
        return session.CreateTable(fullPath, std::move(desc)).GetValueSync();
    }));
}

void TruncateTable(NQuery::TQueryClient& client, std::string_view fullPath) {
    auto status = client.ExecuteQuery(
        "DELETE FROM `" + fullPath + "`",
        NQuery::TTxControl::BeginTx(NQuery::TTxSettings::SerializableRW()).CommitTx())
        .GetValueSync();
    (void)status;  // benchmark — best effort cleanup between modes
}

TValue MakeRow(std::uint32_t producer, std::uint64_t seq) {
    return TValueBuilder()
        .BeginStruct()
        .AddMember("producer").Uint32(producer)
        .AddMember("seq").Uint64(seq)
        .AddMember("payload").Utf8("payload_" + std::to_string(seq))
        .EndStruct()
        .Build();
}

struct TResult {
    std::string Mode;
    std::size_t TotalRows = 0;
    std::size_t Failed = 0;
    double DurationSeconds = 0;
    double Throughput = 0;
    TLatencyStats RowLatencyMs;        // per-row, when available
    TLatencyStats BatchLatencyMs;      // per-batch, when available
};

// ---------------------------------------------------------------------
// Mode 1: per-row UPSERT via Query Service.
// ---------------------------------------------------------------------
TResult RunPerRow(NQuery::TQueryClient& client,
                   std::string_view fullPath,
                   const TConfig& cfg)
{
    TResult r;
    r.Mode = "per_row";
    r.TotalRows = cfg.TotalRows;

    std::mutex latMu;
    std::vector<double> latencies;
    latencies.reserve(cfg.TotalRows);

    std::atomic<std::size_t> failed{0};
    const int rowsPerProd = cfg.TotalRows / cfg.Producers;

    const auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    threads.reserve(cfg.Producers);
    for (int p = 0; p < cfg.Producers; ++p) {
        threads.emplace_back([&, p] {
            for (int s = 0; s < rowsPerProd; ++s) {
                const auto seq = static_cast<std::uint64_t>(p) * rowsPerProd + s;
                auto params = TParamsBuilder()
                    .AddParam("$row", MakeRow(static_cast<std::uint32_t>(p), seq))
                    .Build();
                const auto rt0 = std::chrono::steady_clock::now();
                auto status = client.RetryQuerySync(
                    [&fullPath, &params](NQuery::TSession session) {
                        return session.ExecuteQuery(
                            "DECLARE $row AS Struct<producer: Uint32, seq: Uint64, payload: Utf8>;"
                            "UPSERT INTO `" + fullPath + "` SELECT * FROM AS_TABLE([$row]);",
                            NQuery::TTxControl::BeginTx(NQuery::TTxSettings::SerializableRW())
                                .CommitTx(), params).GetValueSync();
                    });
                const auto rt1 = std::chrono::steady_clock::now();
                if (!status.IsSuccess()) failed.fetch_add(1, std::memory_order_relaxed);
                const auto ms = std::chrono::duration<double, std::milli>(rt1 - rt0).count();
                std::lock_guard g(latMu);
                latencies.push_back(ms);
            }
        });
    }
    for (auto& t : threads) t.join();
    const auto t1 = std::chrono::steady_clock::now();

    r.DurationSeconds = std::chrono::duration<double>(t1 - t0).count();
    r.Throughput = r.DurationSeconds > 0
        ? static_cast<double>(r.TotalRows) / r.DurationSeconds : 0;
    r.Failed = failed.load();
    r.RowLatencyMs = ComputeStats(latencies);
    return r;
}

// ---------------------------------------------------------------------
// Mode 2: manual fixed-size BulkUpsert.
// ---------------------------------------------------------------------
TResult RunManualBulk(NTable::TTableClient& client,
                      std::string_view fullPath,
                      const TConfig& cfg)
{
    TResult r;
    r.Mode = "manual_bulk";
    r.TotalRows = cfg.TotalRows;

    std::mutex latMu;
    std::vector<double> batchLatencies;

    std::atomic<std::size_t> failed{0};
    const int rowsPerProd = cfg.TotalRows / cfg.Producers;
    const int batchRows  = cfg.MaxBatchRows;

    const auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    threads.reserve(cfg.Producers);
    for (int p = 0; p < cfg.Producers; ++p) {
        threads.emplace_back([&, p] {
            for (int s = 0; s < rowsPerProd; s += batchRows) {
                const int n = std::min(batchRows, rowsPerProd - s);
                TValueBuilder builder;
                builder.BeginList();
                for (int i = 0; i < n; ++i) {
                    builder.AddListItem(MakeRow(
                        static_cast<std::uint32_t>(p),
                        static_cast<std::uint64_t>(p) * rowsPerProd + s + i));
                }
                builder.EndList();
                auto rows = builder.Build();
                const auto bt0 = std::chrono::steady_clock::now();
                auto status = client.RetryOperationSync([&](NTable::TTableClient& c) {
                    return c.BulkUpsert(fullPath, NYdb::TValue(rows)).GetValueSync();
                });
                const auto bt1 = std::chrono::steady_clock::now();
                if (!status.IsSuccess()) failed.fetch_add(1, std::memory_order_relaxed);
                const auto ms = std::chrono::duration<double, std::milli>(bt1 - bt0).count();
                std::lock_guard g(latMu);
                batchLatencies.push_back(ms);
            }
        });
    }
    for (auto& t : threads) t.join();
    const auto t1 = std::chrono::steady_clock::now();

    r.DurationSeconds = std::chrono::duration<double>(t1 - t0).count();
    r.Throughput = r.DurationSeconds > 0
        ? static_cast<double>(r.TotalRows) / r.DurationSeconds : 0;
    r.Failed = failed.load();
    r.BatchLatencyMs = ComputeStats(batchLatencies);
    return r;
}

// ---------------------------------------------------------------------
// Mode 3: TBatchWriter.
// ---------------------------------------------------------------------
TResult RunBatchWriter(NTable::TTableClient& client,
                       std::string_view fullPath,
                       const TConfig& cfg)
{
    TResult r;
    r.Mode = "batch_writer";
    r.TotalRows = cfg.TotalRows;

    std::mutex latMu;
    std::vector<double> rowLatencies;
    rowLatencies.reserve(cfg.TotalRows);

    std::atomic<std::size_t> failed{0};
    const int rowsPerProd = cfg.TotalRows / cfg.Producers;

    const auto t0 = std::chrono::steady_clock::now();
    {
        NBatch::TBatchWriter writer(client, fullPath, NBatch::TBatchWriterSettings()
            .MaxBatchRows(static_cast<std::size_t>(cfg.MaxBatchRows))
            .MaxBatchBytes(8u << 20)
            .FlushInterval(TDuration::MilliSeconds(cfg.FlushIntervalMs))
            .MaxQueueRows(static_cast<std::size_t>(cfg.TotalRows))
            .Concurrency(static_cast<std::size_t>(cfg.Concurrency)));

        std::vector<std::thread> threads;
        threads.reserve(cfg.Producers);
        for (int p = 0; p < cfg.Producers; ++p) {
            threads.emplace_back([&, p] {
                for (int s = 0; s < rowsPerProd; ++s) {
                    const auto seq = static_cast<std::uint64_t>(p) * rowsPerProd + s;
                    const auto rt0 = std::chrono::steady_clock::now();
                    writer.Append(MakeRow(static_cast<std::uint32_t>(p), seq))
                        .Subscribe([&latMu, &rowLatencies, &failed, rt0]
                                   (const NThreading::TFuture<TStatus>& f) {
                            const auto rt1 = std::chrono::steady_clock::now();
                            if (!f.GetValue().IsSuccess()) {
                                failed.fetch_add(1, std::memory_order_relaxed);
                            }
                            const auto ms =
                                std::chrono::duration<double, std::milli>(rt1 - rt0).count();
                            std::lock_guard g(latMu);
                            rowLatencies.push_back(ms);
                        });
                }
            });
        }
        for (auto& t : threads) t.join();
        writer.Close().Wait();
    }
    const auto t1 = std::chrono::steady_clock::now();

    r.DurationSeconds = std::chrono::duration<double>(t1 - t0).count();
    r.Throughput = r.DurationSeconds > 0
        ? static_cast<double>(r.TotalRows) / r.DurationSeconds : 0;
    r.Failed = failed.load();
    r.RowLatencyMs = ComputeStats(rowLatencies);
    return r;
}

void PrintReport(const std::vector<TResult>& results) {
    std::cout << "\n=========== Benchmark report ===========\n";
    std::cout << std::left << std::setw(14) << "mode"
              << std::setw(10) << "rows"
              << std::setw(8)  << "fail"
              << std::setw(11) << "dur(s)"
              << std::setw(13) << "rps"
              << std::setw(28) << "row p50/p95/p99 ms"
              << "batch p50/p95/p99 ms" << "\n";
    for (auto& r : results) {
        std::ostringstream rowLat;
        if (r.RowLatencyMs.Count > 0) {
            rowLat << std::fixed << std::setprecision(2)
                   << r.RowLatencyMs.P50 << "/"
                   << r.RowLatencyMs.P95 << "/"
                   << r.RowLatencyMs.P99;
        } else {
            rowLat << "-";
        }
        std::ostringstream batchLat;
        if (r.BatchLatencyMs.Count > 0) {
            batchLat << std::fixed << std::setprecision(2)
                     << r.BatchLatencyMs.P50 << "/"
                     << r.BatchLatencyMs.P95 << "/"
                     << r.BatchLatencyMs.P99;
        } else {
            batchLat << "-";
        }
        std::cout << std::left << std::setw(14) << r.Mode
                  << std::setw(10) << r.TotalRows
                  << std::setw(8)  << r.Failed
                  << std::setw(11) << std::fixed << std::setprecision(2) << r.DurationSeconds
                  << std::setw(13) << std::fixed << std::setprecision(0) << r.Throughput
                  << std::setw(28) << rowLat.str()
                  << batchLat.str() << "\n";
    }
    std::cout << "========================================\n";
}

bool Contains(std::string_view csv, std::string_view token) {
    std::stringstream ss(csv);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (item == token) return true;
    }
    return false;
}

} // namespace

int main(int argc, char** argv) {
    TConfig cfg;
    NLastGetopt::TOpts opts;
    opts.AddLongOption('e', "endpoint", "YDB endpoint")
        .DefaultValue(cfg.Endpoint).StoreResult(&cfg.Endpoint);
    opts.AddLongOption('d', "database", "YDB database")
        .DefaultValue(cfg.Database).StoreResult(&cfg.Database);
    opts.AddLongOption("table", "Demo table name")
        .DefaultValue(cfg.Table).StoreResult(&cfg.Table);
    opts.AddLongOption("total-rows", "Total rows per mode")
        .DefaultValue(std::to_string(cfg.TotalRows)).StoreResult(&cfg.TotalRows);
    opts.AddLongOption("producers", "Concurrent producer threads")
        .DefaultValue(std::to_string(cfg.Producers)).StoreResult(&cfg.Producers);
    opts.AddLongOption("max-batch-rows", "Batch size in rows")
        .DefaultValue(std::to_string(cfg.MaxBatchRows)).StoreResult(&cfg.MaxBatchRows);
    opts.AddLongOption("flush-interval-ms", "Flush interval (ms) for batch_writer mode")
        .DefaultValue(std::to_string(cfg.FlushIntervalMs)).StoreResult(&cfg.FlushIntervalMs);
    opts.AddLongOption("concurrency", "Inflight flushes for batch_writer")
        .DefaultValue(std::to_string(cfg.Concurrency)).StoreResult(&cfg.Concurrency);
    opts.AddLongOption("modes", "Comma-separated subset of: per_row, manual_bulk, batch_writer")
        .DefaultValue(cfg.Modes).StoreResult(&cfg.Modes);

    NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);

    if (cfg.Endpoint.rfind("grpc://", 0) == 0) cfg.Endpoint.erase(0, 7);
    else if (cfg.Endpoint.rfind("grpcs://", 0) == 0) cfg.Endpoint.erase(0, 8);

    auto driverConfig = TDriverConfig()
        .SetEndpoint(cfg.Endpoint)
        .SetDatabase(cfg.Database)
        .SetDiscoveryMode(EDiscoveryMode::Off);

    TDriver driver(driverConfig);
    NTable::TTableClient tableClient(driver);
    NQuery::TQueryClient queryClient(driver);

    const std::string fullPath = cfg.Database + "/" + cfg.Table;
    EnsureTable(tableClient, fullPath);

    std::vector<TResult> results;

    if (Contains(cfg.Modes, "per_row")) {
        TruncateTable(queryClient, fullPath);
        std::cout << "Running per_row..." << std::endl;
        results.push_back(RunPerRow(queryClient, fullPath, cfg));
    }
    if (Contains(cfg.Modes, "manual_bulk")) {
        TruncateTable(queryClient, fullPath);
        std::cout << "Running manual_bulk..." << std::endl;
        results.push_back(RunManualBulk(tableClient, fullPath, cfg));
    }
    if (Contains(cfg.Modes, "batch_writer")) {
        TruncateTable(queryClient, fullPath);
        std::cout << "Running batch_writer..." << std::endl;
        results.push_back(RunBatchWriter(tableClient, fullPath, cfg));
    }

    PrintReport(results);

    ThrowOnError(tableClient.RetryOperationSync([&](NTable::TSession session) {
        return session.DropTable(fullPath).GetValueSync();
    }));
    driver.Stop(true);
    return 0;
}
