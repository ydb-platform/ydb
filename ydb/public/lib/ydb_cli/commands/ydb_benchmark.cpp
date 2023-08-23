#include "ydb_benchmark.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <library/cpp/histogram/hdr/histogram.h>
#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>

namespace NYdb {
namespace NConsoleClient {

namespace {

struct Stats
{
    size_t queries = 0;
    TDuration duration {};
    NHdr::THistogram histogram {60 * 1000 * 1000, 2};

    void Record(TDuration value) {
        ++queries;
        duration += value;
        histogram.RecordValue(value.MicroSeconds());
    }

    void Reset() {
        queries = 0;
        duration = {};
        histogram.Reset();
    }
};

struct BenchmarkStats
{
    Stats totalStats;
    Stats windowStats;

    void Record(TDuration value) {
        totalStats.Record(value);
        windowStats.Record(value);
    }

    void ResetWindowStats() {
        windowStats.Reset();
    }
};

void PrintStats(const Stats & stats) {
    if (stats.queries == 0) {
        return;
    }

    Cout << "Queries executed: " << stats.queries << " QPS " << stats.queries / (stats.duration.MicroSeconds() * 1e-6) << ' ';
    Cout << "Average query time: " << stats.duration / stats.queries << '\n';

    Cout << '\n';

    Cout << "0.000%  " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(0)) << '\n';

    for (size_t i = 1; i < 9; ++i) {
        Cout << i * 10 << ".000% " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(i * 10.0)) << '\n';
    }

    Cout << "99.000% " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(99.0)) << '\n';
    Cout << "99.900% " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(99.9)) << '\n';
    Cout << "99.990% " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(99.99)) << '\n';
    Cout << '\n';
}

}

TCommandBenchmark::TCommandBenchmark()
    : TYdbOperationCommand("benchmark", {}, "Perform benchmark of specified query")
{}

void TCommandBenchmark::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption('q', "query", "Query to execute") .RequiredArgument("[String]").StoreResult(&Query);
    config.Opts->AddLongOption('t', "threads", "Number of parallel threads; 1 if not specified").DefaultValue(1).StoreResult(&Threads);
    config.Opts->AddLongOption('d', "delay", "Interval delay in seconds; 1 if not specified").DefaultValue(1).StoreResult(&IntervalSeconds);
}

int TCommandBenchmark::Run(TConfig& config) {
    SetInterruptHandlers();

    TMutex statsMutex;
    BenchmarkStats stats;

    TThreadPool pool(TThreadPoolParams{});
    pool.Start(Threads);

    for (size_t i = 0; i < Threads; ++i) {
        pool.SafeAddFunc([&] {
            TDriver driver = CreateDriver(config);
            NScripting::TScriptingClient client(driver);

            NScripting::TExecuteYqlRequestSettings settings;
            settings.CollectQueryStats(NTable::ECollectQueryStatsMode::Basic);

            while (!IsInterrupted()) {
                TDuration local_duration;

                auto asyncResult = client.StreamExecuteYqlScript(
                    Query,
                    FillSettings(settings)
                );

                auto result = asyncResult.GetValueSync();
                ThrowOnError(result);

                while (!IsInterrupted())
                {
                    auto streamPart = result.ReadNext().GetValueSync();
                    if (!streamPart.IsSuccess()) {
                        if (streamPart.EOS()) {
                            break;
                        }

                        ThrowOnError(streamPart);
                    }

                    if (streamPart.HasQueryStats()) {
                        const auto& queryStats = streamPart.GetQueryStats();
                        local_duration += queryStats.GetTotalDuration();
                    }
                }

                std::lock_guard<TMutex> lock(statsMutex);
                stats.Record(local_duration);
            }
        });
    }

    while (!IsInterrupted()) {
        Sleep(TDuration::Seconds(IntervalSeconds));

        std::lock_guard<TMutex> lock(statsMutex);
        if (stats.windowStats.queries == 0)
            continue;

        PrintStats(stats.windowStats);
        stats.ResetWindowStats();
    }

    PrintStats(stats.totalStats);

    pool.Stop();

    return EXIT_SUCCESS;
}

}
}

