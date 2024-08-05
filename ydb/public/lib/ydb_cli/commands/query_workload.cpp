#include "query_workload.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <library/cpp/histogram/hdr/histogram.h>
#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>
#include <mutex>

namespace NYdb {
namespace NConsoleClient {

namespace {

struct Stats
{
    size_t queries = 0;
    TDuration duration {};
    NHdr::THistogram histogram {600 * 1000 * 1000, 2}; /// 600 seconds

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
    if (stats.queries == 0 || stats.duration.MicroSeconds() == 0) {
        return;
    }

    Cout << "Queries executed: " << stats.queries << ", QPS " << stats.queries / (stats.duration.SecondsFloat());
    Cout << ", Average query time: " << stats.duration / stats.queries << '\n';

    Cout << '\n';

    Cout << "0.000%  " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(0)).SecondsFloat() << " sec.\n";

    for (size_t i = 1; i <= 9; ++i) {
        Cout << i * 10 << ".000% " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(i * 10.0)).SecondsFloat() << " sec.\n";
    }

    Cout << "99.000% " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(99.0)).SecondsFloat() << " sec.\n";
    Cout << "99.900% " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(99.9)).SecondsFloat() << " sec.\n";
    Cout << "99.990% " << TDuration::MicroSeconds(stats.histogram.GetValueAtPercentile(99.99)).SecondsFloat() << " sec.\n";
    Cout << '\n';
}

}

TCommandQueryWorkload::TCommandQueryWorkload()
    : TClientCommandTree("query", {}, "YDB query workload")
{
    AddCommand(std::make_unique<TCommandQueryWorkloadRun>());
}

TCommandQueryWorkloadRun::TCommandQueryWorkloadRun()
    : TYdbOperationCommand("run", {}, "Run YDB query workload")
{}

void TCommandQueryWorkloadRun::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption('q', "query", "Query to execute") .RequiredArgument("[String]").StoreResult(&Query);
    config.Opts->AddLongOption('t', "threads", "Number of parallel threads; 1 if not specified").DefaultValue(1).StoreResult(&Threads);
    config.Opts->AddLongOption('d', "delay", "Interval delay in seconds; 1 if not specified").DefaultValue(1).StoreResult(&IntervalSeconds);
}

int TCommandQueryWorkloadRun::Run(TConfig& config) {
    SetInterruptHandlers();

    TMutex statsMutex;
    BenchmarkStats stats;

    TThreadPool pool(TThreadPoolParams{});
    pool.Start(Threads);

    std::atomic<bool> ThreadTerminated = false;

    for (size_t i = 0; i < Threads; ++i) {
        pool.SafeAddFunc([&] {
            try {
                TDriver driver = CreateDriver(config);
                NScripting::TScriptingClient client(driver);

                NScripting::TExecuteYqlRequestSettings settings;
                settings.CollectQueryStats(NTable::ECollectQueryStatsMode::Basic);

                while (!IsInterrupted() && !ThreadTerminated.load()) {
                    auto asyncResult = client.StreamExecuteYqlScript(
                        Query,
                        FillSettings(settings)
                    );

                    auto result = asyncResult.GetValueSync();
                    ThrowOnError(result);

                    TDuration local_duration;

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
            } catch (const std::exception & ex) {
                if (ThreadTerminated.load()) {
                    return;
                }

                ThreadTerminated.store(true);

                TStringBuilder builder;
                builder << ex.what() << '\n';
                std::cerr << builder;
            }
        });
    }

    while (!IsInterrupted() && !ThreadTerminated.load()) {
        Sleep(TDuration::Seconds(IntervalSeconds));

        std::lock_guard<TMutex> lock(statsMutex);
        PrintStats(stats.windowStats);
        stats.ResetWindowStats();
    }

    pool.Stop();
    PrintStats(stats.totalStats);

    return EXIT_SUCCESS;
}

}
}
