#include "query_workload.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/plan2svg.h>
#include <ydb/public/lib/ydb_cli/common/progress_indication.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/histogram/hdr/histogram.h>

#include <util/system/mutex.h>
#include <util/system/thread.h>
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
    : TYdbSimpleCommand("run", {}, "Run YDB query workload")
{}

void TCommandQueryWorkloadRun::Config(TConfig& config) {
    TYdbSimpleCommand::Config(config);
    config.Opts->AddLongOption('q', "query", "Query to execute").RequiredArgument("[String]").StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "Path to a file containing the query text to execute. "
        "The path '-' means reading the query text from stdin.").RequiredArgument("PATH").StoreResult(&QueryFile);
    config.Opts->AddLongOption("plan", "Query plans report file name")
        .DefaultValue("")
        .StoreResult(&PlanFileName);
    config.Opts->AddLongOption('t', "threads", "Number of parallel threads; 1 if not specified").DefaultValue(1).StoreResult(&Threads);
    config.Opts->AddLongOption('d', "delay", "Interval delay in seconds; 1 if not specified").DefaultValue(1).StoreResult(&IntervalSeconds);
}

void TCommandQueryWorkloadRun::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    if (Query && QueryFile) {
        throw TMisuseException() << "Both mutually exclusive options \"Text of query\" (\"--query\", \"-q\") "
            << "and \"Path to file with query text\" (\"--file\", \"-f\") were provided.";
    }
    if (QueryFile) {
        if (QueryFile == "-") {
            if (IsStdinInteractive()) {
                throw TMisuseException() << "Path to file with query text is \"-\", meaning that query text should be read "
                    "from stdin. This is only available in non-interactive mode";
            }
            Query = Cin.ReadAll();
        } else {
            Query = ReadFromFile(QueryFile, "query");
        }
    }
    if (Query.empty()) {
        Cerr << "Neither text of query (\"--query\", \"-q\") "
            << "nor path to file with query text (\"--file\", \"-f\") were provided." << Endl;
        config.PrintHelpAndExit();
    }
    if (Threads > 1 && PlanFileName) {
        throw TMisuseException() << "The query plan report (\"--plan\") is supported with only one thread (\"--t\", \"--threads\")." << Endl;
    }
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
                NQuery::TQueryClient client(driver);

                NQuery::TExecuteQuerySettings settings;

                auto statsMode = NQuery::EStatsMode::Basic;
                if (PlanFileName) {
                    statsMode = NQuery::EStatsMode::Full;
                }
                settings.StatsMode(statsMode);

                std::optional<TProgressIndication> progressIndication;

                if (Threads == 1) {
                    if (statsMode == NQuery::EStatsMode::Basic) {
                        settings.StatsCollectPeriod(std::chrono::milliseconds(500));
                    } else {
                        settings.StatsCollectPeriod(std::chrono::milliseconds(3000));
                    }
                    progressIndication = TProgressIndication();
                }

                TString currentPlanFileNameStats;
                TString currentPlanWithStatsFileName;
                TString currentPlanWithStatsFileNameJson;
                if (PlanFileName) {
                    currentPlanFileNameStats = TStringBuilder() << PlanFileName << ".stats";
                    currentPlanWithStatsFileName = TStringBuilder() << PlanFileName << ".svg";
                    currentPlanWithStatsFileNameJson = TStringBuilder() << PlanFileName << ".json";
                }

                while (!IsInterrupted() && !ThreadTerminated.load()) {
                    auto asyncResult = client.StreamExecuteQuery(
                        Query,
                        NQuery::TTxControl::NoTx(),
                        FillSettings(settings)
                    );

                    auto result = asyncResult.GetValueSync();
                    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

                    TDuration local_duration;

                    while (!IsInterrupted())
                    {
                        auto streamPart = result.ReadNext().GetValueSync();
                        if (ThrowOnErrorAndCheckEOS(streamPart)) {
                            break;
                        }

                        if (streamPart.GetStats()) {
                            const auto& queryStats = streamPart.GetStats().value();
                            local_duration += queryStats.GetTotalDuration();

                            if (PlanFileName) {
                                TFileOutput out(currentPlanFileNameStats);
                                out << queryStats.ToString();
                                {
                                    auto plan = queryStats.GetPlan();
                                    if (plan) {
                                        {
                                            TPlanVisualizer pv;
                                            TFileOutput out(currentPlanWithStatsFileName);
                                            try {
                                                pv.LoadPlans(*queryStats.GetPlan());
                                                out << pv.PrintSvg();
                                            } catch (std::exception& e) {
                                                out << "<svg width='1024' height='256' xmlns='http://www.w3.org/2000/svg'><text>" << e.what() << "<text></svg>";
                                            }
                                        }
                                        {
                                            TFileOutput out(currentPlanWithStatsFileNameJson);
                                            TQueryPlanPrinter queryPlanPrinter(EDataFormat::JsonBase64, true, out, 120);
                                            queryPlanPrinter.Print(*queryStats.GetPlan());
                                        }
                                    }
                                }
                            }

                            if (progressIndication) {
                                const auto& protoStats = TProtoAccessor::GetProto(queryStats);
                                for (const auto& queryPhase : protoStats.query_phases()) {
                                    for (const auto& tableAccessStats : queryPhase.table_access()) {
                                        progressIndication->UpdateProgress({tableAccessStats.reads().rows(), tableAccessStats.reads().bytes()});
                                    }
                                }
                                progressIndication->SetDurationUs(protoStats.total_duration_us());

                                progressIndication->Render();
                            }
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
