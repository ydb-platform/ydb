#include "ydb_latency.h"

#include <ydb/public/sdk/cpp/client/ydb_debug/client.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>

#include <library/cpp/histogram/hdr/histogram.h>

#include <util/generic/serialized_enum.h>
#include <util/system/hp_timer.h>

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

namespace NYdb::NConsoleClient {

namespace {

constexpr int DEFAULT_WARMUP_SECONDS = 1;
constexpr int DEFAULT_INTERVAL_SECONDS = 5;
constexpr int DEFAULT_MAX_INFLIGHT = 128;
constexpr int DEFAULT_PERCENTILE = 99.0;

constexpr TCommandLatency::EFormat DEFAULT_FORMAT = TCommandLatency::EFormat::Plain;
constexpr TCommandPing::EPingKind DEFAULT_RUN_KIND = TCommandPing::EPingKind::AllKinds;

// 1-16, 32, 64, ...
constexpr int INCREMENT_UNTIL_THREAD_COUNT = 16;

const TString QUERY = "SELECT 1;";

// Factory returns callable, which makes requests to the DB.
// In case of QueryService, this callable contains associated session.
using TRequestMaker = std::function<bool()>;
using TCallableFactory = std::function<TRequestMaker()>;

struct TResult {
    TCommandPing::EPingKind Kind;
    int ThreadCount = 0;
    int LatencyUs = 0;
    int Throughput = 0;
};

struct alignas(64) TEvaluateResult {
    TEvaluateResult()
        : LatencyHistogramUs(1, 1024, 5)
    {
    }

    ui64 OkCount = 0;
    ui64 ErrorCount = 0;
    int LatencyUs = 0;

    NHdr::THistogram LatencyHistogramUs;
};

void Evaluate(
    TEvaluateResult& total,
    ui64 warmupSeconds,
    ui64 intervalSeconds,
    int threadCount,
    TCallableFactory factory,
    double percentile)
{
    std::atomic<bool> startMeasure{false};
    std::atomic<bool> stop{false};

    // May delay for ~50ms to compute frequency, better to delay here
    volatile auto clockRate = NHPTimer::GetClockRate();
    Y_UNUSED(clockRate);

    auto timer = std::thread([&startMeasure, &stop, warmupSeconds, intervalSeconds]() {
        std::this_thread::sleep_for(std::chrono::seconds(warmupSeconds));
        startMeasure.store(true, std::memory_order_relaxed);

        std::this_thread::sleep_for(std::chrono::seconds(intervalSeconds));
        stop.store(true, std::memory_order_relaxed);
    });

    std::vector<TEvaluateResult> results(threadCount);
    std::vector<std::thread> threads;

    for (int i = 0; i < threadCount; ++i) {
        threads.emplace_back([i, &results, &startMeasure, &stop, &factory]() {
            auto& result = results[i];

            THPTimer timer;

            TRequestMaker requester;
            try {
                requester = factory();
            } catch (yexception ex) {
                Cerr << "Failed to create request maker: " << ex.what() << Endl;
                return;
            }

            while (!startMeasure.load(std::memory_order_relaxed)) {
                try {
                    requester();
                } catch (...) {
                    continue;
                }
            }

            while (!stop.load(std::memory_order_relaxed)) {
                try {
                    timer.Reset();
                    if (requester()) {
                        int usecPassed = static_cast<int>(timer.Passed() * 1'000'000);
                        result.LatencyHistogramUs.RecordValue(usecPassed);
                        ++result.OkCount;
                    } else {
                        ++result.ErrorCount;
                    }
                } catch (...) {
                    ++result.ErrorCount;
                }
            }
        });
    }

    timer.join();
    for (auto& thread : threads) {
        thread.join();
    }

    for (const auto& result: results) {
        total.OkCount += result.OkCount;
        total.ErrorCount += result.ErrorCount;
        total.LatencyHistogramUs.Add(result.LatencyHistogramUs);
    }

    total.LatencyUs = total.LatencyHistogramUs.GetValueAtPercentile(percentile);
}

} // anonymous

TCommandLatency::TCommandLatency()
    : TYdbCommand("latency", {}, "Check basic latency with variable inflight")
    , IntervalSeconds(DEFAULT_INTERVAL_SECONDS)
    , MaxInflight(DEFAULT_MAX_INFLIGHT)
    , Format(DEFAULT_FORMAT)
    , RunKind(DEFAULT_RUN_KIND)
    , Percentile(DEFAULT_PERCENTILE)
{}

void TCommandLatency::Config(TConfig& config) {
    TYdbCommand::Config(config);

    const TString& availableKinds = GetEnumAllNames<TCommandPing::EPingKind>();
    const TString& availableFormats = GetEnumAllNames<TCommandLatency::EFormat>();

    config.Opts->AddLongOption(
        'i', "interval", TStringBuilder() << "Seconds for each latency kind")
            .RequiredArgument("INT").StoreResult(&IntervalSeconds).DefaultValue(DEFAULT_INTERVAL_SECONDS);
    config.Opts->AddLongOption(
        'm', "max-inflight", TStringBuilder() << "Max inflight")
            .RequiredArgument("INT").StoreResult(&MaxInflight).DefaultValue(DEFAULT_MAX_INFLIGHT);
    config.Opts->AddLongOption(
        'p', "percentile", TStringBuilder() << "Latency percentile")
            .RequiredArgument("DOUBLE").StoreResult(&Percentile).DefaultValue(DEFAULT_PERCENTILE);
    config.Opts->AddLongOption(
        'f', "format", TStringBuilder() << "Output format. Available options: " << availableFormats)
            .OptionalArgument("STRING").StoreResult(&Format).DefaultValue(DEFAULT_FORMAT);
    config.Opts->AddLongOption(
        'k', "kind", TStringBuilder() << "Use only specified ping kind. Available options: "<< availableKinds)
            .OptionalArgument("STRING").StoreResult(&RunKind).DefaultValue(DEFAULT_RUN_KIND);
}

void TCommandLatency::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandLatency::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);

    SetInterruptHandlers();

    auto debugClient = std::make_shared<NDebug::TDebugClient>(driver);
    auto queryClient = std::make_shared<NQuery::TQueryClient>(driver);

    auto plainGrpcPingFactory = [debugClient] () {
        return [debugClient] () {
            return TCommandPing::PingPlainGrpc(*debugClient);
        };
    };

    auto grpcPingFactory = [debugClient] () {
        return [debugClient] () {
            return TCommandPing::PingGrpcProxy(*debugClient);
        };
    };

    auto plainKqpPingFactory = [debugClient] () {
        return [debugClient] () {
            return TCommandPing::PingPlainKqp(*debugClient);
        };
    };

    auto schemeCachePingFactory = [debugClient] () {
        return [debugClient] () {
            return TCommandPing::PingSchemeCache(*debugClient);
        };
    };

    auto txProxyPingFactory = [debugClient] () {
        return [debugClient] () {
            return TCommandPing::PingTxProxy(*debugClient);
        };
    };

    auto select1Factory = [queryClient] () {
        // note, that each thread has own session
        auto session = std::make_shared<NQuery::TSession>(queryClient->GetSession().GetValueSync().GetSession());
        return [session] () {
            return TCommandPing::PingKqpSelect1(*session, QUERY);
        };
    };

    using TTaskPair = std::pair<TCommandPing::EPingKind, TCallableFactory>;
    const std::vector<TTaskPair> allTasks = {
        { TCommandPing::EPingKind::PlainGrpc, plainGrpcPingFactory },
        { TCommandPing::EPingKind::GrpcProxy, grpcPingFactory },
        { TCommandPing::EPingKind::PlainKqp, plainKqpPingFactory },
        { TCommandPing::EPingKind::Select1, select1Factory },
        { TCommandPing::EPingKind::SchemeCache, schemeCachePingFactory },
        { TCommandPing::EPingKind::TxProxy, txProxyPingFactory },
    };

    std::vector<TTaskPair> runTasks;
    if (RunKind == TCommandPing::EPingKind::AllKinds) {
        runTasks = allTasks;
    } else {
        for (size_t i = 0; i < allTasks.size(); ++i) {
            if (allTasks[i].first == RunKind) {
                runTasks = { allTasks[i] };
                break;
            }
        }
    }

    if (runTasks.empty()) {
        return -1; // sanity check, never happens
    }

    std::vector<TResult> results;
    for (const auto& [taskKind, factory]: runTasks) {
        for (int threadCount = 1; threadCount <= MaxInflight && !IsInterrupted(); ) {
            TEvaluateResult result;
            Evaluate(result, DEFAULT_WARMUP_SECONDS, IntervalSeconds, threadCount, factory, Percentile);

            bool skip = false;
            if (result.ErrorCount) {
                auto totalRequests = result.ErrorCount + result.OkCount;
                double errorsPercent = 100.0 * result.ErrorCount / totalRequests;
                if (errorsPercent >= 1) {
                    Cerr << "Skipping " << taskKind << ", threads=" << threadCount
                        << ": error rate=" << errorsPercent << "%" << Endl;
                    skip = true;
                }
            }

            if (!skip) {
                ui64 throughput = result.OkCount / IntervalSeconds;
                ui64 throughputPerThread = throughput / threadCount;
                ui64 latencyUsec = result.LatencyUs;

                results.emplace_back(taskKind, threadCount, latencyUsec, throughput);

                if (Format == EFormat::Plain) {
                    Cout << taskKind << " threads=" << threadCount
                        << ", throughput: " << throughput
                        << ", per thread: " << throughputPerThread
                        << ", latency p" << Percentile << " usec: " << latencyUsec
                        << ", ok: " << result.OkCount
                        << ", error: " << result.ErrorCount << Endl;
                }
            }

            if (threadCount < INCREMENT_UNTIL_THREAD_COUNT) {
                ++threadCount;
            } else {
                threadCount *= 2;
            }
        }
    }

    if (Format == EFormat::Plain) {
        return 0;
    }

    TMap<TCommandPing::EPingKind, std::vector<int>> latencies;
    TMap<TCommandPing::EPingKind, std::vector<int>> throughputs;
    for (const auto& result: results) {
        latencies[result.Kind].push_back(result.LatencyUs);
        throughputs[result.Kind].push_back(result.Throughput);
    }

    if (Format == EFormat::CSV) {
        const int maxThreadsMeasured = results.back().ThreadCount;

        Cout << Endl;
        Cout << "Latencies" << Endl;

        TStringStream ss;
        ss << "Kind";
        for (int i = 1; i <= maxThreadsMeasured;) {
            ss << "," << i;
            if (i < INCREMENT_UNTIL_THREAD_COUNT) {
                ++i;
            } else {
                i *= 2;
            }
        }
        ss << Endl;
        TString header = ss.Str();

        Cout << header;
        for (const auto& [kind, vec]: latencies) {
            Cout << kind;
            for (auto value: vec) {
                Cout << "," << value;
            }
            Cout << Endl;
        }
        Cout << Endl;

        Cout << "Througputs" << Endl;
        Cout << header;
        for (const auto& [kind, vec]: throughputs) {
            Cout << kind;
            for (auto value: vec) {
                Cout << "," << value;
            }
            Cout << Endl;
        }
    }

    return 0;
}

} // NYdb::NConsoleClient
