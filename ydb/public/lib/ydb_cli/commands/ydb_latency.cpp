#include "ydb_latency.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/debug/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/histogram/hdr/histogram.h>

#include <util/generic/serialized_enum.h>
#include <util/system/info.h>
#include <util/system/hp_timer.h>

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

namespace NYdb::NConsoleClient {

namespace {

constexpr int DEFAULT_WARMUP_SECONDS = 1;
constexpr int DEFAULT_INTERVAL_SECONDS = 5;
constexpr int DEFAULT_MIN_INFLIGHT = 1;
constexpr int DEFAULT_MAX_INFLIGHT = 128;
const std::vector<double> DEFAULT_PERCENTILES = {50.0, 90, 99.0};

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
    TResult() = default;

    TResult(TCommandPing::EPingKind kind, int threadCount, NHdr::THistogram&& hist, int throughput)
        : Kind(kind)
        , ThreadCount(threadCount)
        , LatencyHistogramUs(std::move(hist))
        , Throughput(throughput)
    {
    }

    TCommandPing::EPingKind Kind;
    int ThreadCount = 0;
    NHdr::THistogram LatencyHistogramUs;
    int Throughput = 0;
};

struct alignas(64) TEvaluateResult {
    TEvaluateResult()
        : LatencyHistogramUs(1, 1024, 5)
    {
    }

    ui64 OkCount = 0;
    ui64 ErrorCount = 0;
    NHdr::THistogram LatencyHistogramUs;
};

void Evaluate(
    TEvaluateResult& total,
    ui64 warmupSeconds,
    ui64 intervalSeconds,
    int threadCount,
    TCallableFactory factory)
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
                } catch (yexception ex) {
                    TStringStream ss;
                    ss << "Failed to perform request: " << ex.what() << Endl;
                    Cerr << ss.Str();
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
}

} // anonymous

TCommandLatency::TCommandLatency()
    : TYdbCommand("latency", {}, "Check basic latency with variable inflight")
    , IntervalSeconds(DEFAULT_INTERVAL_SECONDS)
    , MinInflight(DEFAULT_MIN_INFLIGHT)
    , MaxInflight(DEFAULT_MAX_INFLIGHT)
    , Format(DEFAULT_FORMAT)
    , RunKind(DEFAULT_RUN_KIND)
    , ChainConfig(new NDebug::TActorChainPingSettings())
{}

TCommandLatency::~TCommandLatency() {
}

void TCommandLatency::Config(TConfig& config) {
    TYdbCommand::Config(config);

    const TString& availableKinds = GetEnumAllNames<TCommandPing::EPingKind>();
    const TString& availableFormats = GetEnumAllNames<TCommandLatency::EFormat>();

    config.Opts->AddLongOption(
        'i', "interval", TStringBuilder() << "Seconds for each latency kind")
            .RequiredArgument("INT").StoreResult(&IntervalSeconds).DefaultValue(DEFAULT_INTERVAL_SECONDS);
    config.Opts->AddLongOption(
        "min-inflight", TStringBuilder() << "Min inflight")
            .RequiredArgument("INT").StoreResult(&MinInflight).DefaultValue(DEFAULT_MIN_INFLIGHT);
    config.Opts->AddLongOption(
        'm', "max-inflight", TStringBuilder() << "Max inflight")
            .RequiredArgument("INT").StoreResult(&MaxInflight).DefaultValue(DEFAULT_MAX_INFLIGHT);
    config.Opts->AddLongOption(
        'p', "percentile", TStringBuilder() << "Latency percentile")
            .RequiredArgument("DOUBLE").AppendTo(&Percentiles);
    config.Opts->AddLongOption(
        'f', "format", TStringBuilder() << "Output format. Available options: " << availableFormats)
            .OptionalArgument("STRING").StoreResult(&Format).DefaultValue(DEFAULT_FORMAT);
    config.Opts->AddLongOption(
        'k', "kind", TStringBuilder() << "Use only specified ping kind. Available options: " << availableKinds)
            .OptionalArgument("STRING").StoreResult(&RunKind).DefaultValue(DEFAULT_RUN_KIND);

    // actor chain options
    config.Opts->AddLongOption(
        "chain-length", TStringBuilder() << "Chain length (ActorChain kind only)")
            .OptionalArgument("INT").StoreResult(&ChainConfig->ChainLength_).DefaultValue(ChainConfig->ChainLength_);
    config.Opts->AddLongOption(
        "chain-work-duration", TStringBuilder() << "Duration of work in usec for each actor in the chain (ActorChain kind only)")
            .OptionalArgument("INT").StoreResult(&ChainConfig->WorkUsec_).DefaultValue(ChainConfig->WorkUsec_);
    config.Opts->AddLongOption(
        "no-tail-chain", TStringBuilder() << "Don't use Tail sends and registrations (ActorChain kind only)")
            .StoreTrue(&ChainConfig->NoTailChain_).DefaultValue(ChainConfig->NoTailChain_);
}

void TCommandLatency::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if (MaxInflight >= 2) {
        config.UsePerChannelTcpConnection = true;
    }

    if (Percentiles.empty()) {
        Percentiles = DEFAULT_PERCENTILES;
    }
}

int TCommandLatency::Run(TConfig& config) {
    SetInterruptHandlers();

    const size_t cpuCount = NSystemInfo::CachedNumberOfCpus();
    const size_t driverCount = std::min(MaxInflight, int(cpuCount));

    std::vector<TDriver> drivers;
    for (size_t i = 0; i < driverCount; ++i) {
        drivers.emplace_back(CreateDriver(config));
    }

    // share driver in RR manner
    std::atomic<size_t> currentDriver{0};
    auto getDebugClient = [&currentDriver, &drivers, driverCount] () {
        auto driverIndex = currentDriver.fetch_add(1, std::memory_order_relaxed) % driverCount;
        auto debugClient = std::make_shared<NDebug::TDebugClient>(drivers[driverIndex]);
        return debugClient;
    };
    auto getqueryClient = [&currentDriver, &drivers, driverCount] () {
        auto driverIndex = currentDriver.fetch_add(1, std::memory_order_relaxed) % driverCount;
        auto queryClient = std::make_shared<NQuery::TQueryClient>(drivers[driverIndex]);
        return queryClient;
    };

    // note that each thread (normally) will have own driver: we enforce each thread to has own gRPC channel and own
    // TCP connection (config.UsePerChannelTcpConnection set). This helps to avoid bottleneck here, in the client,
    // in case of low latency network between the client and the server

    auto plainGrpcPingFactory = [&getDebugClient] () {
        auto debugClient = getDebugClient();
        return [debugClient] () {
            return TCommandPing::PingPlainGrpc(*debugClient);
        };
    };

    auto grpcPingFactory = [&getDebugClient] () {
        auto debugClient = getDebugClient();
        return [debugClient] () {
            return TCommandPing::PingGrpcProxy(*debugClient);
        };
    };

    auto plainKqpPingFactory = [&getDebugClient] () {
        auto debugClient = getDebugClient();
        return [debugClient] () {
            return TCommandPing::PingPlainKqp(*debugClient);
        };
    };

    auto schemeCachePingFactory = [&getDebugClient] () {
        auto debugClient = getDebugClient();
        return [debugClient] () {
            return TCommandPing::PingSchemeCache(*debugClient);
        };
    };

    auto txProxyPingFactory = [&getDebugClient] () {
        auto debugClient = getDebugClient();
        return [debugClient] () {
            return TCommandPing::PingTxProxy(*debugClient);
        };
    };

    auto select1Factory = [&getqueryClient] () {
        auto queryClient = getqueryClient();
        // note, that each thread has own session (as well as queryClient / connection)
        auto session = std::make_shared<NQuery::TSession>(queryClient->GetSession().GetValueSync().GetSession());
        return [session] () {
            return TCommandPing::PingKqpSelect1(*session, QUERY);
        };
    };

    auto chainConfig = *ChainConfig;
    auto txActorChainPingFactory = [&getDebugClient, chainConfig] () {
        auto debugClient = getDebugClient();
        return [debugClient, chainConfig] () {
            return TCommandPing::PingActorChain(*debugClient, chainConfig);
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
        { TCommandPing::EPingKind::ActorChain, txActorChainPingFactory },
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
        for (int threadCount = MinInflight; threadCount <= MaxInflight && !IsInterrupted(); ) {
            TEvaluateResult result;
            Evaluate(result, DEFAULT_WARMUP_SECONDS, IntervalSeconds, threadCount, factory);

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

                if (Format == EFormat::Plain) {
                    Cout << taskKind << " threads=" << threadCount
                        << ", throughput: " << throughput
                        << ", per thread: " << throughputPerThread;
                    for (size_t i = 0; i < Percentiles.size(); ++i) {
                        Cout << ", p" << Percentiles[i]
                            << " usec: " << result.LatencyHistogramUs.GetValueAtPercentile(Percentiles[i]);
                    }
                    Cout << ", ok: " << result.OkCount << ", error: " << result.ErrorCount << Endl;
                }

                // note the move
                results.emplace_back(taskKind, threadCount, std::move(result.LatencyHistogramUs), throughput);
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

    TMap<TCommandPing::EPingKind, std::vector<const NHdr::THistogram*>> latencies;
    TMap<TCommandPing::EPingKind, std::vector<int>> throughputs;
    for (const auto& result: results) {
        latencies[result.Kind].push_back(&result.LatencyHistogramUs);
        throughputs[result.Kind].push_back(result.Throughput);
    }

    if (Format == EFormat::CSV) {
        const int maxThreadsMeasured = results.back().ThreadCount;

        Cout << Endl;

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

        for (auto percentile: Percentiles) {
            Cout << "Latencies, p" << percentile << Endl;

            Cout << header;
            for (const auto& [kind, histVec]: latencies) {
                Cout << kind;
                for (const auto& hist: histVec) {
                    Cout << "," << hist->GetValueAtPercentile(percentile);
                }
                Cout << Endl;
            }
            Cout << Endl;
        }

        Cout << "Throughputs" << Endl;
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
