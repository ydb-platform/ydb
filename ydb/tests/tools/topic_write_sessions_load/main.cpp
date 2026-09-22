#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/string/cast.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

using TClock = std::chrono::steady_clock;
using namespace std::chrono_literals;

volatile std::sig_atomic_t Interrupted = 0;

void Interrupt(int) {
    Interrupted = 1;
}

struct TOptions {
    std::string Endpoint;
    std::string Database;
    std::string Topic;
    std::string CaFile;
    ui32 Partition = 0;
    ui32 Rps = 0;
    ui32 Duration = 60;
    ui32 Hold = 60;
    ui32 ConnectTimeout = 300;
    ui64 MaxSessions = 100000;
    bool NormalWrite = false;

    TOptions(int argc, const char* argv[]) {
        auto opts = NLastGetopt::TOpts::Default();
        opts.SetTitle("Open and retain YDB Topic SDK write sessions. Test clusters only.");
        opts.AddLongOption("endpoint", "Cluster endpoint, grpc://host:port or grpcs://host:port")
            .Required().RequiredArgument("URL").StoreResult(&Endpoint);
        opts.AddLongOption("database", "Database path").Required().RequiredArgument("PATH").StoreResult(&Database);
        opts.AddLongOption("topic", "Existing topic path").Required().RequiredArgument("PATH").StoreResult(&Topic);
        opts.AddLongOption("partition", "Target partition").RequiredArgument("ID").StoreResult(&Partition);
        opts.AddLongOption("duration", "Seconds to create new sessions (default: 60)")
            .RequiredArgument("SECONDS").StoreResult(&Duration);
        opts.AddLongOption("hold", "Seconds to retain sessions after creation stops (default: 60)")
            .RequiredArgument("SECONDS").StoreResult(&Hold);
        opts.AddLongOption("max-sessions", "Maximum total creation attempts (default: 100000)")
            .RequiredArgument("COUNT").StoreResult(&MaxSessions);
        opts.AddLongOption("connect-timeout", "SDK connection timeout in seconds (default: 300)")
            .RequiredArgument("SECONDS").StoreResult(&ConnectTimeout);
        opts.AddLongOption("ca-file", "PEM CA certificates for TLS").RequiredArgument("PATH").StoreResult(&CaFile);
        opts.AddLongOption("normal-write", "Disable direct write for comparison").NoArgument().SetFlag(&NormalWrite);
        opts.SetFreeArgsNum(1);
        opts.SetFreeArgTitle(0, "RPS", "Number of new SDK write sessions per second");
        NLastGetopt::TOptsParseResultException result(&opts, argc, argv);
        Rps = FromString<ui32>(result.GetFreeArgs().at(0));
        if (!Rps || Rps > 1000000 || !Duration || !MaxSessions || !ConnectTimeout) {
            throw std::runtime_error("RPS must be 1..1000000; duration, max-sessions and connect-timeout must be positive");
        }
        if (!CaFile.empty() && Endpoint.rfind("grpcs://", 0) != 0) {
            throw std::runtime_error("--ca-file requires a grpcs:// endpoint");
        }
    }
};

struct TStats {
    std::atomic<ui64> Started{0};
    std::atomic<ui64> Pending{0};
    std::atomic<ui64> Initialized{0};
    std::atomic<ui64> InitFailed{0};
    std::atomic<ui64> Closed{0};
    std::atomic<ui64> InitMicros{0};
    std::atomic<bool> Stopping{false};
    std::mutex Mutex;
    std::map<int, ui64> CloseStatuses;
    std::string LastError;
};

struct TSessionState {
    std::atomic<bool> Closed{false};
    TClock::time_point Started = TClock::now();
};

struct TSession {
    std::shared_ptr<NYdb::NTopic::IWriteSession> Writer;
    std::shared_ptr<TSessionState> State;
};

void MarkClosed(const std::shared_ptr<TStats>& stats, const std::shared_ptr<TSessionState>& state) {
    if (!state->Closed.exchange(true) && !stats->Stopping) {
        ++stats->Closed;
    }
}

void OpenSession(NYdb::NTopic::TTopicClient& client, const TOptions& options, const std::string& prefix,
                 const std::shared_ptr<TStats>& stats, std::vector<TSession>& sessions) {
    const auto number = stats->Started.fetch_add(1);
    ++stats->Pending;
    const auto state = std::make_shared<TSessionState>();
    const auto producer = prefix + "-" + std::to_string(number);
    NYdb::NTopic::TWriteSessionSettings settings;
    settings.Path(options.Topic)
        .PartitionId(options.Partition)
        .ProducerId(producer)
        .MessageGroupId(producer)
        .DirectWriteToPartition(!options.NormalWrite)
        .ConnectTimeout(TDuration::Seconds(options.ConnectTimeout))
        .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());
    settings.EventHandlers_.SessionClosedHandler([stats, state](const NYdb::NTopic::TSessionClosedEvent& event) {
        MarkClosed(stats, state);
        if (!stats->Stopping) {
            std::lock_guard guard(stats->Mutex);
            ++stats->CloseStatuses[static_cast<int>(event.GetStatus())];
            stats->LastError = event.GetIssues().ToString().substr(0, 500);
        }
    });
    try {
        auto writer = client.CreateWriteSession(settings);
        // Buffer readiness is not server readiness: wait for the InitResponse.
        writer->GetInitSeqNo().Subscribe([stats, state](const auto& future) {
            --stats->Pending;
            try {
                future.GetValue();
                ++stats->Initialized;
                stats->InitMicros += std::chrono::duration_cast<std::chrono::microseconds>(TClock::now() - state->Started).count();
            } catch (const std::exception&) {
                if (!stats->Stopping) {
                    ++stats->InitFailed;
                }
                MarkClosed(stats, state);
            }
        });
        sessions.push_back({std::move(writer), state});
    } catch (const std::exception& error) {
        --stats->Pending;
        ++stats->InitFailed;
        MarkClosed(stats, state);
        std::lock_guard guard(stats->Mutex);
        stats->LastError = error.what();
    }
}

int Run(const TOptions& options) {
    auto config = NYdb::TDriverConfig(options.Endpoint)
        .SetDatabase(options.Database)
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");
    if (!options.CaFile.empty()) {
        config.UseSecureConnection(TFileInput(options.CaFile).ReadAll());
    }
    NYdb::TDriver driver(config);
    NYdb::NTopic::TTopicClient client(driver);
    auto description = client.DescribePartition(options.Topic, options.Partition);
    if (!description.Wait(TDuration::Seconds(30))) {
        throw std::runtime_error("DescribePartition timed out; no load was started");
    }
    const auto result = description.GetValueSync();
    if (!result.IsSuccess()) {
        throw std::runtime_error("DescribePartition failed: " + result.GetIssues().ToString());
    }

    const auto stats = std::make_shared<TStats>();
    std::vector<TSession> sessions;
    const std::string prefix = "write-session-load-" + std::string(CreateGuidAsString());
    const auto start = TClock::now();
    const auto end = start + std::chrono::seconds(options.Duration);
    const auto interval = std::chrono::nanoseconds(1000000000ULL / options.Rps);
    auto nextSession = start;
    auto nextReport = start + 1s;
    auto lastReport = start;
    ui64 previousStarted = 0;
    ui64 previousInitialized = 0;
    ui64 skipped = 0;

    std::cout << "mode=" << (options.NormalWrite ? "normal" : "direct") << " target_rps=" << options.Rps
              << " partition=" << options.Partition << " duration_s=" << options.Duration
              << " hold_s=" << options.Hold << " max_sessions=" << options.MaxSessions
              << " producer_prefix=" << prefix << " retries=off" << std::endl;

    auto report = [&](const char* phase) {
        sessions.erase(std::remove_if(sessions.begin(), sessions.end(), [](const TSession& session) {
            return session.State->Closed.load();
        }), sessions.end());
        const auto now = TClock::now();
        const double seconds = std::chrono::duration<double>(now - lastReport).count();
        const auto started = stats->Started.load();
        const auto initialized = stats->Initialized.load();
        std::cout << std::fixed << std::setprecision(1)
                  << "elapsed_s=" << std::chrono::duration<double>(now - start).count() << " phase=" << phase
                  << " started=" << started << " started_rps=" << (started - previousStarted) / seconds
                  << " initialized=" << initialized << " initialized_rps=" << (initialized - previousInitialized) / seconds
                  << " pending=" << stats->Pending << " retained=" << sessions.size()
                  << " init_failed=" << stats->InitFailed << " closed=" << stats->Closed
                  << " avg_init_ms=" << (initialized ? stats->InitMicros.load() / (1000.0 * initialized) : 0.0)
                  << " skipped_slots=" << skipped;
        {
            std::lock_guard guard(stats->Mutex);
            for (const auto& [status, count] : stats->CloseStatuses) {
                std::cout << " status_" << status << "=" << count;
            }
            if (!stats->LastError.empty()) {
                std::cout << " last_error=" << std::quoted(stats->LastError);
            }
        }
        std::cout << std::endl;
        previousStarted = started;
        previousInitialized = initialized;
        lastReport = now;
        nextReport = now + 1s;
    };

    while (!Interrupted && TClock::now() < end && stats->Started < options.MaxSessions) {
        if (TClock::now() >= nextReport) {
            report("opening");
        }
        if (TClock::now() >= nextSession) {
            OpenSession(client, options, prefix, stats, sessions);
            nextSession += interval;
            const auto now = TClock::now();
            // Do not turn client-side stalls into catch-up bursts.
            if (now >= nextSession) {
                const auto missed = (now - nextSession) / interval + 1;
                skipped += missed;
                nextSession += interval * missed;
            }
        }
        std::this_thread::sleep_until(std::min({nextSession, nextReport, end, TClock::now() + 50ms}));
    }
    const auto holdEnd = TClock::now() + std::chrono::seconds(options.Hold);
    report("holding");
    while (!Interrupted && TClock::now() < holdEnd) {
        if (TClock::now() >= nextReport) {
            report("holding");
        }
        std::this_thread::sleep_until(std::min({nextReport, holdEnd, TClock::now() + 50ms}));
    }
    report("final");
    const bool failed = stats->InitFailed || stats->Closed;
    stats->Stopping = true;
    std::cerr << "Closing " << sessions.size() << " retained sessions" << std::endl;
    for (auto& session : sessions) {
        session.Writer->Close(TDuration::Zero());
    }
    sessions.clear();
    driver.Stop(true);
    return Interrupted ? 130 : (failed ? 1 : 0);
}

} // namespace

int main(int argc, const char* argv[]) {
    std::signal(SIGINT, Interrupt);
    std::signal(SIGTERM, Interrupt);
    try {
        return Run(TOptions(argc, argv));
    } catch (const std::exception& error) {
        std::cerr << "Error: " << error.what() << std::endl;
        return 2;
    }
}
