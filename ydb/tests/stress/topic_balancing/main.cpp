#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>
#include <util/stream/output.h>
#include <util/system/mutex.h>

#include <atomic>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace NYdb;
using namespace NYdb::NTopic;

namespace {

struct TOptions {
    std::string Endpoint = "localhost:2135";
    std::string Database = "/Root";
    std::string Path = "topic_balancing";
    std::string Consumer = "shared-consumer";
    ui32 DurationSeconds = 1'000'000'000;
    ui32 PartitionCount = 1024;
    ui32 MaxSessions = 2048;
    ui32 Threads = 8;
    ui32 WarmupSeconds = 15;
    TDuration MaxLag = TDuration::Seconds(1);

    TOptions(int argc, const char* argv[]) {
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption('h');
        opts.AddLongOption("endpoint", "YDB endpoint").RequiredArgument("HOST:PORT")
            .StoreResult(&Endpoint);
        opts.AddLongOption("database", "YDB database").RequiredArgument("PATH")
            .StoreResult(&Database);
        opts.AddLongOption("path", "Topic path prefix").RequiredArgument("PATH")
            .StoreResult(&Path);
        opts.AddLongOption("consumer", "Consumer name").RequiredArgument("NAME")
            .StoreResult(&Consumer);
        opts.AddLongOption("duration", "Workload duration in seconds").RequiredArgument("SECONDS")
            .StoreResult(&DurationSeconds);
        opts.AddLongOption("partitions", "Topic partition count").RequiredArgument("COUNT")
            .StoreResult(&PartitionCount);
        opts.AddLongOption("max-sessions", "Upper bound for random live read session count").RequiredArgument("COUNT")
            .StoreResult(&MaxSessions);
        opts.AddLongOption("threads", "Worker threads that open and close sessions").RequiredArgument("COUNT")
            .StoreResult(&Threads);
        opts.AddLongOption("warmup", "Seconds to wait until every partition is assigned at least once").RequiredArgument("SECONDS")
            .StoreResult(&WarmupSeconds);
        ui32 maxLagMs = 1000;
        opts.AddLongOption("max-lag-ms", "Max age of the last active session per partition").RequiredArgument("MS")
            .StoreResult(&maxLagMs);
        opts.SetFreeArgsNum(0);
        NLastGetopt::TOptsParseResult res(&opts, argc, argv);

        MaxLag = TDuration::MilliSeconds(maxLagMs);
        Y_ENSURE(PartitionCount > 0, "partitions must be > 0");
        Y_ENSURE(Threads > 0, "threads must be > 0");
        Y_ENSURE(MaxLag > TDuration::Zero(), "max-lag-ms must be > 0");
    }
};

std::string NormalizeEndpoint(std::string endpoint) {
    static const std::string GrpcPrefix = "grpc://";
    static const std::string GrpcsPrefix = "grpcs://";
    if (endpoint.rfind(GrpcPrefix, 0) == 0) {
        endpoint.erase(0, GrpcPrefix.size());
    } else if (endpoint.rfind(GrpcsPrefix, 0) == 0) {
        endpoint.erase(0, GrpcsPrefix.size());
    }
    return endpoint;
}

std::string MakeTopicPath(const std::string& database, const std::string& path) {
    if (path.empty()) {
        return database + "/topic_balancing";
    }
    if (path[0] == '/') {
        return path;
    }
    if (!database.empty() && database.back() == '/') {
        return database + path;
    }
    return database + "/" + path;
}

struct TAssignmentTracker {
    const ui32 PartitionCount;
    TMutex Mutex;
    std::vector<ui32> RefCount;
    std::vector<TInstant> LastActive;

    explicit TAssignmentTracker(ui32 partitionCount)
        : PartitionCount(partitionCount)
        , RefCount(partitionCount, 0)
        , LastActive(partitionCount, TInstant::Zero())
    {
    }

    void Add(ui32 partitionId) {
        if (partitionId >= PartitionCount) {
            return;
        }
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            ++RefCount[partitionId];
            LastActive[partitionId] = now;
        }
    }

    void Remove(ui32 partitionId) {
        if (partitionId >= PartitionCount) {
            return;
        }
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            if (RefCount[partitionId] == 0) {
                return;
            }
            --RefCount[partitionId];
            LastActive[partitionId] = now;
        }
    }

    ui32 AssignedCount() {
        with_lock (Mutex) {
            ui32 assigned = 0;
            for (ui32 count : RefCount) {
                assigned += count > 0 ? 1 : 0;
            }
            return assigned;
        }
    }

    ui32 SeenCount() {
        with_lock (Mutex) {
            ui32 seen = 0;
            for (const auto lastActive : LastActive) {
                seen += lastActive != TInstant::Zero() ? 1 : 0;
            }
            return seen;
        }
    }

    bool AllSeen() {
        return SeenCount() == PartitionCount;
    }

    TString DebugStale(TDuration maxLag) {
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            ui32 stale = 0;
            ui32 assigned = 0;
            ui32 never = 0;
            TDuration maxAge = TDuration::Zero();
            TStringBuilder staleIds;
            for (ui32 partitionId = 0; partitionId < PartitionCount; ++partitionId) {
                if (RefCount[partitionId] > 0) {
                    ++assigned;
                    continue;
                }
                if (LastActive[partitionId] == TInstant::Zero()) {
                    ++never;
                    ++stale;
                    if (stale <= 32) {
                        if (stale > 1) {
                            staleIds << ",";
                        }
                        staleIds << partitionId;
                    }
                    continue;
                }
                const auto age = now - LastActive[partitionId];
                if (age > maxAge) {
                    maxAge = age;
                }
                if (age > maxLag) {
                    ++stale;
                    if (stale <= 32) {
                        if (stale > 1) {
                            staleIds << ",";
                        }
                        staleIds << partitionId;
                    }
                }
            }
            return TStringBuilder()
                << "assigned=" << assigned
                << " stale=" << stale
                << " never=" << never
                << " maxAge=" << maxAge
                << " staleIds=[" << staleIds << "]";
        }
    }

    void EnsureFresh(TDuration maxLag) {
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            for (ui32 partitionId = 0; partitionId < PartitionCount; ++partitionId) {
                if (RefCount[partitionId] > 0) {
                    continue;
                }
                Y_ENSURE(LastActive[partitionId] != TInstant::Zero(),
                    "Partition " << partitionId << " was never assigned for reading. "
                    << DebugStaleUnlocked(now, maxLag));
                Y_ENSURE(now - LastActive[partitionId] <= maxLag,
                    "Partition " << partitionId << " last active session is older than " << maxLag
                    << " (age=" << (now - LastActive[partitionId]) << "). "
                    << DebugStaleUnlocked(now, maxLag));
            }
        }
    }

private:
    TString DebugStaleUnlocked(TInstant now, TDuration maxLag) const {
        ui32 stale = 0;
        ui32 assigned = 0;
        TStringBuilder staleIds;
        for (ui32 partitionId = 0; partitionId < PartitionCount; ++partitionId) {
            if (RefCount[partitionId] > 0) {
                ++assigned;
                continue;
            }
            const bool isStale = LastActive[partitionId] == TInstant::Zero()
                || now - LastActive[partitionId] > maxLag;
            if (!isStale) {
                continue;
            }
            ++stale;
            if (stale <= 32) {
                if (stale > 1) {
                    staleIds << ",";
                }
                staleIds << partitionId;
            }
        }
        return TStringBuilder()
            << "assigned=" << assigned
            << " stale=" << stale
            << " staleIds=[" << staleIds << "]";
    }
};

struct TSessionState {
    TMutex Mutex;
    std::unordered_set<ui32> Partitions;
    bool Closed = false;
};

struct TTrackedSession {
    std::shared_ptr<IReadSession> Session;
    std::shared_ptr<TSessionState> State;
    std::shared_ptr<TAssignmentTracker> Tracker;

    void ReleaseAll() {
        if (!State) {
            return;
        }
        std::unordered_set<ui32> leftover;
        with_lock (State->Mutex) {
            State->Closed = true;
            leftover.swap(State->Partitions);
        }
        if (Tracker) {
            for (ui32 partitionId : leftover) {
                Tracker->Remove(partitionId);
            }
        }
    }

    void Close() {
        ReleaseAll();
        if (Session) {
            Session->Close(TDuration::Zero());
            Session.reset();
        }
    }
};

struct TSessionPool {
    TMutex Mutex;
    std::vector<TTrackedSession> Sessions;

    ~TSessionPool() {
        CloseAll();
    }

    size_t Size() {
        with_lock (Mutex) {
            return Sessions.size();
        }
    }

    void CloseAll() {
        std::vector<TTrackedSession> leftover;
        with_lock (Mutex) {
            leftover.swap(Sessions);
        }
        for (auto& session : leftover) {
            session.Close();
        }
    }
};

struct TWorkerPool {
    std::atomic<bool> Stop{false};
    std::vector<std::thread> Workers;

    ~TWorkerPool() {
        Join();
    }

    void Join() {
        Stop.store(true);
        for (auto& worker : Workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        Workers.clear();
    }
};

void EnsureStatus(const TStatus& status, const TString& what) {
    Y_ENSURE(status.IsSuccess(), what << ": " << status);
}

} // namespace

int main(int argc, const char* argv[]) {
    const TOptions opts(argc, argv);
    const std::string topicPath = MakeTopicPath(opts.Database, opts.Path);

    auto driverConfig = TDriverConfig()
        .SetNetworkThreadsNum(16)
        .SetEndpoint(NormalizeEndpoint(opts.Endpoint))
        .SetDatabase(opts.Database);
    TDriver driver(driverConfig);

    auto handlersExecutor = CreateThreadPoolExecutor(opts.Threads);
    handlersExecutor->Start();

    TTopicClientSettings clientSettings;
    clientSettings.DefaultHandlersExecutor(handlersExecutor);
    TTopicClient client(driver, clientSettings);

    {
        auto drop = client.DropTopic(topicPath).GetValueSync();
        if (!drop.IsSuccess() && drop.GetStatus() != EStatus::SCHEME_ERROR && drop.GetStatus() != EStatus::NOT_FOUND) {
            EnsureStatus(drop, "DropTopic");
        }
    }

    TCreateTopicSettings createSettings;
    createSettings
        .PartitioningSettings(opts.PartitionCount, opts.PartitionCount)
        .OperationTimeout(TDuration::Minutes(5))
        .ClientTimeout(TDuration::Minutes(5));
    createSettings.BeginAddConsumer(opts.Consumer);
    EnsureStatus(client.CreateTopic(topicPath, createSettings).GetValueSync(), "CreateTopic");

    auto tracker = std::make_shared<TAssignmentTracker>(opts.PartitionCount);
    TSessionPool sessions;
    std::atomic<ui32> targetSessions{opts.PartitionCount};
    std::atomic<ui64> opened{0};
    std::atomic<ui64> closed{0};

    auto createSession = [&]() {
        TTrackedSession tracked;
        tracked.State = std::make_shared<TSessionState>();
        tracked.Tracker = tracker;
        auto state = tracked.State;

        auto releasePartition = [tracker, state](ui32 partitionId) {
            bool removed = false;
            with_lock (state->Mutex) {
                if (state->Closed) {
                    return;
                }
                removed = state->Partitions.erase(partitionId);
            }
            if (removed) {
                tracker->Remove(partitionId);
            }
        };

        TReadSessionSettings settings;
        settings
            .ConsumerName(opts.Consumer)
            .MaxMemoryUsageBytes(1_MB)
            .ConnectTimeout(TDuration::Seconds(30))
            .AppendTopics(topicPath);

        settings.EventHandlers_.HandlersExecutor(handlersExecutor);
        settings.EventHandlers_.StartPartitionSessionHandler(
            [tracker, state](TReadSessionEvent::TStartPartitionSessionEvent& ev) {
                const ui32 partitionId = ev.GetPartitionSession()->GetPartitionId();
                bool inserted = false;
                with_lock (state->Mutex) {
                    if (!state->Closed) {
                        inserted = state->Partitions.insert(partitionId).second;
                    }
                    if (inserted) {
                        tracker->Add(partitionId);
                    }
                }
                ev.Confirm();
            });
        settings.EventHandlers_.StopPartitionSessionHandler(
            [releasePartition](TReadSessionEvent::TStopPartitionSessionEvent& ev) {
                releasePartition(ev.GetPartitionSession()->GetPartitionId());
                ev.Confirm();
            });
        settings.EventHandlers_.PartitionSessionClosedHandler(
            [releasePartition](TReadSessionEvent::TPartitionSessionClosedEvent& ev) {
                releasePartition(ev.GetPartitionSession()->GetPartitionId());
            });

        tracked.Session = client.CreateReadSession(settings);
        return tracked;
    };

    TWorkerPool workers;
    workers.Workers.reserve(opts.Threads);
    for (ui32 i = 0; i < opts.Threads; ++i) {
        workers.Workers.emplace_back([&]() {
            while (!workers.Stop.load()) {
                const ui32 target = targetSessions.load();
                TTrackedSession toClose;
                bool needCreate = false;

                {
                    with_lock (sessions.Mutex) {
                        if (sessions.Sessions.size() < target) {
                            needCreate = true;
                        } else if (sessions.Sessions.size() > target) {
                            toClose = std::move(sessions.Sessions.back());
                            sessions.Sessions.pop_back();
                        } else if (target > 0 && !sessions.Sessions.empty()) {
                            // Keep replacing live sessions so partitions are reassigned continuously.
                            toClose = std::move(sessions.Sessions.back());
                            sessions.Sessions.pop_back();
                            needCreate = true;
                        }
                    }
                }

                if (needCreate) {
                    auto session = createSession();
                    bool keep = false;
                    {
                        with_lock (sessions.Mutex) {
                            if (sessions.Sessions.size() < targetSessions.load()) {
                                sessions.Sessions.push_back(std::move(session));
                                keep = true;
                            }
                        }
                    }
                    if (keep) {
                        opened.fetch_add(1);
                    } else {
                        session.Close();
                        closed.fetch_add(1);
                    }
                }
                if (toClose.Session) {
                    toClose.Close();
                    closed.fetch_add(1);
                }
                if (!needCreate && !toClose.Session) {
                    Sleep(TDuration::MilliSeconds(1));
                }
            }
        });
    }

    const auto deadline = TDuration::Seconds(opts.DurationSeconds).ToDeadLine();
    const auto warmupDeadline = TDuration::Seconds(opts.WarmupSeconds).ToDeadLine();
    bool warmedUp = false;
    auto nextLog = TInstant::Now();
    auto nextRetarget = TInstant::Now();

    while (TInstant::Now() < deadline) {
        const auto now = TInstant::Now();

        if (!warmedUp) {
            Y_ENSURE(now < warmupDeadline,
                "Warmup failed: not every partition was assigned for reading. "
                << tracker->DebugStale(opts.MaxLag));
            if (tracker->AllSeen()) {
                warmedUp = true;
                Cerr << "Warmup done assigned=" << tracker->AssignedCount()
                    << " sessions=" << sessions.Size()
                    << " opened=" << opened.load()
                    << " closed=" << closed.load() << Endl << Flush;
            }
        } else {
            tracker->EnsureFresh(opts.MaxLag);
        }

        if (now >= nextRetarget) {
            if (warmedUp) {
                targetSessions.store(RandomNumber<ui32>(opts.MaxSessions + 1));
            } else {
                targetSessions.store(Min(opts.MaxSessions, Max(opts.PartitionCount, ui32(1))));
            }
            nextRetarget = now + TDuration::MilliSeconds(50);
        }

        if (now >= nextLog) {
            Cerr << (warmedUp ? "Run" : "Warmup")
                << " target=" << targetSessions.load()
                << " sessions=" << sessions.Size()
                << " assigned=" << tracker->AssignedCount()
                << " seen=" << tracker->SeenCount()
                << " opened=" << opened.load()
                << " closed=" << closed.load()
                << " " << tracker->DebugStale(opts.MaxLag) << Endl << Flush;
            nextLog = now + TDuration::Seconds(1);
        }

        Sleep(TDuration::MilliSeconds(10));
    }

    Y_ENSURE(warmedUp, "Stress loop finished before every partition was assigned for reading. "
        << tracker->DebugStale(opts.MaxLag));
    tracker->EnsureFresh(opts.MaxLag);

    targetSessions.store(0);
    workers.Join();
    sessions.CloseAll();
    driver.Stop(true);

    Cerr << "Stress finished sessionsOpened=" << opened.load()
        << " sessionsClosed=" << closed.load() << Endl << Flush;
    return 0;
}
