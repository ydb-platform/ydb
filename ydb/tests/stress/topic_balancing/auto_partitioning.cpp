#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/utility.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

using namespace NYdb;
using namespace NYdb::NTopic;

namespace {

struct TOptions {
    std::string Endpoint = "localhost:2135";
    std::string Database = "/Root";
    std::string Path = "topic_balancing_autopart";
    std::string Consumer = "shared-consumer";
    ui32 DurationSeconds = 240;
    ui32 MinPartitions = 7;
    ui32 WaitPartitions = 40;
    ui32 MaxPartitions = 80;
    ui32 MaxSessions = 100;
    ui32 MinSessions = 32;
    ui32 Writers = 16;
    ui32 Threads = 8;
    ui32 WarmupSeconds = 30;
    ui32 SplitTimeoutSeconds = 120;
    ui32 StabilizationWindowSeconds = 1;
    ui32 UpUtilizationPercent = 1;
    ui32 RewindRps = 0;
    ui32 RetargetMs = 1000;
    ui32 ChurnGapMs = 200;
    std::string RewindTarget = "started";
    bool CommitData = false;
    bool NoAutoPartitioningSupport = false;
    bool AutoPartitioning = false;
    bool PreferredSessions = false;
    TDuration MaxLag = TDuration::Seconds(10);
    TDuration NewPartitionGrace = TDuration::Seconds(15);

    bool RewindStarted() const {
        return RewindTarget == "started";
    }
    bool RewindAssigned() const {
        return RewindTarget == "assigned";
    }
    bool RewindRoots() const {
        return RewindTarget == "roots";
    }

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
        opts.AddLongOption("min-partitions", "Initial partition count").RequiredArgument("COUNT")
            .StoreResult(&MinPartitions);
        opts.AddLongOption("wait-partitions", "Wait until topic has at least this many partitions").RequiredArgument("COUNT")
            .StoreResult(&WaitPartitions);
        opts.AddLongOption("max-partitions", "Auto-split max active partitions").RequiredArgument("COUNT")
            .StoreResult(&MaxPartitions);
        opts.AddLongOption("max-sessions", "Upper bound for random live read session count").RequiredArgument("COUNT")
            .StoreResult(&MaxSessions);
        opts.AddLongOption("min-sessions", "Lower bound for live read sessions after warmup").RequiredArgument("COUNT")
            .StoreResult(&MinSessions);
        opts.AddLongOption("retarget-ms", "How often to pick a new live session target after warmup").RequiredArgument("MS")
            .StoreResult(&RetargetMs);
        opts.AddLongOption("churn-gap-ms", "Minimum delay between replacing one session when already at target")
            .RequiredArgument("MS")
            .StoreResult(&ChurnGapMs);
        opts.AddLongOption("writers", "Write sessions that generate split load").RequiredArgument("COUNT")
            .StoreResult(&Writers);
        opts.AddLongOption("threads", "Worker threads that open and close read sessions").RequiredArgument("COUNT")
            .StoreResult(&Threads);
        opts.AddLongOption("warmup", "Seconds to wait until every active partition is assigned").RequiredArgument("SECONDS")
            .StoreResult(&WarmupSeconds);
        opts.AddLongOption("split-timeout", "Seconds to wait for wait-partitions").RequiredArgument("SECONDS")
            .StoreResult(&SplitTimeoutSeconds);
        opts.AddLongOption("stabilization-window", "Auto-split stabilization window in seconds").RequiredArgument("SECONDS")
            .StoreResult(&StabilizationWindowSeconds);
        opts.AddLongOption("up-utilization", "Auto-split up utilization percent").RequiredArgument("PERCENT")
            .StoreResult(&UpUtilizationPercent);
        opts.AddLongOption("commit-data", "Commit every received data event").NoArgument()
            .SetFlag(&CommitData);
        opts.AddLongOption("auto-partitioning",
                "Run the split/merge workload (writers, dynamic partitions, ScaleAware/old SDK)")
            .NoArgument()
            .SetFlag(&AutoPartitioning);
        opts.AddLongOption("no-auto-partitioning-support",
                "Disable SDK AutoPartitioningSupport (old SDK: Finish is not enough, heuristic delay)")
            .NoArgument()
            .SetFlag(&NoAutoPartitioningSupport);
        opts.AddLongOption("preferred-sessions",
                "Every second read session lists 1-5 random topic partitions")
            .NoArgument()
            .SetFlag(&PreferredSessions);
        opts.AddLongOption("rewind-rps",
                "After warmup, CommitOffset this many times per second to a random already-processed offset")
            .RequiredArgument("COUNT")
            .StoreResult(&RewindRps);
        opts.AddLongOption("rewind-target",
                "Partition set for CommitOffset: started (any partition a session has begun, including finished), "
                "assigned (currently locked), roots")
            .RequiredArgument("MODE")
            .StoreResult(&RewindTarget);
        ui32 maxLagMs = 10000;
        ui32 newPartitionGraceMs = 15000;
        opts.AddLongOption("max-lag-ms", "Max age of the last active session per mature active partition")
            .RequiredArgument("MS")
            .StoreResult(&maxLagMs);
        opts.AddLongOption("new-partition-grace-ms",
                "Ignore active partitions younger than this when checking assignment")
            .RequiredArgument("MS")
            .StoreResult(&newPartitionGraceMs);
        opts.SetFreeArgsNum(0);
        NLastGetopt::TOptsParseResult res(&opts, argc, argv);

        MaxLag = TDuration::MilliSeconds(maxLagMs);
        NewPartitionGrace = TDuration::MilliSeconds(newPartitionGraceMs);
        Y_ENSURE(MinPartitions > 0, "min-partitions must be > 0");
        Y_ENSURE(WaitPartitions >= MinPartitions, "wait-partitions must be >= min-partitions");
        Y_ENSURE(MaxPartitions >= WaitPartitions, "max-partitions must be >= wait-partitions");
        Y_ENSURE(MaxSessions > 0, "max-sessions must be > 0");
        Y_ENSURE(MinSessions > 0, "min-sessions must be > 0");
        Y_ENSURE(MinSessions <= MaxSessions, "min-sessions must be <= max-sessions");
        Y_ENSURE(Writers > 0, "writers must be > 0");
        Y_ENSURE(Threads > 0, "threads must be > 0");
        Y_ENSURE(RetargetMs > 0, "retarget-ms must be > 0");
        Y_ENSURE(ChurnGapMs > 0, "churn-gap-ms must be > 0");
        Y_ENSURE(MaxLag > TDuration::Zero(), "max-lag-ms must be > 0");
        Y_ENSURE(NewPartitionGrace > TDuration::Zero(), "new-partition-grace-ms must be > 0");
        Y_ENSURE(RewindStarted() || RewindAssigned() || RewindRoots(),
            "rewind-target must be started, assigned, or roots");
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
        return database + "/topic_balancing_autopart";
    }
    if (path[0] == '/') {
        return path;
    }
    if (!database.empty() && database.back() == '/') {
        return database + path;
    }
    return database + "/" + path;
}

struct TPartState {
    ui32 RefCount = 0;
    ui64 ProcessedEnd = 0;
    TInstant LastActive = TInstant::Zero();
    TInstant KnownSince = TInstant::Zero();
};

struct TAssignmentTracker {
    TMutex Mutex;
    std::unordered_map<ui32, TPartState> Parts;
    std::unordered_set<ui32> Active;
    std::unordered_set<ui32> Roots;
    std::unordered_set<ui32> Started;

    void SyncDescribe(const TTopicDescription& desc, TInstant now) {
        with_lock (Mutex) {
            Active.clear();
            Roots.clear();
            for (const auto& partition : desc.GetPartitions()) {
                const ui32 id = static_cast<ui32>(partition.GetPartitionId());
                auto& state = Parts[id];
                if (state.KnownSince == TInstant::Zero()) {
                    state.KnownSince = now;
                }
                if (partition.GetActive()) {
                    Active.insert(id);
                }
                if (partition.GetParentPartitionIds().empty()) {
                    Roots.insert(id);
                }
            }
        }
    }

    std::vector<ui32> RootIds() {
        with_lock (Mutex) {
            return {Roots.begin(), Roots.end()};
        }
    }

    std::vector<ui32> AssignedActiveIds() {
        with_lock (Mutex) {
            std::vector<ui32> ids;
            ids.reserve(Active.size());
            for (ui32 id : Active) {
                auto it = Parts.find(id);
                if (it != Parts.end() && it->second.RefCount > 0) {
                    ids.push_back(id);
                }
            }
            return ids;
        }
    }

    std::vector<ui32> StartedIds() {
        with_lock (Mutex) {
            return {Started.begin(), Started.end()};
        }
    }

    std::vector<ui32> AllPartitionIds() {
        with_lock (Mutex) {
            std::vector<ui32> ids;
            ids.reserve(Parts.size());
            for (const auto& [id, _] : Parts) {
                ids.push_back(id);
            }
            return ids;
        }
    }

    std::vector<ui32> PickRandomPartitionIds(ui32 minCount, ui32 maxCount) {
        auto ids = AllPartitionIds();
        if (ids.empty() || maxCount == 0) {
            return {};
        }
        const ui32 hi = Min(maxCount, static_cast<ui32>(ids.size()));
        const ui32 lo = Min(minCount, hi);
        if (lo == 0) {
            return {};
        }
        const ui32 count = lo + RandomNumber<ui32>(hi - lo + 1);
        for (ui32 i = 0; i < count; ++i) {
            std::swap(ids[i], ids[i + RandomNumber<ui32>(ids.size() - i)]);
        }
        ids.resize(count);
        return ids;
    }

    void NoteProcessed(ui32 partitionId, ui64 nextOffset) {
        with_lock (Mutex) {
            auto& state = Parts[partitionId];
            if (nextOffset > state.ProcessedEnd) {
                state.ProcessedEnd = nextOffset;
            }
        }
    }

    // Re-assign: from the end of the partition or from its last 10 messages.
    // Sealed parents always start at end so Finish can unlock children.
    std::optional<ui64> PickResumeReadOffset(ui32 partitionId, ui64 endOffset) {
        with_lock (Mutex) {
            if (!Active.contains(partitionId)) {
                return endOffset;
            }
            auto it = Parts.find(partitionId);
            if (it == Parts.end() || it->second.ProcessedEnd == 0) {
                return std::nullopt;
            }
            if (RandomNumber<ui32>(2) == 0) {
                return endOffset;
            }
            constexpr ui64 tail = 10;
            return endOffset > tail ? endOffset - tail : 0;
        }
    }

    std::optional<std::pair<ui32, ui64>> PickProcessedCommit(const std::vector<ui32>& candidates) {
        if (candidates.empty()) {
            return std::nullopt;
        }
        const ui32 partitionId = candidates[RandomNumber<ui32>(candidates.size())];
        with_lock (Mutex) {
            ui64 processedEnd = 0;
            auto it = Parts.find(partitionId);
            if (it != Parts.end()) {
                processedEnd = it->second.ProcessedEnd;
            }
            // Rewind only within the last 10 messages so a subsequent split
            // can still Finish the parent quickly.
            ui64 offset = 0;
            if (processedEnd > 10) {
                offset = processedEnd - 10 + RandomNumber<ui64>(11);
            } else if (processedEnd > 0) {
                offset = RandomNumber<ui64>(processedEnd + 1);
            }
            return std::make_pair(partitionId, offset);
        }
    }

    void Add(ui32 partitionId) {
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            auto& state = Parts[partitionId];
            ++state.RefCount;
            state.LastActive = now;
            Started.insert(partitionId);
            if (state.KnownSince == TInstant::Zero()) {
                state.KnownSince = now;
            }
        }
    }

    void Remove(ui32 partitionId) {
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            auto it = Parts.find(partitionId);
            if (it == Parts.end() || it->second.RefCount == 0) {
                return;
            }
            --it->second.RefCount;
            it->second.LastActive = now;
        }
    }

    ui32 AssignedCount() {
        with_lock (Mutex) {
            ui32 assigned = 0;
            for (const auto& [_, state] : Parts) {
                assigned += state.RefCount > 0 ? 1 : 0;
            }
            return assigned;
        }
    }

    ui32 ActiveCount() {
        with_lock (Mutex) {
            return Active.size();
        }
    }

    ui32 SeenActiveCount() {
        with_lock (Mutex) {
            ui32 seen = 0;
            for (ui32 id : Active) {
                const auto& state = Parts[id];
                if (state.RefCount > 0 || state.LastActive != TInstant::Zero()) {
                    ++seen;
                }
            }
            return seen;
        }
    }

    bool AllActiveSeen(TDuration newPartitionGrace) {
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            if (Active.empty()) {
                return false;
            }
            bool anyMature = false;
            for (ui32 id : Active) {
                const auto& state = Parts[id];
                if (state.KnownSince == TInstant::Zero() || now - state.KnownSince <= newPartitionGrace) {
                    continue;
                }
                anyMature = true;
                if (state.RefCount == 0 && state.LastActive == TInstant::Zero()) {
                    return false;
                }
            }
            return anyMature;
        }
    }

    TString DebugStale(TDuration newPartitionGrace, TDuration maxLag) {
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            return DebugStaleUnlocked(now, newPartitionGrace, maxLag);
        }
    }

    void EnsureFresh(TDuration newPartitionGrace, TDuration maxLag) {
        const auto now = TInstant::Now();
        with_lock (Mutex) {
            for (ui32 id : Active) {
                const auto& state = Parts[id];
                if (state.KnownSince == TInstant::Zero() || now - state.KnownSince <= newPartitionGrace) {
                    continue;
                }
                if (state.RefCount > 0) {
                    continue;
                }
                Y_ENSURE(state.LastActive != TInstant::Zero(),
                    "Active partition " << id << " was never assigned for reading. "
                    << DebugStaleUnlocked(now, newPartitionGrace, maxLag));
                Y_ENSURE(now - state.LastActive <= maxLag,
                    "Active partition " << id << " last active session is older than " << maxLag
                    << " (age=" << (now - state.LastActive) << "). "
                    << DebugStaleUnlocked(now, newPartitionGrace, maxLag));
            }
        }
    }

private:
    TString DebugStaleUnlocked(TInstant now, TDuration newPartitionGrace, TDuration maxLag) const {
        ui32 stale = 0;
        ui32 assigned = 0;
        ui32 never = 0;
        TDuration maxAge = TDuration::Zero();
        TStringBuilder staleIds;
        for (ui32 id : Active) {
            auto it = Parts.find(id);
            const TPartState empty;
            const auto& state = it == Parts.end() ? empty : it->second;
            if (state.RefCount > 0) {
                ++assigned;
                continue;
            }
            if (state.KnownSince == TInstant::Zero() || now - state.KnownSince <= newPartitionGrace) {
                continue;
            }
            if (state.LastActive == TInstant::Zero()) {
                ++never;
                ++stale;
                if (stale <= 32) {
                    if (stale > 1) {
                        staleIds << ",";
                    }
                    staleIds << id;
                }
                continue;
            }
            const auto age = now - state.LastActive;
            if (age > maxAge) {
                maxAge = age;
            }
            if (age > maxLag) {
                ++stale;
                if (stale <= 32) {
                    if (stale > 1) {
                        staleIds << ",";
                    }
                    staleIds << id;
                }
            }
        }
        return TStringBuilder()
            << "active=" << Active.size()
            << " assigned=" << assigned
            << " stale=" << stale
            << " never=" << never
            << " maxAge=" << maxAge
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
    bool Preferred = false;

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

struct TRandomCommitQueue {
    static constexpr size_t MaxPending = 100'000;

    struct TPendingCommit {
        TPartitionSession::TPtr Session;
        uint64_t Start = 0;
        uint64_t End = 0;
    };

    TMutex Mutex;
    std::vector<TPendingCommit> Pending;
    std::atomic<ui64> Enqueued{0};
    std::atomic<ui64> Committed{0};

    void Add(TPartitionSession::TPtr session, uint64_t start, uint64_t end) {
        std::vector<TPendingCommit> overflow;
        {
            with_lock (Mutex) {
                Pending.push_back(TPendingCommit{std::move(session), start, end});
                Enqueued.fetch_add(1);
                while (Pending.size() > MaxPending) {
                    overflow.push_back(TakeRandomUnlocked());
                }
            }
        }
        for (auto& item : overflow) {
            CommitOne(item);
        }
    }

    size_t CommitRandom(size_t count) {
        std::vector<TPendingCommit> batch;
        batch.reserve(count);
        {
            with_lock (Mutex) {
                const size_t n = Min(count, Pending.size());
                for (size_t i = 0; i < n; ++i) {
                    batch.push_back(TakeRandomUnlocked());
                }
            }
        }
        for (auto& item : batch) {
            CommitOne(item);
        }
        return batch.size();
    }

    void CommitAllRandom() {
        while (CommitRandom(256) > 0) {
        }
    }

    size_t Size() {
        with_lock (Mutex) {
            return Pending.size();
        }
    }

private:
    TPendingCommit TakeRandomUnlocked() {
        const size_t idx = RandomNumber<size_t>(Pending.size());
        TPendingCommit item = std::move(Pending[idx]);
        Pending[idx] = std::move(Pending.back());
        Pending.pop_back();
        return item;
    }

    void CommitOne(const TPendingCommit& item) {
        if (!item.Session) {
            return;
        }
        static_cast<TPartitionSessionControl*>(item.Session.Get())->Commit(item.Start, item.End);
        Committed.fetch_add(1);
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

TTopicDescription Describe(TTopicClient& client, const std::string& topicPath) {
    TDescribeTopicSettings settings;
    settings.ClientTimeout(TDuration::Seconds(5));
    auto result = client.DescribeTopic(topicPath, settings).GetValueSync();
    EnsureStatus(result, "DescribeTopic");
    return result.GetTopicDescription();
}

std::string RandomPartitionKey() {
    std::string key(16, '\0');
    for (char& byte : key) {
        byte = static_cast<char>(RandomNumber<ui8>());
    }
    return key;
}

std::string IdentityPartitionKey(const std::string_view key) {
    return std::string(key);
}

TProducerSettings MakeProducerSettings(const std::string& topicPath, ui32 writerIndex) {
    TProducerSettings settings;
    settings
        .Path(topicPath)
        .Codec(ECodec::RAW)
        .MaxMemoryUsage(8_MB)
        .ConnectTimeout(TDuration::Seconds(30));
    settings.ProducerIdPrefix("writer-" + std::to_string(writerIndex));
    settings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Bound);
    settings.PartitioningKeyHasher(IdentityPartitionKey);
    settings.SubSessionIdleTimeout(TDuration::Seconds(30));
    settings.MaxBlockTimeout(TDuration::Seconds(1));
    return settings;
}

} // namespace

int RunAutoPartitioningWorkload(int argc, const char* argv[]) {
    const TOptions opts(argc, argv);
    Y_ENSURE(opts.AutoPartitioning);
    const std::string topicPath = MakeTopicPath(opts.Database, opts.Path);
    const bool autoPartitioningSupport = !opts.NoAutoPartitioningSupport;
    Cerr << "mode=auto-partitioning AutoPartitioningSupport="
        << (autoPartitioningSupport ? "true" : "false")
        << " PreferredSessions=" << (opts.PreferredSessions ? "true" : "false")
        << Endl << Flush;

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
        .PartitionWriteSpeedBytesPerSecond(4_KB)
        .PartitionWriteBurstBytes(4_KB)
        .OperationTimeout(TDuration::Minutes(5))
        .ClientTimeout(TDuration::Minutes(5))
        .BeginConfigurePartitioningSettings()
            .MinActivePartitions(opts.MinPartitions)
            .MaxActivePartitions(opts.MaxPartitions)
            .BeginConfigureAutoPartitioningSettings()
                .Strategy(EAutoPartitioningStrategy::ScaleUp)
                .StabilizationWindow(TDuration::Seconds(opts.StabilizationWindowSeconds))
                .UpUtilizationPercent(opts.UpUtilizationPercent)
                .DownUtilizationPercent(1)
            .EndConfigureAutoPartitioningSettings()
        .EndConfigurePartitioningSettings();
    createSettings.BeginAddConsumer(opts.Consumer);
    EnsureStatus(client.CreateTopic(topicPath, createSettings).GetValueSync(), "CreateTopic");

    const auto deadline = TDuration::Seconds(opts.DurationSeconds).ToDeadLine();

    const std::string payload(4_KB, 'x');
    std::atomic<ui64> written{0};
    std::vector<std::shared_ptr<IProducer>> writers;
    writers.reserve(opts.Writers);
    for (ui32 i = 0; i < opts.Writers; ++i) {
        writers.push_back(client.CreateProducer(MakeProducerSettings(topicPath, i)));
    }

    TWorkerPool writeWorkers;
    writeWorkers.Workers.reserve(opts.Writers);
    std::atomic<bool> pauseWrites{false};
    for (ui32 i = 0; i < opts.Writers; ++i) {
        writeWorkers.Workers.emplace_back([&, i]() {
            auto producer = writers[i];
            ui64 seqNo = 0;
            while (!writeWorkers.Stop.load()) {
                if (pauseWrites.load()) {
                    Sleep(TDuration::MilliSeconds(10));
                    continue;
                }
                TWriteMessage message(RandomPartitionKey(), payload);
                message.SeqNo(++seqNo);
                const auto result = producer->Write(std::move(message));
                if (result.IsQueued()) {
                    written.fetch_add(1);
                }
            }
        });
    }

    auto tracker = std::make_shared<TAssignmentTracker>();
    const auto splitDeadline = Min(
        TInstant::Now() + TDuration::Seconds(opts.SplitTimeoutSeconds),
        deadline - TDuration::Seconds(opts.WarmupSeconds + 15)
    );
    Y_ENSURE(splitDeadline > TInstant::Now(),
        "duration=" << opts.DurationSeconds << "s is too short for split-timeout + warmup");
    ui32 partitionCount = 0;
    while (TInstant::Now() < splitDeadline) {
        try {
            auto desc = Describe(client, topicPath);
            partitionCount = desc.GetTotalPartitionsCount();
            tracker->SyncDescribe(desc, TInstant::Now());
            Cerr << "Waiting for splits partitions=" << partitionCount
                << " written=" << written.load() << Endl << Flush;
            if (partitionCount >= opts.WaitPartitions) {
                break;
            }
        } catch (const yexception& e) {
            Cerr << "DescribeTopic failed: " << e.what() << Endl << Flush;
        }
        Sleep(TDuration::MilliSeconds(500));
    }
    Y_ENSURE(partitionCount >= opts.WaitPartitions,
        "Topic did not reach " << opts.WaitPartitions << " partitions, last count=" << partitionCount
        << " written=" << written.load());
    Cerr << "Split wait done partitions=" << partitionCount
        << " written=" << written.load() << Endl << Flush;
    pauseWrites.store(true);

    TSessionPool sessions;
    TRandomCommitQueue commitQueue;
    std::atomic<ui32> targetSessions{opts.MaxSessions};
    std::atomic<ui64> opened{0};
    std::atomic<ui64> closed{0};
    std::atomic<ui64> preferredOpened{0};
    std::atomic<ui64> sessionSeq{0};
    std::atomic<ui64> readMessages{0};
    std::atomic<bool> allowSessionChurn{false};
    std::atomic<bool> allowRewind{false};
    std::atomic<bool> allowCommit{false};
    std::atomic<ui64> lastReplaceMs{0};

    auto createSession = [&]() {
        TTrackedSession tracked;
        tracked.State = std::make_shared<TSessionState>();
        tracked.Tracker = tracker;
        auto state = tracked.State;
        const bool commitData = opts.CommitData;

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

        TTopicReadSettings topicSettings(topicPath);
        const bool preferPartitions = opts.PreferredSessions && (sessionSeq.fetch_add(1) % 2 == 1);
        if (preferPartitions) {
            const auto preferredIds = tracker->PickRandomPartitionIds(1, 5);
            if (!preferredIds.empty()) {
                tracked.Preferred = true;
                for (ui32 id : preferredIds) {
                    topicSettings.AppendPartitionIds(id);
                }
            }
        }

        TReadSessionSettings settings;
        settings
            .ConsumerName(opts.Consumer)
            .MaxMemoryUsageBytes(1_MB)
            .ConnectTimeout(TDuration::Seconds(30))
            .AutoPartitioningSupport(autoPartitioningSupport)
            .AppendTopics(std::move(topicSettings));

        settings.EventHandlers_.HandlersExecutor(handlersExecutor);
        settings.EventHandlers_.StartPartitionSessionHandler(
            [tracker, state](TReadSessionEvent::TStartPartitionSessionEvent& ev) {
                const ui32 partitionId = static_cast<ui32>(ev.GetPartitionSession()->GetPartitionId());
                bool inserted = false;
                with_lock (state->Mutex) {
                    if (!state->Closed) {
                        inserted = state->Partitions.insert(partitionId).second;
                    }
                    if (inserted) {
                        tracker->Add(partitionId);
                    }
                }
                if (auto resume = tracker->PickResumeReadOffset(partitionId, ev.GetEndOffset())) {
                    tracker->NoteProcessed(partitionId, *resume);
                    ev.Confirm(*resume);
                } else {
                    tracker->NoteProcessed(partitionId, ev.GetCommittedOffset());
                    ev.Confirm();
                }
            });
        settings.EventHandlers_.StopPartitionSessionHandler(
            [tracker, releasePartition](TReadSessionEvent::TStopPartitionSessionEvent& ev) {
                tracker->NoteProcessed(
                    static_cast<ui32>(ev.GetPartitionSession()->GetPartitionId()),
                    ev.GetCommittedOffset());
                releasePartition(static_cast<ui32>(ev.GetPartitionSession()->GetPartitionId()));
                ev.Confirm();
            });
        settings.EventHandlers_.PartitionSessionClosedHandler(
            [releasePartition](TReadSessionEvent::TPartitionSessionClosedEvent& ev) {
                releasePartition(static_cast<ui32>(ev.GetPartitionSession()->GetPartitionId()));
            });
        settings.EventHandlers_.EndPartitionSessionHandler(
            [](TReadSessionEvent::TEndPartitionSessionEvent& ev) {
                ev.Confirm();
            });
        settings.EventHandlers_.DataReceivedHandler(
            [commitData, &allowCommit, tracker, &readMessages, &commitQueue](TReadSessionEvent::TDataReceivedEvent& ev) {
                readMessages.fetch_add(ev.GetMessagesCount());
                const ui32 partitionId = static_cast<ui32>(ev.GetPartitionSession()->GetPartitionId());
                const bool commitNow = commitData && allowCommit.load();
                for (const auto& message : ev.GetMessages()) {
                    const ui64 nextOffset = message.GetOffset() + message.GetLogicalMessageCount();
                    tracker->NoteProcessed(partitionId, nextOffset);
                    if (!commitNow) {
                        continue;
                    }
                    commitQueue.Add(
                        message.GetPartitionSession(),
                        message.GetOffset(),
                        nextOffset
                    );
                }
            });

        tracked.Session = client.CreateReadSession(settings);
        return tracked;
    };

    TWorkerPool readWorkers;
    readWorkers.Workers.reserve(opts.Threads);
    for (ui32 i = 0; i < opts.Threads; ++i) {
        readWorkers.Workers.emplace_back([&]() {
            while (!readWorkers.Stop.load()) {
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
                        } else if (allowSessionChurn.load() && target > 0 && !sessions.Sessions.empty()) {
                            const ui64 nowMs = TInstant::Now().MilliSeconds();
                            ui64 prev = lastReplaceMs.load();
                            if (nowMs >= prev + opts.ChurnGapMs &&
                                lastReplaceMs.compare_exchange_strong(prev, nowMs))
                            {
                                toClose = std::move(sessions.Sessions.back());
                                sessions.Sessions.pop_back();
                                needCreate = true;
                            }
                        }
                    }
                }

                if (needCreate) {
                    auto session = createSession();
                    const bool preferred = session.Preferred;
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
                        if (preferred) {
                            preferredOpened.fetch_add(1);
                        }
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
                    Sleep(TDuration::MilliSeconds(10));
                }
            }
        });
    }

    TWorkerPool commitWorkers;
    if (opts.CommitData) {
        commitWorkers.Workers.emplace_back([&]() {
            while (!commitWorkers.Stop.load()) {
                if (commitQueue.CommitRandom(64) == 0) {
                    Sleep(TDuration::MilliSeconds(1));
                }
            }
        });
    }

    TWorkerPool rewindWorkers;
    std::atomic<ui64> rewindOk{0};
    std::atomic<ui64> rewindFail{0};
    if (opts.RewindRps > 0) {
        rewindWorkers.Workers.emplace_back([&]() {
            const auto interval = TDuration::MicroSeconds(1'000'000 / Max<ui32>(opts.RewindRps, 1));
            while (!rewindWorkers.Stop.load()) {
                if (!allowRewind.load()) {
                    Sleep(TDuration::MilliSeconds(10));
                    continue;
                }
                std::vector<ui32> candidates;
                if (opts.RewindAssigned()) {
                    candidates = tracker->AssignedActiveIds();
                } else if (opts.RewindRoots()) {
                    candidates = tracker->RootIds();
                } else {
                    candidates = tracker->StartedIds();
                }
                if (candidates.empty()) {
                    Sleep(interval);
                    continue;
                }
                auto pick = tracker->PickProcessedCommit(candidates);
                if (!pick) {
                    Sleep(interval);
                    continue;
                }
                auto status = client.CommitOffset(topicPath, pick->first, opts.Consumer, pick->second).GetValueSync();
                if (status.IsSuccess()) {
                    rewindOk.fetch_add(1);
                } else {
                    rewindFail.fetch_add(1);
                }
                Sleep(interval);
            }
        });
    }

    const auto warmupDeadline = Min(
        TInstant::Now() + TDuration::Seconds(opts.WarmupSeconds),
        deadline
    );
    bool warmedUp = false;
    auto nextLog = TInstant::Now();
    auto nextRetarget = TInstant::Now();
    auto nextDescribe = TInstant::Now();

    while (TInstant::Now() < deadline) {
        const auto now = TInstant::Now();

        if (now >= nextDescribe) {
            try {
                auto desc = Describe(client, topicPath);
                partitionCount = desc.GetTotalPartitionsCount();
                tracker->SyncDescribe(desc, now);
            } catch (const yexception& e) {
                Cerr << "DescribeTopic failed: " << e.what() << Endl << Flush;
            }
            nextDescribe = now + TDuration::Seconds(1);
        }

        if (!warmedUp) {
            Y_ENSURE(now < warmupDeadline,
                "Warmup failed: not every active partition was assigned for reading. "
                << tracker->DebugStale(opts.NewPartitionGrace, opts.MaxLag));
            if (tracker->AllActiveSeen(opts.NewPartitionGrace)) {
                warmedUp = true;
                pauseWrites.store(false);
                allowSessionChurn.store(true);
                allowRewind.store(true);
                allowCommit.store(true);
                Cerr << "Warmup done assigned=" << tracker->AssignedCount()
                    << " active=" << tracker->ActiveCount()
                    << " partitions=" << partitionCount
                    << " sessions=" << sessions.Size()
                    << " opened=" << opened.load()
                    << " closed=" << closed.load() << Endl << Flush;
            }
        } else {
            tracker->EnsureFresh(opts.NewPartitionGrace, opts.MaxLag);
        }

        if (now >= nextRetarget) {
            if (warmedUp) {
                const ui32 minSessions = Min(
                    opts.MaxSessions,
                    Max(opts.MinSessions, tracker->ActiveCount() / 4));
                const ui32 span = opts.MaxSessions - minSessions + 1;
                targetSessions.store(minSessions + RandomNumber<ui32>(span));
            } else {
                targetSessions.store(opts.MaxSessions);
            }
            nextRetarget = now + TDuration::MilliSeconds(opts.RetargetMs);
        }

        if (now >= nextLog) {
            Cerr << (warmedUp ? "Run" : "Warmup")
                << " target=" << targetSessions.load()
                << " sessions=" << sessions.Size()
                << " partitions=" << partitionCount
                << " active=" << tracker->ActiveCount()
                << " assigned=" << tracker->AssignedCount()
                << " seen=" << tracker->SeenActiveCount()
                << " written=" << written.load()
                << " read=" << readMessages.load()
                << " commitPending=" << commitQueue.Size()
                << " committed=" << commitQueue.Committed.load()
                << " opened=" << opened.load()
                << " preferred=" << preferredOpened.load()
                << " closed=" << closed.load()
                << " rewindOk=" << rewindOk.load()
                << " rewindFail=" << rewindFail.load()
                << " " << tracker->DebugStale(opts.NewPartitionGrace, opts.MaxLag) << Endl << Flush;
            nextLog = now + TDuration::Seconds(1);
        }

        Sleep(TDuration::MilliSeconds(10));
    }

    Y_ENSURE(warmedUp, "Stress loop finished before every active partition was assigned for reading. "
        << tracker->DebugStale(opts.NewPartitionGrace, opts.MaxLag));
    tracker->EnsureFresh(opts.NewPartitionGrace, opts.MaxLag);

    pauseWrites.store(true);
    for (auto& writer : writers) {
        Y_UNUSED(writer->Close(TDuration::Seconds(1)));
    }
    commitWorkers.Stop.store(true);
    commitQueue.CommitAllRandom();
    commitWorkers.Join();
    targetSessions.store(0);
    rewindWorkers.Join();
    readWorkers.Join();
    writeWorkers.Join();
    writers.clear();
    sessions.CloseAll();
    // wait=true deadlocks: in-flight session contexts keep CQ from shutting down.
    driver.Stop(false);

    Cerr << "Stress finished partitions=" << partitionCount
        << " sessionsOpened=" << opened.load()
        << " preferredOpened=" << preferredOpened.load()
        << " sessionsClosed=" << closed.load()
        << " written=" << written.load()
        << " read=" << readMessages.load()
        << " committed=" << commitQueue.Committed.load()
        << " rewindOk=" << rewindOk.load()
        << " rewindFail=" << rewindFail.load() << Endl << Flush;
    return 0;
}
