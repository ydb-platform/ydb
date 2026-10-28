#include "workload.h"
#include "stats.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace NDataShardReadLoad {
namespace {

using namespace NYdb;
using namespace NYdb::NTable;
using TClock = std::chrono::steady_clock;
using TJson = NJson::TJsonValue;

static_assert(std::atomic<bool>::is_always_lock_free);
std::atomic<bool> StopRequested = false;

void HandleSignal(int) {
    StopRequested = true;
}

void Check(const TStatus& status, const std::string& operation) {
    if (!status.IsSuccess()) {
        throw std::runtime_error(operation + ": " + ToString(status.GetStatus()).c_str() + ": " + status.GetIssues().ToString().c_str());
    }
}

void WriteJson(const TJson& json) {
    NJsonWriter::TBuf buffer;
    buffer.WriteJsonValue(&json, true);
    Cout << buffer.Str() << Endl;
}

int64_t SinceUs(TClock::time_point start) {
    return std::chrono::duration_cast<std::chrono::microseconds>(TClock::now() - start).count();
}

TExecDataQuerySettings QuerySettings(const TOptions& options, bool cancel) {
    auto settings = TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .OperationTimeout(TDuration::MicroSeconds(options.LongTimeoutUs))
        .ClientTimeout(TDuration::MicroSeconds(options.LongTimeoutUs + options.RpcGraceUs))
        .UseClientTimeoutForOperation(false);
    if (cancel) {
        settings.CancelAfter(TDuration::MicroSeconds(options.CancelAfterUs));
    }
    return settings;
}

TCreateSessionSettings SessionSettings() {
    return TCreateSessionSettings().ClientTimeout(TDuration::Seconds(10)).OperationTimeout(TDuration::Seconds(10));
}

TParams ReadParams(TSession& session, uint64_t from, uint64_t count) {
    return session.GetParamsBuilder()
        .AddParam("$bucket").Uint64(0).Build()
        .AddParam("$from").Uint64(from).Build()
        .AddParam("$to").Uint64(from + count).Build()
        .Build();
}

std::string ReadQuery(const TOptions& options) {
    return "--!syntax_v1\nDECLARE $bucket AS Uint64; DECLARE $from AS Uint64; DECLARE $to AS Uint64;\n"
        "SELECT Item, Payload FROM `" + options.Table + "` WHERE Bucket = $bucket AND Item >= $from AND Item < $to;";
}

uint64_t ResultRows(const TDataQueryResult& result) {
    uint64_t rows = 0;
    for (const auto& set : result.GetResultSets()) {
        rows += set.RowsCount();
    }
    return rows;
}

void Init(TTableClient& client, const TOptions& options) {
    auto created = client.GetSession(SessionSettings()).GetValueSync();
    Check(created, "create initialization session");
    auto session = created.GetSession();
    auto table = TTableBuilder()
        .AddNonNullableColumn("Bucket", EPrimitiveType::Uint64)
        .AddNonNullableColumn("Item", EPrimitiveType::Uint64)
        .AddNullableColumn("Payload", EPrimitiveType::String)
        .SetPrimaryKeyColumns({"Bucket", "Item"})
        .SetUniformPartitions(1)
        .BeginPartitioningSettings()
            .SetPartitioningByLoad(false)
            .SetPartitioningBySize(false)
            .SetMinPartitionsCount(1)
            .SetMaxPartitionsCount(1)
        .EndPartitioningSettings()
        .Build();
    Check(session.CreateTable(options.Table, std::move(table)).GetValueSync(), "create table (existing tables are not reused)");
    const std::string payload(options.PayloadBytes, 'x');
    // Keep each BulkUpsert batch around 1 MiB, including large payloads.
    const uint64_t batchSize = std::max<uint64_t>(1, std::min<uint64_t>(1000, 1048576 / (options.PayloadBytes + 32)));
    for (uint64_t start = 0; start < options.Rows; start += batchSize) {
        if (StopRequested) {
            throw std::runtime_error("initialization interrupted; table may be partially populated");
        }
        TValueBuilder rows;
        rows.BeginList();
        for (uint64_t item = start; item < std::min(options.Rows, start + batchSize); ++item) {
            rows.AddListItem().BeginStruct()
                .AddMember("Bucket").Uint64(0)
                .AddMember("Item").Uint64(item)
                .AddMember("Payload").OptionalString(payload)
                .EndStruct();
        }
        rows.EndList();
        Check(client.BulkUpsert(options.Table, rows.Build(), TBulkUpsertSettings()
            .RetrySettings(TRetryOperationSettings().MaxRetries(0))
            .ClientTimeout(TDuration::Seconds(60))).GetValueSync(), "populate table");
    }
    TJson json(NJson::JSON_MAP);
    json["kind"] = "initialized";
    json["table"] = options.Table;
    json["rows"] = options.Rows;
    json["payload_bytes"] = options.PayloadBytes;
    WriteJson(json);
}

void ValidateTable(TSession& session, const TOptions& options) {
    auto result = session.DescribeTable(options.Table, TDescribeTableSettings()
        .WithTableStatistics(true).WithKeyShardBoundary(true).ClientTimeout(TDuration::Seconds(10))).GetValueSync();
    Check(result, "describe table");
    const auto description = result.GetTableDescription();
    if (description.GetPrimaryKeyColumns() != std::vector<std::string>{"Bucket", "Item"} || description.GetTableColumns().size() != 3) {
        throw std::runtime_error("table must have columns Bucket, Item, Payload and primary key (Bucket, Item)");
    }
    for (const auto& column : description.GetTableColumns()) {
        TTypeParser type(column.Type);
        if (column.Name == "Payload") {
            if (type.GetKind() != TTypeParser::ETypeKind::Optional) {
                throw std::runtime_error("Payload must be nullable String");
            }
            type.OpenOptional();
        } else if (column.Name != "Bucket" && column.Name != "Item") {
            throw std::runtime_error("unexpected column " + column.Name);
        }
        const auto expected = column.Name == "Payload" ? EPrimitiveType::String : EPrimitiveType::Uint64;
        if (type.GetKind() != TTypeParser::ETypeKind::Primitive || type.GetPrimitive() != expected) {
            throw std::runtime_error("unexpected type for column " + column.Name);
        }
    }
    const auto& partitioning = description.GetPartitioningSettings();
    if (description.GetKeyRanges().size() != 1 || partitioning.GetPartitioningByLoad() != false || partitioning.GetPartitioningBySize() != false) {
        throw std::runtime_error("table must have one shard with partitioning by load and size disabled");
    }
}

struct TCompletion {
    size_t Slot = 0;
    bool SessionResult = false;
    bool ReplaceSession = false;
    bool Success = false;
    std::string Status;
    std::string Error;
    uint64_t Rows = 0;
    int64_t LatencyUs = 0;
    std::optional<TSession> Session;
};

struct TCompletionQueue {
    std::mutex Mutex;
    std::condition_variable Ready;
    std::deque<TCompletion> Items;
    std::shared_ptr<std::atomic<uint64_t>> ErrorSamples;

    void Push(TCompletion completion) {
        {
            std::lock_guard lock(Mutex);
            Items.push_back(std::move(completion));
        }
        Ready.notify_one();
    }
};

struct TSlot {
    size_t Population;
    std::optional<TSession> Session;
    size_t Origin = 0;
};

struct TSnapshot {
    TPopulationStats Totals;
    TPopulationStats Interval;
    std::vector<TPopulationStats> Origins;

    explicit TSnapshot(size_t phases)
        : Origins(phases)
    {}
};

class TWorker {
public:
    TWorker(TTableClient& client, const TOptions& options, uint64_t index,
        std::atomic<bool>& abort, std::shared_ptr<std::atomic<uint64_t>> errorSamples)
        : Client_(client)
        , Options_(options)
        , Index_(index)
        , Abort_(abort)
        , Query_(ReadQuery(options))
        , Caps_(WorkerCaps(options, index))
        , Capacity_(Caps_)
        , Random_(options.Seed + index)
        , Origins_(options.Phases.size())
    {
        Queue_->ErrorSamples = std::move(errorSamples);
    }

    void RunPhase(TClock::time_point start, size_t phase, int64_t phaseStart) {
        Start_ = start;
        const auto& spec = Options_.Phases[phase];
        const int64_t end = phaseStart + spec.DurationUs;
        std::array<TArrivalClock, PopulationCount> clocks = {
            MakeClock(Long, spec.Rates[Long], phaseStart, end),
            MakeClock(Short, spec.Rates[Short], phaseStart, end),
            MakeClock(Probe, spec.Rates[Probe], phaseStart, end)};
        for (;;) {
            int64_t next = end;
            {
                std::lock_guard lock(StatsMutex_);
                ProcessCompletions(true);
                const int64_t now = SinceUs(Start_);
                if (Stopping()) {
                    break;
                }
                // Probe first so expensive dispatch work cannot consistently
                // defer the independent useful-work probe.
                for (size_t p : {size_t(Probe), size_t(Long), size_t(Short)}) {
                    const auto batch = clocks[p].PollBatch(SinceUs(Start_), Options_.DispatchBatch);
                    if (batch.Scheduled) {
                        Arrivals(p, phase, batch, clocks[p], end);
                    }
                }
                if (now >= end) {
                    break;
                }
                next = std::min(end, NextSessionRetryUs_);
                for (const auto& clock : clocks) {
                    next = std::min(next, clock.NextUs());
                }
            }
            Wait(next);
        }
        std::lock_guard lock(StatsMutex_);
        ProcessCompletions(true);
    }

    void Drain(TClock::time_point start, int64_t end) {
        Start_ = start;
        while (SinceUs(Start_) < end && !Abort_) {
            {
                std::lock_guard lock(StatsMutex_);
                ProcessCompletions(false);
                if (!Outstanding()) {
                    return;
                }
            }
            Wait(end);
        }
        std::lock_guard lock(StatsMutex_);
        ProcessCompletions(false);
    }

    void Collect(TSnapshot& snapshot, bool resetInterval) {
        std::lock_guard lock(StatsMutex_);
        for (size_t p = 0; p < PopulationCount; ++p) {
            snapshot.Totals[p].Add(Totals_[p]);
            snapshot.Interval[p].Add(Interval_[p]);
            for (size_t phase = 0; phase < Origins_.size(); ++phase) {
                snapshot.Origins[phase][p].Add(Origins_[phase][p]);
            }
        }
        if (resetInterval) {
            Interval_ = TPopulationStats{};
        }
    }

    const std::string& Plan() const {
        return Plan_;
    }

    void Setup() {
        size_t total = 0;
        for (auto cap : Caps_) {
            total += cap;
        }
        if (!total) {
            return;
        }
        Slots_.reserve(total);
        for (size_t p = 0; p < PopulationCount; ++p) {
            for (uint64_t count = 0; count < Caps_[p];) {
                if (StopRequested) {
                    throw std::runtime_error("startup interrupted");
                }
                std::vector<TAsyncCreateSessionResult> pending;
                for (uint64_t n = 0; n < 32 && count < Caps_[p]; ++n, ++count) {
                    pending.push_back(Client_.GetSession(SessionSettings()));
                }
                for (auto& future : pending) {
                    auto result = future.GetValueSync();
                    Check(result, "reserve load session; check server session capacity");
                    Free_[p].push_back(Slots_.size());
                    Slots_.push_back({p, result.GetSession(), 0});
                }
                std::cerr << "Worker " << Index_ << ": reserved " << Slots_.size() << '/' << total << " sessions\r";
            }
        }
        std::cerr << '\n';
        if (Index_ == 0) {
            auto& session = *Slots_.front().Session;
            ValidateTable(session, Options_);
            auto explained = session.ExplainDataQuery(Query_, TExplainDataQuerySettings()
                .ClientTimeout(TDuration::Seconds(30))).GetValueSync();
            Check(explained, "explain range-read query");
            Plan_ = explained.GetPlan();
        }
        std::cerr << "Worker " << Index_ << ": warming sessions\n";
        for (size_t first = 0; first < Slots_.size(); first += 32) {
            WarmBatch(first, std::min<size_t>(32, Slots_.size() - first), 0, false);
        }
        if (Index_ == 0) {
            const size_t batch = std::min<size_t>(32, Slots_.size());
            const uint64_t reads = (Options_.Rows + Options_.RowsPerRead - 1) / Options_.RowsPerRead;
            for (uint64_t first = 0; first < reads; first += batch) {
                WarmBatch(0, std::min<uint64_t>(batch, reads - first), first, true);
            }
        }
    }

private:
    bool Stopping() const {
        return StopRequested || Abort_;
    }

    TArrivalClock MakeClock(size_t p, uint64_t rate, int64_t start, int64_t end) const {
        const uint64_t partitions = std::min(Options_.Threads, Options_.Caps[p]);
        if (Index_ >= partitions) {
            return TArrivalClock(0, start, end);
        }
        return TArrivalClock(rate, start, end, Index_, partitions);
    }

    void WarmBatch(size_t slotStart, size_t count, uint64_t readStart, bool coverData) {
        if (StopRequested) {
            throw std::runtime_error("warmup interrupted");
        }
        std::vector<TAsyncDataQueryResult> futures;
        for (size_t i = 0; i < count; ++i) {
            auto& session = *Slots_[slotStart + i].Session;
            const uint64_t from = coverData ? std::min((readStart + i) * Options_.RowsPerRead, Options_.Rows - Options_.RowsPerRead) : 0;
            futures.push_back(session.ExecuteDataQuery(Query_, TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(),
                ReadParams(session, from, Options_.RowsPerRead), QuerySettings(Options_, false)));
        }
        for (auto& future : futures) {
            const auto& result = future.GetValueSync();
            Check(result, "warm range read");
            if (ResultRows(result) != Options_.RowsPerRead) {
                throw std::runtime_error("warmup row count mismatch; --rows must match the initialized table");
            }
        }
    }

    template <typename F>
    void Update(size_t population, size_t origin, F&& function) {
        function(Totals_[population]);
        function(Interval_[population]);
        function(Origins_[origin][population]);
    }

    void Arrivals(size_t p, size_t origin, const TArrivalBatch& batch, const TArrivalClock& clock, int64_t endUs) {
        Update(p, origin, [&](TStats& stats) {
            stats.Scheduled += batch.Scheduled;
            stats.SkippedScheduler += batch.Scheduled - batch.Dispatch;
        });
        for (uint64_t i = 0; i < batch.Dispatch; ++i) {
            const int64_t now = SinceUs(Start_);
            if (Stopping() || now >= endUs) {
                Update(p, origin, [&](TStats& stats) { stats.SkippedScheduler += batch.Dispatch - i; });
                break;
            }
            if (Free_[p].empty()) {
                Update(p, origin, [&](TStats& stats) { stats.SkippedCapacity += batch.Dispatch - i; });
                break;
            }
            Launch(p, origin, now, clock.DueUs(batch.FirstIndex + i));
        }
    }

    void Launch(size_t p, size_t origin, int64_t now, int64_t dueUs) {
        if (Free_[p].empty() || !Capacity_.Acquire(p)) {
            Update(p, origin, [](TStats& stats) { ++stats.SkippedCapacity; });
            return;
        }
        const size_t id = Free_[p].back();
        Free_[p].pop_back();
        auto& slot = Slots_[id];
        slot.Origin = origin;
        Update(p, origin, [&](TStats& stats) {
            ++stats.Launched;
            stats.SchedulerDelay.RecordValue(std::max<int64_t>(1, now - dueUs));
        });
        auto queue = Queue_;
        const auto start = TClock::now();
        try {
            const auto from = std::uniform_int_distribution<uint64_t>(0, Options_.Rows - Options_.RowsPerRead)(Random_);
            slot.Session->ExecuteDataQuery(Query_, TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(),
                ReadParams(*slot.Session, from, Options_.RowsPerRead), QuerySettings(Options_, p == Short))
                .Subscribe([queue, id, start](const TAsyncDataQueryResult& future) {
                    TCompletion completion;
                    completion.Slot = id;
                    completion.LatencyUs = std::max<int64_t>(1, SinceUs(start));
                    try {
                        const auto& result = future.GetValue();
                        completion.Status = ToString(result.GetStatus()).c_str();
                        completion.Success = result.IsSuccess();
                        completion.Rows = result.IsSuccess() ? ResultRows(result) : 0;
                        completion.ReplaceSession = result.IsTransportError() || result.GetStatus() == EStatus::BAD_SESSION
                            || result.GetStatus() == EStatus::SESSION_EXPIRED || result.GetStatus() == EStatus::SESSION_BUSY;
                        if (!result.IsSuccess() && queue->ErrorSamples->fetch_add(1, std::memory_order_relaxed) < 5) {
                            completion.Error = result.GetIssues().ToString().c_str();
                        }
                    } catch (const std::exception& error) {
                        completion.Status = "CLIENT_EXCEPTION";
                        completion.Error = error.what();
                        completion.ReplaceSession = true;
                    }
                    queue->Push(std::move(completion));
                });
        } catch (const std::exception& error) {
            TCompletion completion;
            completion.Slot = id;
            completion.Status = "CLIENT_EXCEPTION";
            completion.Error = error.what();
            completion.ReplaceSession = true;
            completion.LatencyUs = std::max<int64_t>(1, SinceUs(start));
            queue->Push(std::move(completion));
        }
    }

    void ReplaceSession(size_t id) {
        auto queue = Queue_;
        try {
            Client_.GetSession(SessionSettings()).Subscribe([queue, id](const TAsyncCreateSessionResult& future) {
                TCompletion completion;
                completion.Slot = id;
                completion.SessionResult = true;
                try {
                    const auto& result = future.GetValue();
                    completion.Success = result.IsSuccess();
                    if (result.IsSuccess()) {
                        completion.Session = result.GetSession();
                    } else {
                        completion.Error = result.GetIssues().ToString().c_str();
                    }
                } catch (const std::exception& error) {
                    completion.Error = error.what();
                }
                queue->Push(std::move(completion));
            });
        } catch (const std::exception& error) {
            TCompletion completion;
            completion.Slot = id;
            completion.SessionResult = true;
            completion.Error = error.what();
            queue->Push(std::move(completion));
        }
    }

    void ProcessCompletions(bool replace) {
        std::deque<TCompletion> items;
        {
            std::lock_guard lock(Queue_->Mutex);
            items.swap(Queue_->Items);
        }
        for (auto& completion : items) {
            auto& slot = Slots_[completion.Slot];
            const auto p = slot.Population;
            if (completion.SessionResult) {
                if (completion.Success) {
                    slot.Session = std::move(completion.Session);
                    Free_[p].push_back(completion.Slot);
                    Update(p, slot.Origin, [](TStats& stats) { ++stats.SessionReplacements; });
                } else {
                    Update(p, slot.Origin, [](TStats& stats) { ++stats.SessionFailures; });
                    FailedSessions_.push_back(completion.Slot);
                }
            } else {
                Capacity_.Release(p);
                Update(p, slot.Origin, [&](TStats& stats) {
                    ++stats.Completed;
                    ++stats.Statuses[completion.Status];
                    stats.Latency.RecordValue(completion.LatencyUs);
                    if (completion.Success) {
                        stats.SuccessfulRows += completion.Rows;
                        stats.RowMismatches += completion.Rows != Options_.RowsPerRead;
                        stats.SuccessLatency.RecordValue(completion.LatencyUs);
                    }
                    stats.SessionFailures += completion.ReplaceSession;
                });
                if (completion.ReplaceSession) {
                    slot.Session->Close(TCloseSessionSettings().ClientTimeout(TDuration::Seconds(5)));
                    slot.Session.reset();
                    if (replace) {
                        ReplaceSession(completion.Slot);
                    }
                } else {
                    Free_[p].push_back(completion.Slot);
                }
            }
            // Bound diagnostic logging even under an error flood.
            if (!completion.Error.empty() && ErrorSamples_++ < 5) {
                std::cerr << "Sample error (" << PopulationNames[p] << "): " << completion.Status << ' ' << completion.Error.substr(0, 1000) << '\n';
            }
        }
        const int64_t now = SinceUs(Start_);
        if (replace && now >= NextSessionRetryUs_) {
            NextSessionRetryUs_ = now + 1000000;
            auto failed = std::move(FailedSessions_);
            FailedSessions_.clear();
            for (size_t id : failed) {
                ReplaceSession(id);
            }
        }
    }

    uint64_t Outstanding() const {
        uint64_t result = 0;
        for (size_t p = 0; p < PopulationCount; ++p) {
            result += Capacity_.Outstanding(p);
        }
        return result;
    }

    void Wait(int64_t untilUs) {
        const auto delay = std::clamp<int64_t>(untilUs - SinceUs(Start_), 0, 100000);
        std::unique_lock lock(Queue_->Mutex);
        Queue_->Ready.wait_for(lock, std::chrono::microseconds(delay), [&] { return !Queue_->Items.empty(); });
    }

    TTableClient& Client_;
    const TOptions& Options_;
    const uint64_t Index_;
    std::atomic<bool>& Abort_;
    const std::string Query_;
    std::string Plan_;
    const std::array<uint64_t, PopulationCount> Caps_;
    TCapacity Capacity_;
    std::mt19937_64 Random_;
    std::vector<TSlot> Slots_;
    std::array<std::vector<size_t>, PopulationCount> Free_;
    std::vector<size_t> FailedSessions_;
    std::shared_ptr<TCompletionQueue> Queue_ = std::make_shared<TCompletionQueue>();
    std::mutex StatsMutex_;
    TPopulationStats Totals_;
    TPopulationStats Interval_;
    std::vector<TPopulationStats> Origins_;
    TClock::time_point Start_;
    int64_t NextSessionRetryUs_ = 1000000;
    uint64_t ErrorSamples_ = 0;
};

class TRunner {
public:
    TRunner(TTableClient& client, const TOptions& options)
        : Options_(options)
    {
        auto errorSamples = std::make_shared<std::atomic<uint64_t>>(0);
        for (uint64_t i = 0; i < options.Threads; ++i) {
            Workers_.push_back(std::make_unique<TWorker>(client, options, i, Abort_, errorSamples));
        }
    }

    int Run() {
        for (auto& worker : Workers_) {
            worker->Setup();
        }
        Start_ = TClock::now();
        EmitConfig();
        int64_t phaseStart = 0;
        for (size_t phase = 0; phase < Options_.Phases.size() && !StopRequested; ++phase) {
            const auto& spec = Options_.Phases[phase];
            std::cerr << "Phase " << spec.Name << ": long=" << spec.Rates[Long]
                << "/s short=" << spec.Rates[Short] << "/s threads=" << Options_.Threads << '\n';
            Emit("phase_start", spec.Name, phase);
            RunWorkers(spec.Name, [&](TWorker& worker) {
                worker.RunPhase(Start_, phase, phaseStart);
            });
            Emit("phase_end", spec.Name, phase);
            phaseStart += spec.DurationUs;
        }
        const int64_t drainEnd = SinceUs(Start_) + Options_.DrainTimeoutUs;
        RunWorkers("drain", [&](TWorker& worker) { worker.Drain(Start_, drainEnd); });
        const auto final = Emit("final", "drain");
        const auto outstanding = Outstanding(final);
        if (outstanding) {
            std::cerr << "Drain timed out with " << outstanding << " outstanding requests\n";
            return 2;
        }
        for (const auto& stats : final.Totals) {
            if (stats.RowMismatches) {
                return 1;
            }
        }
        return StopRequested ? 130 : 0;
    }

private:
    template <typename F>
    void RunWorkers(const std::string& phase, F&& work) {
        std::mutex mutex;
        std::condition_variable done;
        size_t remaining = Workers_.size();
        std::exception_ptr error;
        // A phase's threads are joined before the next phase starts, so phase
        // reports never include arrivals launched by the following phase.
        std::vector<std::jthread> threads;
        try {
            for (auto& worker : Workers_) {
                threads.emplace_back([&, worker = worker.get()] {
                    try {
                        work(*worker);
                    } catch (...) {
                        Abort_ = true;
                        std::lock_guard lock(mutex);
                        if (!error) {
                            error = std::current_exception();
                        }
                    }
                    {
                        std::lock_guard lock(mutex);
                        --remaining;
                    }
                    done.notify_one();
                });
            }
            std::unique_lock lock(mutex);
            while (remaining) {
                const auto delay = std::clamp<int64_t>(ReportClock_.NextUs() - SinceUs(Start_), 0, 100000);
                done.wait_for(lock, std::chrono::microseconds(delay), [&] { return !remaining; });
                lock.unlock();
                ReportIfDue(phase);
                lock.lock();
            }
        } catch (...) {
            Abort_ = true;
            throw;
        }
        threads.clear();
        if (error) {
            std::rethrow_exception(error);
        }
    }

    static uint64_t Outstanding(const TSnapshot& snapshot) {
        uint64_t result = 0;
        for (const auto& stats : snapshot.Totals) {
            result += stats.Launched - stats.Completed;
        }
        return result;
    }

    static TJson Populations(const TPopulationStats& stats, bool cumulative) {
        TJson json(NJson::JSON_MAP);
        for (size_t p = 0; p < PopulationCount; ++p) {
            json[PopulationNames[p]] = stats[p].Json(cumulative);
        }
        return json;
    }

    TSnapshot Emit(const std::string& kind, const std::string& phase, std::optional<size_t> origin = {}) {
        TSnapshot snapshot(Options_.Phases.size());
        for (auto& worker : Workers_) {
            worker->Collect(snapshot, kind == "interval");
        }
        const int64_t collectedUs = SinceUs(Start_);
        TJson json(NJson::JSON_MAP);
        json["kind"] = kind;
        json["phase"] = phase;
        json["timestamp"] = TInstant::Now().ToString();
        json["elapsed_s"] = collectedUs / 1000000.0;
        json["totals"] = Populations(snapshot.Totals, true);
        if (origin) {
            json["origin"] = Populations(snapshot.Origins[*origin], true);
        }
        if (kind == "interval") {
            json["interval_s"] = ReportClock_.Record(collectedUs) / 1000000.0;
            json["interval"] = Populations(snapshot.Interval, false);
        }
        if (kind == "final") {
            json["interrupted"] = bool(StopRequested);
            json["drained"] = Outstanding(snapshot) == 0;
            json["origins"].SetType(NJson::JSON_MAP);
            for (size_t i = 0; i < snapshot.Origins.size(); ++i) {
                json["origins"][Options_.Phases[i].Name] = Populations(snapshot.Origins[i], true);
            }
        }
        WriteJson(json);
        return snapshot;
    }

    void EmitConfig() {
        TJson json(NJson::JSON_MAP);
        json["kind"] = "run_start";
        json["endpoint"] = Options_.Endpoint;
        json["database"] = Options_.Database;
        json["table"] = Options_.Table;
        json["seed"] = Options_.Seed;
        json["threads"] = Options_.Threads;
        json["dispatch_batch"] = Options_.DispatchBatch;
        json["rows"] = Options_.Rows;
        json["rows_per_read"] = Options_.RowsPerRead;
        json["cancel_after_us"] = Options_.CancelAfterUs;
        json["long_timeout_us"] = Options_.LongTimeoutUs;
        json["rpc_grace_us"] = Options_.RpcGraceUs;
        json["drain_timeout_us"] = Options_.DrainTimeoutUs;
        json["query"] = ReadQuery(Options_);
        json["query_plan"] = Workers_.front()->Plan();
        json["phases"].SetType(NJson::JSON_ARRAY);
        for (const auto& phase : Options_.Phases) {
            TJson entry(NJson::JSON_MAP);
            entry["name"] = phase.Name;
            entry["duration_us"] = phase.DurationUs;
            for (size_t p = 0; p < PopulationCount; ++p) {
                entry["rates"][PopulationNames[p]] = phase.Rates[p];
            }
            json["phases"].AppendValue(std::move(entry));
        }
        for (size_t p = 0; p < PopulationCount; ++p) {
            json["caps"][PopulationNames[p]] = Options_.Caps[p];
        }
        WriteJson(json);
    }

    void ReportIfDue(const std::string& phase) {
        if (SinceUs(Start_) < ReportClock_.NextUs()) {
            return;
        }
        Emit("interval", phase);
    }

    const TOptions& Options_;
    std::atomic<bool> Abort_ = false;
    std::vector<std::unique_ptr<TWorker>> Workers_;
    TClock::time_point Start_;
    TReportClock ReportClock_;
};

} // namespace
} // namespace NDataShardReadLoad

int main(int argc, char** argv) {
    using namespace NDataShardReadLoad;
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
    try {
        const auto options = ParseOptions(argc, argv);
        uint64_t sessions = 1;
        for (auto cap : options.Caps) {
            sessions += cap;
        }
        auto config = NYdb::TDriverConfig().SetEndpoint(options.Endpoint).SetDatabase(options.Database);
        if (const char* token = std::getenv("YDB_TOKEN")) {
            config.SetAuthToken(token);
        }
        NYdb::TDriver driver(config);
        NYdb::NTable::TTableClient client(driver, NYdb::NTable::TClientSettings()
            .SessionPoolSettings(NYdb::NTable::TSessionPoolSettings()
                .MaxActiveSessions(sessions).MinPoolSize(sessions).RetryLimit(0))
            .AllowRequestMigration(false)
            .RetrySettings(NYdb::NTable::TRetryOperationSettings().MaxRetries(0)));
        int result = 0;
        if (options.Command == "init") {
            Init(client, options);
        } else {
            TRunner runner(client, options);
            result = runner.Run();
        }
        // Bounded draining is handled above. Callbacks retain only their queue,
        // so shutdown never accesses a destroyed runner.
        driver.Stop(false);
        return result;
    } catch (const std::exception& error) {
        std::cerr << "datashard_read_load: " << error.what() << '\n';
        return StopRequested ? 130 : 1;
    }
}
