// The real compute scheduler driven by a simulation: the clock, the random delays and the wake-ups of the tasks
// come from the simulation - no actor system. Shared by main.cpp (the line protocol on stdin/stdout) and api.cpp
// (the same scheduler linked into the caller's process).
#pragma once

#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_base.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_memory.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/kqp/runtime/scheduler/tree/snapshot.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/monotonic_provider.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <algorithm>
#include <cmath>

namespace NKikimr::NKqp::NSimulator {

using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NScheduler;

// hardcoded from ydb/core/protos/table_service_config.proto
constexpr TDelayParams kDelayParams{
    .MaxDelay = TDuration::MicroSeconds(3'000'000),
    .MinDelay = TDuration::MicroSeconds(10),
    .AttemptBonus = TDuration::MicroSeconds(5),
    .MaxRandomDelay = TDuration::MicroSeconds(100),
};

const TString kDatabaseId = "/sim";

// Not zero: the period of the first snapshot is counted from the start
const TMonotonic kStart = TMonotonic::Seconds(1);

// The clock which moves only when the simulation moves it
class TManualTimeProvider : public NMonotonic::IMonotonicTimeProvider {
public:
    TMonotonic Now() override {
        return Current;
    }

    void Set(TMonotonic now) {
        Y_ENSURE(now >= Current, "The clock goes back: " << now.MicroSeconds() << " < " << Current.MicroSeconds());
        Current = now;
    }

private:
    TMonotonic Current = kStart;
};

class TSimulator {
public:
    explicit TSimulator(ui64 seed)
        : Clock(MakeIntrusive<TManualTimeProvider>())
        , Random(CreateDeterministicRandomProvider(seed))
        , Scheduler(MakeIntrusive<NMonitoring::TDynamicCounters>(), NScheduler::TOptions{
            .DelayParams = kDelayParams,
            .TimeProvider = Clock,
        })
    {}

    ~TSimulator() {
        StopTasks();
    }

    // Returns false on quit
    bool Handle(const TString& line, TStringBuilder& reply) {
        const auto args = StringSplitter(line).Split(' ').SkipEmpty().ToList<TString>();
        if (args.empty()) {
            reply << "error empty command";
            return true;
        }

        const auto& cmd = args[0];
        const auto arg = [&](size_t i) -> const TString& {
            Y_ENSURE(i < args.size(), "Not enough arguments for " << cmd);
            return args[i];
        };
        const auto num = [&](size_t i) {
            return FromString<i64>(arg(i));
        };
        const auto nonNegative = [&](size_t i, const char* name) {
            const auto value = num(i);
            Y_ENSURE(value >= 0, name << " must be nonnegative");
            return static_cast<ui64>(value);
        };

        if (cmd == "quit") {
            StopTasks();
            return false;
        } else if (cmd == "limits") {
            auto config = Scheduler.GetStaticTree()->Clone();
            config->TotalCpuLimit = num(1);
            config->TotalMemoryLimit = num(2);
            Scheduler.UpdateConfiguration(std::move(config));
            reply << "ok";
        } else if (cmd == "pool") {
            NHdrf::TStaticAttributes attrs;
            if (const auto bytes = num(2); bytes >= 0) {
                attrs.MemoryLimit = bytes;
            }
            auto config = Scheduler.GetStaticTree()->Clone();
            config->GetOrCreateDatabase(kDatabaseId).GetOrCreatePool(arg(1)).SetAttributes(attrs);
            Scheduler.UpdateConfiguration(std::move(config));
            Y_ENSURE(Scheduler.GetReadQuery(kDatabaseId, arg(1)), "Unable to initialize pool " << arg(1));
            if (std::ranges::find(Pools, arg(1)) == Pools.end()) {
                Pools.push_back(arg(1));
            }
            reply << "ok";
        } else if (cmd == "query") {
            const auto qid = num(1);
            Y_ENSURE(!Queries.contains(qid), "Query already exists " << qid);
            const auto elasticPercent = FromString<double>(arg(3));
            Y_ENSURE(std::isfinite(elasticPercent) && elasticPercent >= 0 && elasticPercent <= 100,
                "Elastic percent must be between 0 and 100");
            auto query = Scheduler.AddOrUpdateQuery(kDatabaseId, arg(2), qid);
            Memories[qid] = std::make_shared<TSchedulableMemory>(query, elasticPercent);
            Queries[qid] = query;
            reply << "ok";
        } else if (cmd == "query_end") {
            const auto qid = num(1);
            if (const auto query = Queries.find(qid); query != Queries.end()) {
                if (const auto bytes = query->second->MemoryUsage.load()) {
                    GetMemory(qid).DecreaseUsage(bytes);
                }
                Memories.erase(qid);
                Queries.erase(query);
                Scheduler.RemoveQuery(qid, /* isForceRemove = */ true);
            }
            reply << "ok";
        } else if (cmd == "task") {
            const auto tid = num(1);
            Y_ENSURE(!Tasks.contains(tid), "Task already exists " << tid);
            auto task = std::make_unique<TSchedulableBase>(TSchedulableOptions{
                .Query = GetQuery(num(2)),
                .IsSchedulable = true,
                .TimeProvider = Clock,
                .Random = [this] { return Random->GenRand(); },
            });
            task->RegisterForResume([this, tid] {
                Woken.push_back(tid);
            });
            Tasks[tid] = std::move(task);
            reply << "ok";
        } else if (cmd == "task_end") {
            const auto tid = num(1);
            auto& task = GetTask(tid);
            if (task.IsExecuting() || task.IsThrottled()) {
                task.StopExecution();
            }
            Tasks.erase(tid);
            reply << "ok";
        } else if (cmd == "at") {
            Clock->Set(kStart + TDuration::MicroSeconds(nonNegative(1, "Timestamp")));
            reply << "ok";
        } else if (cmd == "start") {
            if (const auto delay = GetTask(num(1)).TryStartExecution(Clock->Now())) {
                reply << "delay " << delay->MicroSeconds();
            } else {
                reply << "ok";
            }
        } else if (cmd == "stop") {
            GetTask(num(1)).StopExecution();
            reply << "ok";
        } else if (cmd == "resumed") {
            GetTask(num(1)).NotifyResumed(num(2) != 0);
            reply << "ok";
        } else if (cmd == "mem") {
            reply << (GetMemory(num(1)).TryIncreaseUsage(nonNegative(2, "Bytes"), num(3) != 0) ? "ok" : "no");
        } else if (cmd == "free") {
            const auto qid = num(1);
            const auto bytes = nonNegative(2, "Bytes");
            const auto usage = GetQuery(qid)->MemoryUsage.load();
            Y_ENSURE(bytes <= usage, "Cannot free " << bytes << " bytes: query holds " << usage);
            GetMemory(qid).DecreaseUsage(bytes);
            reply << "ok";
        } else if (cmd == "avail") {
            reply << GetMemory(num(1)).GetAvailability();
        } else if (cmd == "update") {
            Scheduler.UpdateFairShare();
            reply << "ok";
        } else if (cmd == "state") {
            reply << "ok";
            for (const auto& poolId : Pools) {
                // The read query of the pool has the snapshot of the pool as its parent
                const auto query = Scheduler.GetReadQuery(kDatabaseId, poolId);
                const auto snapshot = query ? query->GetSnapshot() : nullptr;
                const auto* pool = snapshot ? snapshot->GetParent() : nullptr;
                if (pool) {
                    reply << " " << poolId << ":" << pool->CpuFairShare << ":" << pool->MemoryFairShare << ":" << pool->MemoryUsage;
                }
            }
        } else {
            reply << "error unknown command " << cmd;
        }

        return true;
    }

    void AppendWoken(TStringBuilder& reply) {
        if (Woken.empty()) {
            return;
        }
        reply << " |";
        for (const auto tid : Woken) {
            reply << " " << tid;
        }
        Woken.clear();
    }

private:
    void StopTasks() {
        // Cancel waiting tasks first so releasing CPU slots does not queue wake-ups during shutdown.
        for (auto& [_, task] : Tasks) {
            if (task->IsThrottled()) {
                task->StopExecution();
            }
        }
        for (auto& [_, task] : Tasks) {
            if (task->IsExecuting()) {
                task->StopExecution();
            }
        }
    }

    NHdrf::NDynamic::TQueryPtr GetQuery(i64 qid) const {
        const auto it = Queries.find(qid);
        Y_ENSURE(it != Queries.end(), "Unknown query " << qid);
        return it->second;
    }

    TSchedulableMemory& GetMemory(i64 qid) const {
        const auto it = Memories.find(qid);
        Y_ENSURE(it != Memories.end(), "Unknown query " << qid);
        return *it->second;
    }

    TSchedulableBase& GetTask(i64 tid) const {
        const auto it = Tasks.find(tid);
        Y_ENSURE(it != Tasks.end(), "Unknown task " << tid);
        return *it->second;
    }

private:
    const TIntrusivePtr<TManualTimeProvider> Clock;
    const TIntrusivePtr<IRandomProvider> Random;
    TComputeScheduler Scheduler;

    TVector<TString> Pools;
    THashMap<i64, NHdrf::NDynamic::TQueryPtr> Queries;
    THashMap<i64, TSchedulableMemoryPtr> Memories;
    TVector<i64> Woken;
    THashMap<i64, std::unique_ptr<TSchedulableBase>> Tasks;
};

}  // namespace NKikimr::NKqp::NSimulator
