#pragma once
#include "events.h"

#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/counters/columnshard.h>
#include <ydb/core/tx/columnshard/operations/batch_builder/builder.h>
#include <ydb/core/tx/columnshard/operations/write_task.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>

#include <optional>
#include <set>
#include <vector>

namespace NKikimr::NColumnShard {

class TCountersManager;
class TOperationsManager;
class TSpaceWatcher;
class TTablesManager;

class TWriteGate {
public:
    struct TRejection {
        TOverloadStatus Overload;
        NKikimrDataEvents::TEvWriteResult::EStatus Answer;
    };

    // the shard starts or answers these in the same handler, so no drop can come between the check and the write operation
    struct TDrainedWrites {
        std::vector<TWriteTask> ToStart;
        std::vector<TWriteTask> TableDropped;
        std::vector<TWriteTask> LockAborted;
        // None: the wait timed out without an overload to name
        std::vector<std::pair<TWriteTask, EOverloadStatus>> TimedOut;
        std::vector<NOlap::TEvPassWriteGate::TPtr> TimedOutWaitingForLoadedPortions;
    };

private:
    const NTabletFlatExecutor::NFlatExecutorSetup::IExecutor& Executor;
    const TTablesManager& TablesManager;
    const TOperationsManager& OperationsManager;
    const TSpaceWatcher& SpaceWatcher;
    const TCountersManager& Counters;
    const ui64 TabletId;
    std::set<TWriteTask> WriteQueue;
    bool RetryScheduled = false;
    bool CompactionOverloadReported = false;

    struct TWaitingForLoadedPortions {
        NOlap::TEvPassWriteGate::TPtr Event;
        std::optional<TMonotonic> Deadline;
    };

    // a held batch is re-sent to the shard as is, so its handler checks it again
    std::vector<TWaitingForLoadedPortions> WaitingForLoadedPortions;

    static TOverloadStatus ResourcesStatusToOverloadStatus(const NOverload::EResourcesStatus status);
    TOverloadStatus CheckExecutorOverload() const;
    EOverloadStatus CheckCompactionBacklog(const TInternalPathId pathId) const;
    void ReportCompactionOverload(const bool compactionWait);
    bool ArePortionsLoaded(const NOlap::TEvPassWriteGate& batch) const;
    void DrainWaitingForLoadedPortions(const TMonotonic now, TDrainedWrites& drained);
    bool HasWaitingDeadline() const;
    void ScheduleRetry(const TActorContext& ctx);

public:
    TWriteGate(const NTabletFlatExecutor::NFlatExecutorSetup::IExecutor& executor, const TTablesManager& tablesManager,
        const TOperationsManager& operationsManager, const TSpaceWatcher& spaceWatcher, const TCountersManager& counters, const ui64 tabletId)
        : Executor(executor)
        , TablesManager(tablesManager)
        , OperationsManager(operationsManager)
        , SpaceWatcher(spaceWatcher)
        , Counters(counters)
        , TabletId(tabletId)
    {
    }

    ~TWriteGate();

    // TEvWrite; nullopt: the write is started and counted in flight
    std::optional<TRejection> TryStartWrite(const bool isDelete, const ui64 dataSize);

    bool IsTableWritable(const TInternalPathId pathId) const;
    bool IsLockAborted(const ui64 lockId) const;

    void AddToWriteQueue(TWriteTask&& task);
    [[nodiscard]] TDrainedWrites DrainWriteQueue(const TActorContext& ctx, const bool isRetryWakeup = false);

    enum class EBatchDecision {
        Pass,
        Hold,
        LockAborted
    };
    EBatchDecision PassPreparedBatch(NOlap::TEvPassWriteGate::TPtr& ev, const TActorContext& ctx);
    std::vector<NOlap::TEvPassWriteGate::TPtr> ExtractWaitingForLock(const ui64 lockId);

    ui64 GetMemoryUsage() const;
};

}   // namespace NKikimr::NColumnShard
