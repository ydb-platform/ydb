#include "write_gate.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_subdomain_path_id.h>
#include <ydb/core/tx/columnshard/counters/counters_manager.h>
#include <ydb/core/tx/columnshard/counters/tablet_counters.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/operations/manager.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/formats/arrow/size_calcer.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_WRITE

namespace NKikimr::NColumnShard {

namespace {

bool WaitsForCompaction(const EOverloadStatus status) {
    switch (status) {
        case EOverloadStatus::OverloadCompaction:
        case EOverloadStatus::SmallBlobsQuota:
        case EOverloadStatus::OverloadMetadata:
            return true;
        case EOverloadStatus::Disk:
        case EOverloadStatus::ShardTxInFly:
        case EOverloadStatus::ShardWritesInFly:
        case EOverloadStatus::ShardWritesSizeInFly:
        case EOverloadStatus::RejectProbability:
        case EOverloadStatus::None:
            return false;
    }
}

}   // namespace

TWriteGate::~TWriteGate() {
    Counters.GetCSCounters().WritingCounters->QueueWaitSize->Sub(WriteQueue.size());
}

TOverloadStatus TWriteGate::ResourcesStatusToOverloadStatus(const NOverload::EResourcesStatus status) {
    switch (status) {
        case NOverload::EResourcesStatus::Ok:
            return TOverloadStatus{ EOverloadStatus::None, {} };
        case NOverload::EResourcesStatus::WritesInFlyLimitReached:
            return TOverloadStatus{ EOverloadStatus::ShardWritesInFly,
                "The limit on the number of in-flight write requests to a shard has been exceeded. Please add more resources or reduce the "
                "database load." };
        case NOverload::EResourcesStatus::WritesSizeInFlyLimitReached:
            return TOverloadStatus{ EOverloadStatus::ShardWritesSizeInFly,
                "The limit on the total size of in-flight write requests to the shard has been exceeded. Please add more resources or reduce "
                "the database load." };
    }
}

TOverloadStatus TWriteGate::CheckExecutorOverload() const {
    if (Executor.GetStats().IsAnyChannelYellowStop) {
        return TOverloadStatus{ EOverloadStatus::Disk, "Channels are overloaded (yellow), please rebalance groups or add new ones" };
    }
    const ui64 txLimit = TSettings::OverloadTxInFlight;

    if (txLimit && Executor.GetStats().TxInFly > txLimit) {
        YDB_LOG_WARN("",
            {"event", "shard_overload"},
            {"reason", "tx_in_fly"},
            {"sum", Executor.GetStats().TxInFly},
            {"limit", txLimit});
        return TOverloadStatus{ EOverloadStatus::ShardTxInFly, TStringBuilder{} << "The local transaction limit has been exceeded "
                                                                                << Executor.GetStats().TxInFly << " of " << txLimit
                                                                                << ". Please add more resources or reduce the database load." };
    }

    if (AppData()->FeatureFlags.GetEnableOlapRejectProbability()) {
        const float rejectProbabilty = Executor.GetRejectProbability();
        if (rejectProbabilty > 0) {
            const float rnd = TAppData::RandomProvider->GenRandReal2();
            if (rnd < rejectProbabilty) {
                YDB_LOG_WARN("",
                    {"event", "shard_overload"},
                    {"reason", "reject_probality"},
                    {"RP", rejectProbabilty});
                return TOverloadStatus{ EOverloadStatus::RejectProbability,
                    "The local database is overloaded. Please add more resources or reduce the database load." };
            }
        }
    }

    return TOverloadStatus{ EOverloadStatus::None, {} };
}

std::optional<TWriteGate::TRejection> TWriteGate::TryStartWrite(const bool isDelete, const ui64 dataSize) {
    if (SpaceWatcher.IsSubDomainOutOfSpace() && !isDelete) {
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD, "",
            {"event", "skip_writing"},
            {"reason", "quota_exceeded"},
            {"source", "dataevent"});
        return TRejection{
            { EOverloadStatus::Disk,
                "The disk quota has been exhausted. Please increase the available database disk resources or delete unused data." },
            NKikimrDataEvents::TEvWriteResult::STATUS_DATABASE_DISK_SPACE_QUOTA_EXCEEDED
        };
    }
    if (TOverloadStatus overload = CheckExecutorOverload(); overload.Status != EOverloadStatus::None) {
        return TRejection{ std::move(overload), NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED };
    }
    if (TOverloadStatus overload = ResourcesStatusToOverloadStatus(Counters.GetWritesMonitor()->OnStartWrite(dataSize));
        overload.Status != EOverloadStatus::None) {
        return TRejection{ std::move(overload), NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED };
    }
    return std::nullopt;
}

bool TWriteGate::IsTableWritable(const TInternalPathId pathId) const {
    return TablesManager.IsReadyForStartWrite(pathId, false);
}

bool TWriteGate::IsLockAborted(const ui64 lockId) const {
    const auto* lock = OperationsManager.GetLockOptional(lockId);
    return lock && lock->NeedsAborting();
}

EOverloadStatus TWriteGate::CheckCompactionBacklog(const TInternalPathId pathId) const {
    if (AppDataVerified().FeatureFlags.GetEnableSmallBlobsQuotaEnforcement() && SpaceWatcher.IsSubDomainSmallBlobsQuotaExceeded()) {
        return EOverloadStatus::SmallBlobsQuota;
    }
    const auto& engine = TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    if (engine.IsOverloadedByMetadata(NOlap::IColumnEngine::GetMetadataLimit())) {
        return EOverloadStatus::OverloadMetadata;
    }
    if (engine.GetGranuleVerified(pathId).GetOptimizerPlanner().IsOverloaded(engine.GetBadPortionsCounter())) {
        return EOverloadStatus::OverloadCompaction;
    }
    return EOverloadStatus::None;
}

void TWriteGate::AddToWriteQueue(TWriteTask&& task) {
    WriteQueue.emplace(std::move(task));
    Counters.GetCSCounters().WritingCounters->QueueWaitSize->Inc();
}

void TWriteGate::ReportCompactionOverload(const bool compactionWait) {
    if (compactionWait == CompactionOverloadReported) {
        return;
    }
    // Only advance local edge after a real send; otherwise retry next release. Reporting is
    // a no-op while the feature flag is off, and advancing here would permanently hide the
    // edge from a later runtime enablement.
    if (NOverload::TOverloadManagerServiceOperator::ReportCompactionOverload(TabletId, compactionWait)) {
        CompactionOverloadReported = compactionWait;
    }
}

TWriteGate::TDrainedWrites TWriteGate::DrainWriteQueue(const TActorContext& ctx, const bool isRetryWakeup) {
    if (isRetryWakeup) {
        RetryScheduled = false;
    }
    TDrainedWrites drained;
    const ui64 queuedBefore = WriteQueue.size();
    bool compactionWait = false;
    const TMonotonic now = ctx.Monotonic();
    std::set<TInternalPathId> overloaded;
    for (auto it = WriteQueue.begin(); it != WriteQueue.end();) {
        const auto task = it++;
        const TInternalPathId pathId = task->GetInternalPathId();
        if (!IsTableWritable(pathId)) {
            drained.TableDropped.push_back(std::move(WriteQueue.extract(task).value()));
        } else if (IsLockAborted(task->GetLockId())) {
            drained.LockAborted.push_back(std::move(WriteQueue.extract(task).value()));
        } else if (task->IsDeprecated(now)) {
            // Waiting timed out: reply OVERLOADED with the concrete reason (SmallBlobs/Compaction/...) for graphs.
            const EOverloadStatus overload = CheckCompactionBacklog(pathId);
            drained.TimedOut.emplace_back(std::move(WriteQueue.extract(task).value()), overload);
            Counters.GetCSCounters().WritingCounters->TimeoutRate->Inc();
        } else if (!overloaded.contains(pathId)) {
            if (const EOverloadStatus overload = CheckCompactionBacklog(pathId); overload != EOverloadStatus::None) {
                overloaded.emplace(pathId);
                Counters.GetCSCounters().OnWaitingOverload(overload);
                compactionWait |= WaitsForCompaction(overload);
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_WRITE, "",
                    {"event", "wait_overload"},
                    {"status", overload},
                    {"pathId", pathId});
            } else {
                drained.ToStart.push_back(std::move(WriteQueue.extract(task).value()));
            }
        }
    }

    ReportCompactionOverload(compactionWait);
    if (!overloaded.empty() && !RetryScheduled) {
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD, "",
            {"event", "queue_on_write"},
            {"size", overloaded.size()});
    }
    DrainWaitingForLoadedPortions(now, drained);
    if (!overloaded.empty() || HasWaitingDeadline()) {
        ScheduleRetry(ctx);
    }
    // the counter is shared by all shards of the node, so each shard changes it only by its own delta
    Counters.GetCSCounters().WritingCounters->QueueWaitSize->Sub(queuedBefore - WriteQueue.size());
    return drained;
}

bool TWriteGate::ArePortionsLoaded(const NOlap::TEvPassWriteGate& batch) const {
    return TablesManager.GetPrimaryIndexSafe().ArePortionsLoadedFrom(
        batch.GetWriteData().GetWriteMeta().GetPathId().InternalPathId, batch.GetMinKey());
}

void TWriteGate::ScheduleRetry(const TActorContext& ctx) {
    if (RetryScheduled) {
        return;
    }
    ctx.Schedule(TDuration::MilliSeconds(300), new NActors::TEvents::TEvWakeup(1));
    RetryScheduled = true;
}

bool TWriteGate::HasWaitingDeadline() const {
    return std::any_of(WaitingForLoadedPortions.begin(), WaitingForLoadedPortions.end(), [](const TWaitingForLoadedPortions& waiting) {
        return waiting.Deadline.has_value();
    });
}

TWriteGate::EBatchDecision TWriteGate::PassPreparedBatch(NOlap::TEvPassWriteGate::TPtr& ev, const TActorContext& ctx) {
    if (const std::optional<ui64> lockId = ev->Get()->GetContext().GetLockId(); lockId && IsLockAborted(*lockId)) {
        return EBatchDecision::LockAborted;
    }
    if (ArePortionsLoaded(*ev->Get())) {
        return EBatchDecision::Pass;
    }
    const std::optional<TMonotonic> deadline =
        OperationsManager.GetOperationVerified((TOperationWriteId)ev->Get()->GetWriteData().GetWriteMeta().GetWriteId())->GetDeadline();
    WaitingForLoadedPortions.push_back(TWaitingForLoadedPortions{ ev.Release(), deadline });
    Counters.GetTabletCounters()->SetCounter(COUNTER_WRITES_WAITING_LOADED_PORTIONS, WaitingForLoadedPortions.size());
    if (deadline) {
        ScheduleRetry(ctx);
    }
    return EBatchDecision::Hold;
}

void TWriteGate::DrainWaitingForLoadedPortions(const TMonotonic now, TDrainedWrites& drained) {
    const auto firstLoaded =
        std::stable_partition(WaitingForLoadedPortions.begin(), WaitingForLoadedPortions.end(), [&](const TWaitingForLoadedPortions& waiting) {
            return !ArePortionsLoaded(*waiting.Event->Get());
        });
    for (auto it = firstLoaded; it != WaitingForLoadedPortions.end(); ++it) {
        TActivationContext::Send(it->Event.Release());
    }
    WaitingForLoadedPortions.erase(firstLoaded, WaitingForLoadedPortions.end());

    const auto firstTimedOut =
        std::stable_partition(WaitingForLoadedPortions.begin(), WaitingForLoadedPortions.end(), [&](const TWaitingForLoadedPortions& waiting) {
            return !waiting.Deadline || now < *waiting.Deadline;
        });
    for (auto it = firstTimedOut; it != WaitingForLoadedPortions.end(); ++it) {
        Counters.GetCSCounters().WritingCounters->TimeoutRate->Inc();
        drained.TimedOutWaitingForLoadedPortions.push_back(it->Event.Release());
    }
    WaitingForLoadedPortions.erase(firstTimedOut, WaitingForLoadedPortions.end());
    Counters.GetTabletCounters()->SetCounter(COUNTER_WRITES_WAITING_LOADED_PORTIONS, WaitingForLoadedPortions.size());
}

std::vector<NOlap::TEvPassWriteGate::TPtr> TWriteGate::ExtractWaitingForLock(const ui64 lockId) {
    const auto firstOfLock =
        std::stable_partition(WaitingForLoadedPortions.begin(), WaitingForLoadedPortions.end(), [&](const TWaitingForLoadedPortions& waiting) {
            return waiting.Event->Get()->GetContext().GetLockId() != lockId;
        });
    std::vector<NOlap::TEvPassWriteGate::TPtr> extracted;
    for (auto it = firstOfLock; it != WaitingForLoadedPortions.end(); ++it) {
        extracted.push_back(it->Event.Release());
    }
    WaitingForLoadedPortions.erase(firstOfLock, WaitingForLoadedPortions.end());
    Counters.GetTabletCounters()->SetCounter(COUNTER_WRITES_WAITING_LOADED_PORTIONS, WaitingForLoadedPortions.size());
    return extracted;
}

ui64 TWriteGate::GetMemoryUsage() const {
    ui64 memory = WriteQueue.size() * sizeof(TWriteTask);
    for (const TWaitingForLoadedPortions& waiting : WaitingForLoadedPortions) {
        memory += sizeof(NOlap::TEvPassWriteGate) + NArrow::GetBatchDataSize(waiting.Event->Get()->GetBatch().GetContainer());
    }
    return memory;
}

}   // namespace NKikimr::NColumnShard
