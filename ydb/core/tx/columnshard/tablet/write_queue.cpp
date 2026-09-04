#include "write_queue.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>
#include <ydb/core/tx/columnshard/tracing/probes.h>
#include <ydb/core/tx/columnshard/tracing/write_orbit.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {

LWTRACE_USING(YDB_CS);

namespace {
TString OverloadReason(const EOverloadStatus status) {
    switch (status) {
        case EOverloadStatus::SmallBlobsQuota:
            return "The small blobs quota has been exhausted. Please wait for compaction to finish or delete unused data.";
        case EOverloadStatus::OverloadMetadata:
            return "The index metadata limit has been exceeded. Please wait for compaction to finish or delete unused data.";
        case EOverloadStatus::OverloadCompaction:
            return "The compaction queue is overloaded. Please wait for compaction to finish or reduce the database load.";
        case EOverloadStatus::Disk:
            return "The disk quota has been exhausted. Please increase the available database disk resources or delete unused data.";
        case EOverloadStatus::ShardTxInFly:
            return "The local transaction limit has been exceeded. Please add more resources or reduce the database load.";
        case EOverloadStatus::ShardWritesInFly:
            return "The limit on the number of in-flight write requests to a shard has been exceeded. Please add more resources or reduce the "
                   "database load.";
        case EOverloadStatus::ShardWritesSizeInFly:
            return "The limit on the total size of in-flight write requests to the shard has been exceeded. Please add more resources or reduce "
                   "the database load.";
        case EOverloadStatus::RejectProbability:
            return "The local database is overloaded. Please add more resources or reduce the database load.";
        case EOverloadStatus::None:
            return {};
    }
}

// Statuses that mean "this shard cannot accept writes until compaction frees something up".
// They are what the OverloadManager republishes as node-level overload to the flow control
// managers, so a shard blocked on the small-blobs quota or on the metadata limit has to mark
// its node hot exactly like a shard blocked on the compaction queue.
bool IsCompactionWaitStatus(const EOverloadStatus status) {
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

bool TWriteTask::Execute(TColumnShard* owner, const TActorContext& ctx) const {
    owner->Counters.GetCSCounters().WritingCounters->OnWritingTaskDequeue(ctx.Monotonic() - Created);
    if (Orbit) {
        LWTRACK(WriteDequeued, *Orbit, PathId.GetInternalPathId().GetRawValue(), owner->TabletID(), TxId, Cookie, ctx.Monotonic() - Created);
    }

    if (const auto lock = owner->OperationsManager->GetLockOptional(LockId); lock) {
        if (lock->NeedsAborting()) {
            Abort(owner, "transaction is aborted", ctx, NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
            return true;
        }
    }

    owner->OperationsManager->RegisterLock(LockId, owner->Generation());
    owner->SubscribeLockIfNotAlready(LockId, LockNodeId);
    auto writeOperation =
        owner->OperationsManager->CreateWriteOperation(PathId, LockId, Cookie, GranuleShardingVersionId, ModificationType, IsBulk);

    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_WRITE, "",
        {"writingSize", ArrowData->GetSize()},
        {"operationId", writeOperation->GetIdentifier()},
        {"inFlight", NOverload::TOverloadManagerServiceOperator::GetShardWritesInFly()},
        {"sizeInFlight", NOverload::TOverloadManagerServiceOperator::GetShardWritesSizeInFly()});

    AFL_VERIFY(writeOperation);
    writeOperation->SetBehaviour(Behaviour);
    const auto applyToMvccSnapshot = MvccSnapshot.Valid() ? MvccSnapshot : owner->GetMaxReadVersionForSchema(Schema->GetVersion());
    NOlap::TWritingContext wContext(owner->TabletID(), owner->SelfId(), Schema, owner->StoragesManager,
        owner->Counters.GetIndexationCounters().SplitterCounters, owner->Counters.GetCSCounters().WritingCounters, applyToMvccSnapshot, LockId,
        LockMode, writeOperation->GetActivityChecker(), Behaviour == EOperationBehaviour::NoTxWrite, owner->BufferizationPortionsWriteActorId,
        IsBulk);
    // We don't need to split here portions by the last level
    // ArrowData->SetSeparationPoints(owner->GetIndexAs<NOlap::TColumnEngineForLogs>().GetGranulePtrVerified(PathId.InternalPathId)->GetBucketPositions());
    writeOperation->Start(*owner, ArrowData, SourceId, wContext, Orbit, TxId, ReceivedAt);
    return true;
}

void TWriteTask::Abort(
    TColumnShard* owner, const TString& reason, const TActorContext& ctx, const NKikimrDataEvents::TEvWriteResult::EStatus& status) const {
    if (Orbit) {
        TrackWriteFailed(*Orbit, PathId.GetInternalPathId().GetRawValue(), owner->TabletID(), TxId, Cookie, SourceId.ToString(), "write_queue",
            ToString(status), reason);
    }
    auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(owner->TabletID(), TxId, status, reason);
    owner->Counters.GetWritesMonitor()->OnFinishWrite(ArrowData->GetSize());
    if (status == NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED && OverloadSubscribeSeqNo) {
        result->Record.SetOverloadSubscribed(*OverloadSubscribeSeqNo);
        ctx.Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
            std::make_unique<NOverload::TEvOverloadSubscribe>(
                NOverload::TColumnShardInfo{ .ColumnShardId = owner->SelfId(), .TabletId = owner->TabletID() },
                NOverload::TPipeServerInfo{
                    .PipeServerId = RecipientId, .InterconnectSessionId = owner->PipeServersInterconnectSessions[RecipientId] },
                NOverload::TOverloadSubscriberInfo{
                    .PipeServerId = RecipientId, .OverloadSubscriberId = SourceId, .SeqNo = *OverloadSubscribeSeqNo }));
    }
    ctx.Send(SourceId, result.release(), 0, Cookie);
}

void TWriteTask::FailByOverload(TColumnShard* owner, const EOverloadStatus overloadStatus, const TActorContext& ctx) const {
    AFL_VERIFY(overloadStatus != EOverloadStatus::None);
    const TString reason = TStringBuilder{} << "Column shard " << owner->TabletID()
                                            << " is overloaded. Reason: " << OverloadReason(overloadStatus);
    if (Orbit) {
        TrackWriteFailed(*Orbit, PathId.GetInternalPathId().GetRawValue(), owner->TabletID(), TxId, Cookie, SourceId.ToString(), "write_queue",
            ToString(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED), reason);
    }
    auto result =
        NEvents::TDataEvents::TEvWriteResult::BuildError(owner->TabletID(), TxId, NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED, reason);
    owner->Counters.GetWritesMonitor()->OnFinishWrite(ArrowData->GetSize());
    if (OverloadSubscribeSeqNo) {
        result->Record.SetOverloadSubscribed(*OverloadSubscribeSeqNo);
        ctx.Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
            std::make_unique<NOverload::TEvOverloadSubscribe>(
                NOverload::TColumnShardInfo{ .ColumnShardId = owner->SelfId(), .TabletId = owner->TabletID() },
                NOverload::TPipeServerInfo{
                    .PipeServerId = RecipientId, .InterconnectSessionId = owner->PipeServersInterconnectSessions[RecipientId] },
                NOverload::TOverloadSubscriberInfo{
                    .PipeServerId = RecipientId, .OverloadSubscriberId = SourceId, .SeqNo = *OverloadSubscribeSeqNo }));
    }
    owner->OverloadWriteFail(overloadStatus, NEvWrite::TWriteMeta(0, PathId, SourceId, {}, TGUID::CreateTimebased().AsGuidString(),
                                                 owner->Counters.GetCSCounters().WritingCounters->GetWriteFlowCounters()), ArrowData->GetSize(),
        Cookie, std::move(result), ctx);
}

bool TWriteTasksQueue::Drain(const bool onWakeup, const TActorContext& ctx) {
    if (onWakeup) {
        WriteTasksOverloadCheckerScheduled = false;
    }
    ui32 countTasks = 0;
    bool compactionWait = false;
    const TMonotonic now = ctx.Monotonic();
    std::set<TInternalPathId> overloaded;
    for (auto it = WriteTasks.begin(); it != WriteTasks.end();) {
        if (it->IsDeprecated(now)) {
            // Waiting timed out: reply OVERLOADED with the concrete reason (SmallBlobs/Compaction/...) for graphs.
            const auto overloadStatus = Owner->CheckOverloadedWait(it->GetInternalPathId());
            if (overloadStatus != EOverloadStatus::None) {
                it->FailByOverload(Owner, overloadStatus, ctx);
            } else {
                it->Abort(Owner, "timeout", ctx, NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
            }
            Owner->Counters.GetCSCounters().WritingCounters->TimeoutRate->Inc();
            it = WriteTasks.erase(it);
        } else if (!overloaded.contains(it->GetInternalPathId())) {
            const auto overloadStatus = Owner->CheckOverloadedWait(it->GetInternalPathId());
            if (overloadStatus != EOverloadStatus::None) {
                overloaded.emplace(it->GetInternalPathId());
                Owner->Counters.GetCSCounters().OnWaitingOverload(overloadStatus);
                compactionWait |= IsCompactionWaitStatus(overloadStatus);
                ++countTasks;
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_WRITE, "",
                    {"event", "wait_overload"},
                    {"status", overloadStatus},
                    {"pathId", it->GetInternalPathId()});
                ++it;
            } else {
                it->Execute(Owner, ctx);
                it = WriteTasks.erase(it);
            }
        } else {
            ++it;
        }
    }

    if (compactionWait != CompactionOverloadReported) {
        auto* actorSystem = NActors::TActivationContext::ActorSystem();
        if (actorSystem) {
            // Only advance local edge after a real send; otherwise retry next Drain. Reporting is
            // a no-op while the feature flag is off, and advancing here would permanently hide the
            // edge from a later runtime enablement.
            if (NOverload::TOverloadManagerServiceOperator::ReportCompactionOverload(Owner->TabletID(), compactionWait)) {
                CompactionOverloadReported = compactionWait;
            }
        }
    }

    if (countTasks && !WriteTasksOverloadCheckerScheduled) {
        Owner->Schedule(TDuration::MilliSeconds(300), new NActors::TEvents::TEvWakeup(1));
        WriteTasksOverloadCheckerScheduled = true;
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD, "",
            {"event", "queue_on_write"},
            {"size", countTasks});
    }
    Owner->Counters.GetCSCounters().WritingCounters->QueueWaitSize->Set(WriteTasks.size());
    return !countTasks;
}

void TWriteTasksQueue::Enqueue(TWriteTask&& task) {
    WriteTasks.emplace(std::move(task));
}

TWriteTasksQueue::~TWriteTasksQueue() {
    if (CompactionOverloadReported) {
        auto* actorSystem = NActors::TActivationContext::ActorSystem();
        if (actorSystem) {
            NOverload::TOverloadManagerServiceOperator::ReportCompactionOverload(Owner->TabletID(), false);
            CompactionOverloadReported = false;
        }
    }
    Owner->Counters.GetCSCounters().WritingCounters->QueueWaitSize->Sub(WriteTasks.size());
}

}   // namespace NKikimr::NColumnShard
