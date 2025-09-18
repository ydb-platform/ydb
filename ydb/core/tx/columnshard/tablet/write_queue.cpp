#include "write_queue.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>
#include <ydb/core/tx/columnshard/tracing/probes.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {

LWTRACE_USING(YDB_CS);

bool TWriteTask::Execute(TColumnShard* owner, const TActorContext& /* ctx */) const {
    owner->Counters.GetCSCounters().WritingCounters->OnWritingTaskDequeue(TMonotonic::Now() - Created);
    owner->OperationsManager->RegisterLock(LockId, owner->Generation());
    auto writeOperation = owner->OperationsManager->CreateWriteOperation(PathId, LockId, Cookie, GranuleShardingVersionId, ModificationType, IsBulk);

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("writing_size", ArrowData->GetSize())("operation_id", writeOperation->GetIdentifier())(
        "in_flight", NOverload::TOverloadManagerServiceOperator::GetShardWritesInFly())(
        "size_in_flight", NOverload::TOverloadManagerServiceOperator::GetShardWritesSizeInFly());

    AFL_VERIFY(writeOperation);
    writeOperation->SetBehaviour(Behaviour);
    const auto& applyToMvccSnapshot = MvccSnapshot.Valid() ? MvccSnapshot : NOlap::TSnapshot::Max();
    NOlap::TWritingContext wContext(owner->TabletID(), owner->SelfId(), Schema, owner->StoragesManager,
        owner->Counters.GetIndexationCounters().SplitterCounters, owner->Counters.GetCSCounters().WritingCounters, applyToMvccSnapshot, LockId,
        writeOperation->GetActivityChecker(), Behaviour == EOperationBehaviour::NoTxWrite, owner->BufferizationPortionsWriteActorId, IsBulk);
    // We don't need to split here portions by the last level
    // ArrowData->SetSeparationPoints(owner->GetIndexAs<NOlap::TColumnEngineForLogs>().GetGranulePtrVerified(PathId.InternalPathId)->GetBucketPositions());
    writeOperation->Start(*owner, ArrowData, SourceId, wContext);
    return true;
}

void TWriteTask::Abort(TColumnShard* owner, const TString& reason, const TActorContext& ctx, const NKikimrDataEvents::TEvWriteResult::EStatus& status) const {
    LWPROBE(EvWriteResult, owner->TabletID(), SourceId.ToString(), TxId, Cookie, "write_queue", false, reason);
    auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(
        owner->TabletID(), TxId, status, reason);
    owner->Counters.GetWritesMonitor()->OnFinishWrite(ArrowData->GetSize());
    if (status == NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED && OverloadSubscribeSeqNo) {
        result->Record.SetOverloadSubscribed(*OverloadSubscribeSeqNo);
        ctx.Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
            std::make_unique<NOverload::TEvOverloadSubscribe>(NOverload::TColumnShardInfo{.ColumnShardId = owner->SelfId(), .TabletId = owner->TabletID()},
                NOverload::TPipeServerInfo{.PipeServerId = RecipientId, .InterconnectSessionId = owner->PipeServersInterconnectSessions[RecipientId]},
                NOverload::TOverloadSubscriberInfo{.PipeServerId = RecipientId, .OverloadSubscriberId = SourceId, .SeqNo = *OverloadSubscribeSeqNo}));
    }
    ctx.Send(SourceId, result.release(), 0, Cookie);
}

bool TWriteTasksQueue::Drain(const bool onWakeup, const TActorContext& ctx) {
    if (onWakeup) {
        WriteTasksOverloadCheckerScheduled = false;
    }
    ui32 countTasks = 0;
    const TMonotonic now = TMonotonic::Now();
    std::set<TInternalPathId> overloaded;
    for (auto it = WriteTasks.begin(); it != WriteTasks.end();) {
        if (it->IsDeprecated(now)) {
            it->Abort(Owner, "timeout", ctx, NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
            Owner->Counters.GetCSCounters().WritingCounters->TimeoutRate->Inc();
            it = WriteTasks.erase(it);
        } else if (!overloaded.contains(it->GetInternalPathId())) {
            auto overloadStatus = Owner->CheckOverloadedWait(it->GetInternalPathId());
            if (overloadStatus != TColumnShard::EOverloadStatus::None) {
                overloaded.emplace(it->GetInternalPathId());
                Owner->Counters.GetCSCounters().OnWaitingOverload(overloadStatus);
                ++countTasks;
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "wait_overload")("status", overloadStatus)(
                    "path_id", it->GetInternalPathId());
                ++it;
            } else {
                it->Execute(Owner, ctx);
                it = WriteTasks.erase(it);
            }
        } else {
            ++it;
        }
    }

    if (countTasks && !WriteTasksOverloadCheckerScheduled) {
        Owner->Schedule(TDuration::MilliSeconds(300), new NActors::TEvents::TEvWakeup(1));
        WriteTasksOverloadCheckerScheduled = true;
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "queue_on_write")("size", countTasks);
    }
    Owner->Counters.GetCSCounters().WritingCounters->QueueWaitSize->Set(WriteTasks.size());
    return !countTasks;
}

void TWriteTasksQueue::Enqueue(TWriteTask&& task) {
    WriteTasks.emplace(std::move(task));
}

TWriteTasksQueue::~TWriteTasksQueue() {
    Owner->Counters.GetCSCounters().WritingCounters->QueueWaitSize->Sub(WriteTasks.size());
}

}   // namespace NKikimr::NColumnShard
