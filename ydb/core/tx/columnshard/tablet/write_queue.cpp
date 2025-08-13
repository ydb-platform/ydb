#include "write_queue.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {

bool TWriteTask::Execute(TColumnShard* owner, const TActorContext& /* ctx */) const {
    owner->Counters.GetCSCounters().WritingCounters->OnWritingTaskDequeue(TMonotonic::Now() - Created);
    owner->OperationsManager->RegisterLock(LockId, owner->Generation());
    auto writeOperation = owner->OperationsManager->CreateWriteOperation(PathId, LockId, Cookie, GranuleShardingVersionId, ModificationType, IsBulk);

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("writing_size", ArrowData->GetSize())("operation_id", writeOperation->GetIdentifier())(
        "in_flight", owner->Counters.GetWritesMonitor()->GetWritesInFlight())(
        "size_in_flight", owner->Counters.GetWritesMonitor()->GetWritesSizeInFlight());

    AFL_VERIFY(writeOperation);
    writeOperation->SetBehaviour(Behaviour);
    NOlap::TWritingContext wContext(owner->TabletID(), owner->SelfId(), Schema, owner->StoragesManager,
        owner->Counters.GetIndexationCounters().SplitterCounters, owner->Counters.GetCSCounters().WritingCounters, NOlap::TSnapshot::Max(),
        writeOperation->GetActivityChecker(), Behaviour == EOperationBehaviour::NoTxWrite, owner->BufferizationPortionsWriteActorId);
    ArrowData->SetSeparationPoints(owner->GetIndexAs<NOlap::TColumnEngineForLogs>().GetGranulePtrVerified(PathId.InternalPathId)->GetBucketPositions());
    writeOperation->Start(*owner, ArrowData, SourceId, wContext);
    return true;
}

void TWriteTask::Abort(TColumnShard* owner, const TString& reason, const TActorContext& ctx) const {
    auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(
        owner->TabletID(), TxId, NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, reason);
    owner->Counters.GetWritesMonitor()->OnFinishWrite(ArrowData->GetSize());
    owner->UpdateOverloadsStatus();
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
            it->Abort(Owner, "timeout", ctx);
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
