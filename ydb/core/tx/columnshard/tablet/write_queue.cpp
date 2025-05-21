#include "write_queue.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {

bool TWriteTask::Execute(TColumnShard* owner, const TActorContext& /* ctx */) {
    owner->Counters.GetCSCounters().WritingCounters->OnWritingTaskDequeue(TMonotonic::Now() - Created);
    owner->OperationsManager->RegisterLock(LockId, owner->Generation());
    auto writeOperation = owner->OperationsManager->RegisterOperation(
        PathId, LockId, Cookie, GranuleShardingVersionId, ModificationType, AppDataVerified().FeatureFlags.GetEnableWritePortionsOnInsert());

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("writing_size", ArrowData->GetSize())("operation_id", writeOperation->GetIdentifier())(
        "in_flight", owner->Counters.GetWritesMonitor()->GetWritesInFlight())(
        "size_in_flight", owner->Counters.GetWritesMonitor()->GetWritesSizeInFlight());

    AFL_VERIFY(writeOperation);
    writeOperation->SetBehaviour(Behaviour);
    NOlap::TWritingContext wContext(owner->TabletID(), owner->SelfId(), Schema, owner->StoragesManager,
        owner->Counters.GetIndexationCounters().SplitterCounters, owner->Counters.GetCSCounters().WritingCounters, NOlap::TSnapshot::Max(),
        writeOperation->GetActivityChecker(), Behaviour == EOperationBehaviour::NoTxWrite, owner->BufferizationInsertionWriteActorId,
        owner->BufferizationPortionsWriteActorId);
    ArrowData->SetSeparationPoints(owner->GetIndexAs<NOlap::TColumnEngineForLogs>().GetGranulePtrVerified(PathId)->GetBucketPositions());
    writeOperation->Start(*owner, ArrowData, SourceId, wContext);
    return true;
}

bool TWriteTasksQueue::Drain(const bool onWakeup, const TActorContext& ctx) {
    if (onWakeup) {
        WriteTasksOverloadCheckerScheduled = false;
    }
    std::vector<TInternalPathId> toRemove;
    ui32 countTasks = 0;
    for (auto&& i : WriteTasks) {
        auto overloadStatus = Owner->CheckOverloadedWait(i.first);
        if (overloadStatus != TColumnShard::EOverloadStatus::None) {
            Owner->Counters.GetCSCounters().OnWaitingOverload(overloadStatus);
            countTasks += i.second.size();
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "wait_overload")("status", overloadStatus)("path_id", i.first)(
                "size", i.second.size());
            continue;
        }
        for (auto&& t : i.second) {
            t.Execute(Owner, ctx);
        }
        toRemove.emplace_back(i.first);
    }

    for (auto&& i : toRemove) {
        AFL_VERIFY(WriteTasks.erase(i));
    }

    if (countTasks && !WriteTasksOverloadCheckerScheduled) {
        Owner->Schedule(TDuration::MilliSeconds(300), new NActors::TEvents::TEvWakeup(1));
        WriteTasksOverloadCheckerScheduled = true;
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "queue_on_write")("size", countTasks);
    }
    Owner->Counters.GetCSCounters().WritingCounters->QueueWaitSize->Add((i64)countTasks - PredWriteTasksSize);
    PredWriteTasksSize = (i64)countTasks;
    return !countTasks;
}

void TWriteTasksQueue::Enqueue(TWriteTask&& task) {
    const TInternalPathId pathId = task.GetPathId();
    WriteTasks[pathId].emplace_back(std::move(task));
}

TWriteTasksQueue::~TWriteTasksQueue() {
    Owner->Counters.GetCSCounters().WritingCounters->QueueWaitSize->Sub(PredWriteTasksSize);
}

}   // namespace NKikimr::NColumnShard
