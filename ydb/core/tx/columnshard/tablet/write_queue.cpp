#include "write_queue.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {

bool TWriteTask::Execute(TColumnShard* owner, const TActorContext& /* ctx */) {
    auto overloadStatus = owner->CheckOverloadedWait(PathId);
    if (overloadStatus != TColumnShard::EOverloadStatus::None) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "wait_overload")("status", overloadStatus);
        return false;
    }

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
    while (WriteTasks.size() && WriteTasks.front().Execute(Owner, ctx)) {
        WriteTasks.pop_front();
    }
    if (WriteTasks.size() && !WriteTasksOverloadCheckerScheduled) {
        Owner->Schedule(TDuration::MilliSeconds(300), new NActors::TEvents::TEvWakeup(1));
        WriteTasksOverloadCheckerScheduled = true;
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "queue_on_write")("size", WriteTasks.size());
    }
    Owner->Counters.GetCSCounters().WritingCounters->QueueWaitSize->Add((i64)WriteTasks.size() - PredWriteTasksSize);
    PredWriteTasksSize = (i64)WriteTasks.size();
    return !WriteTasks.size();
}

void TWriteTasksQueue::Enqueue(TWriteTask&& task) {
    WriteTasks.emplace_back(std::move(task));
}

TWriteTasksQueue::~TWriteTasksQueue() {
    Owner->Counters.GetCSCounters().WritingCounters->QueueWaitSize->Sub(PredWriteTasksSize);
}

}   // namespace NKikimr::NColumnShard
