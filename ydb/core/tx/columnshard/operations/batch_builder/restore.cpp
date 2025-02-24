#include "restore.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>
#include <ydb/core/tx/columnshard/operations/slice_builder/builder.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap {

std::unique_ptr<TEvColumnShard::TEvInternalScan> TModificationRestoreTask::DoBuildRequestInitiator() const {
    auto request = std::make_unique<TEvColumnShard::TEvInternalScan>(LocalPathId, Snapshot, WriteData.GetWriteMeta().GetLockIdOptional());
    request->TaskIdentifier = GetTaskId();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "restore_start")("count", IncomingData.HasContainer() ? IncomingData->num_rows() : 0)(
        "task_id", WriteData.GetWriteMeta().GetId());
    auto pkData = NArrow::TColumnOperator().VerifyIfAbsent().Extract(IncomingData.GetContainer(), Context.GetActualSchema()->GetPKColumnNames());
    request->RangesFilter = TPKRangesFilter::BuildFromRecordBatchLines(pkData, false);
    for (auto&& i : Context.GetActualSchema()->GetIndexInfo().GetColumnIds(false)) {
        request->AddColumn(i);
    }
    return request;
}

TConclusionStatus TModificationRestoreTask::DoOnDataChunk(const std::shared_ptr<arrow::Table>& data) {
    auto result = Merger->AddExistsDataOrdered(data);
    if (result.IsFail()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "merge_data_problems")("write_id", WriteData.GetWriteMeta().GetWriteId())(
            "tablet_id", GetTabletId())("message", result.GetErrorMessage());
        SendErrorMessage(result.GetErrorMessage(), NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Request);
    }
    return result;
}

void TModificationRestoreTask::DoOnError(const TString& errorMessage) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_RESTORE)("event", "restore_data_problems")("write_id", WriteData.GetWriteMeta().GetWriteId())(
        "tablet_id", GetTabletId())("message", errorMessage);
    SendErrorMessage(errorMessage, NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
}

NKikimr::TConclusionStatus TModificationRestoreTask::DoOnFinished() {
    {
        auto result = Merger->Finish();
        if (result.IsFail()) {
            OnError("cannot finish merger: " + result.GetErrorMessage());
            return result;
        }
    }

    auto batchResult = Merger->BuildResultBatch();
    if (!WriteData.GetWritePortions() || !Context.GetNoTxWrite()) {
        std::shared_ptr<NConveyor::ITask> task =
            std::make_shared<NOlap::TBuildSlicesTask>(std::move(WriteData), batchResult.GetContainer(), Context);
        NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
    } else {
        NActors::TActivationContext::ActorSystem()->Send(
            Context.GetBufferizationPortionsActorId(), new NWritingPortions::TEvAddInsertedDataToBuffer(
                               std::make_shared<NEvWrite::TWriteData>(WriteData), batchResult, std::make_shared<TWritingContext>(Context)));
    }
    return TConclusionStatus::Success();
}

TModificationRestoreTask::TModificationRestoreTask(NEvWrite::TWriteData&& writeData, const std::shared_ptr<IMerger>& merger,
    const TSnapshot actualSnapshot, const NArrow::TContainerWithIndexes<arrow::RecordBatch>& incomingData,
    const TWritingContext& context)
    : TBase(context.GetTabletId(), context.GetTabletActorId(),
          writeData.GetWriteMeta().GetId() + "::" + ::ToString(writeData.GetWriteMeta().GetWriteId()))
    , WriteData(std::move(writeData))
    , Merger(merger)
    , LocalPathId(WriteData.GetWriteMeta().GetTableId())
    , Snapshot(actualSnapshot)
    , IncomingData(incomingData)
    , Context(context) {
        AFL_VERIFY(actualSnapshot.Valid());
}

void TModificationRestoreTask::SendErrorMessage(
    const TString& errorMessage, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass) {
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(*writeDataPtr) });
    auto evResult =
        NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), errorMessage, errorClass);
    TActorContext::AsActorContext().Send(Context.GetTabletActorId(), evResult.release());
}

TDuration TModificationRestoreTask::GetTimeout() const {
    static const TDuration criticalTimeoutDuration = TDuration::Seconds(1200);
    if (!HasAppData()) {
        return criticalTimeoutDuration;
    }
    if (!AppDataVerified().ColumnShardConfig.HasRestoreDataOnWriteTimeoutSeconds() ||
        !AppDataVerified().ColumnShardConfig.GetRestoreDataOnWriteTimeoutSeconds()) {
        return criticalTimeoutDuration;
    }
    return TDuration::Seconds(AppDataVerified().ColumnShardConfig.GetRestoreDataOnWriteTimeoutSeconds());
}

}   // namespace NKikimr::NOlap
