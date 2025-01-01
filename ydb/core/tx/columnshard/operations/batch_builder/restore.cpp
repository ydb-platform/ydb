#include "restore.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/operations/slice_builder/builder.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap {

std::unique_ptr<NKikimr::TEvColumnShard::TEvInternalScan> TModificationRestoreTask::DoBuildRequestInitiator() const {
    auto request = std::make_unique<TEvColumnShard::TEvInternalScan>(LocalPathId, WriteData.GetWriteMeta().GetLockIdOptional());
    request->ReadToSnapshot = Snapshot;
    auto pkData = NArrow::TColumnOperator().VerifyIfAbsent().Extract(IncomingData, Context.GetActualSchema()->GetPKColumnNames());
    request->RangesFilter = TPKRangesFilter::BuildFromRecordBatchLines(pkData, false);
    for (auto&& i : Context.GetActualSchema()->GetIndexInfo().GetColumnIds(false)) {
        request->AddColumn(i, Context.GetActualSchema()->GetIndexInfo().GetColumnName(i));
    }
    return request;
}

NKikimr::TConclusionStatus TModificationRestoreTask::DoOnDataChunk(const std::shared_ptr<arrow::Table>& data) {
    auto result = Merger->AddExistsDataOrdered(data);
    if (result.IsFail()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "merge_data_problems")("write_id", WriteData.GetWriteMeta().GetWriteId())(
            "tablet_id", GetTabletId())("message", result.GetErrorMessage());
        SendErrorMessage(result.GetErrorMessage(), NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Request);
    }
    return result;
}

void TModificationRestoreTask::DoOnError(const TString& errorMessage) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "restore_data_problems")("write_id", WriteData.GetWriteMeta().GetWriteId())(
        "tablet_id", GetTabletId())("message", errorMessage);
    SendErrorMessage(errorMessage, NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
}

NKikimr::TConclusionStatus TModificationRestoreTask::DoOnFinished() {
    {
        auto result = Merger->Finish();
        if (result.IsFail()) {
            return result;
        }
    }

    auto batchResult = Merger->BuildResultBatch();
    std::shared_ptr<NConveyor::ITask> task =
        std::make_shared<NOlap::TBuildSlicesTask>(BufferActorId, std::move(WriteData), batchResult, Context);
    NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
    return TConclusionStatus::Success();
}

TModificationRestoreTask::TModificationRestoreTask(const NActors::TActorId bufferActorId, NEvWrite::TWriteData&& writeData,
    const std::shared_ptr<IMerger>& merger, const TSnapshot actualSnapshot, const std::shared_ptr<arrow::RecordBatch>& incomingData,
    const TWritingContext& context)
    : TBase(context.GetTabletId(), context.GetTabletActorId())
    , WriteData(std::move(writeData))
    , BufferActorId(bufferActorId)
    , Merger(merger)
    , LocalPathId(WriteData.GetWriteMeta().GetTableId())
    , Snapshot(actualSnapshot)
    , IncomingData(incomingData)
    , Context(context) {
}

void TModificationRestoreTask::SendErrorMessage(
    const TString& errorMessage, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass) {
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(*writeDataPtr) });
    auto evResult =
        NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), errorMessage, errorClass);
    TActorContext::AsActorContext().Send(Context.GetTabletActorId(), evResult.release());
}

}   // namespace NKikimr::NOlap
