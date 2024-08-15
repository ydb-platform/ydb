#include "restore.h"
#include <ydb/core/tx/columnshard/operations/slice_builder/builder.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap {

std::unique_ptr<NKikimr::TEvColumnShard::TEvInternalScan> TModificationRestoreTask::DoBuildRequestInitiator() const {
    auto request = std::make_unique<TEvColumnShard::TEvInternalScan>(LocalPathId);
    request->ReadToSnapshot = Snapshot;
    request->RangesFilter = std::make_shared<TPKRangesFilter>(false);
    auto pkData = NArrow::TColumnOperator().VerifyIfAbsent().Extract(IncomingData, ActualSchema->GetPKColumnNames());
    for (ui32 i = 0; i < pkData->num_rows(); ++i) {
        auto batch = pkData->Slice(i, 1);
        auto pFrom = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::GreaterEqual, batch);
        auto pTo = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::LessEqual, batch);
        AFL_VERIFY(request->RangesFilter->Add(pFrom, pTo, &ActualSchema->GetIndexInfo()));
    }
    for (auto&& i : ActualSchema->GetIndexInfo().GetColumnIds(false)) {
        request->AddColumn(i, ActualSchema->GetIndexInfo().GetColumnName(i));
    }
    return request;
}

NKikimr::TConclusionStatus TModificationRestoreTask::DoOnDataChunk(const std::shared_ptr<arrow::Table>& data) {
    auto result = Merger->AddExistsDataOrdered(data);
    if (result.IsFail()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "merge_data_problems")
            ("write_id", WriteData.GetWriteMeta().GetWriteId())("tablet_id", TabletId)("message", result.GetErrorMessage());
        SendErrorMessage(result.GetErrorMessage());
    }
    return result;
}

void TModificationRestoreTask::DoOnError(const TString& errorMessage) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "restore_data_problems")("write_id", WriteData.GetWriteMeta().GetWriteId())(
        "tablet_id", TabletId)("message", errorMessage);
    SendErrorMessage(errorMessage);
}

NKikimr::TConclusionStatus TModificationRestoreTask::DoOnFinished() {
    {
        auto result = Merger->Finish();
        if (result.IsFail()) {
            return result;
        }
    }

    auto batchResult = Merger->BuildResultBatch();
    std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildSlicesTask>(
        TabletId, ParentActorId, BufferActorId, std::move(WriteData), batchResult, ActualSchema);
    NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
    return TConclusionStatus::Success();
}

TModificationRestoreTask::TModificationRestoreTask(const ui64 tabletId, const NActors::TActorId parentActorId, const NActors::TActorId bufferActorId, NEvWrite::TWriteData&& writeData, const std::shared_ptr<IMerger>& merger, const std::shared_ptr<ISnapshotSchema>& actualSchema, const TSnapshot actualSnapshot, const std::shared_ptr<arrow::RecordBatch>& incomingData)
    : TBase(tabletId, parentActorId)
    , WriteData(std::move(writeData))
    , TabletId(tabletId)
    , ParentActorId(parentActorId)
    , BufferActorId(bufferActorId)
    , Merger(merger)
    , ActualSchema(actualSchema)
    , LocalPathId(WriteData.GetWriteMeta().GetTableId())
    , Snapshot(actualSnapshot)
    , IncomingData(incomingData) {

}

void TModificationRestoreTask::SendErrorMessage(const TString& errorMessage) {
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(*writeDataPtr) });
    auto evResult =
        NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), result.GetErrorMessage());
    TActorContext::AsActorContext().Send(ParentActorId, evResult.release());
}

}
