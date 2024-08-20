#include "builder.h"
#include "merger.h"
#include "restore.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/operations/slice_builder/builder.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/data_events/common/modification_type.h>

namespace NKikimr::NOlap {

void TBuildBatchesTask::ReplyError(const TString& message) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", "cannot build batch for insert")("reason", message)("data", WriteData.GetWriteMeta().GetLongTxIdOptional());
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(*writeDataPtr) });
    auto result = NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), message);
    TActorContext::AsActorContext().Send(ParentActorId, result.release());
}

TConclusionStatus TBuildBatchesTask::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    TConclusion<std::shared_ptr<arrow::RecordBatch>> batchConclusion = WriteData.GetData()->ExtractBatch();
    if (batchConclusion.IsFail()) {
        ReplyError("cannot extract incoming batch: " + batchConclusion.GetErrorMessage());
        return TConclusionStatus::Fail("cannot extract incoming batch: " + batchConclusion.GetErrorMessage());
    }

    auto preparedConclusion =
        ActualSchema->PrepareForModification(batchConclusion.DetachResult(), WriteData.GetWriteMeta().GetModificationType());
    if (preparedConclusion.IsFail()) {
        ReplyError("cannot prepare incoming batch: " + preparedConclusion.GetErrorMessage());
        return TConclusionStatus::Fail("cannot prepare incoming batch: " + preparedConclusion.GetErrorMessage());
    }
    auto batch = preparedConclusion.DetachResult();
    const std::vector<std::shared_ptr<arrow::Field>> defaultFields = ActualSchema->GetAbsentFields(batch->schema());
    std::shared_ptr<IMerger> merger;
    switch (WriteData.GetWriteMeta().GetModificationType()) {
        case NEvWrite::EModificationType::Upsert: {
            if (defaultFields.empty()) {
                std::shared_ptr<NConveyor::ITask> task =
                    std::make_shared<NOlap::TBuildSlicesTask>(TabletId, ParentActorId, BufferActorId, std::move(WriteData), batch, ActualSchema);
                NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
                return TConclusionStatus::Success();
            } else {
                auto insertionConclusion = ActualSchema->CheckColumnsDefault(defaultFields);
                auto conclusion = ActualSchema->BuildDefaultBatch(ActualSchema->GetIndexInfo().ArrowSchema()->fields(), 1, true);
                AFL_VERIFY(!conclusion.IsFail())("error", conclusion.GetErrorMessage());
                auto batchDefault = conclusion.DetachResult();
                NArrow::NMerger::TSortableBatchPosition pos(
                    batchDefault, 0, batchDefault->schema()->field_names(), batchDefault->schema()->field_names(), false);
                merger = std::make_shared<TUpdateMerger>(
                    batch, ActualSchema, insertionConclusion.IsSuccess() ? "" : insertionConclusion.GetErrorMessage(), pos);
                break;
            }
        }
        case NEvWrite::EModificationType::Insert: {
            merger = std::make_shared<TInsertMerger>(batch, ActualSchema);
            break;
        }
        case NEvWrite::EModificationType::Update: {
            merger = std::make_shared<TUpdateMerger>(batch, ActualSchema, "");
            break;
        }
        case NEvWrite::EModificationType::Replace:
        case NEvWrite::EModificationType::Delete: {
            std::shared_ptr<NConveyor::ITask> task =
                std::make_shared<NOlap::TBuildSlicesTask>(TabletId, ParentActorId, BufferActorId, std::move(WriteData), batch, ActualSchema);
            NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
            return TConclusionStatus::Success();
        }
    }
    std::shared_ptr<NDataReader::IRestoreTask> task = std::make_shared<NOlap::TModificationRestoreTask>(
        TabletId, ParentActorId, BufferActorId, std::move(WriteData), merger, ActualSchema, ActualSnapshot, batch);
    NActors::TActivationContext::AsActorContext().Register(new NDataReader::TActor(task));

    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap
