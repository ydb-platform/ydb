#include "builder.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/defs.h>

namespace NKikimr::NOlap {

std::optional<std::vector<NKikimr::NArrow::TSerializedBatch>> TBuildSlicesTask::BuildSlices() {
    if (!OriginalBatch->num_rows()) {
        return std::vector<NKikimr::NArrow::TSerializedBatch>();
    }
    NArrow::TBatchSplitttingContext context(NColumnShard::TLimits::GetMaxBlobSize());
    context.SetFieldsForSpecialKeys(WriteData.GetPrimaryKeySchema());
    auto splitResult = NArrow::SplitByBlobSize(OriginalBatch, context);
    if (splitResult.IsFail()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", TStringBuilder() << "cannot split batch in according to limits: " + splitResult.GetErrorMessage());
        return {};
    }
    auto result = splitResult.DetachResult();
    if (result.size() > 1) {
        for (auto&& i : result) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "strange_blobs_splitting")("blob", i.DebugString())("original_size", WriteData.GetSize());
        }
    }
    return result;
}

void TBuildSlicesTask::ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass) {
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), { std::make_shared<TWriteAggregation>(*writeDataPtr) });
    auto result = NColumnShard::TEvPrivate::TEvWriteBlobsResult::Error(
        NKikimrProto::EReplyStatus::CORRUPTED, std::move(buffer), message, errorClass);
    TActorContext::AsActorContext().Send(ParentActorId, result.release());
}

TConclusionStatus TBuildSlicesTask::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent_id", ParentActorId));
    if (!OriginalBatch) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_data")("write_id", WriteData.GetWriteMeta().GetWriteId())("table_id", WriteData.GetWriteMeta().GetTableId());
        ReplyError("no data in batch", NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return TConclusionStatus::Fail("no data in batch");
    }
    const auto& indexSchema = ActualSchema->GetIndexInfo().ArrowSchema();
    auto subsetConclusion = NArrow::TColumnOperator().IgnoreOnDifferentFieldTypes().BuildSequentialSubset(OriginalBatch, indexSchema);
    if (subsetConclusion.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "unadaptable schemas")("index", indexSchema->ToString())(
            "problem", subsetConclusion.GetErrorMessage());
        ReplyError(
            "unadaptable schema: " + subsetConclusion.GetErrorMessage(),
            NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return TConclusionStatus::Fail("cannot reorder schema: " + subsetConclusion.GetErrorMessage());
    }
    NArrow::TSchemaSubset subset = subsetConclusion.DetachResult();

    if (OriginalBatch->num_columns() != indexSchema->num_fields()) {
        AFL_VERIFY(OriginalBatch->num_columns() < indexSchema->num_fields())("original", OriginalBatch->num_columns())(
                                                      "index", indexSchema->num_fields());
        if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableOptionalColumnsInColumnShard() &&
            WriteData.GetWriteMeta().GetModificationType() != NEvWrite::EModificationType::Delete) {
            subset = NArrow::TSchemaSubset::AllFieldsAccepted();
            const std::vector<ui32>& columnIdsVector = ActualSchema->GetIndexInfo().GetColumnIds(false);
            const std::set<ui32> columnIdsSet(columnIdsVector.begin(), columnIdsVector.end());
            auto normalized =
                ActualSchema->NormalizeBatch(*ActualSchema, std::make_shared<NArrow::TGeneralContainer>(OriginalBatch), columnIdsSet).DetachResult();
            OriginalBatch = NArrow::ToBatch(normalized->BuildTableVerified(), true);
        }
    }
    WriteData.MutableWriteMeta().SetWriteMiddle2StartInstant(TMonotonic::Now());
    auto batches = BuildSlices();
    WriteData.MutableWriteMeta().SetWriteMiddle3StartInstant(TMonotonic::Now());
    if (batches) {
        auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
        writeDataPtr->SetSchemaSubset(std::move(subset));
        std::shared_ptr<arrow::RecordBatch> pkBatch;
        if (!writeDataPtr->GetWriteMeta().HasLongTxId()) {
            pkBatch = NArrow::TColumnOperator().Extract(OriginalBatch, ActualSchema->GetIndexInfo().GetPrimaryKey()->fields());
        }
        auto result = std::make_unique<NColumnShard::NWriting::TEvAddInsertedDataToBuffer>(writeDataPtr, std::move(*batches), pkBatch);
        TActorContext::AsActorContext().Send(BufferActorId, result.release());
    } else {
        ReplyError("Cannot slice input to batches", NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass::Internal);
        return TConclusionStatus::Fail("Cannot slice input to batches");
    }

    return TConclusionStatus::Success();
}

}
