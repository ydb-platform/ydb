#include "slice_builder.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/writer/buffer/events.h>

namespace NKikimr::NOlap {

std::optional<std::vector<NKikimr::NArrow::TSerializedBatch>> TBuildSlicesTask::BuildSlices() {
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent_id", ParentActorId));
    const auto& writeMeta = WriteData.GetWriteMeta();
    const ui64 tableId = writeMeta.GetTableId();
    const ui64 writeId = writeMeta.GetWriteId();

    std::shared_ptr<arrow::RecordBatch> batch = WriteData.GetData()->ExtractBatch();

    if (!batch) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_data")("write_id", writeId)("table_id", tableId);
        return {};
    }

    NArrow::TBatchSplitttingContext context(NColumnShard::TLimits::GetMaxBlobSize());
    context.SetFieldsForSpecialKeys(WriteData.GetPrimaryKeySchema());
    auto splitResult = NArrow::SplitByBlobSize(batch, context);
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

bool TBuildSlicesTask::DoExecute() {
    WriteData.MutableWriteMeta().SetWriteMiddle2StartInstant(TMonotonic::Now());
    auto batches = BuildSlices();
    WriteData.MutableWriteMeta().SetWriteMiddle3StartInstant(TMonotonic::Now());
    auto writeDataPtr = std::make_shared<NEvWrite::TWriteData>(std::move(WriteData));
    if (batches) {
        auto result = std::make_unique<NColumnShard::NWriting::TEvAddInsertedDataToBuffer>(writeDataPtr, std::move(*batches));
        TActorContext::AsActorContext().Send(BufferActorId, result.release());
    } else {
        TWritingBuffer buffer(writeDataPtr->GetBlobsAction(), {std::make_shared<TWriteAggregation>(writeDataPtr)});
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(std::make_shared<NColumnShard::TBlobPutResult>(NKikimrProto::EReplyStatus::CORRUPTED), std::move(buffer));
        TActorContext::AsActorContext().Send(ParentActorId, result.release());
    }

    return true;
}

}
