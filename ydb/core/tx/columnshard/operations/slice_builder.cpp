#include "slice_builder.h"
#include <library/cpp/actors/core/log.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

std::optional<std::vector<NKikimr::NArrow::TSerializedBatch>> TBuildSlicesTask::BuildSlices() {
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent_id", ParentActorId));
    const auto& writeMeta = WriteData.GetWriteMeta();
    const ui64 tableId = writeMeta.GetTableId();
    const ui64 writeId = writeMeta.GetWriteId();

    std::shared_ptr<arrow::RecordBatch> batch = WriteData.GetData().GetArrowBatch();

    if (!batch) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_data")("write_id", writeId)("table_id", tableId);
        return {};
    }

    auto splitResult = NArrow::SplitByBlobSize(batch, NColumnShard::TLimits::GetMaxBlobSize());
    if (!splitResult) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", TStringBuilder() << "cannot split batch in according to limits: " + splitResult.GetErrorMessage());
        return {};
    }
    auto result = splitResult.ReleaseResult();
    if (result.size() > 1) {
        for (auto&& i : result) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "strange_blobs_splitting")("blob", i.DebugString())("original_size", WriteData.GetSize());
        }
    }
    return result;
}

bool TBuildSlicesTask::DoExecute() {
    auto batches = BuildSlices();
    if (batches) {
        auto writeController = std::make_shared<NOlap::TIndexedWriteController>(ParentActorId, WriteData, Action, std::move(*batches));
        if (batches && Action->NeedDraftTransaction()) {
            TActorContext::AsActorContext().Send(ParentActorId, std::make_unique<NColumnShard::TEvPrivate::TEvWriteDraft>(writeController));
        } else {
            TActorContext::AsActorContext().Register(NColumnShard::CreateWriteActor(TabletId, writeController, TInstant::Max()));
        }
    } else {
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(std::make_shared<NColumnShard::TBlobPutResult>(NKikimrProto::EReplyStatus::CORRUPTED), WriteData.GetWriteMeta());
        TActorContext::AsActorContext().Send(ParentActorId, result.release());
    }

    return true;
}

}
