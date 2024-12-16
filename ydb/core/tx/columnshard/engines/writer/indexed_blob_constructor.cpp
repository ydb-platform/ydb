#include "indexed_blob_constructor.h"

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/defs.h>

namespace NKikimr::NOlap {

TIndexedWriteController::TIndexedWriteController(
    const TActorId& dstActor, const std::shared_ptr<IBlobsWritingAction>& action, std::vector<std::shared_ptr<TWriteAggregation>>&& aggregations)
    : Buffer(action, std::move(aggregations))
    , DstActor(dstActor) {
    auto blobs = Buffer.GroupIntoBlobs();
    for (auto&& b : blobs) {
        auto& task = AddWriteTask(TBlobWriteInfo::BuildWriteTask(b.ExtractBlobData(), action));
        b.InitBlobId(task.GetBlobId());
    }
}

void TIndexedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    Buffer.InitReadyInstant(TMonotonic::Now());
    auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, std::move(Buffer));
    ctx.Send(DstActor, result.release());
}

void TIndexedWriteController::DoOnStartSending() {
    Buffer.InitStartSending(TMonotonic::Now());
}

void TWideSerializedBatch::InitBlobId(const TUnifiedBlobId& id) {
    AFL_VERIFY(!Range.BlobId.GetTabletId());
    Range.BlobId = id;
}

std::shared_ptr<NKikimr::NOlap::TUserData> TWideSerializedBatch::BuildInsertionUserData(const NColumnShard::TColumnShard& owner) const {
    NKikimrTxColumnShard::TLogicalMetadata meta;
    meta.SetNumRows(SplittedBlobs.GetRowsCount());
    meta.SetRawBytes(SplittedBlobs.GetRawBytes());
    meta.SetDirtyWriteTimeSeconds(GetStartInstant().Seconds());
    meta.SetSpecialKeysRawData(SplittedBlobs.GetSpecialKeysFullSafe());
    meta.SetSpecialKeysPayloadData(SplittedBlobs.GetSpecialKeysPayloadSafe());

    const auto& blobRange = Range;
    Y_ABORT_UNLESS(blobRange.GetBlobId().IsValid());

    const auto& writeMeta = GetAggregation().GetWriteMeta();
    meta.SetModificationType(TEnumOperator<NEvWrite::EModificationType>::SerializeToProto(writeMeta.GetModificationType()));
    *meta.MutableSchemaSubset() = GetAggregation().GetSchemaSubset().SerializeToProto();
    auto schemeVersion = GetAggregation().GetSchemaVersion();
    auto tableSchema = owner.GetTablesManager().GetPrimaryIndex()->GetVersionedIndex().GetSchemaVerified(schemeVersion);

    return std::make_shared<NOlap::TUserData>(writeMeta.GetTableId(), blobRange, meta, tableSchema->GetVersion(), SplittedBlobs.GetData());
}

void TWritingBuffer::InitReadyInstant(const TMonotonic instant) {
    for (auto&& aggr : Aggregations) {
        aggr->MutableWriteMeta().SetWriteMiddle5StartInstant(instant);
    }
}

void TWritingBuffer::InitStartSending(const TMonotonic instant) {
    for (auto&& aggr : Aggregations) {
        aggr->MutableWriteMeta().SetWriteMiddle4StartInstant(instant);
    }
}

void TWritingBuffer::InitReplyReceived(const TMonotonic instant) {
    for (auto&& aggr : Aggregations) {
        aggr->MutableWriteMeta().SetWriteMiddle6StartInstant(instant);
    }
}

std::vector<NKikimr::NOlap::TWritingBlob> TWritingBuffer::GroupIntoBlobs() {
    std::vector<TWritingBlob> result;
    TWritingBlob currentBlob;
    ui64 sumSize = 0;
    for (auto&& aggr : Aggregations) {
        for (auto&& bInfo : aggr->MutableSplittedBlobs()) {
            if (!currentBlob.AddData(bInfo)) {
                result.emplace_back(std::move(currentBlob));
                currentBlob = TWritingBlob();
                AFL_VERIFY(currentBlob.AddData(bInfo));
            }
            sumSize += bInfo.GetSplittedBlobs().GetSize();
        }
    }
    if (currentBlob.GetSize()) {
        result.emplace_back(std::move(currentBlob));
    }
    if (result.size()) {
        if (sumSize / result.size() < 4 * 1024 * 1024 && result.size() != 1) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "error_splitting")("size", sumSize)("count", result.size());
        }
    }
    return result;
}

TString TWritingBlob::ExtractBlobData() {
    AFL_VERIFY(BlobSize);
    AFL_VERIFY(!Extracted);
    Extracted = true;
    TString result;
    result.reserve(BlobSize);
    for (auto&& i : BlobData) {
        result.append(i);
    }
    BlobData.clear();
    return result;
}

}   // namespace NKikimr::NOlap
