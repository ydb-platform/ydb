#include "indexed_blob_constructor.h"

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/defs.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap {

TIndexedWriteController::TIndexedWriteController(
    const TActorId& dstActor, const std::shared_ptr<IBlobsWritingAction>& action, std::vector<std::shared_ptr<TWriteAggregation>>&& aggregations)
    : Buffer(action, std::move(aggregations))
    , DstActor(dstActor)
{
    auto blobs = Buffer.GroupIntoBlobs();
    for (auto&& b : blobs) {
        auto& task = AddWriteTask(TBlobWriteInfo::BuildWriteTask(b.ExtractBlobData(), action));
        b.InitBlobId(task.GetBlobId());
    }
}

void TIndexedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, std::move(Buffer));
    ctx.Send(DstActor, result.release());
}

void TWideSerializedBatch::InitBlobId(const TUnifiedBlobId& id) {
    AFL_VERIFY(!Range.BlobId.GetTabletId());
    Range.BlobId = id;
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
            YDB_LOG_ERROR("",
                {"event", "error_splitting"},
                {"size", sumSize},
                {"count", result.size()});
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
