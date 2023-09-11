#include "indexed_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

std::optional<TBlobWriteInfo> TIndexedWriteController::Next() {
    if (CurrentIndex == BlobsSplitted.size()) {
        return {};
    }
    CurrentIndex++;
    auto& bInfo = BlobsSplitted[CurrentIndex - 1];
    auto result = TBlobWriteInfo::BuildWriteTask(bInfo.GetData(), Action);
    BlobData.emplace_back(TBlobRange(result.GetBlobId(), 0, result.GetBlobId().BlobSize()), bInfo.GetSpecialKeys(), bInfo.GetRowsCount(), bInfo.GetRawBytes(), AppData()->TimeProvider->Now());
    return result;
}

TIndexedWriteController::TIndexedWriteController(const TActorId& dstActor, const NEvWrite::TWriteData& writeData, const std::shared_ptr<IBlobsAction>& action, std::vector<NArrow::TSerializedBatch>&& blobsSplitted)
    : BlobsSplitted(std::move(blobsSplitted))
    , WriteData(writeData)
    , DstActor(dstActor)
    , Action(action)
{
    ResourceUsage.SourceMemorySize = WriteData.GetSize();
}

void TIndexedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    if (putResult->GetPutStatus() == NKikimrProto::OK) {
        std::vector<std::shared_ptr<IBlobsAction>> actions = {Action};
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, std::move(BlobData), actions, WriteData.GetWriteMeta(), WriteData.GetData().GetSchemaVersion());
        ctx.Send(DstActor, result.release());
    } else {
        auto result = std::make_unique<NColumnShard::TEvPrivate::TEvWriteBlobsResult>(putResult, WriteData.GetWriteMeta());
        ctx.Send(DstActor, result.release());
    }
}

}
