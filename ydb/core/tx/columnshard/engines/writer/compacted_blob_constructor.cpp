#include "compacted_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

std::optional<TBlobWriteInfo> TCompactedWriteController::Next() {
    auto& changes = *WriteIndexEv->IndexChanges;
    while (CurrentPortion < changes.GetWritePortionsCount()) {
        auto* pInfo = changes.GetWritePortionInfo(CurrentPortion);
        Y_VERIFY(pInfo);
        TPortionInfoWithBlobs& portionWithBlobs = *pInfo;
        if (CurrentBlobIndex < portionWithBlobs.GetBlobs().size() && changes.NeedWritePortion(CurrentPortion)) {
            CurrentBlobInfo = &portionWithBlobs.GetBlobs()[CurrentBlobIndex];
            ++CurrentBlobIndex;
            auto result = TBlobWriteInfo::BuildWriteTask(CurrentBlobInfo->GetBlob(), WriteIndexEv->BlobsAction);
            CurrentBlobInfo->RegisterBlobId(portionWithBlobs, result.GetBlobId());
            return result;
        } else {
            ++CurrentPortion;
            CurrentBlobIndex = 0;
        }
    }
    return {};
}

TCompactedWriteController::TCompactedWriteController(const TActorId& dstActor, TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeEv, bool /*blobGrouppingEnabled*/)
    : WriteIndexEv(writeEv)
    , Action(WriteIndexEv->BlobsAction)
    , DstActor(dstActor)
{}

void TCompactedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    WriteIndexEv->PutResult = putResult;
    ctx.Send(DstActor, WriteIndexEv.Release());
}

}
