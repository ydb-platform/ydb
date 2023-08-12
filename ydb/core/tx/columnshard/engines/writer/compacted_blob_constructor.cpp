#include "compacted_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

TCompactedWriteController::TBlobsConstructor::TBlobsConstructor(TCompactedWriteController& owner)
    : Owner(owner)
    , IndexChanges(*Owner.WriteIndexEv->IndexChanges)
{
}

const TString& TCompactedWriteController::TBlobsConstructor::GetBlob() const {
    return CurrentBlobInfo->GetBlob();
}

bool TCompactedWriteController::TBlobsConstructor::RegisterBlobId(const TUnifiedBlobId& blobId) {
    Y_VERIFY(CurrentBlobInfo);
    CurrentBlobInfo->RegisterBlobId(*IndexChanges.GetWritePortionInfo(CurrentPortion), blobId);
    return true;
}

IBlobConstructor::EStatus TCompactedWriteController::TBlobsConstructor::BuildNext() {
    while (CurrentPortion < IndexChanges.GetWritePortionsCount()) {
        TPortionInfoWithBlobs& portionWithBlobs = *IndexChanges.GetWritePortionInfo(CurrentPortion);
        if (CurrentBlobIndex < portionWithBlobs.GetBlobs().size() && IndexChanges.NeedWritePortion(CurrentPortion)) {
            CurrentBlobInfo = &portionWithBlobs.GetBlobs()[CurrentBlobIndex];
            ++CurrentBlobIndex;
            return EStatus::Ok;
        } else {
            ++CurrentPortion;
            CurrentBlobIndex = 0;
        }
    }
    return EStatus::Finished;
}

TCompactedWriteController::TCompactedWriteController(const TActorId& dstActor, TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeEv, bool /*blobGrouppingEnabled*/)
    : WriteIndexEv(writeEv)
    , BlobConstructor(std::make_shared<TBlobsConstructor>(*this))
    , DstActor(dstActor)
{}

void TCompactedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    WriteIndexEv->PutResult = putResult;
    ctx.Send(DstActor, WriteIndexEv.Release());
}

}
