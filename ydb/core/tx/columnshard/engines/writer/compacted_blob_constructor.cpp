#include "compacted_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>

namespace NKikimr::NOlap {

TCompactedWriteController::TCompactedWriteController(const TActorId& dstActor, TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeEv)
    : WriteIndexEv(writeEv)
    , DstActor(dstActor)
{
    auto& changes = *WriteIndexEv->IndexChanges;
    for (ui32 i = 0; i < changes.GetWritePortionsCount(); ++i) {
        if (!changes.NeedWritePortion(i)) {
            continue;
        }
        auto* pInfo = changes.GetWritePortionInfo(i);
        Y_ABORT_UNLESS(pInfo);
        TPortionInfoWithBlobs& portionWithBlobs = *pInfo;
        auto action = changes.MutableBlobsAction().GetWriting(portionWithBlobs.GetPortionInfo());
        for (auto&& b : portionWithBlobs.GetBlobs()) {
            auto& task = AddWriteTask(TBlobWriteInfo::BuildWriteTask(b.GetBlob(), action));
            b.RegisterBlobId(portionWithBlobs, task.GetBlobId());
            WriteVolume += b.GetSize();
        }
    }
}

void TCompactedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    WriteIndexEv->PutResult = putResult;
    ctx.Send(DstActor, WriteIndexEv.Release());
}

TCompactedWriteController::~TCompactedWriteController() {
    if (WriteIndexEv && WriteIndexEv->IndexChanges) {
        WriteIndexEv->IndexChanges->AbortEmergency();
    }
}

}

void TCompactedWriteController::DoAbort(const TString& reason) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "TCompactedWriteController::DoAbort")("reason", reason);
}

}
