#include "compacted_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

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
        for (auto&& b : portionWithBlobs.GetBlobs()) {
            auto& task = AddWriteTask(TBlobWriteInfo::BuildWriteTask(b.GetBlob(), changes.MutableBlobsAction().GetWriting(b.GetOperator()->GetStorageId())));
            b.RegisterBlobId(portionWithBlobs, task.GetBlobId());
            WriteVolume += b.GetSize();
        }
    }
}

void TCompactedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    WriteIndexEv->PutResult = NYDBTest::TControllers::GetColumnShardController()->OverrideBlobPutResultOnCompaction(putResult, GetBlobActions());
    ctx.Send(DstActor, WriteIndexEv.Release());
}

TCompactedWriteController::~TCompactedWriteController() {
    if (WriteIndexEv && WriteIndexEv->IndexChanges) {
        WriteIndexEv->IndexChanges->AbortEmergency("TCompactedWriteController destructed with WriteIndexEv and WriteIndexEv->IndexChanges");
    }
}

const NKikimr::NOlap::TBlobsAction& TCompactedWriteController::GetBlobsAction() {
    return WriteIndexEv->IndexChanges->GetBlobsAction();
}

void TCompactedWriteController::DoAbort(const TString& reason) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "TCompactedWriteController::DoAbort")("reason", reason);
}

}
