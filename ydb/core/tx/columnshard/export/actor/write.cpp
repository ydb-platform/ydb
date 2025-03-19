#include "write.h"
#include <ydb/core/tx/columnshard/export/events/events.h>

namespace NKikimr::NOlap::NExport {

void TWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    if (putResult->GetPutStatus() == NKikimrProto::OK) {
        ctx.Send(ExportActorId, new NEvents::TEvExportWritingFinished());
    } else {
        ctx.Send(ExportActorId, new NEvents::TEvExportWritingFailed());
    }
}

TWriteController::TWriteController(const TActorId& exportActorId, const std::vector<TString>& blobsToWrite, const std::shared_ptr<IBlobsWritingAction>& writeAction, 
    const TCursor& cursor, const TTabletId tabletId, const NColumnShard::TInternalPathId pathId)
    : ExportActorId(exportActorId) 
{
    for (auto&& i : blobsToWrite) {
        auto blobId = TUnifiedBlobId((ui64)tabletId, (pathId.GetInternalPathIdValue() << 24) >> 40, pathId.GetInternalPathIdValue() >> 40, cursor.GetChunkIdx(), pathId.GetInternalPathIdValue() & Max<ui8>(), Max<ui32>(), i.size());
        AFL_VERIFY((((ui64)blobId.GetLogoBlobId().Step() >> 8) << 40) + ((ui64)blobId.GetLogoBlobId().Generation() << 8) + blobId.GetLogoBlobId().Channel() == pathId.GetInternalPathIdValue());
        auto info = NOlap::TBlobWriteInfo::BuildWriteTask(i, writeAction, blobId);
        AddWriteTask(std::move(info));
    }
}

}