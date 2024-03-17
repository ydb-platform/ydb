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
    const TCursor& cursor, const TTabletId tabletId)
    : ExportActorId(exportActorId) 
{
    for (auto&& i : blobsToWrite) {
        auto info = NOlap::TBlobWriteInfo::BuildWriteTask(i, writeAction, TUnifiedBlobId::BuildRaw(cursor.GetChunkIdx(), (ui64)tabletId, 0, 0));
        AddWriteTask(std::move(info));
    }
}

}