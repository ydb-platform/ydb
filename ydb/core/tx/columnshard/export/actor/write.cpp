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
    const TCursor& cursor, const TTabletId tabletId, const TInternalPathId pathId)
    : ExportActorId(exportActorId) 
{
    const auto pathIdValue = pathId.GetRawValue();
    for (auto&& i : blobsToWrite) {
        auto blobId = TUnifiedBlobId((ui64)tabletId, (pathIdValue << 24) >> 40, pathIdValue >> 40, cursor.GetChunkIdx(), pathIdValue & Max<ui8>(), Max<ui32>(), i.size());
        AFL_VERIFY((((ui64)blobId.GetLogoBlobId().Step() >> 8) << 40) + ((ui64)blobId.GetLogoBlobId().Generation() << 8) + blobId.GetLogoBlobId().Channel() == pathIdValue);
        auto info = NOlap::TBlobWriteInfo::BuildWriteTask(i, writeAction, blobId);
        AddWriteTask(std::move(info));
    }
}

}