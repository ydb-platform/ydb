#pragma once
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/columnshard/export/session/cursor.h>

namespace NKikimr::NOlap::NExport {

class TWriteController: public NColumnShard::IWriteController {
private:
    const TActorId ExportActorId;
protected:
    virtual void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult);
public:
    TWriteController(const TActorId& exportActorId, const std::vector<TString>& blobsToWrite, const std::shared_ptr<IBlobsWritingAction>& writeAction,
        const TCursor& cursor, const TTabletId tabletId, const ui64 pathId);
};

}