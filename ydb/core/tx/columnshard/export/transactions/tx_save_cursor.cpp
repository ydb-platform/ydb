#include "tx_save_cursor.h"
#include <ydb/core/tx/columnshard/export/session/session.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NExport {

bool TTxSaveCursor::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TSession copy = *Session;
    copy.SetCursor(Cursor);
    copy.SaveCursorToDB(txc.DB);
    return true;
}

void TTxSaveCursor::Complete(const TActorContext& ctx) {
    Session->SetCursor(Cursor);
    if (!Cursor.IsFinished()) {
        ctx.Send(ExportActorId, new NEvents::TEvExportCursorSaved);
    } else {
        NYDBTest::TControllers::GetColumnShardController()->OnExportFinished();
    }
}

}
