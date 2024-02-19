#include "tx_finish_ack_to_source.h"

namespace NKikimr::NOlap::NDataSharing {
bool TTxFinishAckToSource::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NKikimr::NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::SourceSessions>().Key(Session->GetSessionId()).Delete();
    return true;
}

void TTxFinishAckToSource::DoComplete(const TActorContext& /*ctx*/) {
    Self->SharingSessionsManager->RemoveSourceSession(Session->GetSessionId());
}

}