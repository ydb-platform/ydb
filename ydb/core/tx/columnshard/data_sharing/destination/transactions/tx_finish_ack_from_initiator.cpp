#include "tx_finish_ack_from_initiator.h"

namespace NKikimr::NOlap::NDataSharing {

bool TTxFinishAckFromInitiator::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NKikimr::NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::DestinationSessions>().Key(Session->GetSessionId()).Delete();
    return true;
}

void TTxFinishAckFromInitiator::DoComplete(const TActorContext& /*ctx*/) {
    Self->SharingSessionsManager->RemoveDestinationSession(Session->GetSessionId());
}

}