#include "tx_start_from_initiator.h"

namespace NKikimr::NOlap::NDataSharing {

bool TTxStartFromInitiator::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::DestinationSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::DestinationSessions::Details>(Session->SerializeDataToProto().SerializeAsString()));
    return true;
}

void TTxStartFromInitiator::DoComplete(const TActorContext& /*ctx*/) {
    Session->Start(*Self);
    Session->GetInitiatorController().StartSuccess(Session->GetSessionId());
}

}