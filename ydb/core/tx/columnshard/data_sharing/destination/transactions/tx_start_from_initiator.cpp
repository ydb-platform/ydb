#include "tx_start_from_initiator.h"

namespace NKikimr::NOlap::NDataSharing {

bool TTxProposeFromInitiator::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::DestinationSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::DestinationSessions::Details>(Session->SerializeDataToProto().SerializeAsString()));
    return true;
}

void TTxProposeFromInitiator::DoComplete(const TActorContext& /*ctx*/) {
    AFL_VERIFY(!Session->IsConfirmed());
    AFL_VERIFY(Sessions->emplace(Session->GetSessionId(), Session).second);
    Session->GetInitiatorController().ProposeSuccess(Session->GetSessionId());
}

bool TTxConfirmFromInitiator::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    Session->Confirm(true);
    db.Table<Schema::DestinationSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::DestinationSessions::Cursor>(Session->SerializeCursorToProto().SerializeAsString()));
    return true;
}

void TTxConfirmFromInitiator::DoComplete(const TActorContext& /*ctx*/) {
    Session->Start(*Self);
    Session->GetInitiatorController().ConfirmSuccess(Session->GetSessionId());
}

}