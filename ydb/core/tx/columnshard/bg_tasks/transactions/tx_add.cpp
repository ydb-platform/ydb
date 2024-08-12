#include "tx_add.h"
#include <ydb/core/tx/columnshard/bg_tasks/session/storage.h>

namespace NKikimr::NOlap::NBackground {

bool TTxAddSession::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    Adapter->SaveSessionToLocalDatabase(txc, Session->SerializeToLocalDatabaseRecord());
    return true;
}

void TTxAddSession::Complete(const TActorContext& /*ctx*/) {
    Sessions->AddSession(Session).Validate("on add background session");
    Session->GetChannelContainer()->OnAdded();
    if (Session->GetLogicContainer()->IsReadyForStart() && !Session->GetLogicContainer()->IsFinished()) {
        TStartContext context(Session, Adapter);
        Session->StartActor(context);
    }
}

}
