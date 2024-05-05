#include "tx_add.h"
#include <ydb/core/tx/columnshard/bg_tasks/session/storage.h>

namespace NKikimr::NOlap::NBackground {

bool TTxAddSession::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    Adapter->SaveSessionToLocalDatabase(txc, Session->SerializeToLocalDatabaseRecord()).Validate("on AddSession");
    return true;
}

void TTxAddSession::Complete(const TActorContext& ctx) {
    Sessions->AddSession(Session).Validate("on add background session");
    Session->GetChannelContainer()->OnAdded();
    if (Session->GetLogicContainer()->IsReadyForStart()) {
        TStartContext context(ctx.SelfID, Session->GetChannelContainer());
        Session->StartActor(context);
    }
}

}
