#include "tx_save_state.h"
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>

namespace NKikimr::NOlap::NBackground {

bool TTxSaveSessionState::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    Adapter->SaveStateToLocalDatabase(txc, Session->SerializeToLocalDatabaseRecord());
    return true;
}

void TTxSaveSessionState::DoComplete(const TActorContext& ctx) {
    if (Session->GetLogicContainer()->IsFinished() && Session->GetLogicContainer()->IsReadyForRemoveOnFinished()) {
        ctx.Send(Adapter->GetTabletActorId(), new TEvRemoveSession(Session->GetLogicClassName(), Session->GetIdentifier()));
    } else if (!Session->IsRunning() && Session->GetLogicContainer()->IsReadyForStart()) {
        TStartContext context(Session, Adapter);
        Session->StartActor(context);
    }
}

}
