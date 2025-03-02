#include "actor_worker.h"

namespace NKikimr::NConveyor {

void TActorWorker::HandleMain(TEvExecution::TEvRegisterActor::TPtr& ev) {
    TActorId actorId = RegisterWithSameMailbox(ev.Get()->Get()->ExtractActor().release());
    Send(ev->Get()->GetRecipient(), new TEvExecution::TEvRegisterActorResponse(actorId, ev->Get()->GetType(), ev->Get()->GetDetailedInfo(), ev->Get()->GetCookie()));
}

}
