#include "query.h"
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NKqp::NPrivateEvents {

void TEvQueryRequest::SetClientLostAction(TActorId actorId, NActors::TActorSystem* as) {
    if (RequestCtx) {
        RequestCtx->SetFinishAction([actorId, as]() {
            as->Send(actorId, new NGRpcService::TEvClientLost());
            });
    } else if (Record.HasCancelationActor()) {
        auto cancelationActor = ActorIdFromProto(Record.GetCancelationActor());
        NGRpcService::SubscribeRemoteCancel(cancelationActor, actorId, as);
    }
}

} // namespace NKikimr::NKqp
