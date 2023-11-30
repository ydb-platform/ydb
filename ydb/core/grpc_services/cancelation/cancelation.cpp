#include "cancelation.h"
#include "cancelation_event.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {
namespace NGRpcService {

void PassSubscription(const TEvSubscribeGrpcCancel* ev, IRequestCtxMtSafe* requestCtx,
    NActors::TActorSystem* as)
{
    auto subscriber = ActorIdFromProto(ev->Record.GetSubscriber());
    requestCtx->SetFinishAction([subscriber, as]() {
        as->Send(subscriber, new TEvClientLost);
    });
}

void SubscribeRemoteCancel(const NActors::TActorId& service, const NActors::TActorId& subscriber,
    NActors::TActorSystem* as)
{
    as->Send(service, new TEvSubscribeGrpcCancel(subscriber));
}

}
}
