#include "cancelation.h"
#include "cancelation_event.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>

#include <library/cpp/actors/core/actorsystem.h>

namespace NKikimr {
namespace NGRpcService {

void PassSubscription(const TEvSubscribeGrpcCancel* ev, IRequestCtxMtSafe* requestCtx,
    NActors::TActorSystem* as)
{
    auto subscriber = ActorIdFromProto(ev->Record.GetSubscriber());
    auto tag = ev->Record.GetWakeupTag();
    requestCtx->SetClientLostAction([subscriber, tag, as]() {
        as->Send(subscriber, new TEvents::TEvWakeup(tag));
    });
}

void SubscribeRemoteCancel(const NActors::TActorId& service, const NActors::TActorId& subscriber,
    ui64 wakeupTag, NActors::TActorSystem* as)
{
    as->Send(service, new TEvSubscribeGrpcCancel(subscriber, wakeupTag));
}

}
}
