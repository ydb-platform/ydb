#include "cancelation.h"
#include "cancelation_event.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event.h>

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
    // Track delivery: `service` is the per-request gRPC actor, which may have already
    // raced ahead and died on its own client-lost before this subscription arrives.
    // In that case the subscription would be silently dropped and `subscriber` would
    // never learn the client is gone (the query would leak until its deadline). With
    // FlagTrackDelivery the actor system bounces TEvUndelivered back to `subscriber`,
    // which can then treat it as client lost. The sender is set to `subscriber` so the
    // undelivered notification is routed there.
    as->Send(new NActors::IEventHandle(service, subscriber, new TEvSubscribeGrpcCancel(subscriber),
        NActors::IEventHandle::FlagTrackDelivery));
}

}
}
