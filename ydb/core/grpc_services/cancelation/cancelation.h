#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NGRpcService {

struct TEvSubscribeGrpcCancel;
class IRequestCtxMtSafe;

void PassSubscription(const TEvSubscribeGrpcCancel* ev, IRequestCtxMtSafe* requestCtx,
    NActors::TActorSystem* as);

void SubscribeRemoteCancel(const NActors::TActorId& service, const NActors::TActorId& subscriber,
    NActors::TActorSystem* as);

}
}
