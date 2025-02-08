#pragma once

#include "etcd_shared.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NGRpcService {
    class IRequestNoOpCtx;
}

namespace NEtcd {
    NActors::IActor* MakeRange(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakePut(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeDeleteRange(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeTxn(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeCompact(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);

    NActors::IActor* MakeLeaseGrant(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeLeaseRevoke(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeLeaseTimeToLive(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeLeaseLeases(NKikimr::NGRpcService::IRequestNoOpCtx* p, TSharedStuff::TPtr stuff);
}
