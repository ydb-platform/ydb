#pragma once

namespace NActors {
    class IActor;
    class IEventBase;
}

namespace NKikimr::NGRpcService {
    class IRequestNoOpCtx;
}

namespace NEtcd {
    NActors::IActor* MakeRange(NKikimr::NGRpcService::IRequestNoOpCtx* p);
    NActors::IActor* MakePut(NKikimr::NGRpcService::IRequestNoOpCtx* p);
    NActors::IActor* MakeDeleteRange(NKikimr::NGRpcService::IRequestNoOpCtx* p);
    NActors::IActor* MakeTxn(NKikimr::NGRpcService::IRequestNoOpCtx* p);
    NActors::IActor* MakeCompact(NKikimr::NGRpcService::IRequestNoOpCtx* p);

    NActors::IActor* MakeLeaseGrant(NKikimr::NGRpcService::IRequestNoOpCtx* p);
    NActors::IActor* MakeLeaseRevoke(NKikimr::NGRpcService::IRequestNoOpCtx* p);
    NActors::IActor* MakeLeaseTimeToLive(NKikimr::NGRpcService::IRequestNoOpCtx* p);
    NActors::IActor* MakeLeaseLeases(NKikimr::NGRpcService::IRequestNoOpCtx* p);
}
