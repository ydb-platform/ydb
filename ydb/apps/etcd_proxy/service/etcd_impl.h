#pragma once

namespace NActors {
    class IActor;
    class IEventBase;
}

namespace NKikimr::NGRpcService {
    class IRequestOpCtx;

    NActors::IActor* MakeRange(IRequestOpCtx* p);
    NActors::IActor* MakePut(IRequestOpCtx* p);
    NActors::IActor* MakeDeleteRange(IRequestOpCtx* p);
    NActors::IActor* MakeTxn(IRequestOpCtx* p);
    NActors::IActor* MakeCompact(IRequestOpCtx* p);

    NActors::IActor* MakeLeaseGrant(IRequestOpCtx* p);
    NActors::IActor* MakeLeaseRevoke(IRequestOpCtx* p);
    NActors::IActor* MakeLeaseTimeToLive(IRequestOpCtx* p);
    NActors::IActor* MakeLeaseLeases(IRequestOpCtx* p);
} // NKikimr::NGRpcService
