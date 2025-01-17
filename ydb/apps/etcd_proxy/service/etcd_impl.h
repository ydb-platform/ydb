#pragma once

namespace NActors {
    class IActor;
}

namespace NKikimr::NGRpcService {
    class IRequestOpCtx;
    class IFacilityProvider;

    NActors::IActor* MakeRange(IRequestOpCtx* p);
    NActors::IActor* MakePut(IRequestOpCtx* p);
    NActors::IActor* MakeDeleteRange(IRequestOpCtx* p);
    NActors::IActor* MakeCompact(IRequestOpCtx* p);
} // NKikimr::NGRpcService
