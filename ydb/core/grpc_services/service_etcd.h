#pragma once

namespace NActors {
    class IActor;
}

namespace NKikimr::NGRpcService {
    class IRequestOpCtx;
    class IFacilityProvider;

    NActors::IActor* DoRange(IRequestOpCtx* p);
    NActors::IActor* DoPut(IRequestOpCtx* p);
    NActors::IActor* DoDeleteRange(IRequestOpCtx* p);
} // NKikimr::NGRpcService
