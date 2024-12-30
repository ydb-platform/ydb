#pragma once

#include <memory>

namespace NKikimr::NGRpcService {
    class IRequestOpCtx;
    class IFacilityProvider;

    void DoRange(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoPut(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoDeleteRange(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
} // NKikimr::NGRpcService
