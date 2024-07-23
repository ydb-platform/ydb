#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoInitRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
