#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoReplaceBSConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoFetchBSConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
