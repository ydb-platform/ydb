#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoSwitchClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoGetClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
