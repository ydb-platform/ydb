#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoReplaceConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoFetchConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoBootstrapCluster(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
