#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoDefineBSConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoFetchBSConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
