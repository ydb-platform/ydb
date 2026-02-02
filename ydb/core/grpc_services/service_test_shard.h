#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoCreateTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoDeleteTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
