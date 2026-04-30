#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoCreateTestShardSet(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoDeleteTestShardSet(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
