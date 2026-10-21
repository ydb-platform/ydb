#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IRequestNoOpCtx;
    class IFacilityProvider;

    void DoStreamStorageState(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
    void DoReassignVDisk(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // namespace NKikimr::NGRpcService
