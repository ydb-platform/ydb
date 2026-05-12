#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoCreatePartition(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoDeletePartition(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoGetLoadActorAdapterActorId(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoListPartitions(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoWriteBlocks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoReadBlocks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
