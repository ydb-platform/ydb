#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoUpdateClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoGetClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoListPileSynchronization(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoListDatabaseSynchronization(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoListGroupSynchronization(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoConvertToBridge(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoAddPile(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoRemovePile(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoGetRollbackStatus(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoRollback(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoSetDatabaseBridgeMode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
