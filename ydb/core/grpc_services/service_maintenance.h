#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoListClusterNodes(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCreateMaintenanceTask(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoRefreshMaintenanceTask(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoGetMaintenanceTask(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListMaintenanceTasks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDropMaintenanceTask(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCompleteAction(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
