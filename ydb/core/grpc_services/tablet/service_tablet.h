#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

void DoExecuteTabletMiniKQLRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoChangeTabletSchemaRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoRestartTabletRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NKikimr::NGRpcService
