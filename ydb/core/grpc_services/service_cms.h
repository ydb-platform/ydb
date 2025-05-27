#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

void DoCreateTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoAlterTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoGetTenantStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListTenantsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoRemoveTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeTenantOptionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoGetScaleRecommendationRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

}
}
