#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoCreateTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoAlterTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoGetTenantStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoListTenantsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoRemoveTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDescribeTenantOptionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
