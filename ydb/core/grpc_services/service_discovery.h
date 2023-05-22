#pragma once

#include <memory>

namespace NKikimr {

struct TDynamicNodeAuthorizationParams;

namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoListEndpointsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoWhoAmIRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoNodeRegistrationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f, const TDynamicNodeAuthorizationParams& dynamicNodeAuthorizationParams);

}
}
