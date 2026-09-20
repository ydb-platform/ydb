#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoBeginPublicationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoPublishRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCancelPublicationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListPublicationsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribePublicationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
