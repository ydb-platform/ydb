#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoCreateRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoAlterRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDropRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoListRateLimiterResources(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDescribeRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoAcquireRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
