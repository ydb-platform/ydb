#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoCreateRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoAlterRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDropRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListRateLimiterResources(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoAcquireRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
