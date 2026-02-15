#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoActorTracingStart(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoActorTracingStop(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoActorTracingFetch(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

} // namespace NKikimr::NGRpcService
