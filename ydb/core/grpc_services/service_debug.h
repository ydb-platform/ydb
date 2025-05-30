#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

void DoGrpcProxyPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoKqpPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoSchemeCachePing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoTxProxyPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoActorChainPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NKikimr::NGRpcService
