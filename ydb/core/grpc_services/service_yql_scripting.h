#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

void DoExecuteYqlScript(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoStreamExecuteYqlScript(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& facility);
void DoExplainYqlScript(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);

} // namespace NGRpcService
} // namespace NKikimr
