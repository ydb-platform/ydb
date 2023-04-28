#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoApplyConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

void DoDropConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

void DoAddVolatileConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

void DoRemoveVolatileConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

void DoGetConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

void DoResolveConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

void DoResolveAllConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
