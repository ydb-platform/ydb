#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

void DoGetOperationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoCancelOperationRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
void DoForgetOperationRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
void DoListOperationsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);

}
}
