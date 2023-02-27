#pragma once
#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoCreateCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoAlterCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDropCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}

