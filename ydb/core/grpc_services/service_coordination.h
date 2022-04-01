#pragma once
#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoCreateCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoAlterCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDropCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDescribeCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}

