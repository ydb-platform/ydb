#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoDescribeReplication(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
