#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoDescribeView(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
