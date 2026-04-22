#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoDescribeSecretRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
