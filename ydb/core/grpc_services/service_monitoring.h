#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoSelfCheckRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
//void DoNodeCheckRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
