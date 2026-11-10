#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoExportSecretRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

void DoDescribeSecretRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
