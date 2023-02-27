#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoExportToYtRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoExportToS3Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
