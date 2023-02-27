#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoMakeDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoRemoveDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribePathRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoModifyPermissionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
