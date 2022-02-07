#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoMakeDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoRemoveDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoListDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDescribePathRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoModifyPermissionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
