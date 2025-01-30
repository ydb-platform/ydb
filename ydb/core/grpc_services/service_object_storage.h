#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestNoOpCtx;
class IFacilityProvider;

void DoObjectStorageListingRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NGRpcService
} // namespace NKikimr
