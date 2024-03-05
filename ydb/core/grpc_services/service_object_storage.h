#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoObjectStorageListingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

} // namespace NGRpcService
} // namespace NKikimr
