#include "client_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

TClientProvider::TClientProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : TRefreshingProviderBase(config, std::move(facility), standalone)
{
}

TTokenCache TClientProvider::AcquireToken() {
    return GetProtocol().ClientGrant();
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
