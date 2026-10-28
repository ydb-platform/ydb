#include "client_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

TClientProvider::TClientProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility)
    : TRefreshingProviderBase(config, std::move(facility))
{
    Start();
}

TClientProvider::~TClientProvider() {
    Stop();
}

TTokenCache TClientProvider::AcquireToken() {
    return GetProtocol().ClientGrant();
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
