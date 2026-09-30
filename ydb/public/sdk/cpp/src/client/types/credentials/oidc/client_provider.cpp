#include "client_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

TClientProvider::TClientProvider(const TOidcConfig& config)
    : TRefreshingProviderBase(config)
{
}

TClientProvider::~TClientProvider() {
    Stop();
}

TTokenCache TClientProvider::AcquireToken() {
    return GetProtocol().ClientGrant();
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
