#include "client_provider.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/provider_base.h>

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
