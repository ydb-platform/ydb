#include "device_provider.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/provider_base.h>

#include <util/datetime/base.h>

namespace NYdb::inline Dev::NOidc::NPrivate {

TDeviceProvider::TDeviceProvider(const TOidcConfig& config)
    : TRefreshingProviderBase(config)
{
}

TDeviceProvider::~TDeviceProvider() {
    Stop();
}

TTokenCache TDeviceProvider::AcquireToken() {
    return GetProtocol().DeviceGrant([this](TDuration delay) { return Wait(delay); });
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
