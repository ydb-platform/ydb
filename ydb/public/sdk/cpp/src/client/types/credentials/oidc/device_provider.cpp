#include "device_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

TDeviceProvider::TDeviceProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : TRefreshingProviderBase(config, std::move(facility), standalone)
{
}

TTokenCache TDeviceProvider::AcquireToken() {
    return GetProtocol().DeviceGrant([this](TDuration delay) { return Wait(delay); });
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
