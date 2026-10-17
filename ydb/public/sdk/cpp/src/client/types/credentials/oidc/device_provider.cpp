#include "device_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

TDeviceProvider::TDeviceProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility)
    : TRefreshingProviderBase(config, std::move(facility))
{
    Start();
}

TDeviceProvider::~TDeviceProvider() {
    Stop();
}

TTokenCache TDeviceProvider::AcquireToken() {
    return GetProtocol().DeviceGrant([this](TDuration delay) { return Wait(delay); });
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
