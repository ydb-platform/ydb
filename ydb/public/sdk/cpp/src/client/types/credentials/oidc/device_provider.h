#pragma once

#include "provider_base.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

class TDeviceProvider final: public TRefreshingProviderBase {
public:
    TDeviceProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone);

private:
    TTokenCache AcquireToken() override;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
