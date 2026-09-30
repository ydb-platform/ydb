#pragma once

#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/provider_base.h>

namespace NYdb::inline Dev::NOidc::NPrivate {

class TDeviceProvider final: public TRefreshingProviderBase {
public:
    explicit TDeviceProvider(const TOidcConfig& config);
    ~TDeviceProvider() override;

private:
    TTokenCache AcquireToken() override;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
