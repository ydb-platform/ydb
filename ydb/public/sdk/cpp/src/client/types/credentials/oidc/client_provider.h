#pragma once

#include "provider_base.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

class TClientProvider final: public TRefreshingProviderBase {
public:
    explicit TClientProvider(const TOidcConfig& config);
    ~TClientProvider() override;

private:
    TTokenCache AcquireToken() override;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
