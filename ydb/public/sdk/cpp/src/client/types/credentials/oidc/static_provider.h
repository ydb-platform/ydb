#pragma once

#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/provider_base.h>

namespace NYdb::inline Dev::NOidc::NPrivate {

class TStaticProvider final: public TProviderBase {
public:
    explicit TStaticProvider(const TOidcConfig& config);
    ~TStaticProvider() override;

private:
    void RunTokens() override;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
