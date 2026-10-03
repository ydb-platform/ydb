#include "static_provider.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/provider_base.h>

#include <util/datetime/base.h>

#include <exception>
#include <variant>

namespace NYdb::inline Dev::NOidc::NPrivate {

TStaticProvider::TStaticProvider(const TOidcConfig& config)
    : TProviderBase(config)
{
}

TStaticProvider::~TStaticProvider() {
    Stop();
}

void TStaticProvider::RunTokens() {
    const auto& flow = std::get<TStaticOidcConfig>(Config.FlowConfig);
    TTokenCache current;
    current.AccessToken = {flow.AccessToken, flow.ExpiresAt};
    if (!current.AccessToken.ExpiresAt.has_value()) {
        current.AccessToken.ExpiresAt = JwtExpiry(current.AccessToken.Token);
    }
    if (!current.AccessToken.IsValid(TInstant::Now())) {
        throw TError("static credentials have expired", false, {});
    }
    Publish(current, true);
    if (current.AccessToken.ExpiresAt.has_value()) {
        Fail(std::make_exception_ptr(TError("static credentials have expired", false, {})));
    }
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
