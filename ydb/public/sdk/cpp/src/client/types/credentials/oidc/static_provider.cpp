#include "static_provider.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

TStaticProvider::TStaticProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : TProviderBase(config, std::move(facility), standalone)
{
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
    Write(current);
    Publish(current);
    if (current.AccessToken.ExpiresAt) {
        Fail(std::make_exception_ptr(TError("static credentials have expired", false, {})));
    }
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
