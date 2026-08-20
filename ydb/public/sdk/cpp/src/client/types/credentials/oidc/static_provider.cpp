#include "static_provider.h"

namespace NYdb::inline Dev::NOidc {

    TOidcStaticProvider::TOidcStaticProvider(
        std::string issuer,
        TStaticOidcConfig config,
        std::shared_ptr<IOidcTokenCache> tokenCache,
        std::weak_ptr<ICoreFacility> facility)
        : TOidcProvider(std::move(issuer), std::move(tokenCache), std::move(facility))
        , Config_(std::move(config))
    {
    }

    void TOidcStaticProvider::Bootstrap() {
        const TInstant now = TInstant::Now();
        const bool canRefresh = !Config_.ClientId_.empty() && Config_.Tokens_.RefreshToken.has_value() && Config_.Tokens_.RefreshToken->IsUsable(now, REFRESH_SKEW);

        if (!Config_.Tokens_.AccessToken.IsUsable(now, REFRESH_SKEW) && canRefresh) {
            Restore(Config_.Tokens_);
            return Refresh();
        }

        Publish(Config_.Tokens_);
        if (!Config_.Tokens_.AccessToken.ExpiresAt.has_value() || !canRefresh) {
            StopRefreshing();
        }
    }

    void TOidcStaticProvider::Refresh() {
        const TOidcTokenSet tokens = GetTokens();
        if (Config_.ClientId_.empty() || !tokens.RefreshToken.has_value() || !tokens.RefreshToken->IsUsable(TInstant::Now(), REFRESH_SKEW))
        {
            throw TOidcException("OIDC static credentials cannot be refreshed without client id and usable refresh token");
        }

        Publish(Protocol().RefreshToken(
            tokens.RefreshToken.value(),
            tokens.RefreshToken,
            Config_.ClientId_,
            {}));
    }

} // namespace NYdb::inline Dev::NOidc
