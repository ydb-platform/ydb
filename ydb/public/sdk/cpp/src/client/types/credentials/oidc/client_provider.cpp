#include "client_provider.h"

namespace NYdb::inline Dev::NOidc {

    TOidcClientProvider::TOidcClientProvider(
        std::string issuer,
        TClientOidcConfig config,
        std::shared_ptr<IOidcTokenCache> tokenCache,
        std::weak_ptr<ICoreFacility> facility)
        : TOidcProvider(std::move(issuer), std::move(tokenCache), std::move(facility))
        , Config_(std::move(config))
    {
    }

    void TOidcClientProvider::Bootstrap() {
        const TInstant now = TInstant::Now();
        if (const auto cached = ReadCache(); cached.has_value() && cached->IsUsable(now, REFRESH_SKEW)) {
            if (cached->AccessToken.IsUsable(now, REFRESH_SKEW)) {
                return Publish(cached.value());
            }
            Restore(cached.value());
        }
        Publish(AcquireTokens());
    }

    void TOidcClientProvider::Refresh() {
        Publish(AcquireTokens());
    }

    TOidcTokenSet TOidcClientProvider::AcquireTokens() {
        const TOidcTokenSet tokens = GetTokens();
        if (tokens.RefreshToken.has_value() && tokens.RefreshToken->IsUsable(TInstant::Now(), REFRESH_SKEW)) {
            try {
                return Protocol().RefreshToken(
                    tokens.RefreshToken.value(),
                    tokens.RefreshToken,
                    Config_.ClientId_,
                    Config_.ClientSecret_);
            } catch (const TOidcException& error) {
                if (error.OAuthError != "invalid_grant") {
                    throw;
                }
            }
        }
        return Protocol().ClientCredentialsGrant(Config_);
    }

} // namespace NYdb::inline Dev::NOidc
