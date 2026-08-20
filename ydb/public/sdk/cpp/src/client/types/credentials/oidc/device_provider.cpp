#include "device_provider.h"

namespace NYdb::inline Dev::NOidc {

    TOidcDeviceProvider::TOidcDeviceProvider(
        std::string issuer,
        TDeviceOidcConfig config,
        std::shared_ptr<IOidcAuthorizationAcceptor> acceptor,
        std::shared_ptr<IOidcTokenCache> tokenCache,
        std::weak_ptr<ICoreFacility> facility)
        : TOidcProvider(std::move(issuer), std::move(tokenCache), std::move(facility))
        , Config_(std::move(config))
        , Acceptor_(std::move(acceptor))
    {
        if (Acceptor_ == nullptr) {
            throw std::invalid_argument("OIDC device authorization acceptor is not set");
        }
    }

    void TOidcDeviceProvider::Bootstrap() {
        const TInstant now = TInstant::Now();
        if (const auto cached = ReadCache(); cached.has_value() && cached->IsUsable(now, REFRESH_SKEW)) {
            if (cached->AccessToken.IsUsable(now, REFRESH_SKEW)) {
                return Publish(cached.value());
            }
            Restore(cached.value());
        }
        Refresh();
    }

    void TOidcDeviceProvider::Refresh() {
        const TOidcTokenSet tokens = GetTokens();
        if (tokens.RefreshToken.has_value() && tokens.RefreshToken->IsUsable(TInstant::Now(), REFRESH_SKEW)) {
            try {
                return Publish(Protocol().RefreshToken(
                    tokens.RefreshToken.value(),
                    tokens.RefreshToken,
                    Config_.ClientId_,
                    {}));
            } catch (const TOidcException& error) {
                if (error.OAuthError != "invalid_grant") {
                    throw;
                }
            }
        }

        if (Authorization_.has_value()) {
            return PollAuthorization();
        }
        BeginAuthorization();
    }

    void TOidcDeviceProvider::BeginAuthorization() {
        Authorization_ = Protocol().StartDeviceAuthorization(Config_);
        Acceptor_->Accept(Authorization_->UserInfo);
        ScheduleAfter(Authorization_->Interval);
    }

    void TOidcDeviceProvider::PollAuthorization() {
        if (!Authorization_.has_value()) {
            throw TOidcException("OIDC device authorization is not initialized");
        }
        auto& authorization = Authorization_.value();
        if (TInstant::Now() >= authorization.UserInfo.ExpiresAt) {
            throw TOidcException("OIDC device authorization expired");
        }

        try {
            TOidcTokenSet tokens = Protocol().PollDeviceToken(Config_, authorization.DeviceCode);
            Publish(std::move(tokens));
            Authorization_.reset();
        } catch (const TOidcException& error) {
            if (error.OAuthError == "authorization_pending") {
                return ScheduleAfter(authorization.Interval);
            }
            if (error.OAuthError == "slow_down") {
                authorization.Interval += DEVICE_SLOW_DOWN_INCREMENT;
                return ScheduleAfter(authorization.Interval);
            }
            if (TOidcProtocolClient::IsRetryable(error)) {
                return ScheduleAfter(authorization.Interval);
            }
            throw;
        }
    }

} // namespace NYdb::inline Dev::NOidc
