#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/simple/http_client.h>

#include <optional>
#include <string>

namespace NYdb::inline Dev::NOidc {

    inline constexpr TDuration SOCKET_TIMEOUT = TDuration::Seconds(5);
    inline constexpr TDuration CONNECT_TIMEOUT = TDuration::Seconds(30);
    inline constexpr TDuration REFRESH_SKEW = TDuration::Seconds(30);
    inline constexpr TDuration RETRY_DELAY = TDuration::Seconds(1);
    inline constexpr TDuration PERIODIC_TICK = TDuration::Seconds(1);
    inline constexpr TDuration DEFAULT_DEVICE_POLL_INTERVAL = TDuration::Seconds(5);
    inline constexpr TDuration DEVICE_SLOW_DOWN_INCREMENT = TDuration::Seconds(5);

    struct TDiscoveryDocument {
        std::string TokenEndpoint;
        std::optional<std::string> DeviceAuthorizationEndpoint;
    };

    struct TDeviceAuthorizationResponse {
        TOidcDeviceAuthorizationInfo UserInfo;
        std::string DeviceCode;
        TDuration Interval;
    };

    class TOidcException final: public std::runtime_error {
    public:
        TOidcException(
            std::string message,
            TKeepAliveHttpClient::THttpCode httpCode = 0,
            std::string oauthError = {});

        const TKeepAliveHttpClient::THttpCode HttpCode;
        const std::string OAuthError;
    };

    class TOidcProtocolClient {
    public:
        explicit TOidcProtocolClient(std::string issuer);

        const std::string& GetIssuer() const;

        TOidcTokenSet RefreshToken(
            const TOidcToken& refreshToken,
            const std::optional<TOidcToken>& previousRefreshToken,
            const std::string& clientId,
            const std::string& clientSecret);

        TOidcTokenSet ClientCredentialsGrant(const TClientOidcConfig& config);
        TDeviceAuthorizationResponse StartDeviceAuthorization(const TDeviceOidcConfig& config);
        TOidcTokenSet PollDeviceToken(const TDeviceOidcConfig& config, const std::string& deviceCode);

        static bool IsRetryable(const std::exception& error);

    private:
        const TDiscoveryDocument& GetDiscoveryDocument();
        TOidcTokenSet RequestTokens(
            const TCgiParameters& form,
            TKeepAliveHttpClient::THeaders headers,
            const std::optional<TOidcToken>& previousRefreshToken);

    private:
        std::string Issuer_;
        std::optional<TDiscoveryDocument> Discovery_;
    };

} // namespace NYdb::inline Dev::NOidc
