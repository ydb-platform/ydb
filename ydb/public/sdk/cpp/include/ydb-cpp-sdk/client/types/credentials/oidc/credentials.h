#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>

#include <util/datetime/base.h>

#include <memory>
#include <optional>
#include <string>
#include <variant>

namespace NYdb::inline Dev {

    struct TOidcToken {
        std::string Token;
        std::optional<TInstant> ExpiresAt;

        bool IsUsable(TInstant now, TDuration skew = TDuration::Zero()) const;
    };

    struct TOidcTokenSet {
        TOidcToken AccessToken;
        std::optional<TOidcToken> RefreshToken;

        bool IsUsable(TInstant now, TDuration skew = TDuration::Zero()) const;
    };

    class IOidcTokenCache {
    public:
        virtual ~IOidcTokenCache() = default;
        virtual std::optional<TOidcTokenSet> Read() const = 0;
        virtual void Write(const TOidcTokenSet& tokens) = 0;
    };

    struct TOidcDeviceAuthorizationInfo {
        std::string UserCode;
        std::string VerificationUri;
        std::optional<std::string> VerificationUriComplete;
        TInstant ExpiresAt;
    };

    class IOidcAuthorizationAcceptor {
    public:
        virtual ~IOidcAuthorizationAcceptor() = default;
        virtual void Accept(const TOidcDeviceAuthorizationInfo& info) = 0;
    };

    struct TStaticOidcConfig {
        using TSelf = TStaticOidcConfig;

        FLUENT_SETTING(std::string, ClientId);
        FLUENT_SETTING(TOidcTokenSet, Tokens);
    };

    struct TClientOidcConfig {
        using TSelf = TClientOidcConfig;

        FLUENT_SETTING(std::string, ClientId);
        FLUENT_SETTING(std::string, ClientSecret);
        FLUENT_SETTING_VECTOR(std::string, Scope);
    };

    struct TDeviceOidcConfig {
        using TSelf = TDeviceOidcConfig;

        FLUENT_SETTING(std::string, ClientId);
        FLUENT_SETTING_VECTOR(std::string, Scope);
    };

    using TOidcFlowConfig = std::variant<
        TStaticOidcConfig,
        TClientOidcConfig,
        TDeviceOidcConfig>;

    struct TOidcConfig {
        using TSelf = TOidcConfig;

        static TOidcConfig Parse(const std::string& configPath);

        FLUENT_SETTING(std::string, Issuer);
        FLUENT_SETTING(TOidcFlowConfig, Flow);
        FLUENT_SETTING(std::shared_ptr<IOidcTokenCache>, TokenCache);
        FLUENT_SETTING(std::shared_ptr<IOidcAuthorizationAcceptor>, Acceptor);
    };

    std::shared_ptr<ICredentialsProviderFactory> CreateOidcCredentialsProviderFactory(const TOidcConfig& config);

} // namespace NYdb::inline Dev
