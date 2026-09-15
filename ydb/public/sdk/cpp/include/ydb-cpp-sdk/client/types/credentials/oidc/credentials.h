#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>

#include <util/datetime/base.h>

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace NYdb::inline Dev {

struct TOAuthToken {
    std::string Token;
    std::optional<TInstant> ExpiresAt;

    bool IsValid(TInstant now) const;
};

struct TTokenCache {
    TOAuthToken AccessToken;
    std::optional<TOAuthToken> RefreshToken;
};

class ITokenCacher {
public:
    virtual ~ITokenCacher();
    virtual std::optional<TTokenCache> Read() const = 0;
    virtual void Write(const TTokenCache& cache) = 0;
};

struct TDeviceAuthInfo {
    std::string UserCode;
    std::string VerificationUrl;
    std::optional<std::string> VerificationUrlComplete;
    TInstant ExpiresAt;
};

class IAuthAcceptor {
public:
    virtual ~IAuthAcceptor();
    virtual void Accept(const TDeviceAuthInfo& info) = 0;
};

struct TStaticOidcConfig {
    std::string AccessToken;
    std::optional<TInstant> ExpiresAt;
};

struct TClientOidcConfig {
    std::string ClientId;
    std::string ClientSecret;
    std::vector<std::string> Scopes;
};

struct TDeviceOidcConfig {
    std::string ClientId;
    std::vector<std::string> Scopes;
};

using TFlowConfig = std::variant<TStaticOidcConfig, TClientOidcConfig, TDeviceOidcConfig>;

struct TOidcConfig {
    using TSelf = TOidcConfig;

    std::string Issuer;
    TFlowConfig FlowConfig;

    FLUENT_SETTING(std::shared_ptr<ITokenCacher>, Cacher);
    FLUENT_SETTING(std::shared_ptr<IAuthAcceptor>, Acceptor);
};

void ValidateOidcConfig(const TOidcConfig& config);

std::string GetOidcClientIdentity(const TOidcConfig& config);

std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config);

} // namespace NYdb::inline Dev
