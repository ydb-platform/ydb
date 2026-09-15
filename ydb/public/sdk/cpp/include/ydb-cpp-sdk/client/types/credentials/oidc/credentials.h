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
    // Unknown expiry is allowed for externally supplied opaque tokens.
    std::optional<TInstant> ExpiresAt;

    bool IsValid(TInstant now) const;
};

struct TTokenCache {
    TOAuthToken AccessToken;
    std::optional<TOAuthToken> RefreshToken;
};

class ITokenCacher {
public:
    virtual ~ITokenCacher() = default;
    virtual std::optional<TTokenCache> Read() const = 0;
    virtual void Write(const TTokenCache& cache) = 0;
};

class ITokenCacheLock {
public:
    virtual ~ITokenCacheLock() = default;
};

// Optional interface for serializing a read/refresh/write transaction across
// providers and processes. A null result means the lock is currently busy.
// Custom cachers without this interface coordinate shared storage themselves.
class ILockingTokenCacher {
public:
    virtual ~ILockingTokenCacher() = default;
    virtual std::unique_ptr<ITokenCacheLock> TryLock() = 0;
};

struct TDeviceAuthInfo {
    std::string UserCode;
    std::string VerificationUrl;
    std::optional<std::string> VerificationUrlComplete;
    TInstant ExpiresAt;
};

class IAuthAcceptor {
public:
    virtual ~IAuthAcceptor() = default;
    // Called on a credentials worker without provider locks. Display the data
    // and return promptly; the provider waits for the user's authorization.
    virtual void Accept(const TDeviceAuthInfo& info) = 0;
};

struct TStaticOidcConfig {
    std::string AccessToken;
    std::string RefreshToken;
    std::string ClientId;
    std::string ClientSecret;
    std::optional<TInstant> ExpiresAt;
    std::optional<TInstant> RefreshExpiresAt;
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
    FLUENT_SETTING_DEFAULT(TDuration, SocketTimeout, TDuration::Seconds(5));
    FLUENT_SETTING_DEFAULT(TDuration, ConnectTimeout, TDuration::Seconds(30));
    FLUENT_SETTING_DEFAULT(bool, AllowInsecureHttp, false);
    FLUENT_SETTING_DEFAULT(std::string, TokenEndpointAuthMethod, "client_secret_basic");
};

// Performs local validation only, without contacting the issuer or reading cache.
void ValidateOidcConfig(const TOidcConfig& config);

// Stable digest of the issuer, flow, client credentials and normalized scopes.
// Suitable for binding a persistent token cache to one configuration.
std::string GetOidcClientIdentity(const TOidcConfig& config);

std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config);

} // namespace NYdb::inline Dev
