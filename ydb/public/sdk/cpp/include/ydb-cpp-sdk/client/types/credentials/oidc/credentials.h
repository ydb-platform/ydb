#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>

#include <util/datetime/base.h>

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace NYdb::inline Dev::NOidc {

struct TOAuthToken {
    std::string Token;
    // An absent expiry means the lifetime is unknown; automatic refresh cannot be scheduled.
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
    // Providers may share a cacher across worker threads. Read() and Write()
    // must be thread-safe; interprocess synchronization is not required.
    // Calls are synchronous on the provider worker and must return promptly.
    // Read failures are treated as cache misses; Write failures leave the token
    // usable in memory. Implementations should report persistence errors themselves.
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
    // Called synchronously on a provider worker. Return promptly so polling and
    // provider destruction can proceed; do not wait for the user to finish sign-in.
    // Copy info before handing it to another thread. A shared acceptor may receive
    // concurrent calls from different providers. Exceptions fail authentication.
    virtual void Accept(const TDeviceAuthInfo& info) = 0;
};

struct TStaticOidcConfig {
    std::string AccessToken;
    std::optional<TInstant> ExpiresAt;
};

struct TClientOidcConfig {
    std::string ClientId;
    std::string ClientSecret;
    // The provider adds "openid" if it is not listed, including for client credentials.
    std::vector<std::string> Scopes;
};

struct TDeviceOidcConfig {
    // Expiry or denial ends the current sign-in attempt. To try again after user
    // interaction, create a new provider (a new factory for parameterless CreateProvider()).
    std::string ClientId;
    // The provider adds "openid" if it is not listed.
    std::vector<std::string> Scopes;
};

using TFlowConfig = std::variant<TStaticOidcConfig, TClientOidcConfig, TDeviceOidcConfig>;

struct TOidcConfig {
    using TSelf = TOidcConfig;

    // Must exactly match the issuer advertised by OpenID Discovery, including a trailing slash.
    std::string Issuer;
    TFlowConfig FlowConfig;

    FLUENT_SETTING(std::shared_ptr<ITokenCacher>, Cacher);
    FLUENT_SETTING(std::shared_ptr<IAuthAcceptor>, Acceptor);
};

// Factory identity is stable for the same credentials and custom hook instances.
// Different hook instances isolate independent user sessions.
// Parameterless CreateProvider() reuses one provider; CreateProvider(facility) creates an independent provider for each call.
// Each device provider can prompt if no usable cached credentials exist. Sharing a
// cacher reuses stored tokens but does not coalesce concurrent authorization flows.
//
// GetAuthInfo() blocks until credentials or an error are available; prefer
// GetAuthInfoAsync() when waiting for interactive sign-in.
//
// HTTP runs synchronously on the provider worker. Destruction stops polling and
// joins that worker; active HTTP may wait for socket/connect timeouts (5 s / 30 s).
// DNS resolution is subject to the system resolver's timeout. Transport timeouts
// bound individual socket operations, not the total duration of a streaming response.
// Keep provider/factory owners alive until their hooks and future callbacks return;
// synchronous destruction from those callbacks is not supported.
std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config);

} // namespace NYdb::inline Dev::NOidc
