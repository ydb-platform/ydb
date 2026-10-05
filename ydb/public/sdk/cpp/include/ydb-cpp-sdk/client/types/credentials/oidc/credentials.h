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
    // interaction, create a new factory.
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
// One factory shares tokens, refresh and a device sign-in attempt across its providers.
// CreateProvider(facility) returns a separate provider bound to the supplied facility;
// its pending requests are cancelled on provider destruction or when facility
// expiration is detected. Detection can wait for the current synchronous operation.
// Parameterless CreateProvider() reuses one provider owning a standalone facility.
// Authentication state lives while the factory or any of its providers is retained.
// Different factories do not coalesce authorization flows, even with a shared cacher.
// For every flow, a terminal authentication error or worker-start failure is retained
// for the lifetime of the factory. Creating another provider does not retry it;
// create a new factory to start a fresh attempt. Retryable errors are retried by the
// shared worker, and an unexpired token remains usable until its expiration.
//
// GetAuthInfo() blocks until credentials or an error are available; prefer
// GetAuthInfoAsync() when waiting for interactive sign-in.
//
// HTTP runs synchronously on the shared authentication worker. Releasing its last
// factory/provider owner stops polling and joins that worker; active HTTP may wait
// for socket/connect timeouts (5 s / 30 s).
// DNS resolution is subject to the system resolver's timeout. Transport timeouts
// bound individual socket operations, not the total duration of a streaming response.
// Keep provider/factory owners alive until their hooks and callbacks on a driver's
// response queue return; synchronous destruction from those callbacks is not supported.
// Worker startup errors, cancellation and failed delivery detected by the authentication
// worker use a shared fallback executor; those callbacks may release the last provider/factory owner.
std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config);

} // namespace NYdb::inline Dev::NOidc
