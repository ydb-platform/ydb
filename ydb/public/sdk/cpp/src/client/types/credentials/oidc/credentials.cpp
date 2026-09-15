#include "private.h"
#include "static_provider.h"
#include "client_provider.h"
#include "device_provider.h"

#include <library/cpp/openssl/crypto/sha.h>
#include <util/generic/guid.h>

#include <algorithm>
#include <mutex>
#include <type_traits>

namespace NYdb::inline Dev {

bool TOAuthToken::IsValid(TInstant now) const {
    return !Token.empty() && (!ExpiresAt || *ExpiresAt > now);
}

namespace NOidc::NPrivate {

std::string ClientId(const TOidcConfig& config) {
    return std::visit([](const auto& flow) { return flow.ClientId; }, config.FlowConfig);
}

std::string ClientSecret(const TOidcConfig& config) {
    return std::visit([](const auto& flow) -> std::string {
        if constexpr (std::is_same_v<std::decay_t<decltype(flow)>, TDeviceOidcConfig>) {
            return {};
        } else {
            return flow.ClientSecret;
        }
    }, config.FlowConfig);
}

std::vector<std::string> Scopes(const TOidcConfig& config) {
    return std::visit([](const auto& flow) -> std::vector<std::string> {
        if constexpr (std::is_same_v<std::decay_t<decltype(flow)>, TStaticOidcConfig>) {
            return {};
        } else {
            return flow.Scopes;
        }
    }, config.FlowConfig);
}

namespace {

class TFactory final: public ICredentialsProviderFactory {
public:
    explicit TFactory(TOidcConfig config)
        : Config(std::move(config))
        , Identity(GetOidcClientIdentity(Config))
    {
        if (std::holds_alternative<TDeviceOidcConfig>(Config.FlowConfig)) {
            // A public client registration identifies an application, not its
            // user. Driver-state reuse must not merge two login sessions.
            Identity += ":" + std::string(CreateGuidAsString());
        }
    }

    TCredentialsProviderPtr CreateProvider() const override {
        std::lock_guard lock(Mutex);
        if (!Provider) {
            // The owned credentials worker also delivers standalone futures.
            // It supports last-owner release inside a completion callback.
            Provider = CreateProviderImpl(std::weak_ptr<ICoreFacility>{}, true);
        }
        return Provider;
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return CreateProviderImpl(std::move(facility), false);
    }

    std::string GetClientIdentity() const override {
        return Identity;
    }

private:
    TCredentialsProviderPtr CreateProviderImpl(std::weak_ptr<ICoreFacility> facility, bool standalone) const {
        return std::visit([&](const auto& flow) -> TCredentialsProviderPtr {
            using TFlow = std::decay_t<decltype(flow)>;
            if constexpr (std::is_same_v<TFlow, TStaticOidcConfig>) {
                return std::make_shared<TStaticProvider>(Config, std::move(facility), standalone);
            } else if constexpr (std::is_same_v<TFlow, TClientOidcConfig>) {
                return std::make_shared<TClientProvider>(Config, std::move(facility), standalone);
            } else {
                return std::make_shared<TDeviceProvider>(Config, std::move(facility), standalone);
            }
        }, Config.FlowConfig);
    }

    TOidcConfig Config;
    std::string Identity;
    mutable std::mutex Mutex;
    mutable TCredentialsProviderPtr Provider;
};

} // namespace
} // namespace NOidc::NPrivate

void ValidateOidcConfig(const TOidcConfig& config) {
    using namespace NOidc::NPrivate;
    ParseUrl(config.Issuer, config.AllowInsecureHttp_, true);
    if (config.SocketTimeout_ == TDuration::Zero() || config.SocketTimeout_ > TDuration::Hours(1) ||
        config.ConnectTimeout_ == TDuration::Zero() || config.ConnectTimeout_ > TDuration::Hours(1)) {
        throw std::invalid_argument("OIDC credentials: HTTP timeouts must be positive and at most one hour");
    }
    if (config.TokenEndpointAuthMethod_ != "client_secret_basic" && config.TokenEndpointAuthMethod_ != "client_secret_post") {
        throw std::invalid_argument("OIDC credentials: unsupported token_endpoint_auth_method");
    }
    std::visit([](const auto& flow) {
        if constexpr (std::is_same_v<std::decay_t<decltype(flow)>, TStaticOidcConfig>) {
            if (flow.AccessToken.empty()) {
                throw std::invalid_argument("OIDC credentials: static_credentials requires access_token");
            }
            if (!flow.RefreshToken.empty() && flow.ClientId.empty()) {
                throw std::invalid_argument("OIDC credentials: static refresh_token requires client_id");
            }
        } else {
            if (flow.ClientId.empty()) {
                throw std::invalid_argument("OIDC credentials: client_id is required");
            }
            if constexpr (std::is_same_v<std::decay_t<decltype(flow)>, TClientOidcConfig>) {
                if (flow.ClientSecret.empty()) {
                    throw std::invalid_argument("OIDC credentials: client_secret is required");
                }
            }
            for (const auto& scope : flow.Scopes) {
                if (scope.empty() || std::any_of(scope.begin(), scope.end(), [](unsigned char c) {
                        return c <= 0x20 || c >= 0x7f || c == '"' || c == '\\';
                    })) {
                    throw std::invalid_argument("OIDC credentials: invalid scope");
                }
            }
        }
    }, config.FlowConfig);
}

std::string GetOidcClientIdentity(const TOidcConfig& config) {
    using namespace NOidc::NPrivate;
    std::string data;
    const auto append = [&data](const std::string& value) {
        data += std::to_string(value.size()) + ":" + value;
    };
    append(config.Issuer);
    append(std::to_string(config.FlowConfig.index()));
    append(ClientId(config));
    append(ClientSecret(config));
    append(config.TokenEndpointAuthMethod_);
    auto scopes = Scopes(config);
    std::sort(scopes.begin(), scopes.end());
    scopes.erase(std::unique(scopes.begin(), scopes.end()), scopes.end());
    for (const auto& scope : scopes) {
        append(scope);
    }
    if (const auto* flow = std::get_if<TStaticOidcConfig>(&config.FlowConfig)) {
        append(flow->AccessToken);
        append(flow->RefreshToken);
        append(flow->ExpiresAt ? std::to_string(flow->ExpiresAt->MicroSeconds()) : "");
        append(flow->RefreshExpiresAt ? std::to_string(flow->RefreshExpiresAt->MicroSeconds()) : "");
    }
    const auto hash = NOpenSsl::NSha256::Calc(data.data(), data.size());
    static constexpr char Hex[] = "0123456789abcdef";
    std::string identity = "oidc:";
    for (const auto byte : hash) {
        identity += Hex[byte >> 4];
        identity += Hex[byte & 15];
    }
    return identity;
}

std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config) {
    ValidateOidcConfig(config);
    return std::make_shared<NOidc::NPrivate::TFactory>(config);
}

} // namespace NYdb::inline Dev
