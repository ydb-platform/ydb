#include "private.h"
#include "static_provider.h"
#include "client_provider.h"
#include "device_provider.h"

#include <library/cpp/openssl/crypto/sha.h>
#include <util/generic/guid.h>
#include <util/generic/overloaded.h>
#include <util/system/mutex.h>

#include <algorithm>

namespace NYdb::inline Dev {

ITokenCacher::~ITokenCacher() = default;

IAuthAcceptor::~IAuthAcceptor() = default;

bool TOAuthToken::IsValid(TInstant now) const {
    return !Token.empty() && (!ExpiresAt.has_value() || ExpiresAt.value() > now);
}

namespace NOidc::NPrivate {

namespace {

class TFactory final: public ICredentialsProviderFactory {
public:
    explicit TFactory(TOidcConfig config);

    TCredentialsProviderPtr CreateProvider() const override;

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override;

    std::string GetClientIdentity() const override;

private:
    TCredentialsProviderPtr CreateProviderImpl(std::weak_ptr<ICoreFacility> facility, bool standalone) const;

    TOidcConfig Config;
    std::string Identity;
    mutable TMutex Mutex;
    mutable TCredentialsProviderPtr Provider;
};

TFactory::TFactory(TOidcConfig config)
    : Config(std::move(config))
    , Identity(GetOidcClientIdentity(Config))
{
    if (std::holds_alternative<TDeviceOidcConfig>(Config.FlowConfig)) {
        Identity += ":" + std::string(CreateGuidAsString());
    }
}

TCredentialsProviderPtr TFactory::CreateProvider() const {
    with_lock (Mutex) {
        if (!Provider) {
            Provider = CreateProviderImpl(std::weak_ptr<ICoreFacility>{}, true);
        }
        return Provider;
    }
}

TCredentialsProviderPtr TFactory::CreateProvider(std::weak_ptr<ICoreFacility> facility) const {
    return CreateProviderImpl(std::move(facility), false);
}

std::string TFactory::GetClientIdentity() const {
    return Identity;
}

TCredentialsProviderPtr TFactory::CreateProviderImpl(std::weak_ptr<ICoreFacility> facility, bool standalone) const {
    auto provider = std::visit(TOverloaded{
        [&](const TStaticOidcConfig&) -> std::shared_ptr<TProviderBase> {
            return std::make_shared<TStaticProvider>(Config, std::move(facility), standalone);
        },
        [&](const TClientOidcConfig&) -> std::shared_ptr<TProviderBase> {
            return std::make_shared<TClientProvider>(Config, std::move(facility), standalone);
        },
        [&](const TDeviceOidcConfig&) -> std::shared_ptr<TProviderBase> {
            return std::make_shared<TDeviceProvider>(Config, std::move(facility), standalone);
        },
    }, Config.FlowConfig);
    return std::make_shared<TCredentialsProviderAdapter>(std::move(provider));
}

} // namespace
} // namespace NOidc::NPrivate

void ValidateOidcConfig(const TOidcConfig& config) {
    using namespace NOidc::NPrivate;
    ParseUrl(config.Issuer, true);
    std::visit(TOverloaded{
        [](const TStaticOidcConfig& flow) {
            if (flow.AccessToken.empty()) {
                throw std::invalid_argument("OIDC credentials: static_credentials requires access_token");
            }
        },
        [](const TClientOidcConfig& flow) {
            if (flow.ClientId.empty()) {
                throw std::invalid_argument("OIDC credentials: client_id is required");
            }
            if (flow.ClientSecret.empty()) {
                throw std::invalid_argument("OIDC credentials: client_secret is required");
            }
        },
        [](const TDeviceOidcConfig& flow) {
            if (flow.ClientId.empty()) {
                throw std::invalid_argument("OIDC credentials: client_id is required");
            }
        },
    }, config.FlowConfig);
    for (const auto& scope : Scopes(config)) {
        if (scope.empty() || std::any_of(scope.begin(), scope.end(), [](unsigned char c) {
                return c <= 0x20 || c >= 0x7f || c == '"' || c == '\\';
            })) {
            throw std::invalid_argument("OIDC credentials: invalid scope");
        }
    }
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
    auto scopes = Scopes(config);
    std::sort(scopes.begin(), scopes.end());
    scopes.erase(std::unique(scopes.begin(), scopes.end()), scopes.end());
    for (const auto& scope : scopes) {
        append(scope);
    }
    if (const auto* flow = std::get_if<TStaticOidcConfig>(&config.FlowConfig)) {
        append(flow->AccessToken);
        append(flow->ExpiresAt ? std::to_string(flow->ExpiresAt->MicroSeconds()) : "");
    }
    const auto hash = NOpenSsl::NSha256::Calc(data.data(), data.size());
    static constexpr char Hex[] = "0123456789abcdef";
    std::string identity = "oidc:";
    for (const auto byte : hash) {
        identity += Hex[byte >> 4];
        identity += Hex[byte & 0xF];
    }
    return identity;
}

std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config) {
    ValidateOidcConfig(config);
    return std::make_shared<NOidc::NPrivate::TFactory>(config);
}

} // namespace NYdb::inline Dev
