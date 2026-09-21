#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/static_provider.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/client_provider.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/device_provider.h>

#include <library/cpp/openssl/crypto/sha.h>

#include <util/generic/overloaded.h>
#include <util/system/mutex.h>

#include <algorithm>
#include <cstdint>

namespace NYdb::inline Dev::NOidc {

ITokenCacher::~ITokenCacher() = default;

IAuthAcceptor::~IAuthAcceptor() = default;

bool TOAuthToken::IsValid(TInstant now) const {
    return !Token.empty() && (!ExpiresAt.has_value() || ExpiresAt.value() > now);
}

namespace NPrivate {

namespace {

std::string HashIdentity(const std::string& data) {
    const auto hash = NOpenSsl::NSha256::Calc(data.data(), data.size());
    static constexpr char Hex[] = "0123456789abcdef";
    std::string identity = "oidc:";
    for (const auto byte : hash) {
        identity += Hex[byte >> 4];
        identity += Hex[byte & 0xF];
    }
    return identity;
}

class TFactory final: public ICredentialsProviderFactory {
public:
    explicit TFactory(TOidcConfig config);

    TCredentialsProviderPtr CreateProvider() const override;

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override;

    std::string GetClientIdentity() const override;

private:
    TCredentialsProviderPtr CreateProviderImpl(std::weak_ptr<ICoreFacility> facility) const;

    TOidcConfig Config;
    std::string Identity;
    mutable TMutex Mutex;
    mutable TCredentialsProviderPtr Provider;
};

TFactory::TFactory(TOidcConfig config)
    : Config(std::move(config))
    , Identity(GetOidcClientIdentity(Config))
{
    // The factory is identified before authorization, so a token's sub claim
    // is not available for client/device grants. Keep the credential fingerprint
    // stable and distinguish custom hooks by their process-local instance identity.
    if (Config.Cacher_ != nullptr || Config.Acceptor_ != nullptr) {
        Identity = HashIdentity(Identity + ":" +
            std::to_string(reinterpret_cast<uintptr_t>(Config.Cacher_.get())) + ":" +
            std::to_string(reinterpret_cast<uintptr_t>(Config.Acceptor_.get())));
    }
}

TCredentialsProviderPtr TFactory::CreateProvider() const {
    with_lock (Mutex) {
        if (Provider == nullptr) {
            auto facility = CreateSimpleCoreFacility();
            Provider = std::make_shared<NCredentials::NDetail::TOwningFacilityCredentialsProvider>(
                facility, CreateProviderImpl(facility));
        }
        return Provider;
    }
}

TCredentialsProviderPtr TFactory::CreateProvider(std::weak_ptr<ICoreFacility> facility) const {
    return CreateProviderImpl(std::move(facility));
}

std::string TFactory::GetClientIdentity() const {
    return Identity;
}

TCredentialsProviderPtr TFactory::CreateProviderImpl(std::weak_ptr<ICoreFacility> facility) const {
    return std::visit(TOverloaded{
        [&](const TStaticOidcConfig&) -> TCredentialsProviderPtr {
            return std::make_shared<TStaticProvider>(Config, std::move(facility));
        },
        [&](const TClientOidcConfig&) -> TCredentialsProviderPtr {
            return std::make_shared<TClientProvider>(Config, std::move(facility));
        },
        [&](const TDeviceOidcConfig&) -> TCredentialsProviderPtr {
            return std::make_shared<TDeviceProvider>(Config, std::move(facility));
        },
    }, Config.FlowConfig);
}

} // namespace
} // namespace NPrivate

void ValidateOidcConfig(const TOidcConfig& config) {
    using namespace NPrivate;
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
    using namespace NPrivate;
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
    if (const auto* flow = std::get_if<TStaticOidcConfig>(&config.FlowConfig); flow != nullptr) {
        append(flow->AccessToken);
        append(flow->ExpiresAt.has_value() ? std::to_string(flow->ExpiresAt->MicroSeconds()) : "");
    }
    return HashIdentity(data);
}

std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config) {
    ValidateOidcConfig(config);
    return std::make_shared<NPrivate::TFactory>(config);
}

} // namespace NYdb::inline Dev::NOidc
