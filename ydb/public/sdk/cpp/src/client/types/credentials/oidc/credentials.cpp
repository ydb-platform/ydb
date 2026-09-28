#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/static_provider.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/client_provider.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/device_provider.h>

#include <util/generic/overloaded.h>
#include <util/system/mutex.h>

#include <cstdint>
#include <utility>

namespace NYdb::inline Dev::NOidc {

ITokenCacher::~ITokenCacher() = default;

IAuthAcceptor::~IAuthAcceptor() = default;

bool TOAuthToken::IsValid(TInstant now) const {
    return !Token.empty() && (!ExpiresAt.has_value() || ExpiresAt.value() > now);
}

namespace NPrivate {

namespace {

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

std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config) {
    NPrivate::ValidateOidcConfig(config);
    return std::make_shared<NPrivate::TFactory>(config);
}

} // namespace NYdb::inline Dev::NOidc
