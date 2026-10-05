#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/client_provider.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/device_provider.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/provider_base.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/static_provider.h>

#include <util/datetime/base.h>
#include <util/generic/overloaded.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>

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
    explicit TFactory(const TOidcConfig& config);

    TCredentialsProviderPtr CreateProvider() const override;

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override;

    std::string GetClientIdentity() const override;

private:
    std::string Identity;
    mutable TMutex Mutex;
    mutable TCredentialsProviderPtr Provider;
    const std::shared_ptr<TProviderBase> State;
};

std::shared_ptr<TProviderBase> CreateState(const TOidcConfig& config);

std::shared_ptr<TProviderBase> CreateState(const TOidcConfig& config) {
    return std::visit(TOverloaded{
        [&](const TStaticOidcConfig&) -> std::shared_ptr<TProviderBase> {
            return std::make_shared<TStaticProvider>(config);
        },
        [&](const TClientOidcConfig&) -> std::shared_ptr<TProviderBase> {
            return std::make_shared<TClientProvider>(config);
        },
        [&](const TDeviceOidcConfig&) -> std::shared_ptr<TProviderBase> {
            return std::make_shared<TDeviceProvider>(config);
        },
    }, config.FlowConfig);
}

TFactory::TFactory(const TOidcConfig& config)
    : Identity(GetOidcClientIdentity(config))
    , State(CreateState(config))
{
    // The factory is identified before authorization, so a token's sub claim
    // is not available for client/device grants. Keep the credential fingerprint
    // stable and distinguish custom hooks by their process-local instance identity.
    if (config.Cacher_ != nullptr || config.Acceptor_ != nullptr) {
        Identity = HashIdentity(Identity + ":" +
            std::to_string(reinterpret_cast<uintptr_t>(config.Cacher_.get())) + ":" +
            std::to_string(reinterpret_cast<uintptr_t>(config.Acceptor_.get())));
    }
}

TCredentialsProviderPtr TFactory::CreateProvider() const {
    with_lock (Mutex) {
        if (Provider == nullptr) {
            auto facility = CreateSimpleCoreFacility();
            Provider = std::make_shared<NCredentials::NDetail::TOwningFacilityCredentialsProvider>(
                facility, State->CreateProvider(facility));
        }
        return Provider;
    }
}

TCredentialsProviderPtr TFactory::CreateProvider(std::weak_ptr<ICoreFacility> facility) const {
    return State->CreateProvider(std::move(facility));
}

std::string TFactory::GetClientIdentity() const {
    return Identity;
}

} // namespace

} // namespace NPrivate

std::shared_ptr<ICredentialsProviderFactory> CreateOidcProviderFactory(const TOidcConfig& config) {
    NPrivate::ValidateOidcConfig(config);
    return std::make_shared<NPrivate::TFactory>(config);
}

} // namespace NYdb::inline Dev::NOidc
