#include "client_provider.h"
#include "device_provider.h"
#include "static_provider.h"

#include <util/generic/overloaded.h>

#include <mutex>

namespace NYdb::inline Dev {

    namespace {

        class TOidcCredentialsProviderFactory final: public ICredentialsProviderFactory {
        public:
            explicit TOidcCredentialsProviderFactory(TOidcConfig config)
                : Config_(std::move(config))
            {
            }

            TCredentialsProviderPtr CreateProvider() const override {
                std::call_once(Initialized_, [this] {
                    OwnedFacility_ = CreateSimpleCoreFacility();
                    Provider_ = CreateProviderImpl(OwnedFacility_);
                });
                return Provider_;
            }

            TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
                std::call_once(Initialized_, [this, facility = std::move(facility)]() mutable {
                    Provider_ = CreateProviderImpl(std::move(facility));
                });
                return Provider_;
            }

        private:
            TCredentialsProviderPtr CreateProviderImpl(std::weak_ptr<ICoreFacility> facility) const {
                auto provider = std::visit(TOverloaded{
                                               [&](const TStaticOidcConfig& flow) -> std::shared_ptr<NOidc::TOidcProvider> {
                                                   return std::make_shared<NOidc::TOidcStaticProvider>(
                                                       Config_.Issuer_,
                                                       flow,
                                                       Config_.TokenCache_,
                                                       facility);
                                               },
                                               [&](const TClientOidcConfig& flow) -> std::shared_ptr<NOidc::TOidcProvider> {
                                                   return std::make_shared<NOidc::TOidcClientProvider>(
                                                       Config_.Issuer_,
                                                       flow,
                                                       Config_.TokenCache_,
                                                       facility);
                                               },
                                               [&](const TDeviceOidcConfig& flow) -> std::shared_ptr<NOidc::TOidcProvider> {
                                                   return std::make_shared<NOidc::TOidcDeviceProvider>(
                                                       Config_.Issuer_,
                                                       flow,
                                                       Config_.Acceptor_,
                                                       Config_.TokenCache_,
                                                       facility);
                                               },
                                           }, Config_.Flow_);
                provider->Start();
                return provider;
            }

        private:
            TOidcConfig Config_;
            mutable std::once_flag Initialized_;
            mutable std::shared_ptr<ICoreFacility> OwnedFacility_;
            mutable TCredentialsProviderPtr Provider_;
        };

    } // namespace

    std::shared_ptr<ICredentialsProviderFactory> CreateOidcCredentialsProviderFactory(const TOidcConfig& config) {
        return std::make_shared<TOidcCredentialsProviderFactory>(config);
    }

} // namespace NYdb::inline Dev
