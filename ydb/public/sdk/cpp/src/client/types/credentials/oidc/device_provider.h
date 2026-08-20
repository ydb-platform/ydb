#pragma once

#include "provider.h"

namespace NYdb::inline Dev::NOidc {

    class TOidcDeviceProvider final: public TOidcProvider {
    public:
        TOidcDeviceProvider(
            std::string issuer,
            TDeviceOidcConfig config,
            std::shared_ptr<IOidcAuthorizationAcceptor> acceptor,
            std::shared_ptr<IOidcTokenCache> tokenCache,
            std::weak_ptr<ICoreFacility> facility);

        void Bootstrap() override;

    private:
        void Refresh() override;
        void BeginAuthorization();
        void PollAuthorization();

    private:
        TDeviceOidcConfig Config_;
        std::shared_ptr<IOidcAuthorizationAcceptor> Acceptor_;
        std::optional<TDeviceAuthorizationResponse> Authorization_;
    };

} // namespace NYdb::inline Dev::NOidc
