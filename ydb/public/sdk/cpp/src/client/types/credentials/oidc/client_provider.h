#pragma once

#include "provider.h"

namespace NYdb::inline Dev::NOidc {

    class TOidcClientProvider final: public TOidcProvider {
    public:
        TOidcClientProvider(
            std::string issuer,
            TClientOidcConfig config,
            std::shared_ptr<IOidcTokenCache> tokenCache,
            std::weak_ptr<ICoreFacility> facility);

        void Bootstrap() override;

    private:
        void Refresh() override;
        TOidcTokenSet AcquireTokens();

    private:
        TClientOidcConfig Config_;
    };

} // namespace NYdb::inline Dev::NOidc
