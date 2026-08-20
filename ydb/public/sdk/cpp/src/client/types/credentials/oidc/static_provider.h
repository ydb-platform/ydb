#pragma once

#include "provider.h"

namespace NYdb::inline Dev::NOidc {

    class TOidcStaticProvider final: public TOidcProvider {
    public:
        TOidcStaticProvider(
            std::string issuer,
            TStaticOidcConfig config,
            std::shared_ptr<IOidcTokenCache> tokenCache,
            std::weak_ptr<ICoreFacility> facility);

        void Bootstrap() override;

    private:
        void Refresh() override;

    private:
        TStaticOidcConfig Config_;
    };

} // namespace NYdb::inline Dev::NOidc
