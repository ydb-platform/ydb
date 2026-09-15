#pragma once

#include "provider_base.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

class TStaticProvider final: public TProviderBase {
public:
    TStaticProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone);

private:
    void RunTokens() override;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
