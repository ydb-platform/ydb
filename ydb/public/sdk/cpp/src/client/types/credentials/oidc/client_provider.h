#pragma once

#include "provider_base.h"

namespace NYdb::inline Dev::NOidc::NPrivate {

class TClientProvider final: public TProviderBase {
public:
    TClientProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone);

private:
    class TState;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
