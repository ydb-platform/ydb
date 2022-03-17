#pragma once

#include "monotonic.h"

namespace NActors {

class IMonotonicTimeProvider : public TThrRefBase {
public:
    virtual TMonotonic Now() = 0;
};

TIntrusivePtr<IMonotonicTimeProvider> CreateDefaultMonotonicTimeProvider();

} // namespace NActors
