#pragma once

#include <util/datetime/base.h>
#include "monotonic.h"

class IMonotonicTimeProvider: public TThrRefBase {
public:
    virtual TMonotonic Now() = 0;
};

class TMonotonicOperator {
public:
    static void RegisterProvider(TIntrusivePtr<IMonotonicTimeProvider> provider);
    static TMonotonic Now();
};

namespace NMonotonic {

TIntrusivePtr<IMonotonicTimeProvider> CreateDefaultMonotonicTimeProvider();

}
