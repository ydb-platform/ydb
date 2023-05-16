#pragma once

#include <util/datetime/base.h>
#include "monotonic.h"

namespace NMonotonic {

    class IMonotonicTimeProvider: public TThrRefBase {
    public:
        virtual TMonotonic Now() = 0;
    };

    class TMonotonicOperator {
    public:
        static void RegisterProvider(TIntrusivePtr<IMonotonicTimeProvider> provider);
        static TMonotonic Now();
    };

    TIntrusivePtr<IMonotonicTimeProvider> CreateDefaultMonotonicTimeProvider();

}

// TODO: remove, alias for compatibility
using IMonotonicTimeProvider = NMonotonic::IMonotonicTimeProvider;

// TODO: remove, alias for compatibility
using NMonotonic::CreateDefaultMonotonicTimeProvider;
