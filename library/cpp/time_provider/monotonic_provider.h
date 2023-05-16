#pragma once

#include <util/datetime/base.h>
#include "monotonic.h"

namespace NMonotonic {

    class IMonotonicTimeProvider: public TThrRefBase {
    public:
        virtual TMonotonic Now() = 0;
    };

    class IBootTimeProvider: public TThrRefBase {
    public:
        virtual TBootTime Now() = 0;
    };

    class TMonotonicOperator {
    public:
        static void RegisterProvider(TIntrusivePtr<IMonotonicTimeProvider> provider);
        static TMonotonic Now();
    };

    TIntrusivePtr<IMonotonicTimeProvider> CreateDefaultMonotonicTimeProvider();
    TIntrusivePtr<IBootTimeProvider> CreateDefaultBootTimeProvider();

}

// TODO: remove, alias for compatibility
using IMonotonicTimeProvider = NMonotonic::IMonotonicTimeProvider;

// TODO: remove, alias for compatibility
using NMonotonic::CreateDefaultMonotonicTimeProvider;
