#pragma once

#include <util/datetime/base.h>

class ITimeProvider: public TThrRefBase {
public:
    virtual TInstant Now() = 0;
};

class TInstantOperator {
    static void RegisterProvider(TIntrusivePtr<ITimeProvider> provider);
    static TInstant Now();
};

TIntrusivePtr<ITimeProvider> CreateDefaultTimeProvider();
TIntrusivePtr<ITimeProvider> CreateDeterministicTimeProvider(ui64 seed);
