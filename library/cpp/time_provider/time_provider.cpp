#include "time_provider.h"

class TDefaultTimeProvider: public ITimeProvider {
public:
    TInstant Now() override {
        return TInstant::Now();
    }
};

class TDeterministicTimeProvider: public ITimeProvider {
public:
    TDeterministicTimeProvider(ui64 seed) {
        Value = TInstant::Seconds(seed);
    }

    TInstant Now() override {
        return Value;
    }

private:
    TInstant Value;
};

TIntrusivePtr<ITimeProvider> CreateDefaultTimeProvider() {
    return TIntrusivePtr<ITimeProvider>(new TDefaultTimeProvider());
}

TIntrusivePtr<ITimeProvider> CreateDeterministicTimeProvider(ui64 seed) {
    return TIntrusivePtr<ITimeProvider>(new TDeterministicTimeProvider(seed));
}
