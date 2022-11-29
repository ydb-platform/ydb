#include "monotonic_provider.h"

namespace {
TIntrusivePtr<IMonotonicTimeProvider> GlobalMonotonicTimeProvider;
}

void TMonotonicOperator::RegisterProvider(TIntrusivePtr<IMonotonicTimeProvider> provider) {
    GlobalMonotonicTimeProvider = provider;
}

NMonotonic::TMonotonic TMonotonicOperator::Now() {
    if (GlobalMonotonicTimeProvider) {
        return GlobalMonotonicTimeProvider->Now();
    } else {
        return TMonotonic::Now();
    }
}

namespace NMonotonic {

class TDefaultMonotonicTimeProvider: public IMonotonicTimeProvider {
public:
    TMonotonic Now() override {
        return TMonotonic::Now();
    }
};

TIntrusivePtr<IMonotonicTimeProvider> CreateDefaultMonotonicTimeProvider() {
    return TIntrusivePtr<IMonotonicTimeProvider>(new TDefaultMonotonicTimeProvider);
}

}
