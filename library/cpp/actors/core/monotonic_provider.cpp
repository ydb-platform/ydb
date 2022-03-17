#include "monotonic_provider.h"

namespace NActors {

class TDefaultMonotonicTimeProvider : public IMonotonicTimeProvider {
public:
    TMonotonic Now() override {
        return TMonotonic::Now();
    }
};

TIntrusivePtr<IMonotonicTimeProvider> CreateDefaultMonotonicTimeProvider() {
    return TIntrusivePtr<IMonotonicTimeProvider>(new TDefaultMonotonicTimeProvider);
}

} // namespace NActors
