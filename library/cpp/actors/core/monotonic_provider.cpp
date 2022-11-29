#include "monotonic_provider.h"

namespace NActors {

TIntrusivePtr<NActors::IMonotonicTimeProvider> CreateDefaultMonotonicTimeProvider() {
    return NMonotonic::CreateDefaultMonotonicTimeProvider();
}

} // namespace NActors
