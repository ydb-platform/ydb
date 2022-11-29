#include "monotonic.h"

namespace NActors {

ui64 GetMonotonicMicroSeconds() {
    return NMonotonic::GetMonotonicMicroSeconds();
}

} // namespace NActors
