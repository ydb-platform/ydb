#pragma once

#include <util/datetime/base.h>
#include <library/cpp/time_provider/monotonic.h>

namespace NActors {

using NMonotonic::GetMonotonicMicroSeconds;

using TMonotonic = NMonotonic::TMonotonic;

} // namespace NActors
