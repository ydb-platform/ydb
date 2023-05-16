#pragma once

#include <util/datetime/base.h>
#include <library/cpp/time_provider/monotonic.h>

namespace NActors {

using NMonotonic::GetMonotonicMicroSeconds;
using NMonotonic::GetBootTimeMicroSeconds;

using TMonotonic = NMonotonic::TMonotonic;
using TBootTime = NMonotonic::TBootTime;

} // namespace NActors
