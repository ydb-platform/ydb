#pragma once

#include <library/cpp/time_provider/monotonic_provider.h>

namespace NActors {

using IMonotonicTimeProvider = NMonotonic::IMonotonicTimeProvider;

using NMonotonic::CreateDefaultMonotonicTimeProvider;

} // namespace NActors
