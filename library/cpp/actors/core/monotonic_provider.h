#pragma once

#include <library/cpp/time_provider/monotonic_provider.h>

namespace NActors {

using IMonotonicTimeProvider = NMonotonic::IMonotonicTimeProvider;
using IBootTimeProvider = NMonotonic::IBootTimeProvider;

using NMonotonic::CreateDefaultMonotonicTimeProvider;
using NMonotonic::CreateDefaultBootTimeProvider;

} // namespace NActors
