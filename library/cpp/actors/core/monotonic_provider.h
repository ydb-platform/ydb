#pragma once

#include <library/cpp/time_provider/monotonic_provider.h>

namespace NActors {

using IMonotonicTimeProvider = ::IMonotonicTimeProvider;

TIntrusivePtr<IMonotonicTimeProvider> CreateDefaultMonotonicTimeProvider();

} // namespace NActors
