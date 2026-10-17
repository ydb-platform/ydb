#pragma once

#include <ydb/library/actors/core/monotonic_provider.h>

namespace NKikimr {

    TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> CreateActorSystemMonotonicTimeProvider();

} // namespace NKikimr
