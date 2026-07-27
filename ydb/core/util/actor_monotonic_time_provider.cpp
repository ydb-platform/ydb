#include "actor_monotonic_time_provider.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

    namespace {
        class TActorSystemMonotonicTimeProvider: public NMonotonic::IMonotonicTimeProvider {
        public:
            TMonotonic Now() override {
                return NActors::TActivationContext::Monotonic();
            }
        };
    }

    TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> CreateActorSystemMonotonicTimeProvider() {
        return MakeIntrusive<TActorSystemMonotonicTimeProvider>();
    }

} // namespace NKikimr
