#include "actor_monotonic_time_provider.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace {

    class TActorSystemMonotonicTimeProvider : public NMonotonic::IMonotonicTimeProvider {
    public:
        TMonotonic Now() override {
            return NActors::TActivationContext::Monotonic();
        }
    };

} // anonymous namespace

    TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> CreateActorSystemMonotonicTimeProvider() {
        if (!NActors::TlsActivationContext) {
            return NMonotonic::CreateDefaultMonotonicTimeProvider();
        }
        return MakeIntrusive<TActorSystemMonotonicTimeProvider>();
    }

} // namespace NKikimr
