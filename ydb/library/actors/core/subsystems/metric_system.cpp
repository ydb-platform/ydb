#include "metric_system.h"
#include "inmemory_metrics.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>

namespace NActors {
    IMetricSystem* GetMetricSystem(TActorSystem& system) {
        if (auto* metrics = system.GetSubSystem<IMetricSystem>()) {
            return metrics;
        }
        // Preserve existing setups registered under the concrete subsystem type.
        return GetInMemoryMetrics(system);
    }

    const IMetricSystem* GetMetricSystem(const TActorSystem& system) {
        if (const auto* metrics = system.GetSubSystem<IMetricSystem>()) {
            return metrics;
        }
        return GetInMemoryMetrics(system);
    }

    IMetricSystem* GetMetricSystem() {
        return TlsActivationContext ? GetMetricSystem(*TActivationContext::ActorSystem()) : nullptr;
    }
} // namespace NActors
