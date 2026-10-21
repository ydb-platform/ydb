#pragma once

#include <ydb/core/base/memory_controller_iface.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NKikimr::NMemory {

// Reserve each latest limit and any reported usage above it.
// Increases wait until the complete requested limit fits the reported budget.
class TMemoryLimitBudget {
public:
    using TKey = std::pair<EMemoryConsumerKind, TActorId>;

    struct TRequest {
        TKey Key;
        ui64 Used = 0;
        ui64 Desired = 0;
        ui64 Multiplier = 1;
        // Client requests must be granted in full. Only default targets scale.
        bool ExplicitDemand = false;
    };

    struct TGrant {
        TKey Key;
        ui64 Bytes = 0;
        bool ShouldSend = true;
    };

    void Register(const TKey& key, ui64 initialLimit, ui64 multiplier);
    // Retain explicit grants, then admit complete increases in request order.
    // Defaults are returned unchanged; callers share the remaining budget.
    TVector<ui64> PlanExplicitLimits(const TVector<TRequest>& requests, ui64 available) const;
    TVector<TGrant> Allocate(TVector<TRequest> requests, ui64 available);
    void Forget(const TKey& key);

    ui64 GetCommitted() const;
    ui64 GetLimit(const TKey& key) const;
    ui64 GetUnusedReservation(EMemoryConsumerKind kind) const;

private:
    struct TState {
        ui64 Used = 0;
        ui64 Multiplier = 1;
        ui64 Limit = 0;
    };

    static ui64 GetCharge(const TState& state);

    TMap<TKey, TState> States;
};

} // namespace NKikimr::NMemory
