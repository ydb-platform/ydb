#pragma once

#include <ydb/core/base/memory_controller_iface.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <limits>

namespace NKikimr::NMemory {

inline ui64 SaturatingAdd(ui64 a, ui64 b) {
    return a + Min(b, std::numeric_limits<ui64>::max() - a);
}

inline ui64 SaturatingMultiply(ui64 bytes, ui64 multiplier) {
    const ui64 max = std::numeric_limits<ui64>::max();
    return multiplier && bytes > max / multiplier ? max : bytes * multiplier;
}

// Reserve each latest limit and any reported usage above it.
// Increases wait until the complete requested limit fits the reported budget.
class TMemoryGrantLedger {
public:
    using TKey = std::pair<EMemoryConsumerKind, TActorId>;

    enum class ETargetSource {
        MemoryController,
        ConsumerRequest,
    };

    struct TRequest {
        TKey Key;
        ui64 Used = 0;
        ui64 TargetLimit = 0;
        ui64 Multiplier = 1;
        // Client requests must be granted in full. Only default targets scale.
        ETargetSource Source = ETargetSource::MemoryController;
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

    struct TTargetBudgets {
        unsigned __int128 Automatic = 0;
        unsigned __int128 Requested = 0;
    };

    static TTargetBudgets ComputeTargetBudgets(const TVector<TRequest>& requests, const TVector<ui64>& planned);
    static void ScaleAutomaticTargets(TVector<TRequest>& requests, const TVector<ui64>& planned, ui64 available);
    TVector<ui64> ApplyReductions(const TVector<TRequest>& requests, ui64 available);
    static ui64 GetCharge(const TState& state);

    THashMap<TKey, TState> States;
};

} // namespace NKikimr::NMemory
