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
        ui64 RequestOrder = 0;
    };

    struct TGrant {
        TKey Key;
        ui64 Bytes = 0;
        bool ShouldSend = true;
    };

    using TExplicitPlan = THashMap<TKey, ui64>;

    void Register(const TKey& key, ui64 initialLimit, ui64 multiplier);
    // Protect automatic minima, then plan explicit increases in arrival order.
    TExplicitPlan PlanExplicitLimits(const TVector<TRequest>& requests, ui64 available, ui64 automaticMinimum = 0) const;
    TVector<TGrant> Allocate(TVector<TRequest> requests, ui64 available, const TExplicitPlan& planned);
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

    static TVector<size_t> GetRequestOrder(const TVector<TRequest>& requests);
    static TTargetBudgets ComputeTargetBudgets(const TVector<TRequest>& requests, const TExplicitPlan& planned);
    static void ScaleAutomaticTargets(TVector<TRequest>& requests, const TExplicitPlan& planned, ui64 available);
    TVector<ui64> ApplyReductions(const TVector<TRequest>& requests, ui64 available, const TExplicitPlan& planned);
    static ui64 GetCharge(const TState& state);

    THashMap<TKey, TState> States;
};

} // namespace NKikimr::NMemory
