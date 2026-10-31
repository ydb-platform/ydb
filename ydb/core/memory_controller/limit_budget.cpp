#include "limit_budget.h"

#include <ranges>

namespace NKikimr::NMemory {

void TMemoryGrantLedger::Register(const TKey& key, ui64 initialLimit, ui64 multiplier) {
    Y_ABORT_UNLESS(multiplier > 0);
    auto& state = States[key];
    state.Multiplier = multiplier;
    state.Limit = Max(state.Limit, initialLimit);
}

TVector<ui64> TMemoryGrantLedger::PlanExplicitLimits(const TVector<TRequest>& requests, ui64 available) const {
    TVector<ui64> planned;
    planned.reserve(requests.size());
    ui64 headroom = available;
    bool overBudget = false;
    for (const auto& request : requests) {
        Y_ABORT_UNLESS(request.Multiplier > 0);
        const ui64 retained = request.Source == ETargetSource::ConsumerRequest
            ? Min(GetLimit(request.Key), request.TargetLimit) : request.TargetLimit;
        planned.push_back(retained);
        if (request.Source == ETargetSource::ConsumerRequest) {
            if (static_cast<unsigned __int128>(retained) * request.Multiplier > headroom) {
                overBudget = true;
                headroom = 0;
            } else {
                headroom -= retained * request.Multiplier;
            }
        }
    }
    for (size_t i = 0; i < requests.size() && !overBudget; ++i) {
        const auto& request = requests[i];
        if (request.Source == ETargetSource::ConsumerRequest) {
            const ui64 increase = request.TargetLimit - planned[i];
            if (static_cast<unsigned __int128>(increase) * request.Multiplier <= headroom) {
                headroom -= increase * request.Multiplier;
                planned[i] = request.TargetLimit;
            }
        }
    }
    return planned;
}

TMemoryGrantLedger::TTargetBudgets TMemoryGrantLedger::ComputeTargetBudgets(
    const TVector<TRequest>& requests, const TVector<ui64>& planned)
{
    TTargetBudgets budgets;
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (request.Source == ETargetSource::ConsumerRequest) {
            budgets.Requested += static_cast<unsigned __int128>(planned[i]) * request.Multiplier;
        } else {
            budgets.Automatic += static_cast<unsigned __int128>(request.TargetLimit) * request.Multiplier;
        }
    }
    return budgets;
}

void TMemoryGrantLedger::ScaleAutomaticTargets(TVector<TRequest>& requests, const TVector<ui64>& planned, ui64 available) {
    const auto [automaticBudget, requestedBudget] = ComputeTargetBudgets(requests, planned);
    const ui64 automaticAvailable = requestedBudget >= available ? 0 : available - static_cast<ui64>(requestedBudget);
    if (automaticBudget <= automaticAvailable) {
        return;
    }
    auto automaticRequests = requests | std::views::filter([](const TRequest& request) {
        return request.Source == ETargetSource::MemoryController;
    });
    for (auto& request : automaticRequests) {
        request.TargetLimit = static_cast<unsigned __int128>(request.TargetLimit) * automaticAvailable / automaticBudget;
    }
}

TVector<ui64> TMemoryGrantLedger::ApplyReductions(const TVector<TRequest>& requests, ui64 available) {
    // Send reductions without waiting for a reply. Reported usage above the
    // reduced limit remains charged until the consumer actually releases it.
    TVector<ui64> previousLimits;
    previousLimits.reserve(requests.size());
    for (const auto& request : requests) {
        auto& state = States[request.Key];
        previousLimits.push_back(state.Limit);
        state.Used = request.Used;
        state.Multiplier = request.Multiplier;
        if (request.TargetLimit < state.Limit) {
            state.Limit = request.TargetLimit;
        }
    }
    // Hard can fall below already issued limits. Reduce those permissions even
    // if a client's requested increase is still pending; keep its target intact.
    unsigned __int128 issued = 0;
    for (const auto& [key, state] : States) {
        issued += static_cast<unsigned __int128>(state.Limit) * state.Multiplier;
    }
    if (issued > available) {
        for (auto& [key, state] : States) {
            state.Limit = static_cast<unsigned __int128>(state.Limit) * available / issued;
        }
    }

    return previousLimits;
}

TVector<TMemoryGrantLedger::TGrant> TMemoryGrantLedger::Allocate(TVector<TRequest> requests, ui64 available) {
    // Waiting requests reserve their retained grant, not mutually exclusive targets.
    const auto planned = PlanExplicitLimits(requests, available);
    ScaleAutomaticTargets(requests, planned, available);
    const auto previousLimits = ApplyReductions(requests, available);

    ui64 headroom = available;
    bool overBudget = false;
    for (const auto& [key, state] : States) {
        // Widen caps before comparison so overflow cannot create headroom.
        if (state.Used > headroom || static_cast<unsigned __int128>(state.Limit) * state.Multiplier > headroom) {
            overBudget = true;
            headroom = 0;
        } else {
            headroom -= GetCharge(state);
        }
    }
    TVector<TGrant> result;
    result.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto& state = States.at(request.Key);
        bool shouldSend = true;
        if (request.TargetLimit > state.Limit) {
            // Do not send partial increases. Keep the previous limit until
            // reported consumption leaves room for the complete new grant.
            const bool targetFits = static_cast<unsigned __int128>(request.TargetLimit) * state.Multiplier <= available
                && (request.Source == ETargetSource::MemoryController || planned[i] == request.TargetLimit);
            const ui64 proposed = targetFits ? request.TargetLimit * state.Multiplier : 0;
            const ui64 increase = proposed - Min(proposed, GetCharge(state));
            if (!overBudget && targetFits && increase <= headroom) {
                state.Limit = request.TargetLimit;
                headroom -= increase;
            } else {
                // A necessary reduction is not an answer to the pending request.
                shouldSend = state.Limit < previousLimits[i];
            }
        }
        result.push_back({request.Key, state.Limit, shouldSend});
    }
    return result;
}

void TMemoryGrantLedger::Forget(const TKey& key) {
    States.erase(key);
}

ui64 TMemoryGrantLedger::GetLimit(const TKey& key) const {
    const auto* state = States.FindPtr(key);
    return state ? state->Limit : 0;
}

ui64 TMemoryGrantLedger::GetCommitted() const {
    ui64 total = 0;
    for (const auto& [key, state] : States) {
        total = SaturatingAdd(total, GetCharge(state));
    }
    return total;
}

ui64 TMemoryGrantLedger::GetUnusedReservation(EMemoryConsumerKind kind) const {
    ui64 total = 0;
    for (const auto& [key, state] : States) {
        if (key.first == kind) {
            total = SaturatingAdd(total, GetCharge(state) - state.Used);
        }
    }
    return total;
}

ui64 TMemoryGrantLedger::GetCharge(const TState& state) {
    return Max(state.Used, SaturatingMultiply(state.Limit, state.Multiplier));
}

} // namespace NKikimr::NMemory
