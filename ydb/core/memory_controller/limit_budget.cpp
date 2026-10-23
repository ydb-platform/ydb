#include "limit_budget.h"

#include <limits>

namespace NKikimr::NMemory {

namespace {

ui64 SaturatingMultiply(ui64 bytes, ui64 multiplier) {
    const ui64 max = std::numeric_limits<ui64>::max();
    return bytes > max / multiplier ? max : bytes * multiplier;
}

} // namespace

void TMemoryLimitBudget::Register(const TKey& key, ui64 initialLimit, ui64 multiplier) {
    Y_ABORT_UNLESS(multiplier > 0);
    auto& state = States[key];
    state.Multiplier = multiplier;
    state.Limit = Max(state.Limit, initialLimit);
}

TVector<ui64> TMemoryLimitBudget::PlanExplicitLimits(const TVector<TRequest>& requests, ui64 available) const {
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
            if (retained > headroom / request.Multiplier) {
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
            if (increase <= headroom / request.Multiplier) {
                headroom -= increase * request.Multiplier;
                planned[i] = request.TargetLimit;
            }
        }
    }
    return planned;
}

TVector<TMemoryLimitBudget::TGrant> TMemoryLimitBudget::Allocate(TVector<TRequest> requests, ui64 available) {
    // Waiting requests reserve their retained grant, not mutually exclusive targets.
    // Widen products before proportional division to avoid overflow.
    const auto planned = PlanExplicitLimits(requests, available);
    unsigned __int128 controllerBudget = 0;
    unsigned __int128 consumerBudget = 0;
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (request.Source == ETargetSource::ConsumerRequest) {
            consumerBudget += static_cast<unsigned __int128>(planned[i]) * request.Multiplier;
        } else {
            controllerBudget += static_cast<unsigned __int128>(request.TargetLimit) * request.Multiplier;
        }
    }
    const ui64 defaultAvailable = consumerBudget >= available ? 0 : available - static_cast<ui64>(consumerBudget);
    if (controllerBudget > defaultAvailable) {
        for (auto& request : requests) {
            if (request.Source == ETargetSource::MemoryController) {
                request.TargetLimit = static_cast<unsigned __int128>(request.TargetLimit) * defaultAvailable / controllerBudget;
            }
        }
    }
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

    ui64 headroom = available;
    bool overBudget = false;
    for (const auto& [key, state] : States) {
        // Compare before multiplying/subtracting, so even invalid large reports
        // or startup caps cannot wrap around and create permission to grow.
        if (state.Used > headroom || state.Limit > headroom / state.Multiplier) {
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
            const bool targetFits = request.TargetLimit <= available / state.Multiplier
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

void TMemoryLimitBudget::Forget(const TKey& key) {
    States.erase(key);
}

ui64 TMemoryLimitBudget::GetLimit(const TKey& key) const {
    const auto* state = States.FindPtr(key);
    return state ? state->Limit : 0;
}

ui64 TMemoryLimitBudget::GetCommitted() const {
    ui64 total = 0;
    for (const auto& [key, state] : States) {
        total += Min(GetCharge(state), std::numeric_limits<ui64>::max() - total);
    }
    return total;
}

ui64 TMemoryLimitBudget::GetUnusedReservation(EMemoryConsumerKind kind) const {
    ui64 total = 0;
    for (const auto& [key, state] : States) {
        if (key.first == kind) {
            total += Min(GetCharge(state) - state.Used, std::numeric_limits<ui64>::max() - total);
        }
    }
    return total;
}

ui64 TMemoryLimitBudget::GetCharge(const TState& state) {
    return Max(state.Used, SaturatingMultiply(state.Limit, state.Multiplier));
}

} // namespace NKikimr::NMemory
