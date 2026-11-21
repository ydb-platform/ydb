#include "limit_budget.h"

#include <algorithm>
#include <ranges>

namespace NKikimr::NMemory {

void TMemoryGrantLedger::Register(const TKey& key, ui64 initialLimit, ui64 multiplier) {
    Y_ABORT_UNLESS(multiplier > 0);
    auto& state = States[key];
    state.Multiplier = multiplier;
    state.Limit = Max(state.Limit, initialLimit);
}

TVector<size_t> TMemoryGrantLedger::GetRequestOrder(const TVector<TRequest>& requests) {
    TVector<size_t> order;
    order.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        order.push_back(i);
    }
    std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        if (requests[a].Source != requests[b].Source) {
            return requests[a].Source == ETargetSource::ConsumerRequest;
        }
        return requests[a].Source == ETargetSource::ConsumerRequest
            && requests[a].RequestOrder < requests[b].RequestOrder;
    });
    return order;
}

TMemoryGrantLedger::TExplicitPlan TMemoryGrantLedger::PlanExplicitLimits(
    const TVector<TRequest>& requests, ui64 available, ui64 automaticMinimum) const
{
    TExplicitPlan planned;
    unsigned __int128 required = automaticMinimum;
    for (const auto& request : requests) {
        Y_ABORT_UNLESS(request.Multiplier > 0);
        if (request.Source == ETargetSource::ConsumerRequest) {
            const ui64 retained = Min(GetLimit(request.Key), request.TargetLimit);
            planned.emplace(request.Key, retained);
            required += static_cast<unsigned __int128>(retained) * request.Multiplier;
        }
    }
    if (required > available) {
        // Existing permissions and minima no longer fit; reduce them proportionally.
        for (auto& [key, limit] : planned) {
            limit = static_cast<unsigned __int128>(limit) * available / required;
        }
        return planned;
    }
    ui64 headroom = available - static_cast<ui64>(required);
    for (const size_t i : GetRequestOrder(requests)) {
        const auto& request = requests[i];
        if (request.Source != ETargetSource::ConsumerRequest) {
            continue;
        }
        auto& limit = planned.at(request.Key);
        const auto target = static_cast<unsigned __int128>(request.TargetLimit) * request.Multiplier;
        if (target > available - automaticMinimum) {
            continue; // An impossible target must not block feasible requests.
        }
        const auto increase = static_cast<unsigned __int128>(request.TargetLimit - limit) * request.Multiplier;
        if (increase > headroom) {
            break; // Younger requests must not consume the oldest feasible waiter's headroom.
        }
        headroom -= static_cast<ui64>(increase);
        limit = request.TargetLimit;
    }
    return planned;
}

TMemoryGrantLedger::TTargetBudgets TMemoryGrantLedger::ComputeTargetBudgets(
    const TVector<TRequest>& requests, const TExplicitPlan& planned)
{
    TTargetBudgets budgets;
    for (const auto& request : requests) {
        if (request.Source == ETargetSource::ConsumerRequest) {
            budgets.Requested += static_cast<unsigned __int128>(planned.at(request.Key)) * request.Multiplier;
        } else {
            budgets.Automatic += static_cast<unsigned __int128>(request.TargetLimit) * request.Multiplier;
        }
    }
    return budgets;
}

void TMemoryGrantLedger::ScaleAutomaticTargets(TVector<TRequest>& requests, const TExplicitPlan& planned, ui64 available) {
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

TVector<ui64> TMemoryGrantLedger::ApplyReductions(
    const TVector<TRequest>& requests, ui64 available, const TExplicitPlan& planned) {
    // Send reductions without waiting for a reply. Reported usage above the
    // reduced limit remains charged until the consumer actually releases it.
    TVector<ui64> previousLimits;
    previousLimits.reserve(requests.size());
    for (const auto& request : requests) {
        auto& state = States[request.Key];
        previousLimits.push_back(state.Limit);
        state.Used = request.Used;
        state.Multiplier = request.Multiplier;
        const ui64 target = request.Source == ETargetSource::ConsumerRequest
            ? Min(request.TargetLimit, planned.at(request.Key)) : request.TargetLimit;
        state.Limit = Min(state.Limit, target);
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

TVector<TMemoryGrantLedger::TGrant> TMemoryGrantLedger::Allocate(
    TVector<TRequest> requests, ui64 available, const TExplicitPlan& planned) {
    ScaleAutomaticTargets(requests, planned, available);
    const auto previousLimits = ApplyReductions(requests, available, planned);

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
    TVector<TGrant> result(requests.size());
    bool waitingForHeadroom = false;
    for (const size_t i : GetRequestOrder(requests)) {
        const auto& request = requests[i];
        auto& state = States.at(request.Key);
        bool shouldSend = true;
        if (request.TargetLimit > state.Limit) {
            // Do not send partial increases. Keep the previous limit until
            // reported consumption leaves room for the complete new grant.
            const bool targetFits = static_cast<unsigned __int128>(request.TargetLimit) * state.Multiplier <= available
                && (request.Source == ETargetSource::MemoryController || planned.at(request.Key) == request.TargetLimit);
            const ui64 proposed = targetFits ? request.TargetLimit * state.Multiplier : 0;
            const ui64 increase = proposed - Min(proposed, GetCharge(state));
            if (!waitingForHeadroom && !overBudget && targetFits && increase <= headroom) {
                state.Limit = request.TargetLimit;
                headroom -= increase;
            } else {
                // A necessary reduction is not an answer to the pending request.
                shouldSend = state.Limit < previousLimits[i];
                waitingForHeadroom |= request.Source == ETargetSource::ConsumerRequest && targetFits;
            }
        }
        result[i] = {request.Key, state.Limit, shouldSend};
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
