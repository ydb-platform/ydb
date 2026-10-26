#include "mutation.h"
#include "health.h"

#include <algorithm>

namespace NBridgeSkipper {

bool MayResend(
    std::optional<TMonotonic> lastSendAt,
    std::optional<EMutationOutcome> lastOutcome,
    TMonotonic now,
    TDuration mutationClientTimeout,
    ui64 ambiguousResends)
{
    if (!lastSendAt) {
        return true; // One immediate resend after a restart.
    }
    if (!lastOutcome) {
        return false; // The invocation is still outstanding.
    }
    switch (*lastOutcome) {
        case EMutationOutcome::NotAppliedHere:
            return true; // The actor excludes the receiving endpoint on this retry.
        case EMutationOutcome::Ambiguous: {
            // Doubles after each ambiguous outcome, capped at five minutes (B11).
            const ui64 factor = ui64{1} << std::min<ui64>(ambiguousResends, 20);
            const auto backoff = std::min(mutationClientTimeout * factor, TDuration::Minutes(5));
            return now - *lastSendAt >= backoff;
        }
        case EMutationOutcome::Success:
        case EMutationOutcome::GenerationMoved:
        case EMutationOutcome::Rejected:
            return false;
    }
    return false;
}

bool ResolvedBy(const TPendingMutation& pending, const TClusterView& view) {
    return view.Generation > pending.ExpectedGeneration;
}

TPersistentState ApplyOutcome(const TPersistentState& state, EMutationOutcome outcome) {
    auto result = state;
    if (result.PendingMutation) {
        switch (outcome) {
            case EMutationOutcome::Success:
            case EMutationOutcome::GenerationMoved:
                // ABORTED also comes from a distconf root that lags behind G. Raising the mark then is
                // fail-safe: views at G are STALE_HISTORY until a fresh view reaches G+1, and nothing
                // acts on a generation the controller may have moved.
                result.GenerationHighWater = std::max(result.GenerationHighWater,
                    result.PendingMutation->ExpectedGeneration + 1);
                result.PendingMutation.reset();
                break;
            case EMutationOutcome::Rejected:
                // Only this invocation was refused before distconf. Earlier
                // ambiguous or superseded attempts may still commit, including
                // after restart, so retain the same-generation overlay lock.
            case EMutationOutcome::NotAppliedHere:
            case EMutationOutcome::Ambiguous:
                break;
        }
    }
    return result;
}

bool SeesSurvivors(
    const std::vector<TConnectivityReply>& valid,
    const TString& endpoint,
    const TString& vantagePile,
    const std::set<TString>& survivors)
{
    return std::any_of(valid.begin(), valid.end(), [&](const TConnectivityReply& reply) {
        const auto& report = reply.Report;
        return reply.Endpoint == endpoint && report.VantagePile == vantagePile
            && std::all_of(survivors.begin(), survivors.end(), [&](const TString& pile) {
                const auto item = report.Piles.find(pile);
                return item != report.Piles.end() && item->second.HasNodeQuorum;
            });
    });
}

std::optional<TString> SelectMutationEndpoint(
    const TClusterView& view,
    const std::vector<TEndpointState>& endpoints,
    const TExactUpdate& update,
    const std::vector<TConnectivityReply>& validConnectivity,
    const std::set<TString>& exclude)
{
    auto finalStates = view.Piles;
    for (const auto& [name, state] : update) {
        finalStates[name] = state;
    }
    // Config-quorum piles after the update, ranked for dispatch (lower first).
    // CMS is about to disconnect a SUSPENDED pile and stop its listener.
    std::map<TString, int> ranks;
    std::set<TString> survivors;
    for (const auto& [name, state] : finalStates) {
        std::optional<int> rank;
        if (state == EPileState::Primary) {
            rank = 0;
        } else if (IsSynchronized(state)) {
            rank = 1;
        } else if (state == EPileState::NotSynchronized && !update.contains(name)) {
            // An explicit NOT_SYNCHRONIZED overlay resets the pile to stage 1.
            if (IsParticipating(name, state, validConnectivity).value_or(false)) {
                rank = 2;
            }
        } else if (state == EPileState::Suspended) {
            rank = 3;
        }
        if (rank) {
            ranks[name] = *rank;
            survivors.insert(name);
        }
    }
    const TEndpointState* selected = nullptr;
    int selectedRank = 0;
    for (const auto& endpoint : endpoints) {
        const auto rank = ranks.find(endpoint.Pile);
        if (endpoint.Identity != EEndpointIdentity::Verified || !endpoint.Current || exclude.contains(endpoint.Endpoint)
            || rank == ranks.end() || !SeesSurvivors(validConnectivity, endpoint.Endpoint, endpoint.Pile, survivors)) {
            continue;
        }
        if (!selected || rank->second < selectedRank
            || (rank->second == selectedRank && endpoint.Endpoint < selected->Endpoint)) {
            selected = &endpoint;
            selectedRank = rank->second;
        }
    }
    if (selected) {
        return selected->Endpoint;
    }
    return {};
}

} // namespace NBridgeSkipper
