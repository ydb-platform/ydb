#pragma once

#include "health.h"

namespace NBridgeSkipper {

// The caller has already checked that the plan is the pending update (D7 in Decide).
bool MayResend(
    std::optional<TMonotonic> lastSendAt,
    std::optional<EMutationOutcome> lastOutcome,
    TMonotonic now,
    TDuration mutationClientTimeout,
    ui64 ambiguousResends);

bool ResolvedBy(const TPendingMutation& pending, const TClusterView& view);

// Call only for the matching pending invocation; persist the returned state
// before allowing a later mutation.
TPersistentState ApplyOutcome(const TPersistentState& state, EMutationOutcome outcome);

// True when a current report queried through endpoint in vantagePile shows node
// quorum for every survivor. A same-pile endpoint cannot lend its component.
bool SeesSurvivors(
    const std::vector<TConnectivityReply>& valid,
    const TString& endpoint,
    const TString& vantagePile,
    const std::set<TString>& survivors);

// Admits only Verified, Current endpoints in a pile that remains in the config
// quorum after the update and that SeesSurvivors for every such pile.
std::optional<TString> SelectMutationEndpoint(
    const TClusterView& view,
    const std::vector<TEndpointState>& endpoints,
    const TExactUpdate& update,
    const std::vector<TConnectivityReply>& validConnectivity,
    const std::set<TString>& exclude = {});

} // namespace NBridgeSkipper
