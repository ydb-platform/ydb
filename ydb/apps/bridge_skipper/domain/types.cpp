#include "types.h"

#include <util/string/builder.h>

namespace NBridgeSkipper {

TString ToString(EMode mode) {
    switch (mode) {
        case EMode::Observe: return "observe";
        case EMode::Manual: return "manual";
        case EMode::Auto: return "auto";
    }
    return "observe";
}

TString ToString(EOperationKind kind) {
    switch (kind) {
        case EOperationKind::Failover: return "failover";
        case EOperationKind::Switchover: return "switchover";
        case EOperationKind::TakeOffline: return "take_offline";
        case EOperationKind::Rejoin: return "rejoin";
    }
    return "unknown";
}

TString ToString(EDispatchOrigin origin) {
    return origin == EDispatchOrigin::Manual ? "manual" : "auto";
}

namespace {

TString Unknown(ui32 value) {
    return TStringBuilder() << "UNKNOWN(" << value << ')';
}

} // namespace

bool IsKnownState(EPileState state) {
    return state >= EPileState::Primary && state <= EPileState::Inactive;
}

bool IsSynchronized(EPileState state) {
    return state == EPileState::Primary || state == EPileState::Promoted || state == EPileState::Synchronized;
}

TString ValidateClusterView(const TClusterView& view) {
    size_t primaries = 0;
    size_t promoted = 0;
    for (const auto& [name, state] : view.Piles) {
        (void)name;
        if (!IsKnownState(state)) {
            return "UNSUPPORTED_PUBLIC_STATE";
        }
        primaries += state == EPileState::Primary;
        promoted += state == EPileState::Promoted;
    }
    if (primaries != 1 || promoted > 1) {
        return "INVALID_PUBLIC_ROLES";
    }
    return {};
}

bool IsFailoverShape(const TClusterView& view, const TExactUpdate& update) {
    auto result = view.Piles;
    bool newPrimary = false;
    for (const auto& [name, state] : update) {
        const auto old = view.Piles.find(name);
        if (old == view.Piles.end() || old->second == EPileState::Suspended || old->second == EPileState::Inactive) {
            return false;
        }
        newPrimary = newPrimary || (state == EPileState::Primary && old->second != EPileState::Primary);
        result[name] = state;
    }
    size_t primaries = 0;
    size_t promoted = 0;
    for (const auto& [name, state] : result) {
        (void)name;
        primaries += state == EPileState::Primary;
        promoted += state == EPileState::Promoted;
    }
    return primaries == 1 && promoted <= (newPrimary ? 0 : 1);
}

TString ToString(EPileState state) {
    switch (state) {
        case EPileState::Unspecified: return "UNSPECIFIED";
        case EPileState::Primary: return "PRIMARY";
        case EPileState::Promoted: return "PROMOTED";
        case EPileState::Synchronized: return "SYNCHRONIZED";
        case EPileState::NotSynchronized: return "NOT_SYNCHRONIZED";
        case EPileState::Suspended: return "SUSPENDED";
        case EPileState::Disconnected: return "DISCONNECTED";
        case EPileState::Inactive: return "INACTIVE";
    }
    return Unknown(static_cast<ui32>(state));
}

TString ToString(EPileVerdict verdict) {
    switch (verdict) {
        case EPileVerdict::Healthy: return "HEALTHY";
        case EPileVerdict::Failed: return "FAILED";
        case EPileVerdict::Lost: return "LOST";
        case EPileVerdict::Replicating: return "REPLICATING";
        case EPileVerdict::Unknown: return "UNKNOWN";
    }
    return Unknown(static_cast<ui32>(verdict));
}

TString ToString(EBlockerReason reason) {
    switch (reason) {
        case EBlockerReason::NoView: return "NO_VIEW";
        case EBlockerReason::ViewConflict: return "VIEW_CONFLICT";
        case EBlockerReason::StaleHistory: return "STALE_HISTORY";
        case EBlockerReason::PendingMutation: return "PENDING_MUTATION";
        case EBlockerReason::NoCandidate: return "NO_CANDIDATE";
        case EBlockerReason::SuspendedPileLost: return "SUSPENDED_PILE_LOST";
        case EBlockerReason::NoMutationEndpoint: return "NO_MUTATION_ENDPOINT";
        case EBlockerReason::StateWriteFailed: return "STATE_WRITE_FAILED";
    }
    return Unknown(static_cast<ui32>(reason));
}

TString ToString(EDecisionKind kind) {
    switch (kind) {
        case EDecisionKind::Stable: return "STABLE";
        case EDecisionKind::Unknown: return "UNKNOWN";
        case EDecisionKind::Confirming: return "CONFIRMING";
        case EDecisionKind::Blocked: return "BLOCKED";
        case EDecisionKind::Ready: return "READY";
    }
    return Unknown(static_cast<ui32>(kind));
}

TString ToString(EMutationOutcome outcome) {
    switch (outcome) {
        case EMutationOutcome::Success: return "SUCCESS";
        case EMutationOutcome::GenerationMoved: return "GENERATION_MOVED";
        case EMutationOutcome::NotAppliedHere: return "NOT_APPLIED_HERE";
        case EMutationOutcome::Rejected: return "REJECTED";
        case EMutationOutcome::Ambiguous: return "AMBIGUOUS";
    }
    return Unknown(static_cast<ui32>(outcome));
}

TString ToString(EEndpointIdentity identity) {
    switch (identity) {
        case EEndpointIdentity::Unknown: return "UNKNOWN";
        case EEndpointIdentity::Verified: return "VERIFIED";
        case EEndpointIdentity::Mismatch: return "MISMATCH";
    }
    return Unknown(static_cast<ui32>(identity));
}

} // namespace NBridgeSkipper
