#pragma once

#include <library/cpp/time_provider/monotonic.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <map>
#include <optional>
#include <set>
#include <vector>

namespace NBridgeSkipper {

// Public wire values are retained, including unknown future enum values.
enum class EPileState : ui32 {
    Unspecified = 0,
    Primary = 1,
    Promoted = 2,
    Synchronized = 3,
    NotSynchronized = 4,
    Suspended = 5,
    Disconnected = 6,
    Inactive = 7,
};

using TNamedPileStates = std::map<TString, EPileState>;
using TExactUpdate = TNamedPileStates;

struct TPendingOperation {
    TString Kind;
    // Presence is distinct from the valid empty pile name.
    std::optional<TString> Pile;
    TString Error;

    bool operator==(const TPendingOperation&) const = default;
};

struct TClusterView {
    ui64 Generation = 0;
    TNamedPileStates Piles;
    std::optional<TPendingOperation> Pending;
    bool ConversionInProgress = false;
    TString Endpoint;
    TMonotonic SentAt;
};

struct TPileConnectivity {
    ui32 StaticNodes = 0;
    ui32 ConnectedStaticNodes = 0;
    bool HasNodeQuorum = false;
    bool InConfigQuorum = false;
    bool InDataQuorum = false;
};

struct TConnectivity {
    TString VantagePile;
    ui32 ReporterNodeId = 0;
    bool ReporterIsRoot = false;
    bool BindingInProgress = false;
    ui64 Generation = 0;
    TMonotonic SentAt;
    std::map<TString, TPileConnectivity> Piles;
};

enum class EPileVerdict { Healthy, Failed, Lost, Replicating, Unknown };

struct TPileVerdict {
    EPileVerdict Verdict = EPileVerdict::Unknown;
    std::vector<TString> Reasons;
    // A fresh fallback-mode reply attributes a group part (RED, or GREY "no data") to
    // the pile. Never Down evidence; it vetoes promotion on connectivity alone.
    bool FallbackRed = false;
    bool SelfQuorum = false;
    TMonotonic NewestEvidenceSentAt;
};

enum class EBlockerReason {
    NoView,
    ViewConflict,
    StaleHistory,
    PendingMutation,
    NoCandidate,
    SuspendedPileLost,
    NoMutationEndpoint,
    StateWriteFailed,
};

struct TPlan {
    ui64 ExpectedGeneration = 0;
    TExactUpdate Update;
    TString Endpoint;

    bool operator==(const TPlan&) const = default;
};

enum class EDecisionKind { Stable, Unknown, Confirming, Blocked, Ready };

struct TDecision {
    EDecisionKind Kind = EDecisionKind::Unknown;
    TString Reason;
    std::optional<EBlockerReason> Blocker;
    std::optional<TPlan> Plan;
    TString CliCommand;
};

enum class EMutationOutcome { Success, GenerationMoved, NotAppliedHere, Rejected, Ambiguous };

struct TPendingMutation {
    ui64 ExpectedGeneration = 0;
    TExactUpdate Update;

    bool operator==(const TPendingMutation&) const = default;
};

struct TPersistentState {
    TString ClusterId;
    ui64 GenerationHighWater = 0;
    std::optional<TPendingMutation> PendingMutation;

    bool operator==(const TPersistentState&) const = default;
};

// A SelfCheck reply without a pile name leaves the identity unchanged.
enum class EEndpointIdentity { Unknown, Verified, Mismatch };

// Latest observation of a configured endpoint; no SDK objects cross this boundary.
// Current means the endpoint's fresh reply is at the selected generation.
struct TEndpointState {
    TString Endpoint;
    TString Pile;
    EEndpointIdentity Identity = EEndpointIdentity::Unknown;
    bool Current = false;
};

bool IsKnownState(EPileState state);
bool IsSynchronized(EPileState state);
TString ValidateClusterView(const TClusterView& view);
// The shape of a failover update: it names no SUSPENDED or INACTIVE pile and,
// after the overlay, leaves exactly one PRIMARY and at most one PROMOTED, none
// beside a new PRIMARY. The server enforces the full rules.
bool IsFailoverShape(const TClusterView& view, const TExactUpdate& update);
TString ToString(EPileState state);
TString ToString(EPileVerdict verdict);
TString ToString(EBlockerReason reason);
TString ToString(EDecisionKind kind);
TString ToString(EMutationOutcome outcome);
TString ToString(EEndpointIdentity identity);

} // namespace NBridgeSkipper
