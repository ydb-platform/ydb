#pragma once

#include "types.h"

namespace Ydb::Monitoring {
class SelfCheckResult;
}

namespace NBridgeSkipper {

enum class ESelfCheckMode { Bsc, Fallback };
// Incomplete: a reason of a RED group is missing from the reply (truncation).
enum class EPart { RedPart, ReplicatingPart, FallbackRed, Incomplete };

constexpr size_t MaxSelfCheckNotes = 16;

// A pure function of one reply; attribution against a view happens in EvaluatePiles.
struct TSelfCheckClassification {
    ESelfCheckMode Mode = ESelfCheckMode::Bsc;
    bool Truncated = false;
    bool SizeWarning = false;
    TString SelfCheckResult;
    // Keyed by every pile name in the reply, including names absent from the view.
    // RedPart dominates Incomplete, which dominates ReplicatingPart.
    std::map<TString, EPart> Parts;
    // RED group issues without a pile location.
    ui64 UnnamedRed = 0;
    // "<TYPE> <STATUS> pile=<name>: <message>" for PILE_*, BRIDGE_PILE and
    // non-fallback STORAGE issues; " pile=<name>" only when the issue names a pile.
    std::vector<TString> Notes;
    ui64 OmittedNotes = 0;
};

TSelfCheckClassification ClassifySelfCheck(const Ydb::Monitoring::SelfCheckResult& result);

struct TSelfCheckReply {
    TString Endpoint;
    // Missing location must not verify the valid empty pile name.
    std::optional<TString> ResponderPile;
    TMonotonic SentAt;
    TSelfCheckClassification Classification;
};

struct TConnectivityReply {
    TString Endpoint;
    TConnectivity Report;
};

struct THealthReplies {
    std::vector<TEndpointState> Endpoints;
    std::vector<TSelfCheckReply> SelfChecks;
    std::vector<TConnectivityReply> Connectivity;
};

struct THealthParams {
    TDuration EvidenceTtl = TDuration::Seconds(30);
    TDuration BscWarmup = TDuration::Seconds(30);
};

// Process-local state. A cleanliness window starts empty at startup and resets
// on a PRIMARY or PROMOTED change, a non-clean evaluation, a gap of more than one
// TTL between consecutive clean replies, a fresh fallback reply from an endpoint
// of the BSC-host pile (PROMOTED, else PRIMARY) while it is fresh, and an
// endpoint returning from fallback to BSC mode; an evaluation with no fresh
// SelfCheck reply keeps it.
struct THealthState {
    // A run of clean BSC evaluations, measured on the send times of its replies.
    struct TCleanWindow {
        TMonotonic Since;
        TMonotonic NewestSentAt;
    };

    std::optional<TString> Primary;
    std::optional<TString> Promoted;
    // Observed role changes invalidate older sends as warm-up support. Initial
    // role establishment still permits an in-flight startup reply's send time.
    std::optional<TMonotonic> RolesChangedAt;
    std::map<TString, TCleanWindow> CleanSince;
    // The mode of each endpoint's latest fresh reply.
    std::map<TString, ESelfCheckMode> EndpointModes;
};

struct THealthEvaluation {
    std::map<TString, TPileVerdict> Piles;
    std::set<TString> Participating;
    bool BscAvailable = false;
    // Current reports, binding included, retaining the queried URI for routing.
    std::vector<TConnectivityReply> Connectivity;
    // Earliest inclusive expiry of admitted health/connectivity observations.
    TMonotonic ValidUntil = TMonotonic::Max();
};

// Current: a fresh report at the view's generation from a valid endpoint. A report
// sent while the root is binding counts for positive claims (self quorum,
// participation, survivor visibility); the Lost verdict uses settled reports only.
bool IsConnectivityValid(
    const TClusterView& view,
    const TEndpointState& endpoint,
    const TConnectivity& report,
    TMonotonic now,
    const THealthParams& params);

// Base public-state/connectivity participation, shared by health and endpoint
// ranking. Health assessment additionally uses BSC parts and treats unknown
// stage conservatively. std::nullopt: a NOT_SYNCHRONIZED pile no valid report lists.
std::optional<bool> IsParticipating(
    const TString& pile,
    EPileState state,
    const std::vector<TConnectivityReply>& validReports);

THealthEvaluation EvaluatePiles(
    const TClusterView& view,
    const THealthReplies& replies,
    TMonotonic now,
    const THealthParams& params,
    THealthState& state);

} // namespace NBridgeSkipper
