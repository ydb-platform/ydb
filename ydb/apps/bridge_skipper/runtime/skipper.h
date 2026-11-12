#pragma once

#include "state_file.h"
#include "diagnostics.h"
#include "ydb_clients.h"

#include <ydb/apps/bridge_skipper/domain/health.h>
#include <ydb/apps/bridge_skipper/domain/policy.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <library/cpp/json/json_value.h>

#include <mutex>
#include <atomic>

namespace NBridgeSkipper {

constexpr int SkipperLogComponent = NActorsServices::EServiceCommon_MAX + 1;

struct TEndpointSnapshot {
    TString Uri;
    TString Pile;
    EEndpointIdentity Identity = EEndpointIdentity::Unknown;
    // The pile is absent, inactive or disconnected in the last selected view.
    bool Ignored = false;
    // The newest valid view this endpoint returned.
    std::optional<TClusterView> View;
    // The latest pile-named successful SelfCheck: SentAt and Classification (Mode,
    // Truncated, SizeWarning, SelfCheckResult, redacted Notes, OmittedNotes).
    std::optional<TSelfCheckReply> SelfCheck;
    TString LastError;
};

struct TPileSnapshot {
    TPileVerdict Verdict;
    std::vector<TConnectivity> Connectivity;
    std::optional<TMonotonic> DownSince;
    bool Confirmed = false;
};

// The latest invocation; the mutation_sent and mutation_outcome logs keep history.
struct TMutationInvocation {
    // The request sequence, used only to match the outcome.
    ui64 Id = 0;
    TMonotonic SentAt;
    TPlan Plan;
    TInstant SentUtc;
    EOperationKind Kind = EOperationKind::Failover;
    EDispatchOrigin Origin = EDispatchOrigin::Auto;
    std::optional<TString> Status;
    std::optional<EMutationOutcome> Outcome;
    // Resends after ambiguous outcomes for the record, this invocation included; 0 for the first send.
    ui64 AmbiguousResends = 0;
};

struct TSkipperCounters {
    // Status is SdkStatusName, a local failure name, or OTHER for an unknown value.
    std::map<std::pair<EYdbMethod, TString>, ui64> RpcTotal;
    std::map<EMutationOutcome, ui64> MutationTotal;
    ui64 UnexplainedStateChanges = 0;
};

using TPileActionMap = std::map<TString, std::map<EOperationKind, TDecision>>;

struct TSkipperSnapshot {
    TString ClusterId;
    EMode Mode = EMode::Observe;
    TString ControllerBootId;
    ui64 ModeRevision = 1;
    TString SnapshotRevision;
    bool ControllerReady = false;
    bool Stopping = false;
    TDuration EvidenceTtl = TDuration::Seconds(30);
    // Last observed values remain available through observation gaps.
    std::optional<TClusterView> LastObservedView;
    bool MayApplyElsewhere = false;
    ui64 PendingAttempts = 0;
    std::optional<TInstant> PendingFirstSendUtc;
    std::optional<TInstant> PendingLastSendUtc;
    std::optional<ui64> RejectedGeneration;
    std::optional<ui64> LifecycleRejectedGeneration;
    std::optional<TMonotonic> RetryAt;
    TMonotonic BootAt;
    TInstant BootUtc;
    std::shared_ptr<TDiagnosticStore> Diagnostics;
    // Per-action canonical eligibility, with a plan only when available.
    TPileActionMap Actions;
    TMonotonic LastTickAt;
    TViewSelection Selection;
    ui64 GenerationHighWater = 0;
    std::vector<TEndpointSnapshot> Endpoints;
    // Connectivity holds only the valid reports that list the pile.
    std::map<TString, TPileSnapshot> Piles;
    std::map<TString, TPileSnapshot> LastKnownPiles;
    TPileParticipation Participation;
    TDecision Decision;
    TPersistentState PersistentState;
    TString StateWriteError;
    std::optional<TMutationInvocation> LastMutation;
    TSkipperCounters Counters;
};

class TSnapshotStore {
public:
    void Publish(std::shared_ptr<const TSkipperSnapshot> snapshot);
    std::shared_ptr<const TSkipperSnapshot> Read() const;

private:
    mutable std::mutex Mutex;
    std::shared_ptr<const TSkipperSnapshot> Snapshot;
};

// Shared by logs and /api/state; expected_generation is a decimal string.
NJson::TJsonValue UpdateJson(const TExactUpdate& update);
NJson::TJsonValue PlanJson(const TPlan& plan);

struct TSkipperOptions {
    TConfig Config;
    std::shared_ptr<IYdbClients> Clients;
    std::shared_ptr<TStateFile> StateFile;
    std::shared_ptr<TSnapshotStore> Snapshots;
    // Optional adapter; follows TStateFile::Write's durability/memory contract.
    std::function<TString(const TPersistentState&)> WriteState;
    // Fixtures explicitly deliver ticks while controlling simulated time.
    bool ScheduleTicks = true;
    TString BootId;
    std::shared_ptr<std::atomic<bool>> ShutdownRequested;
    std::shared_ptr<TDiagnosticStore> Diagnostics;
};

NActors::IActor* CreateSkipperActor(TSkipperOptions options);

} // namespace NBridgeSkipper
