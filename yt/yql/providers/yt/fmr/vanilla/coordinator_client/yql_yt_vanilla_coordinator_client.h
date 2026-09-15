#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/vanilla/peer_tracker/yql_yt_vanilla_peer_tracker.h>
#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql::NFmr {

////////////////////////////////////////////////////////////////////////////////

struct TVanillaFmrCoordinatorClientSettings {
    // If set, this tracker is reused directly (it must remain alive for the
    // coordinator client's lifetime). Alias resolution and peer-tracker creation
    // are skipped. Only CoordinatorPort and RefreshInterval are used from these settings.
    IVanillaExternalPeerTracker* ExternalPeerTracker = nullptr;

    // Used only when ExternalPeerTracker is null:
    TString Cluster;
    TString OperationId;  // may start with '*' for alias resolution
    TMaybe<TString> Token;
    TDuration ListJobsInterval = TDuration::Seconds(1);
    ui64 MaxFails = 3;

    ui16 CoordinatorPort = 8001;
    TDuration RefreshInterval = TDuration::Seconds(5);
};

// IFmrCoordinator that lazily resolves the coordinator address:
//   — If ExternalPeerTracker is set: uses it directly (skips alias resolution and tracker creation).
//   — Otherwise: if OperationId starts with '*', resolves the alias via GetOperation in a
//     background thread; then creates a TVanillaExternalPeerTracker with WaitForPeers=true.
// In both cases, the coordinator IP is refreshed from the cookie=0 job in a background thread.
IFmrCoordinator::TPtr MakeVanillaFmrCoordinatorClient(
    const TVanillaFmrCoordinatorClientSettings& settings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
