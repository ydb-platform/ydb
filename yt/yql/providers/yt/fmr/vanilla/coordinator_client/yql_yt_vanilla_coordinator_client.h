#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/vanilla/peer_tracker/yql_yt_vanilla_peer_tracker.h>
#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NYql::NFmr {

////////////////////////////////////////////////////////////////////////////////

struct TVanillaFmrCoordinatorClientSettings {
    ui16 CoordinatorPort = 8001;
    TDuration RefreshInterval = TDuration::Seconds(5);
};

// IFmrCoordinator that resolves the coordinator address by watching the cookie=0
// vanilla job via ListJobs and forwarding all requests to it.
// The address is refreshed in a background thread.
IFmrCoordinator::TPtr MakeVanillaFmrCoordinatorClient(
    const IVanillaExternalPeerTracker& peerTracker,
    const TVanillaFmrCoordinatorClientSettings& settings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
