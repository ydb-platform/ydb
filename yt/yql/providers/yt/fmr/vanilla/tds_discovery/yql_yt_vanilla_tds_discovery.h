#pragma once

#include <yt/yql/providers/yt/fmr/table_data_service/discovery/interface/yql_yt_service_discovery.h>
#include <yt/yql/providers/yt/fmr/vanilla/peer_tracker/yql_yt_vanilla_peer_tracker.h>

#include <atomic>

namespace NYql::NFmr {

////////////////////////////////////////////////////////////////////////////////

struct TVanillaTdsDiscoverySettings {
    ui16 TdsPort = 8002;
    TDuration RefreshInterval = TDuration::Seconds(5);
};

ITableDataServiceDiscovery::TPtr MakeVanillaTdsDiscovery(
    const IVanillaExternalPeerTracker& peerTracker,
    const TVanillaTdsDiscoverySettings& settings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
