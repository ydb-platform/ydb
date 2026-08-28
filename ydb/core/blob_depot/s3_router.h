#pragma once

#include "defs.h"

#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NBlobDepot {

    // Per-node, per-tablet S3 router actor.
    //
    // Forwards every TEvExternalStorage::TEv*Request received from BlobDepot tablet/agents
    // to an internally managed S3 wrapper. The actual S3 endpoint is fetched periodically
    // from the BalancerHost specified in TS3BackendSettings. Endpoint is also refreshed
    // immediately when the inner wrapper returns an HTTP 5xx response.
    //
    // Created/destroyed by Node Warden based on TEvNodeWardenAcquire/ReleaseBlobDepotS3Router
    // messages from clients (tablet + agents that talk to that tablet on this node).
    IActor* CreateBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings);

} // NKikimr::NBlobDepot
