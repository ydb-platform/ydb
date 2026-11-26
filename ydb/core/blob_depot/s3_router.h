#pragma once

#include "defs.h"

#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/library/actors/core/actor.h>

#include <util/system/guard.h>
#include <util/system/mutex.h>

namespace NKikimr::NBlobDepot {

    struct TS3BalancerEndpoint {
        void Set(TString host, ui16 port) {
            TGuard guard(Mutex);
            Address = {std::move(host), port};
        }

        std::pair<TString, ui16> Get() const {
            TGuard guard(Mutex);
            return Address;
        }

    private:
        mutable TMutex Mutex;
        std::pair<TString, ui16> Address;
    };

    // Per-node, per-tablet S3 router actor.
    //
    // Forwards every TEvExternalStorage::TEv*Request received from BlobDepot tablet/agents
    // to an internally managed S3 wrapper for the configured endpoint. When BalancerHost
    // is set, the router refreshes the HTTP proxy periodically and after a 5xx response.
    //
    // Created/destroyed by Node Warden based on TEvNodeWardenAcquire/ReleaseBlobDepotS3Router
    // messages from clients (tablet + agents that talk to that tablet on this node).
    IActor* CreateBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings settings, ui64 tabletId,
        std::shared_ptr<TS3BalancerEndpoint> lastEndpoint = {});

} // NKikimr::NBlobDepot
