#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NStorage {

    // Sent by a BlobDepot tablet or agent to its local NodeWarden to ask for a per-node
    // S3 router actor that resolves the actual S3 endpoint dynamically. The router is
    // created on first acquire and destroyed when the last consumer (any tablet or
    // agent referring to the same BlobDepot tabletId) releases it.
    //
    // Fire-and-forget: NodeWarden registers the router under the well-known service id
    // MakeBlobDepotS3RouterID(TabletId) synchronously while handling this event, before
    // any subsequent event from the same sender can be delivered. There is no result
    // event; callers simply talk to the service id afterwards.
    struct TEvNodeWardenAcquireBlobDepotS3Router
        : TEventLocal<TEvNodeWardenAcquireBlobDepotS3Router, TEvBlobStorage::EvNodeWardenAcquireBlobDepotS3Router>
    {
        ui64 TabletId;
        NKikimrBlobDepot::TS3BackendSettings Settings;

        TEvNodeWardenAcquireBlobDepotS3Router(ui64 tabletId, NKikimrBlobDepot::TS3BackendSettings settings)
            : TabletId(tabletId)
            , Settings(std::move(settings))
        {}
    };

    struct TEvNodeWardenReleaseBlobDepotS3Router
        : TEventLocal<TEvNodeWardenReleaseBlobDepotS3Router, TEvBlobStorage::EvNodeWardenReleaseBlobDepotS3Router>
    {
        ui64 TabletId;

        explicit TEvNodeWardenReleaseBlobDepotS3Router(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

} // NKikimr::NStorage
