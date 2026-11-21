#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NBlobDepot {

    struct TS3PutContext : NWrappers::NExternalStorage::IRequestContext {
        enum class ESource { Agent, Cooling };
        const TMonotonic Deadline;
        const ESource Source;

        TS3PutContext(TMonotonic deadline, ESource source)
            : Deadline(deadline)
            , Source(source)
        {}
    };

} // NKikimr::NBlobDepot

namespace NKikimr::NStorage {

    // Sent by a BlobDepot tablet or agent to its local NodeWarden to ask for a per-node
    // S3 router actor. NodeWarden creates one on first acquire, replaces it when a newer
    // tablet generation supplies changed settings, and stops it on the last release.
    //
    // Fire-and-forget: NodeWarden registers the router under the well-known service id
    // MakeBlobDepotS3RouterID(TabletId) synchronously while handling this event, before
    // any subsequent event from the same sender can be delivered. There is no result
    // event; callers simply talk to the service id afterwards.
    struct TEvNodeWardenAcquireBlobDepotS3Router
        : TEventLocal<TEvNodeWardenAcquireBlobDepotS3Router, TEvBlobStorage::EvNodeWardenAcquireBlobDepotS3Router>
    {
        ui64 TabletId;
        ui32 Generation;
        NKikimrBlobDepot::TS3BackendSettings Settings;

        TEvNodeWardenAcquireBlobDepotS3Router(ui64 tabletId, ui32 generation, NKikimrBlobDepot::TS3BackendSettings settings)
            : TabletId(tabletId)
            , Generation(generation)
            , Settings(std::move(settings))
        {}
    };

    struct TEvNodeWardenReleaseBlobDepotS3Router
        : TEventLocal<TEvNodeWardenReleaseBlobDepotS3Router, TEvBlobStorage::EvNodeWardenReleaseBlobDepotS3Router>
    {
        ui64 TabletId;
        ui32 Generation;

        TEvNodeWardenReleaseBlobDepotS3Router(ui64 tabletId, ui32 generation)
            : TabletId(tabletId)
            , Generation(generation)
        {}
    };

} // NKikimr::NStorage
