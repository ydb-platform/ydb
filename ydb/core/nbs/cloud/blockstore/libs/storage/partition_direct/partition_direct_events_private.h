#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_model.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TFastPathService;

////////////////////////////////////////////////////////////////////////////////

// Offset for the partition_direct actor's local-only events within
// ES_NBS_V2_SERVICE, kept clear of the public TEvService event IDs.
constexpr ui32 LocalEventsOffset = 1000;

// Local-only events for the partition_direct actor.
struct TEvPartitionDirectPrivate
{
    enum EEvents
    {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_NBS_V2_SERVICE) +
                  LocalEventsOffset,

        EvUpdateVChunkConfig,
        EvFastPathServiceReady,

        EvFastPathServiceShutdown,
        EvFastPathServiceStopped,

        EvMonDbgSnapshotReady,
        EvMonRenderTimeout,

        EvEnd,
    };

    struct TEvUpdateVChunkConfig
        : public NActors::
              TEventLocal<TEvUpdateVChunkConfig, EvUpdateVChunkConfig>
    {
        TVChunkConfig VChunkConfig;

        explicit TEvUpdateVChunkConfig(TVChunkConfig cfg)
            : VChunkConfig(std::move(cfg))
        {}
    };

    // Signals that FastPathServiceReady (and its DBGs) are ready.
    struct TEvFastPathServiceReady
        : public NActors::
              TEventLocal<TEvFastPathServiceReady, EvFastPathServiceReady>
    {
    };

    // Triggers the shutdown of the fast path service
    struct TEvFastPathServiceShutdown
        : public NActors::
              TEventLocal<TEvFastPathServiceShutdown, EvFastPathServiceShutdown>
    {
    };

    // Signals that FastPathService stopped.
    struct TEvFastPathServiceStopped
        : public NActors::
              TEventLocal<TEvFastPathServiceStopped, EvFastPathServiceStopped>
    {
    };

    // Carries one DBG's gathered host snapshot back to the actor assembling a
    // monitoring page. Cookie identifies the in-flight page request; the DBG
    // this snapshot belongs to is Snapshot.Index.
    struct TEvMonDbgSnapshotReady
        : public NActors::
              TEventLocal<TEvMonDbgSnapshotReady, EvMonDbgSnapshotReady>
    {
        ui64 Cookie;
        TDbgSnapshot Snapshot;

        TEvMonDbgSnapshotReady(ui64 cookie, TDbgSnapshot snapshot)
            : Cookie(cookie)
            , Snapshot(std::move(snapshot))
        {}
    };

    // Deadline for assembling a monitoring page: whatever DBG snapshots have
    // arrived by then are rendered; any DBG that has not reported yet is
    // omitted from the page.
    struct TEvMonRenderTimeout
        : public NActors::TEventLocal<TEvMonRenderTimeout, EvMonRenderTimeout>
    {
        ui64 Cookie;

        explicit TEvMonRenderTimeout(ui64 cookie)
            : Cookie(cookie)
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
