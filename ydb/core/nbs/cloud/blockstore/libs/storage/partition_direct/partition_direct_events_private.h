#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
<<<<<<< HEAD
=======
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_model.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
>>>>>>> 7af514ad642 ([NBS-7516] Extend the partition direct overview page with actual storage placement for touched VChunks (#53472))

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
<<<<<<< HEAD
=======
        EvUpdateDirtyMapState,
        EvSetVChunkTouched,
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
        EvFastPathServiceReady,
        EvRenderMonPage,

        EvFastPathServiceShutdown,
        EvFastPathServiceStopped,
        EvAddHostToDBG,
<<<<<<< HEAD
=======
        EvRemoveHostFromDBG,
        EvPartitionCleanupCompleted,
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061))

        EvPersistHostHealth,

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

<<<<<<< HEAD
=======
    struct TEvUpdateDirtyMapState
        : public NActors::
              TEventLocal<TEvUpdateDirtyMapState, EvUpdateDirtyMapState>
    {
        ui32 VChunkIndex;
        TDirtyMapStateProto State;
        TPersistResultPromise UpdateCompleted =
            NThreading::NewPromise<EPersistResult>();

        TEvUpdateDirtyMapState(ui32 vChunkIndex, TDirtyMapStateProto state)
            : VChunkIndex(vChunkIndex)
            , State(std::move(state))
        {}
    };

    struct TEvSetVChunkTouched
        : public NActors::TEventLocal<TEvSetVChunkTouched, EvSetVChunkTouched>
    {
        const ui32 VChunkIndex;
        TPersistResultPromise UpdateCompleted =
            NThreading::NewPromise<EPersistResult>();

        explicit TEvSetVChunkTouched(ui32 vChunkIndex)
            : VChunkIndex(vChunkIndex)
        {}
    };

>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
    // Signals that FastPathServiceReady (and its DBGs) are ready.
    struct TEvFastPathServiceReady
        : public NActors::
              TEventLocal<TEvFastPathServiceReady, EvFastPathServiceReady>
    {
    };

    struct TEvRenderMonPage
        : public NActors::TEventLocal<TEvRenderMonPage, EvRenderMonPage>
    {
        NActors::TActorId Requester;
        TMonPageData Data;

        TEvRenderMonPage(NActors::TActorId requester, TMonPageData data)
            : Requester(requester)
            , Data(std::move(data))
        {}
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

    struct TEvAddHostToDBG
        : public NActors::TEventLocal<TEvAddHostToDBG, EvAddHostToDBG>
    {
        size_t DirectBlockGroupId;

        explicit TEvAddHostToDBG(size_t dbgId)
            : DirectBlockGroupId(dbgId)
<<<<<<< HEAD
=======
            , DBGConnectionsConfigGeneration(dbgConnectionsConfigGeneration)
        {}
    };

    // Asks the partition to durably remove the host from the group.
    struct TEvRemoveHostFromDBG
        : public NActors::TEventLocal<TEvRemoveHostFromDBG, EvRemoveHostFromDBG>
    {
        const size_t DirectBlockGroupId;
        const size_t HostIndex;
        const ui32 DBGConnectionsConfigGeneration;

        TEvRemoveHostFromDBG(
            size_t dbgId,
            size_t hostIndex,
            ui32 dbgConnectionsConfigGeneration)
            : DirectBlockGroupId(dbgId)
            , HostIndex(hostIndex)
            , DBGConnectionsConfigGeneration(dbgConnectionsConfigGeneration)
        {}
    };

    // Cleanup actor reports wipe + BSC deallocate outcome to the tablet.
    struct TEvPartitionCleanupCompleted
        : public NActors::TEventLocal<
              TEvPartitionCleanupCompleted,
              EvPartitionCleanupCompleted>
    {
        NProto::TError Error;

        TEvPartitionCleanupCompleted() = default;

        explicit TEvPartitionCleanupCompleted(NProto::TError error)
            : Error(std::move(error))
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061))
        {}
    };

    struct TEvPersistHostHealth
        : public NActors::TEventLocal<TEvPersistHostHealth, EvPersistHostHealth>
    {
        size_t DirectBlockGroupId;
        size_t HostIndex;
        EHostHealth OldHealth;
        EHostHealth NewHealth;

        TEvPersistHostHealth(
            size_t direct_block_group_id,
            size_t host_index,
            EHostHealth old_health,
            EHostHealth new_health)
            : DirectBlockGroupId(direct_block_group_id)
            , HostIndex(host_index)
            , OldHealth(old_health)
            , NewHealth(new_health)
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
