#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

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
