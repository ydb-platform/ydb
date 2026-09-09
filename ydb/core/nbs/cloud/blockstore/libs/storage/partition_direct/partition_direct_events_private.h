#pragma once

#include "partition_direct_service.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/threading/future/core/future.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

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
        EvUpdateDirtyMapState,
        EvFastPathServiceReady,

        EvFastPathServiceShutdown,
        EvFastPathServiceStopped,
        EvPoisonByBlockedGeneration,
        EvAddHostToDBG,
        EvPartitionCleanupCompleted,

        EvEnd,
    };

    struct TEvUpdateVChunkConfig
        : public NActors::
              TEventLocal<TEvUpdateVChunkConfig, EvUpdateVChunkConfig>
    {
        TVChunkConfig VChunkConfig;
        TPersistResultPromise UpdateCompleted =
            NThreading::NewPromise<EPersistResult>();

        explicit TEvUpdateVChunkConfig(TVChunkConfig cfg)
            : VChunkConfig(std::move(cfg))
        {}
    };

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

    // DDisk replied BLOCKED: the current tablet generation is stale, so the
    // tablet must suicide. Carries diagnostics coordinates and a reason string.
    struct TEvPoison
        : public NActors::TEventLocal<TEvPoison, EvPoisonByBlockedGeneration>
    {
        const TString Reason;

        explicit TEvPoison(TString reason)
            : Reason(std::move(reason))
        {}
    };

    struct TEvAddHostToDBG
        : public NActors::TEventLocal<TEvAddHostToDBG, EvAddHostToDBG>
    {
        const size_t DirectBlockGroupId;
        const ui32 DBGConnectionsConfigGeneration;

        TEvAddHostToDBG(size_t dbgId, ui32 dbgConnectionsConfigGeneration)
            : DirectBlockGroupId(dbgId)
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
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
