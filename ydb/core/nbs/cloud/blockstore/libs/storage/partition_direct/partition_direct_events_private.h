#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>

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
        EvFastPathServiceReady,

        EvFastPathServiceShutdown,
        EvFastPathServiceStopped,
        EvPoisonByBlockedGeneration,
        EvAddHostToDBG,

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
        size_t DirectBlockGroupId;
        size_t NewHostIndex;

        TEvAddHostToDBG(size_t dbgId, size_t newHostIndex)
            : DirectBlockGroupId(dbgId)
            , NewHostIndex(newHostIndex)
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
