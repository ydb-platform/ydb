#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>

#include <util/generic/hash.h>

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
        EvBarrierCleanupWakeup,
        EvBarrierLsnsReady,
        EvBarrierCycleDone,

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

    // Tablet-internal trigger for one barrier cleanup cycle. Re-scheduled
    // after each cycle's IssueBarrier futures complete.
    struct TEvBarrierCleanupWakeup
        : public NActors::
              TEventLocal<TEvBarrierCleanupWakeup, EvBarrierCleanupWakeup>
    {
    };

    // Carries per-DBG min LSNs computed on DBG executor threads back to the
    // tablet thread so the persist Tx can run.
    struct TEvBarrierLsnsReady
        : public NActors::TEventLocal<TEvBarrierLsnsReady, EvBarrierLsnsReady>
    {
        THashMap<ui32, ui64> PerDbgLsn;

        explicit TEvBarrierLsnsReady(THashMap<ui32, ui64> perDbgLsn)
            : PerDbgLsn(std::move(perDbgLsn))
        {}
    };

    // Fired (via ActorSystem->Send) when all per-DBG IssueBarrier futures
    // for the current cycle have resolved. Tablet clears the in-flight flag
    // and schedules the next cycle.
    struct TEvBarrierCycleDone
        : public NActors::TEventLocal<TEvBarrierCycleDone, EvBarrierCycleDone>
    {
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
