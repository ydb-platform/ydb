#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>

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
        EvStoreBarrierLsn,

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

    // Posted by a DirectBlockGroup (via IPartitionDirectService) to persist
    // its newly advanced barrier LSN in the partition's local DB.
    struct TEvStoreBarrierLsn
        : public NActors::TEventLocal<TEvStoreBarrierLsn, EvStoreBarrierLsn>
    {
        ui32 DirectBlockGroupIndex;
        ui64 Lsn;

        TEvStoreBarrierLsn(ui32 directBlockGroupIndex, ui64 lsn)
            : DirectBlockGroupIndex(directBlockGroupIndex)
            , Lsn(lsn)
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
