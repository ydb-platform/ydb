#pragma once

#include "schemeshard_info_types_base.h"

#include <util/generic/guid.h>

namespace NKikimr {
namespace NSchemeShard {

struct TRtmrPartitionInfo: TSimpleRefCount<TRtmrPartitionInfo> {
    using TPtr = TIntrusivePtr<TRtmrPartitionInfo>;
    TGUID Id;
    ui64 BusKey;
    TShardIdx ShardIdx;
    TTabletId TabletId;

    TRtmrPartitionInfo(TGUID id, ui64 busKey, TShardIdx shardIdx, TTabletId tabletId = InvalidTabletId):
        Id(id), BusKey(busKey), ShardIdx(shardIdx), TabletId(tabletId)
    {}
};

struct TRtmrVolumeInfo: TSimpleRefCount<TRtmrVolumeInfo> {
    using TPtr = TIntrusivePtr<TRtmrVolumeInfo>;

    THashMap<TShardIdx, TRtmrPartitionInfo::TPtr> Partitions;
};

}
}
