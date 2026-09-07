#pragma once

#include "schemeshard_info_types_base.h"

namespace NKikimr {
namespace NSchemeShard {

struct TSolomonPartitionInfo: TSimpleRefCount<TSolomonPartitionInfo> {
    using TPtr = TIntrusivePtr<TSolomonPartitionInfo>;
    ui64 PartitionId;
    TTabletId TabletId;

    TSolomonPartitionInfo(ui64 partId, TTabletId tabletId = InvalidTabletId)
        : PartitionId(partId)
        , TabletId(tabletId)
    {}
};

struct TSolomonVolumeInfo: TSimpleRefCount<TSolomonVolumeInfo> {
    using TPtr = TIntrusivePtr<TSolomonVolumeInfo>;

    THashMap<TShardIdx, TSolomonPartitionInfo::TPtr> Partitions;
    ui64 Version;
    TSolomonVolumeInfo::TPtr AlterData;

    TSolomonVolumeInfo(ui64 version)
        : Version(version)
    {
    }

    TSolomonVolumeInfo::TPtr CreateAlter() const {
        return CreateAlter(Version + 1);
    }

    TSolomonVolumeInfo::TPtr CreateAlter(ui64 version) const {
        Y_ENSURE(Version < version);
        TSolomonVolumeInfo::TPtr alter = new TSolomonVolumeInfo(*this);
        alter->Version = version;
        return alter;
    }
};

}
}
