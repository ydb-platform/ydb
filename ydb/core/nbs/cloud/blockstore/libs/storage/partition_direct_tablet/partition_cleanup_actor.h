#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>

#include <ydb/library/actors/core/actor.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration PartitionCleanupTimeout = TDuration::Seconds(60);

struct TPartitionCleanupParams
{
    NActors::TActorId Parent;
    ui64 TabletId = 0;
    ui32 Generation = 0;
    TString DiskId;
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections
        Connections;
    TString DDiskPoolName;
    TString PersistentBufferDDiskPoolName;
    size_t DirectBlockGroupsCount = 0;
};

NActors::IActor* CreatePartitionCleanupActor(TPartitionCleanupParams params);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
