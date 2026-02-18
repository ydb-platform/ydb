#pragma once

#include <ydb/library/actors/core/actor.h>

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId CreatePartitionTablet(
    const NActors::TActorId& owner,
    NYdb::NBS::NProto::TStorageConfig storageConfig,
    NKikimrBlockStore::TVolumeConfig volumeConfig
);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
