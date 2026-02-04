#pragma once

#include <ydb/library/actors/core/actor.h>

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NYdb::NBS::NProto;

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId CreatePartitionTablet(
    const NActors::TActorId& owner,
    TStorageConfig storageConfig);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
