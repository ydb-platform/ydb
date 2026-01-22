#pragma once

#include <ydb/library/actors/core/actor.h>


namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

NActors::TActorId CreatePartitionTablet(
    const NActors::TActorId& owner
);

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
