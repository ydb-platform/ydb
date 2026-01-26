#pragma once

#include <ydb/library/actors/core/actor.h>


namespace NYdb::NBS::NStorage::NPartitionDirect {

NActors::TActorId CreatePartitionTablet(
    const NActors::TActorId& owner
);

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
