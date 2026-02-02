#include "partition_direct_actor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/appdata_fwd.h>

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TActorId CreatePartitionTablet(
    const NActors::TActorId& owner,
    TStorageConfig storageConfig)
{
    auto actor = std::make_unique<TPartitionActor>(
        std::move(storageConfig));

    return TActivationContext::Register(
        actor.release(),
        owner,
        TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

} // namespace NYdb::NBS::NStorage::NPartitionDirect
