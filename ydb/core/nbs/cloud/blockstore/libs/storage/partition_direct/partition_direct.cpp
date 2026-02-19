#include "partition_direct_actor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/appdata_fwd.h>


namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId CreatePartitionTablet(
    const NActors::TActorId& owner,
    NYdb::NBS::NProto::TStorageConfig storageConfig,
    NKikimrBlockStore::TVolumeConfig volumeConfig)
{
    auto actor = std::make_unique<TPartitionActor>(
        std::move(storageConfig),
        std::move(volumeConfig));

    return NActors::TActivationContext::Register(
        actor.release(),
        owner,
        NActors::TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
