#include "partition_direct_actor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/appdata_fwd.h>


namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TActorId CreatePartitionTablet(
    const NActors::TActorId& owner,
    TStorageConfig storageConfig,
    NKikimrBlockStore::TVolumeConfig volumeConfig)
{
    auto actor = std::make_unique<TPartitionActor>(
        std::move(storageConfig),
        std::move(volumeConfig),
        AppData()->Counters);

    return TActivationContext::Register(
        actor.release(),
        owner,
        TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

} // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
