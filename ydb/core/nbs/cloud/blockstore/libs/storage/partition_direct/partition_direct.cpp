#include "partition_direct_actor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/appdata_fwd.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TActorId CreatePartitionTablet(
    const NActors::TActorId& owner)
{
    auto actor = std::make_unique<TPartitionActor>();

    return TActivationContext::Register(
        actor.release(),
        owner,
        TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
