#include "partition_direct_actor.h"

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActor* CreatePartitionTablet(const TActorId& tablet, TTabletStorageInfo* info)
{
    return new TPartitionActor(tablet, info);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
