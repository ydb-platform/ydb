#include "volume.h"

#include "volume_actor.h"

#include <ydb/core/base/appdata_fwd.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

IActor* CreateVolumeTablet(const TActorId& tablet, TTabletStorageInfo* info)
{
    return new TVolumeActor(tablet, info);
}

}   // namespace NYdb::NBS::NStorage
