#include "dbs_controller.h"

#include "dbs_controller_actor.h"

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActor* CreateDbsControllerTablet(
    const TActorId& tablet,
    TTabletStorageInfo* info)
{
    return new TDbsControllerActor(tablet, info);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
