#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
class TTabletStorageInfo;
}   // namespace NKikimr

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreateDbsControllerTablet(
    const NActors::TActorId& tablet,
    NKikimr::TTabletStorageInfo* info);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
