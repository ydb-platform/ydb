#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>


namespace NKikimr {

class TTabletStorageInfo;

NActors::IActor* CreatePersQueue(const NActors::TActorId& tablet, TTabletStorageInfo *info);

} // namespace NKikimr
