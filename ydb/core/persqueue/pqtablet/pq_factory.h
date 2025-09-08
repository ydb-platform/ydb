#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>


namespace NKikimr {

class TTabletStorageInfo;

namespace NPQ {

NActors::IActor* CreatePersQueue(const NActors::TActorId& tablet, TTabletStorageInfo *info);

} // namespace NPQ
} // namespace NKikimr
