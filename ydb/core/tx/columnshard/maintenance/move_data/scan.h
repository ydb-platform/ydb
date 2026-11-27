#pragma once

#include "events.h"

#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard {

NActors::IActor* CreateMoveDataScan(
    TActorId owner, std::vector<TMoveDataScanPortion>&& portions, const THashSet<ui32>& groups, ui64 memoryLimit);

}   // namespace NKikimr::NColumnShard
