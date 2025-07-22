#pragma once

#include "partition_id.h"

#include <util/system/types.h>

namespace NActors {

class IActor;
struct TActorId;

}

namespace NKikimrPQ {

class TOffloadConfig;

}

namespace NKikimr::NPQ {

NActors::IActor* CreateOffloadActor(NActors::TActorId parentTablet, ui64 tabletId, TPartitionId partition, const NKikimrPQ::TOffloadConfig& config);

} // namespace NKikimr::NPQ
