#pragma once

#include "datashard.h"

namespace NKikimr::NDataShard {

    class TDataShard;

    IActor* CreateReplicationSourceOffsetsClient(TActorId owner, ui64 srcTabletId, const TPathId& pathId);

} // namespace NKikimr::NDataShard
