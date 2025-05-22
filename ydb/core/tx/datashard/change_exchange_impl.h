#pragma once

#include "defs.h"
#include "change_exchange_helpers.h"

namespace NKikimr {
namespace NDataShard {

IActor* CreateAsyncIndexChangeSender(const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& indexPathId);
IActor* CreateCdcStreamChangeSender(const TDataShardId& dataShard, const TPathId& streamPathId);
IActor* CreateIncrRestoreChangeSender(const TActorId& changeServerActor, const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& restoreTargetPathId);

} // NDataShard
} // NKikimr
