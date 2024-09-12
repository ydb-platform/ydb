#pragma once

#include "defs.h"
#include "change_exchange_helpers.h"

#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr {
namespace NDataShard {

IActor* CreateAsyncIndexChangeSender(const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& indexPathId);
IActor* CreateCdcStreamChangeSender(const TDataShardId& dataShard, const TPathId& streamPathId);

} // NDataShard
} // NKikimr
