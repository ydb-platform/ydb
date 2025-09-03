#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr::NColumnShard::NOverload {

using TSeqNo = ui64;
using TTabletId = ui64;
using TColumnShardId = TActorId;
using TPipeServerId = TActorId;
using TInterconnectSessionId = TActorId;
using TOverloadSubscriberId = TActorId;

struct TColumnShardInfo {
    TTabletId TabletId;
    TColumnShardId ColumnShardId;
};

struct TPipeServerInfo {
    TPipeServerId PipeServerId;
    TInterconnectSessionId InterconnectSessionId;
};

struct TOverloadSubscriberInfo {
    TPipeServerId PipeServerId;
    TOverloadSubscriberId OverloadSubscriberId;
    TSeqNo SeqNo;
};

} // namespace NKikimr::NDataShard
