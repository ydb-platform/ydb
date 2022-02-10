#pragma once
#include "read_table.h"

namespace NKikimr {
namespace NTxProxy {

    enum class EReadTableWorkerShardState {
        Unknown,
        SnapshotProposeSent,
        SnapshotPrepared,
        SnapshotPlanned,
        ReadTableNeedTxId,
        ReadTableProposeSent,
        ReadTableClearancePending,
        ReadTableStreaming,
        ReadTableNeedRetry,
        Finished,
        Error,
    };

    enum class EReadTableWorkerSnapshotState {
        Unknown,
        Confirmed,
        Refreshing,
        RefreshNeedRetry,
        Unavailable,
    };

} // namespace NTxProxy
} // namespace NKikimr
