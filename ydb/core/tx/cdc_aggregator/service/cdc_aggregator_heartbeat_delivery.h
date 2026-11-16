#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/persqueue/common/heartbeat.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <optional>
#include <variant>

namespace NKikimr::NCdcAggregator {

using TCdcAggregatorHeartbeatBatch = THashMap<ui64, NPQ::THeartbeat>;

struct TCdcAggregatorWriteHeartbeatsResult {
    THashMap<ui64, TRowVersion> WrittenVersionsBySourceDataShardId;
    THashMap<ui64, TString> RejectionErrorsBySourceDataShardId;
};

struct TCdcAggregatorHeartbeatWritten {
    TRowVersion Version;
};

struct TCdcAggregatorHeartbeatRetry {
    TString Error;
};

struct TCdcAggregatorHeartbeatRejected {
    TString Error;
};

using TCdcAggregatorHeartbeatDeliveryReply = std::variant<TCdcAggregatorHeartbeatWritten, TCdcAggregatorHeartbeatRetry, TCdcAggregatorHeartbeatRejected>;

using TCdcAggregatorHeartbeatBatchResult = THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply>;

class TCdcAggregatorHeartbeatDelivery {
public:
    explicit TCdcAggregatorHeartbeatDelivery(ui32 maxBatchSize);

    void AddHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat);
    std::optional<TCdcAggregatorHeartbeatBatch> PrepareNextBatchIfPossible();
    TCdcAggregatorHeartbeatBatchResult CompleteBatch(const TCdcAggregatorWriteHeartbeatsResult& writeHeartbeatsResult);
    TCdcAggregatorHeartbeatBatchResult FailBatch(const TString& error);

    bool IsIdle() const {
        return PendingHeartbeatsBySourceDataShardId.empty() && AwaitingReplySourceDataShardIds.empty();
    }

private:
    const ui32 MaxBatchSize;

    THashMap<ui64, NPQ::THeartbeat> PendingHeartbeatsBySourceDataShardId;
    TDeque<ui64> PendingSourceDataShardIdQueue;
    TVector<ui64> AwaitingReplySourceDataShardIds;
};

} // namespace NKikimr::NCdcAggregator
