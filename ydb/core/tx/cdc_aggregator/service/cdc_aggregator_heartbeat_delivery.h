#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/persqueue/common/heartbeat.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

struct TCdcAggregatorHeartbeatBatchItem {
    ui64 SourceDataShardId = 0;
    NPQ::THeartbeat Heartbeat;
};

using TCdcAggregatorHeartbeatBatch = TVector<TCdcAggregatorHeartbeatBatchItem>;

struct TCdcAggregatorHeartbeatDeliveryReply {
    enum class EStatus {
        Written,
        Retry,
        Rejected,
    };

    ui64 SourceDataShardId = 0;
    EStatus Status = EStatus::Retry;
    TRowVersion WrittenVersion = TRowVersion::Min();
    TString Error;
};

class TCdcAggregatorHeartbeatDelivery {
public:
    explicit TCdcAggregatorHeartbeatDelivery(ui32 maxBatchSize);

    void AddHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat);
    std::optional<TCdcAggregatorHeartbeatBatch> PrepareNextBatchIfPossible();
    TVector<TCdcAggregatorHeartbeatDeliveryReply> CompleteBatch(const THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply>& tabletRepliesBySourceId);
    TVector<TCdcAggregatorHeartbeatDeliveryReply> FailBatch(const TString& error);

    bool IsIdle() const {
        return PendingHeartbeatsBySourceId.empty() && AwaitingReplySourceIds.empty();
    }

private:
    const ui32 MaxBatchSize;

    THashMap<ui64, NPQ::THeartbeat> PendingHeartbeatsBySourceId;
    TDeque<ui64> PendingSourceIdQueue;
    TVector<ui64> AwaitingReplySourceIds;
};

} // namespace NKikimr::NCdcAggregator
