#include "cdc_aggregator_heartbeat_delivery.h"

#include <util/generic/yexception.h>

namespace NKikimr::NCdcAggregator {

namespace {

using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

TCdcAggregatorHeartbeatBatchItem PopOldestPending(TDeque<ui64>& sourceDataShardIdQueue, THashMap<ui64, NPQ::THeartbeat>& heartbeatsBySourceDataShardId) {
    const ui64 sourceDataShardId = sourceDataShardIdQueue.front();
    sourceDataShardIdQueue.pop_front();
    TCdcAggregatorHeartbeatBatchItem item{
        .SourceDataShardId = sourceDataShardId,
        .Heartbeat = std::move(heartbeatsBySourceDataShardId.at(sourceDataShardId)),
    };
    heartbeatsBySourceDataShardId.erase(sourceDataShardId);
    return item;
}

} // namespace

TCdcAggregatorHeartbeatDelivery::TCdcAggregatorHeartbeatDelivery(ui32 maxBatchSize)
    : MaxBatchSize(maxBatchSize)
{
    Y_ENSURE(MaxBatchSize > 0);
}

void TCdcAggregatorHeartbeatDelivery::AddHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat) {
    auto it = PendingHeartbeatsBySourceDataShardId.find(sourceDataShardId);
    if (it == PendingHeartbeatsBySourceDataShardId.end()) {
        PendingHeartbeatsBySourceDataShardId.emplace(sourceDataShardId, heartbeat);
        PendingSourceDataShardIdQueue.push_back(sourceDataShardId);
    } else if (it->second.Version < heartbeat.Version) {
        it->second = heartbeat;
    }
}

std::optional<TCdcAggregatorHeartbeatBatch> TCdcAggregatorHeartbeatDelivery::PrepareNextBatchIfPossible() {
    if (!AwaitingReplySourceDataShardIds.empty() || PendingSourceDataShardIdQueue.empty()) {
        return std::nullopt;
    }

    TCdcAggregatorHeartbeatBatch batch;
    while (!PendingSourceDataShardIdQueue.empty() && batch.size() < MaxBatchSize) {
        TCdcAggregatorHeartbeatBatchItem item = PopOldestPending(PendingSourceDataShardIdQueue, PendingHeartbeatsBySourceDataShardId);
        AwaitingReplySourceDataShardIds.push_back(item.SourceDataShardId);
        batch.push_back(std::move(item));
    }

    return batch;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::CompleteBatch(const THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply>& tabletRepliesBySourceDataShardId) {
    TVector<TCdcAggregatorHeartbeatDeliveryReply> replies;
    for (const ui64 sourceDataShardId : AwaitingReplySourceDataShardIds) {
        if (const auto* reply = tabletRepliesBySourceDataShardId.FindPtr(sourceDataShardId)) {
            replies.push_back(*reply);
        } else {
            replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Retry, .Error = "Heartbeat is not confirmed"});
        }
    }

    AwaitingReplySourceDataShardIds.clear();
    return replies;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::FailBatch(const TString& error) {
    if (AwaitingReplySourceDataShardIds.empty()) {
        return {};
    }

    TVector<TCdcAggregatorHeartbeatDeliveryReply> replies;
    for (const ui64 sourceDataShardId : AwaitingReplySourceDataShardIds) {
        replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Retry, .Error = error});
    }
    for (const ui64 sourceDataShardId : PendingSourceDataShardIdQueue) {
        replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Retry, .Error = error});
    }

    PendingHeartbeatsBySourceDataShardId.clear();
    PendingSourceDataShardIdQueue.clear();
    AwaitingReplySourceDataShardIds.clear();

    return replies;
}

} // namespace NKikimr::NCdcAggregator
