#include "cdc_aggregator_heartbeat_delivery.h"

#include <util/generic/yexception.h>

namespace NKikimr::NCdcAggregator {

namespace {

using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

TCdcAggregatorHeartbeatBatchItem PopOldestPending(TDeque<ui64>& sourceIdQueue, THashMap<ui64, NPQ::THeartbeat>& heartbeatsBySourceId) {
    const ui64 sourceDataShardId = sourceIdQueue.front();
    sourceIdQueue.pop_front();
    TCdcAggregatorHeartbeatBatchItem item{
        .SourceDataShardId = sourceDataShardId,
        .Heartbeat = std::move(heartbeatsBySourceId.at(sourceDataShardId)),
    };
    heartbeatsBySourceId.erase(sourceDataShardId);
    return item;
}

} // namespace

TCdcAggregatorHeartbeatDelivery::TCdcAggregatorHeartbeatDelivery(ui32 maxBatchSize)
    : MaxBatchSize(maxBatchSize)
{
    Y_ENSURE(MaxBatchSize > 0);
}

void TCdcAggregatorHeartbeatDelivery::AddHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat) {
    auto it = PendingHeartbeatsBySourceId.find(sourceDataShardId);
    if (it == PendingHeartbeatsBySourceId.end()) {
        PendingHeartbeatsBySourceId.emplace(sourceDataShardId, heartbeat);
        PendingSourceIdQueue.push_back(sourceDataShardId);
    } else if (it->second.Version < heartbeat.Version) {
        it->second = heartbeat;
    }
}

std::optional<TCdcAggregatorHeartbeatBatch> TCdcAggregatorHeartbeatDelivery::PrepareNextBatchIfPossible() {
    if (!InFlight.empty() || PendingSourceIdQueue.empty()) {
        return std::nullopt;
    }

    TCdcAggregatorHeartbeatBatch batch;
    while (!PendingSourceIdQueue.empty() && batch.size() < MaxBatchSize) {
        TCdcAggregatorHeartbeatBatchItem item = PopOldestPending(PendingSourceIdQueue, PendingHeartbeatsBySourceId);
        InFlight.push_back(item.SourceDataShardId);
        batch.push_back(std::move(item));
    }

    return batch;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::CompleteBatch(const THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply>& tabletRepliesBySourceId) {
    TVector<TCdcAggregatorHeartbeatDeliveryReply> replies;
    for (const ui64 sourceDataShardId : InFlight) {
        if (const auto* reply = tabletRepliesBySourceId.FindPtr(sourceDataShardId)) {
            replies.push_back(*reply);
        } else {
            replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Retry, .Error = "Heartbeat is not confirmed"});
        }
    }

    InFlight.clear();
    return replies;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::FailBatch(const TString& error) {
    if (InFlight.empty()) {
        return {};
    }

    TVector<TCdcAggregatorHeartbeatDeliveryReply> replies;
    for (const ui64 sourceDataShardId : InFlight) {
        replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Retry, .Error = error});
    }
    for (const ui64 sourceDataShardId : PendingSourceIdQueue) {
        replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Retry, .Error = error});
    }

    PendingHeartbeatsBySourceId.clear();
    PendingSourceIdQueue.clear();
    InFlight.clear();

    return replies;
}

} // namespace NKikimr::NCdcAggregator
