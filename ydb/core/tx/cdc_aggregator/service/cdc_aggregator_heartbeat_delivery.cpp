#include "cdc_aggregator_heartbeat_delivery.h"

#include <util/generic/yexception.h>

namespace NKikimr::NCdcAggregator {

namespace {

using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

ui64 PopLeft(TDeque<ui64>& queue) {
    const ui64 front = queue.front();
    queue.pop_front();
    return front;
}

NPQ::THeartbeat PopHeartbeat(THashMap<ui64, NPQ::THeartbeat>& heartbeatsBySourceDataShardId, ui64 sourceDataShardId) {
    NPQ::THeartbeat heartbeat = std::move(heartbeatsBySourceDataShardId.at(sourceDataShardId));
    heartbeatsBySourceDataShardId.erase(sourceDataShardId);
    return heartbeat;
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
        const ui64 sourceDataShardId = PopLeft(PendingSourceDataShardIdQueue);
        batch[sourceDataShardId] = PopHeartbeat(PendingHeartbeatsBySourceDataShardId, sourceDataShardId);
        AwaitingReplySourceDataShardIds.push_back(sourceDataShardId);
    }

    return batch;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::CompleteBatch(const TCdcAggregatorHeartbeatBatchResult& tabletBatchResult) {
    TVector<TCdcAggregatorHeartbeatDeliveryReply> replies;
    for (const ui64 sourceDataShardId : AwaitingReplySourceDataShardIds) {
        if (const auto* writtenVersion = tabletBatchResult.WrittenVersionsBySourceDataShardId.FindPtr(sourceDataShardId)) {
            replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Written, .WrittenVersion = *writtenVersion});
        } else if (const auto* error = tabletBatchResult.RejectionErrorsBySourceDataShardId.FindPtr(sourceDataShardId)) {
            replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Rejected, .Error = *error});
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
