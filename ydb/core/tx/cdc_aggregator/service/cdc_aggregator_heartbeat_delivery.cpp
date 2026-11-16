#include "cdc_aggregator_heartbeat_delivery.h"

#include <util/generic/yexception.h>

namespace NKikimr::NCdcAggregator {

namespace {

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

THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::CompleteBatch(const TCdcAggregatorHeartbeatBatchResult& tabletBatchResult) {
    THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply> repliesBySourceDataShardId;
    for (const ui64 sourceDataShardId : AwaitingReplySourceDataShardIds) {
        if (const auto* writtenVersion = tabletBatchResult.WrittenVersionsBySourceDataShardId.FindPtr(sourceDataShardId)) {
            repliesBySourceDataShardId.emplace(sourceDataShardId, TCdcAggregatorHeartbeatWritten{.Version = *writtenVersion});
        } else if (const auto* error = tabletBatchResult.RejectionErrorsBySourceDataShardId.FindPtr(sourceDataShardId)) {
            repliesBySourceDataShardId.emplace(sourceDataShardId, TCdcAggregatorHeartbeatRejected{.Error = *error});
        } else {
            repliesBySourceDataShardId.emplace(sourceDataShardId, TCdcAggregatorHeartbeatRetry{.Error = "Heartbeat is not confirmed"});
        }
    }

    AwaitingReplySourceDataShardIds.clear();
    return repliesBySourceDataShardId;
}

THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::FailBatch(const TString& error) {
    if (AwaitingReplySourceDataShardIds.empty()) {
        return {};
    }

    THashMap<ui64, TCdcAggregatorHeartbeatDeliveryReply> repliesBySourceDataShardId;
    for (const ui64 sourceDataShardId : AwaitingReplySourceDataShardIds) {
        repliesBySourceDataShardId.emplace(sourceDataShardId, TCdcAggregatorHeartbeatRetry{.Error = error});
    }
    for (const ui64 sourceDataShardId : PendingSourceDataShardIdQueue) {
        repliesBySourceDataShardId.emplace(sourceDataShardId, TCdcAggregatorHeartbeatRetry{.Error = error});
    }

    PendingHeartbeatsBySourceDataShardId.clear();
    PendingSourceDataShardIdQueue.clear();
    AwaitingReplySourceDataShardIds.clear();

    return repliesBySourceDataShardId;
}

} // namespace NKikimr::NCdcAggregator
