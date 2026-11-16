#include "cdc_aggregator_heartbeat_delivery.h"

#include <util/generic/yexception.h>

namespace NKikimr::NCdcAggregator {

namespace {

using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

} // namespace

TCdcAggregatorHeartbeatDelivery::TCdcAggregatorHeartbeatDelivery(ui32 maxBatchSize)
    : MaxBatchSize(maxBatchSize)
{
    Y_ENSURE(MaxBatchSize > 0);
}

void TCdcAggregatorHeartbeatDelivery::AddHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat) {
    auto it = Pending.find(sourceDataShardId);
    if (it == Pending.end()) {
        Pending.emplace(sourceDataShardId, heartbeat);
        PendingSourceIds.push_back(sourceDataShardId);
    } else if (it->second.Version < heartbeat.Version) {
        it->second = heartbeat;
    }
}

TVector<TCdcAggregatorHeartbeatDeliveryItem> TCdcAggregatorHeartbeatDelivery::TakeBatch() {
    Y_ENSURE(InFlight.empty());

    TVector<TCdcAggregatorHeartbeatDeliveryItem> batch;
    while (!PendingSourceIds.empty() && batch.size() < MaxBatchSize) {
        const ui64 sourceDataShardId = PendingSourceIds.front();
        PendingSourceIds.pop_front();

        batch.push_back(TCdcAggregatorHeartbeatDeliveryItem{
            .SourceDataShardId = sourceDataShardId,
            .Heartbeat = std::move(Pending.at(sourceDataShardId)),
        });
        Pending.erase(sourceDataShardId);
        InFlight.push_back(sourceDataShardId);
    }

    return batch;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::CompleteBatch(const TCdcAggregatorHeartbeatDeliveryResult& result) {
    TVector<TCdcAggregatorHeartbeatDeliveryReply> replies;
    for (const ui64 sourceDataShardId : InFlight) {
        if (const auto* writtenVersion = result.WrittenVersions.FindPtr(sourceDataShardId)) {
            replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Written, .WrittenVersion = *writtenVersion});
        } else if (const auto* error = result.RejectionErrors.FindPtr(sourceDataShardId)) {
            replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Rejected, .Error = *error});
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
    for (const ui64 sourceDataShardId : PendingSourceIds) {
        replies.push_back({.SourceDataShardId = sourceDataShardId, .Status = EStatus::Retry, .Error = error});
    }

    Pending.clear();
    PendingSourceIds.clear();
    InFlight.clear();

    return replies;
}

} // namespace NKikimr::NCdcAggregator
