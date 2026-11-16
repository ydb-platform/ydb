#include "cdc_aggregator_heartbeat_delivery.h"

#include <util/generic/yexception.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

namespace {

using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

} // namespace

TCdcAggregatorHeartbeatDelivery::TCdcAggregatorHeartbeatDelivery(ui32 maxBatchSize, ui32 maxWaiters)
    : MaxBatchSize(maxBatchSize)
    , MaxWaiters(maxWaiters)
{
    Y_ENSURE(MaxBatchSize > 0);
}

bool TCdcAggregatorHeartbeatDelivery::AddHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat, const TCdcAggregatorHeartbeatDeliveryWaiter& waiter) {
    if (WaitersCount >= MaxWaiters) {
        return false;
    }

    auto& source = Sources[sourceDataShardId];
    auto inFlight = InFlight.find(sourceDataShardId);
    if (inFlight == InFlight.end() || inFlight->second < heartbeat.Version) {
        if (!source.Pending) {
            PendingSourceIds.push_back(sourceDataShardId);
        }
        if (!source.Pending || source.Pending->Version < heartbeat.Version) {
            source.Pending = heartbeat;
        }
    }

    source.Waiters.push_back(TVersionedWaiter{
        .Waiter = waiter,
        .Version = heartbeat.Version,
    });
    ++WaitersCount;

    return true;
}

TVector<TCdcAggregatorHeartbeatDeliveryItem> TCdcAggregatorHeartbeatDelivery::TakeBatch() {
    Y_ENSURE(InFlight.empty());

    TVector<TCdcAggregatorHeartbeatDeliveryItem> batch;
    while (!PendingSourceIds.empty() && batch.size() < MaxBatchSize) {
        const ui64 sourceDataShardId = PendingSourceIds.front();
        PendingSourceIds.pop_front();

        auto& source = Sources.at(sourceDataShardId);
        InFlight.emplace(sourceDataShardId, source.Pending->Version);
        batch.push_back(TCdcAggregatorHeartbeatDeliveryItem{
            .SourceDataShardId = sourceDataShardId,
            .Heartbeat = std::move(*source.Pending),
        });
        source.Pending.reset();
    }

    return batch;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::CompleteBatch(const TCdcAggregatorHeartbeatDeliveryResult& result) {
    TVector<TCdcAggregatorHeartbeatDeliveryReply> replies;
    for (const auto& [sourceDataShardId, version] : InFlight) {
        auto it = Sources.find(sourceDataShardId);
        Y_ENSURE(it != Sources.end());
        auto& source = it->second;

        if (const auto* writtenVersion = result.WrittenVersions.FindPtr(sourceDataShardId)) {
            for (const auto& waiter : ExtractWaiters(source, *writtenVersion)) {
                replies.push_back({.Waiter = waiter, .Status = EStatus::Written, .WrittenVersion = *writtenVersion});
            }
        } else if (const auto* error = result.RejectionErrors.FindPtr(sourceDataShardId)) {
            for (const auto& waiter : ExtractWaiters(source, version)) {
                replies.push_back({.Waiter = waiter, .Status = EStatus::Rejected, .Error = *error});
            }
        }
        for (const auto& waiter : ExtractWaiters(source, version)) {
            replies.push_back({.Waiter = waiter, .Status = EStatus::Retry, .Error = "Heartbeat is not confirmed"});
        }

        if (!source.Pending && source.Waiters.empty()) {
            Sources.erase(it);
        }
    }

    InFlight.clear();
    return replies;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::FailBatch(const TString& error) {
    if (InFlight.empty()) {
        return {};
    }

    return RetryAllWaiters(error);
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> TCdcAggregatorHeartbeatDelivery::RetryAllWaiters(const TString& error) {
    TVector<TCdcAggregatorHeartbeatDeliveryReply> replies;
    for (auto& [_, source] : Sources) {
        for (const auto& waiter : ExtractWaiters(source, TRowVersion::Max())) {
            replies.push_back({.Waiter = waiter, .Status = EStatus::Retry, .Error = error});
        }
    }

    Sources.clear();
    PendingSourceIds.clear();
    InFlight.clear();

    return replies;
}

TVector<TCdcAggregatorHeartbeatDeliveryWaiter> TCdcAggregatorHeartbeatDelivery::ExtractWaiters(TSource& source, const TRowVersion& upTo) {
    auto covered = std::stable_partition(source.Waiters.begin(), source.Waiters.end(), [&upTo](const TVersionedWaiter& waiter) {
        return upTo < waiter.Version;
    });

    TVector<TCdcAggregatorHeartbeatDeliveryWaiter> waiters;
    for (auto it = covered; it != source.Waiters.end(); ++it) {
        waiters.push_back(it->Waiter);
    }

    WaitersCount -= waiters.size();
    source.Waiters.erase(covered, source.Waiters.end());
    return waiters;
}

} // namespace NKikimr::NCdcAggregator
