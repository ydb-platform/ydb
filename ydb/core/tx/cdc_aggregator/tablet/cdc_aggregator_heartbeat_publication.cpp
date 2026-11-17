#include "cdc_aggregator_heartbeat_publication.h"

#include <algorithm>

namespace NKikimr::NCdcAggregator {

TCdcAggregatorHeartbeatPublication::TCdcAggregatorHeartbeatPublication(const TVector<ui32>& partitionIds) {
    for (const auto partitionId : partitionIds) {
        EmittedVersionByPartitionId.emplace(partitionId, TRowVersion::Min());
    }
}

void TCdcAggregatorHeartbeatPublication::SetHeartbeat(const NPQ::THeartbeat& heartbeat) {
    if (!Heartbeat || Heartbeat->Version < heartbeat.Version) {
        Heartbeat = heartbeat;
    }
}

TVector<ui32> TCdcAggregatorHeartbeatPublication::TakePartitionsToEmit() {
    TVector<ui32> partitionIds;
    if (!Heartbeat) {
        return partitionIds;
    }

    for (const auto& [partitionId, emittedVersion] : EmittedVersionByPartitionId) {
        if (emittedVersion < Heartbeat->Version
            && !InFlightVersionByPartitionId.contains(partitionId)
            && !FailedPartitionIds.contains(partitionId))
        {
            InFlightVersionByPartitionId.emplace(partitionId, Heartbeat->Version);
            partitionIds.push_back(partitionId);
        }
    }
    std::sort(partitionIds.begin(), partitionIds.end());
    return partitionIds;
}

bool TCdcAggregatorHeartbeatPublication::CompleteEmit(ui32 partitionId, const TRowVersion& emittedVersion) {
    const auto inFlight = InFlightVersionByPartitionId.find(partitionId);
    if (inFlight == InFlightVersionByPartitionId.end()) {
        return false;
    }
    const auto inFlightVersion = inFlight->second;
    InFlightVersionByPartitionId.erase(inFlight);

    auto& partitionEmittedVersion = EmittedVersionByPartitionId.at(partitionId);
    partitionEmittedVersion = std::max(partitionEmittedVersion, emittedVersion);
    if (partitionEmittedVersion < inFlightVersion) {
        FailedPartitionIds.insert(partitionId);
        return false;
    }
    if (partitionEmittedVersion < Heartbeat->Version) {
        InFlightVersionByPartitionId.emplace(partitionId, Heartbeat->Version);
        return true;
    }
    return false;
}

void TCdcAggregatorHeartbeatPublication::FailEmit(ui32 partitionId) {
    if (InFlightVersionByPartitionId.erase(partitionId)) {
        FailedPartitionIds.insert(partitionId);
    }
}

void TCdcAggregatorHeartbeatPublication::RetryFailedEmits() {
    FailedPartitionIds.clear();
}

} // namespace NKikimr::NCdcAggregator
