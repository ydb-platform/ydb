#include "cdc_aggregator_stream.h"

namespace NKikimr::NCdcAggregator {

namespace {

std::optional<NPQ::THeartbeat> FindMinLastHeartbeat(const TCdcAggregatorStream::TLastHeartbeats& lastHeartbeatBySourceDataShardId) {
    std::optional<NPQ::THeartbeat> minHeartbeat;
    for (const auto& [_, lastHeartbeat] : lastHeartbeatBySourceDataShardId) {
        if (!lastHeartbeat) {
            return std::nullopt;
        }
        if (!minHeartbeat || lastHeartbeat->Version < minHeartbeat->Version) {
            minHeartbeat = lastHeartbeat;
        }
    }
    return minHeartbeat;
}

} // namespace

TCdcAggregatorStream::TCdcAggregatorStream(const TCdcAggregatorStreamConfig& config)
    : Topic(config.Topic)
{
    for (const auto sourceDataShardId : config.SourceDataShardIds) {
        LastHeartbeatBySourceDataShardId.emplace(sourceDataShardId, std::nullopt);
    }
}

TCdcAggregatorStream::TCdcAggregatorStream(
        TCdcAggregatorStreamTopic topic,
        TLastHeartbeats lastHeartbeatBySourceDataShardId,
        std::optional<NPQ::THeartbeat> currentHeartbeat,
        TPartitioningChanges partitioningChanges)
    : Topic(std::move(topic))
    , LastHeartbeatBySourceDataShardId(std::move(lastHeartbeatBySourceDataShardId))
    , CurrentHeartbeat(std::move(currentHeartbeat))
    , PartitioningChanges(std::move(partitioningChanges))
{
}

TCdcAggregatorStreamHeartbeatsDiff TCdcAggregatorStream::WriteHeartbeats(const THashMap<ui64, NPQ::THeartbeat>& heartbeatBySourceDataShardId) {
    return {
        .LastHeartbeatBySourceDataShardId = RegisterHeartbeats(heartbeatBySourceDataShardId),
        .CurrentHeartbeat = UpdateCurrentHeartbeatIfNeeded(),
    };
}

THashMap<ui64, NPQ::THeartbeat> TCdcAggregatorStream::RegisterHeartbeats(const THashMap<ui64, NPQ::THeartbeat>& heartbeatBySourceDataShardId) {
    THashMap<ui64, NPQ::THeartbeat> registeredHeartbeats;
    for (const auto& [sourceDataShardId, heartbeat] : heartbeatBySourceDataShardId) {
        auto lastHeartbeat = LastHeartbeatBySourceDataShardId.find(sourceDataShardId);
        if (lastHeartbeat == LastHeartbeatBySourceDataShardId.end()
            || (lastHeartbeat->second && heartbeat.Version <= lastHeartbeat->second->Version))
        {
            continue;
        }
        lastHeartbeat->second = heartbeat;
        registeredHeartbeats.emplace(sourceDataShardId, heartbeat);
    }
    return registeredHeartbeats;
}

std::optional<NPQ::THeartbeat> TCdcAggregatorStream::UpdateCurrentHeartbeatIfNeeded() {
    auto minHeartbeat = FindMinLastHeartbeat(LastHeartbeatBySourceDataShardId);
    if (!minHeartbeat || (CurrentHeartbeat && minHeartbeat->Version <= CurrentHeartbeat->Version)) {
        return std::nullopt;
    }
    CurrentHeartbeat = minHeartbeat;
    return minHeartbeat;
}

std::optional<TCdcAggregatorStreamSourcesDiff> TCdcAggregatorStream::ApplyPartitioningChange(const TCdcAggregatorStreamPartitioningChange& change) {
    auto key = std::make_pair(change.ParentDataShardId, change.OperationId);
    if (const auto applied = PartitioningChanges.find(key); applied != PartitioningChanges.end()) {
        if (applied->second != change.ChildDataShardIds) {
            return std::nullopt;
        }
        return TCdcAggregatorStreamSourcesDiff{};
    }

    if (!LastHeartbeatBySourceDataShardId.erase(change.ParentDataShardId)) {
        return std::nullopt;
    }

    TCdcAggregatorStreamSourcesDiff diff;
    diff.Deleted.insert(change.ParentDataShardId);
    for (const auto childDataShardId : change.ChildDataShardIds) {
        if (LastHeartbeatBySourceDataShardId.emplace(childDataShardId, std::nullopt).second) {
            diff.Inserted.insert(childDataShardId);
        }
    }
    PartitioningChanges.emplace(std::move(key), change.ChildDataShardIds);

    return diff;
}

} // namespace NKikimr::NCdcAggregator
