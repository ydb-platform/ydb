#include "cdc_aggregator_stream.h"

namespace NKikimr::NCdcAggregator {

namespace {

std::optional<NPQ::THeartbeat> FindMinLastHeartbeat(const TCdcAggregatorStream::TLastHeartbeats& lastHeartbeatsBySourceDataShardId) {
    std::optional<NPQ::THeartbeat> minHeartbeat;
    for (const auto& [_, lastHeartbeat] : lastHeartbeatsBySourceDataShardId) {
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
        LastHeartbeatsBySourceDataShardId.emplace(sourceDataShardId, std::nullopt);
    }
}

TCdcAggregatorStream::TCdcAggregatorStream(
        TCdcAggregatorStreamTopic topic,
        TLastHeartbeats lastHeartbeatsBySourceDataShardId,
        std::optional<NPQ::THeartbeat> currentHeartbeat,
        TPartitioningChanges partitioningChanges)
    : Topic(std::move(topic))
    , LastHeartbeatsBySourceDataShardId(std::move(lastHeartbeatsBySourceDataShardId))
    , CurrentHeartbeat(std::move(currentHeartbeat))
    , PartitioningChanges(std::move(partitioningChanges))
{
}

TCdcAggregatorStreamHeartbeatsDiff TCdcAggregatorStream::WriteHeartbeats(const THashMap<ui64, NPQ::THeartbeat>& heartbeatsBySourceDataShardId) {
    return {
        .LastHeartbeatsBySourceDataShardId = RegisterHeartbeats(heartbeatsBySourceDataShardId),
        .CurrentHeartbeat = UpdateCurrentHeartbeatIfNeeded(),
    };
}

THashMap<ui64, NPQ::THeartbeat> TCdcAggregatorStream::RegisterHeartbeats(const THashMap<ui64, NPQ::THeartbeat>& heartbeatsBySourceDataShardId) {
    THashMap<ui64, NPQ::THeartbeat> lastHeartbeats;
    for (const auto& [sourceDataShardId, heartbeat] : heartbeatsBySourceDataShardId) {
        auto lastHeartbeat = LastHeartbeatsBySourceDataShardId.find(sourceDataShardId);
        if (lastHeartbeat == LastHeartbeatsBySourceDataShardId.end()) {
            continue;
        }
        if (!lastHeartbeat->second || lastHeartbeat->second->Version < heartbeat.Version) {
            lastHeartbeat->second = heartbeat;
        }
        lastHeartbeats.emplace(sourceDataShardId, *lastHeartbeat->second);
    }
    return lastHeartbeats;
}

std::optional<NPQ::THeartbeat> TCdcAggregatorStream::UpdateCurrentHeartbeatIfNeeded() {
    auto minHeartbeat = FindMinLastHeartbeat(LastHeartbeatsBySourceDataShardId);
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

    if (!LastHeartbeatsBySourceDataShardId.erase(change.ParentDataShardId)) {
        return std::nullopt;
    }

    TCdcAggregatorStreamSourcesDiff diff;
    diff.Deleted.insert(change.ParentDataShardId);
    for (const auto childDataShardId : change.ChildDataShardIds) {
        if (LastHeartbeatsBySourceDataShardId.emplace(childDataShardId, std::nullopt).second) {
            diff.Inserted.insert(childDataShardId);
        }
    }
    PartitioningChanges.emplace(std::move(key), change.ChildDataShardIds);

    return diff;
}

} // namespace NKikimr::NCdcAggregator
