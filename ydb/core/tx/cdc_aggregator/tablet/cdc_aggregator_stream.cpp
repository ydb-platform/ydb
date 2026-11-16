#include "cdc_aggregator_stream.h"

namespace NKikimr::NCdcAggregator {

namespace {

std::optional<NPQ::THeartbeat> FindMinLastHeartbeat(const TCdcAggregatorStream::TLastHeartbeats& lastHeartbeats) {
    std::optional<NPQ::THeartbeat> minHeartbeat;
    for (const auto& [_, lastHeartbeat] : lastHeartbeats) {
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
        LastHeartbeats.emplace(sourceDataShardId, std::nullopt);
    }
}

TCdcAggregatorStream::TCdcAggregatorStream(
        TCdcAggregatorStreamTopic topic,
        TLastHeartbeats lastHeartbeats,
        std::optional<NPQ::THeartbeat> currentHeartbeat,
        TPartitioningChanges partitioningChanges)
    : Topic(std::move(topic))
    , LastHeartbeats(std::move(lastHeartbeats))
    , CurrentHeartbeat(std::move(currentHeartbeat))
    , PartitioningChanges(std::move(partitioningChanges))
{
}

TCdcAggregatorStreamHeartbeatsDiff TCdcAggregatorStream::WriteHeartbeats(const THashMap<ui64, NPQ::THeartbeat>& heartbeatBySourceDataShardId) {
    return {
        .LastHeartbeats = RegisterHeartbeats(heartbeatBySourceDataShardId),
        .CurrentHeartbeat = UpdateCurrentHeartbeatIfNeeded(),
    };
}

THashMap<ui64, NPQ::THeartbeat> TCdcAggregatorStream::RegisterHeartbeats(const THashMap<ui64, NPQ::THeartbeat>& heartbeatBySourceDataShardId) {
    THashMap<ui64, NPQ::THeartbeat> lastHeartbeats;
    for (const auto& [sourceDataShardId, heartbeat] : heartbeatBySourceDataShardId) {
        auto lastHeartbeat = LastHeartbeats.find(sourceDataShardId);
        if (lastHeartbeat == LastHeartbeats.end()) {
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
    auto minHeartbeat = FindMinLastHeartbeat(LastHeartbeats);
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

    if (!LastHeartbeats.erase(change.ParentDataShardId)) {
        return std::nullopt;
    }

    TCdcAggregatorStreamSourcesDiff diff;
    diff.Deleted.insert(change.ParentDataShardId);
    for (const auto childDataShardId : change.ChildDataShardIds) {
        if (LastHeartbeats.emplace(childDataShardId, std::nullopt).second) {
            diff.Inserted.insert(childDataShardId);
        }
    }
    PartitioningChanges.emplace(std::move(key), change.ChildDataShardIds);

    return diff;
}

} // namespace NKikimr::NCdcAggregator
