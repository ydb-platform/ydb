#include "cdc_aggregator_stream.h"

#include <algorithm>

namespace NKikimr::NCdcAggregator {

namespace {

std::optional<TRowVersion> GetVersion(const std::optional<NPQ::THeartbeat>& heartbeat) {
    if (!heartbeat) {
        return std::nullopt;
    }
    return heartbeat->Version;
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
    const auto& [_, minHeartbeat] = *std::min_element(LastHeartbeatsBySourceDataShardId.begin(), LastHeartbeatsBySourceDataShardId.end(),
        [](const auto& left, const auto& right) {
            return GetVersion(left.second) < GetVersion(right.second);
        });
    if (GetVersion(minHeartbeat) <= GetVersion(CurrentHeartbeat)) {
        return std::nullopt;
    }
    CurrentHeartbeat = minHeartbeat;
    return CurrentHeartbeat;
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
