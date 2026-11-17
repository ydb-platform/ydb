#include "cdc_aggregator_stream.h"

#include <util/generic/algorithm.h>

namespace NKikimr::NCdcAggregator {

TCdcAggregatorStream::TCdcAggregatorStream(const TCdcAggregatorStreamConfig& config)
    : Topic(config.Topic)
    , SourceDataShardIds(config.SourceDataShardIds.begin(), config.SourceDataShardIds.end())
{
}

TCdcAggregatorStream::TCdcAggregatorStream(TCdcAggregatorStreamTopic topic, THashSet<ui64> sourceDataShardIds, TPartitioningChanges partitioningChanges)
    : Topic(std::move(topic))
    , SourceDataShardIds(std::move(sourceDataShardIds))
    , PartitioningChanges(std::move(partitioningChanges))
{
}

std::optional<TCdcAggregatorStreamSourcesDiff> TCdcAggregatorStream::ApplyPartitioningChange(const TCdcAggregatorStreamPartitioningChange& change) {
    auto key = std::make_pair(change.ParentDataShardId, change.OperationId);
    if (const auto applied = PartitioningChanges.find(key); applied != PartitioningChanges.end()) {
        if (applied->second != change.ChildDataShardIds) {
            return std::nullopt;
        }
        return TCdcAggregatorStreamSourcesDiff{};
    }

    if (!SourceDataShardIds.erase(change.ParentDataShardId)) {
        return std::nullopt;
    }

    TCdcAggregatorStreamSourcesDiff diff;
    diff.Deleted.insert(change.ParentDataShardId);
    for (const auto childDataShardId : change.ChildDataShardIds) {
        if (SourceDataShardIds.insert(childDataShardId).second) {
            diff.Inserted.insert(childDataShardId);
        }
    }
    PartitioningChanges.emplace(std::move(key), change.ChildDataShardIds);

    for (auto it = PartitioningChanges.begin(); it != PartitioningChanges.end();) {
        if (AnyOf(it->second, [this](ui64 childDataShardId) { return SourceDataShardIds.contains(childDataShardId); })) {
            ++it;
        } else {
            diff.DeletedPartitioningChanges.push_back(it->first);
            it = PartitioningChanges.erase(it);
        }
    }

    return diff;
}

} // namespace NKikimr::NCdcAggregator
