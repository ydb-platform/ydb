#include "cdc_aggregator_stream.h"

#include <algorithm>

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
    const auto key = std::make_pair(change.ParentDataShardIds.front(), change.OperationId);
    if (const auto applied = PartitioningChanges.find(key); applied != PartitioningChanges.end()) {
        if (applied->second != change) {
            return std::nullopt;
        }
        return TCdcAggregatorStreamSourcesDiff{};
    }

    if (!std::all_of(change.ParentDataShardIds.begin(), change.ParentDataShardIds.end(), [this](ui64 parentDataShardId) {
        return SourceDataShardIds.contains(parentDataShardId);
    })) {
        return std::nullopt;
    }

    TCdcAggregatorStreamSourcesDiff diff;
    for (const auto parentDataShardId : change.ParentDataShardIds) {
        SourceDataShardIds.erase(parentDataShardId);
        diff.Deleted.insert(parentDataShardId);
    }
    for (const auto childDataShardId : change.ChildDataShardIds) {
        if (SourceDataShardIds.insert(childDataShardId).second) {
            diff.Inserted.insert(childDataShardId);
        }
    }
    PartitioningChanges.emplace(key, change);

    return diff;
}

} // namespace NKikimr::NCdcAggregator
