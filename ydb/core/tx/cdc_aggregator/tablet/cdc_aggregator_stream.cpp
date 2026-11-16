#include "cdc_aggregator_stream.h"

#include <algorithm>

namespace NKikimr::NCdcAggregator {

TCdcAggregatorStream::TCdcAggregatorStream(const TCdcAggregatorStreamConfig& config)
    : Topic(config.Topic)
    , SourceDataShardIds(config.SourceDataShardIds.begin(), config.SourceDataShardIds.end())
{
}

TCdcAggregatorStream::TCdcAggregatorStream(TCdcAggregatorStreamTopic topic, THashSet<ui64> sourceDataShardIds)
    : Topic(std::move(topic))
    , SourceDataShardIds(std::move(sourceDataShardIds))
{
}

std::optional<TCdcAggregatorStreamSourcesDiff> TCdcAggregatorStream::ApplyPartitioningChange(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds) {
    if (!SourceDataShardIds.contains(parentDataShardId)) {
        const bool applied = std::all_of(childDataShardIds.begin(), childDataShardIds.end(), [this](ui64 childDataShardId) {
            return SourceDataShardIds.contains(childDataShardId);
        });
        if (!applied) {
            return std::nullopt;
        }
        return TCdcAggregatorStreamSourcesDiff{};
    }

    TCdcAggregatorStreamSourcesDiff diff;
    SourceDataShardIds.erase(parentDataShardId);
    diff.Deleted.insert(parentDataShardId);
    for (const auto childDataShardId : childDataShardIds) {
        if (SourceDataShardIds.insert(childDataShardId).second) {
            diff.Inserted.insert(childDataShardId);
        }
    }

    return diff;
}

} // namespace NKikimr::NCdcAggregator
