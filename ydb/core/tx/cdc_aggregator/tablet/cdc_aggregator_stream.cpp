#include "cdc_aggregator_stream.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

bool ParseCdcAggregatorStreamTopic(const NKikimrCdcAggregator::TTrackingConfig& proto, TCdcAggregatorStreamTopic& topic, TString& error) {
    topic.StreamPathId = TPathId::FromProto(proto.GetStreamPathId());
    if (!topic.StreamPathId.OwnerId || !topic.StreamPathId.LocalPathId) {
        error = "Stream path id is not set";
        return false;
    }
    topic.StreamPath = proto.GetStreamPath();

    auto& partitions = topic.Partitions;
    for (const auto& partition : proto.GetTargetPartitions()) {
        if (!partition.GetTabletId()) {
            error = TStringBuilder() << "Partition " << partition.GetPartitionId() << " has no tablet id";
            return false;
        }
        partitions.push_back({.PartitionId = partition.GetPartitionId(), .TabletId = partition.GetTabletId()});
    }
    if (partitions.empty()) {
        error = "Target partitions are missing";
        return false;
    }
    std::sort(partitions.begin(), partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId < right.PartitionId;
    });
    const auto duplicate = std::adjacent_find(partitions.begin(), partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId == right.PartitionId;
    });
    if (duplicate != partitions.end()) {
        error = TStringBuilder() << "Duplicate partition " << duplicate->PartitionId;
        return false;
    }

    return true;
}

bool ParseCdcAggregatorStreamConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TCdcAggregatorStreamConfig& config, TString& error) {
    if (!ParseCdcAggregatorStreamTopic(proto, config.Topic, error)) {
        return false;
    }

    auto& sources = config.SourceDataShardIds;
    sources.assign(proto.GetSourceDataShardIds().begin(), proto.GetSourceDataShardIds().end());
    std::sort(sources.begin(), sources.end());
    if (sources.empty() || sources.front() == 0) {
        error = "Source DataShard ids are missing or zero";
        return false;
    }
    if (std::adjacent_find(sources.begin(), sources.end()) != sources.end()) {
        error = "Duplicate source DataShard id";
        return false;
    }

    return true;
}

TString SerializeCdcAggregatorStreamTopic(const TCdcAggregatorStreamTopic& topic) {
    NKikimrCdcAggregator::TTrackingConfig proto;
    topic.StreamPathId.ToProto(proto.MutableStreamPathId());
    proto.SetStreamPath(topic.StreamPath);
    for (const auto& partition : topic.Partitions) {
        auto& protoPartition = *proto.AddTargetPartitions();
        protoPartition.SetPartitionId(partition.PartitionId);
        protoPartition.SetTabletId(partition.TabletId);
    }
    return proto.SerializeAsString();
}

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

bool TCdcAggregatorStream::IsSameConfig(const TCdcAggregatorStreamConfig& config) const {
    return Topic == config.Topic
        && SourceDataShardIds.size() == config.SourceDataShardIds.size()
        && std::all_of(config.SourceDataShardIds.begin(), config.SourceDataShardIds.end(), [this](ui64 sourceDataShardId) {
            return SourceDataShardIds.contains(sourceDataShardId);
        });
}

TCdcAggregatorStreamUpdateSourcesResult TCdcAggregatorStream::UpdateSources(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds) {
    TCdcAggregatorStreamUpdateSourcesResult result;
    if (!SourceDataShardIds.contains(parentDataShardId)) {
        const bool applied = std::all_of(childDataShardIds.begin(), childDataShardIds.end(), [this](ui64 childDataShardId) {
            return SourceDataShardIds.contains(childDataShardId);
        });
        result.Outcome = applied ? ESourceOutcome::Success : ESourceOutcome::UnknownSource;
        return result;
    }

    SourceDataShardIds.erase(parentDataShardId);
    result.Diff.Deleted.insert(parentDataShardId);
    for (const auto childDataShardId : childDataShardIds) {
        if (SourceDataShardIds.insert(childDataShardId).second) {
            result.Diff.Inserted.insert(childDataShardId);
        }
    }

    return result;
}

} // namespace NKikimr::NCdcAggregator
