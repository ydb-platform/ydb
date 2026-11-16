#include "cdc_aggregator_stream_proto.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

std::optional<TCdcAggregatorStreamTopic> ParseCdcAggregatorStreamTopic(const NKikimrCdcAggregator::TTrackingConfig& proto, TString& error) {
    TCdcAggregatorStreamTopic topic;
    topic.StreamPathId = TPathId::FromProto(proto.GetStreamPathId());
    if (!topic.StreamPathId.OwnerId || !topic.StreamPathId.LocalPathId) {
        error = "Stream path id is not set";
        return std::nullopt;
    }
    topic.StreamPath = proto.GetStreamPath();

    auto& partitions = topic.Partitions;
    for (const auto& partition : proto.GetTargetPartitions()) {
        if (!partition.GetTabletId()) {
            error = TStringBuilder() << "Partition " << partition.GetPartitionId() << " has no tablet id";
            return std::nullopt;
        }
        partitions.push_back({.PartitionId = partition.GetPartitionId(), .TabletId = partition.GetTabletId()});
    }
    if (partitions.empty()) {
        error = "Target partitions are missing";
        return std::nullopt;
    }
    std::sort(partitions.begin(), partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId < right.PartitionId;
    });
    const auto duplicate = std::adjacent_find(partitions.begin(), partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId == right.PartitionId;
    });
    if (duplicate != partitions.end()) {
        error = TStringBuilder() << "Duplicate partition " << duplicate->PartitionId;
        return std::nullopt;
    }

    return topic;
}

std::optional<TCdcAggregatorStreamConfig> ParseCdcAggregatorStreamConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TString& error) {
    auto topic = ParseCdcAggregatorStreamTopic(proto, error);
    if (!topic) {
        return std::nullopt;
    }

    TCdcAggregatorStreamConfig config{.Topic = std::move(*topic)};
    auto& sources = config.SourceDataShardIds;
    sources.assign(proto.GetSourceDataShardIds().begin(), proto.GetSourceDataShardIds().end());
    std::sort(sources.begin(), sources.end());
    if (sources.empty() || sources.front() == 0) {
        error = "Source DataShard ids are missing or zero";
        return std::nullopt;
    }
    if (std::adjacent_find(sources.begin(), sources.end()) != sources.end()) {
        error = "Duplicate source DataShard id";
        return std::nullopt;
    }

    return config;
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

} // namespace NKikimr::NCdcAggregator
