#include "cdc_aggregator_stream_proto.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

std::pair<std::optional<TCdcAggregatorStreamTopic>, TString> TCdcAggregatorStreamProtoConverter::ParseTopic(const NKikimrCdcAggregator::TTrackingConfig& proto) const {
    TCdcAggregatorStreamTopic topic;
    topic.StreamPathId = TPathId::FromProto(proto.GetStreamPathId());
    if (!topic.StreamPathId.OwnerId || !topic.StreamPathId.LocalPathId) {
        return {std::nullopt, "Stream path id is not set"};
    }
    topic.StreamPath = proto.GetStreamPath();

    auto& partitions = topic.Partitions;
    for (const auto& partition : proto.GetTargetPartitions()) {
        if (!partition.GetTabletId()) {
            return {std::nullopt, TStringBuilder() << "Partition " << partition.GetPartitionId() << " has no tablet id"};
        }
        partitions.push_back({.PartitionId = partition.GetPartitionId(), .TabletId = partition.GetTabletId()});
    }
    if (partitions.empty()) {
        return {std::nullopt, "Target partitions are missing"};
    }
    std::sort(partitions.begin(), partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId < right.PartitionId;
    });
    const auto duplicate = std::adjacent_find(partitions.begin(), partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId == right.PartitionId;
    });
    if (duplicate != partitions.end()) {
        return {std::nullopt, TStringBuilder() << "Duplicate partition " << duplicate->PartitionId};
    }

    return {std::move(topic), {}};
}

std::pair<std::optional<TCdcAggregatorStreamConfig>, TString> TCdcAggregatorStreamProtoConverter::ParseConfig(const NKikimrCdcAggregator::TTrackingConfig& proto) const {
    auto [topic, error] = ParseTopic(proto);
    if (!topic) {
        return {std::nullopt, std::move(error)};
    }

    TVector<ui64> sources(proto.GetSourceDataShardIds().begin(), proto.GetSourceDataShardIds().end());
    std::sort(sources.begin(), sources.end());
    if (sources.empty() || sources.front() == 0) {
        return {std::nullopt, "Source DataShard ids are missing or zero"};
    }
    if (std::adjacent_find(sources.begin(), sources.end()) != sources.end()) {
        return {std::nullopt, "Duplicate source DataShard id"};
    }

    return {TCdcAggregatorStreamConfig{.Topic = std::move(*topic), .SourceDataShardIds = std::move(sources)}, {}};
}

TString TCdcAggregatorStreamProtoConverter::SerializeTopic(const TCdcAggregatorStreamTopic& topic) const {
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
