#include "cdc_aggregator_stream_proto.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

namespace {

template <typename TProto>
std::pair<std::optional<TCdcAggregatorStreamTopic>, TString> ParseTopicFields(const TProto& proto) {
    TCdcAggregatorStreamTopic topic;
    if (!proto.HasStreamPathId()) {
        return {std::nullopt, TString("Stream path id is not set")};
    }
    topic.StreamPathId = TPathId::FromProto(proto.GetStreamPathId());
    if (!topic.StreamPathId || !topic.StreamPathId.OwnerId || !topic.StreamPathId.LocalPathId) {
        return {std::nullopt, TString("Stream path id is invalid")};
    }
    topic.StreamPath = proto.GetStreamPath();
    if (topic.StreamPath.empty()) {
        return {std::nullopt, TString("Stream path is not set")};
    }

    auto& partitions = topic.Partitions;
    for (const auto& partition : proto.GetTargetPartitions()) {
        if (!partition.GetTabletId()) {
            return {std::nullopt, TStringBuilder() << "Partition " << partition.GetPartitionId() << " has no tablet id"};
        }
        partitions.push_back({.PartitionId = partition.GetPartitionId(), .TabletId = partition.GetTabletId()});
    }
    if (partitions.empty()) {
        return {std::nullopt, TString("Target partitions are missing")};
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

    return {std::move(topic), TString()};
}

} // namespace

std::pair<std::optional<TCdcAggregatorStreamConfig>, TString> TCdcAggregatorStreamProtoConverter::ParseConfig(const NKikimrCdcAggregator::TTrackingConfig& proto) const {
    auto [topic, error] = ParseTopicFields(proto);
    if (!topic) {
        return {std::nullopt, std::move(error)};
    }

    TVector<ui64> sources(proto.GetSourceDataShardIds().begin(), proto.GetSourceDataShardIds().end());
    std::sort(sources.begin(), sources.end());
    if (sources.empty() || sources.front() == 0) {
        return {std::nullopt, TString("Source DataShard ids are missing or zero")};
    }
    if (std::adjacent_find(sources.begin(), sources.end()) != sources.end()) {
        return {std::nullopt, TString("Duplicate source DataShard id")};
    }

    return {TCdcAggregatorStreamConfig{.Topic = std::move(*topic), .SourceDataShardIds = std::move(sources)}, TString()};
}

std::pair<std::optional<TCdcAggregatorStreamTopic>, TString> TCdcAggregatorStreamProtoConverter::ParseTopic(const NKikimrCdcAggregator::TCdcAggregatorStreamTopic& proto) const {
    return ParseTopicFields(proto);
}

std::pair<std::optional<TCdcAggregatorStreamPartitioningChange>, TString> TCdcAggregatorStreamProtoConverter::ParsePartitioningChange(const NKikimrCdcAggregator::TEvPartitioningChanged& proto) const {
    TCdcAggregatorStreamPartitioningChange change{
        .OperationId = proto.GetOperationId(),
        .ParentDataShardIds = {proto.GetParentDataShardIds().begin(), proto.GetParentDataShardIds().end()},
        .ChildDataShardIds = {proto.GetChildDataShardIds().begin(), proto.GetChildDataShardIds().end()},
    };
    if (proto.HasParentDataShardId()) {
        change.ParentDataShardIds.push_back(proto.GetParentDataShardId());
    }
    std::sort(change.ParentDataShardIds.begin(), change.ParentDataShardIds.end());
    std::sort(change.ChildDataShardIds.begin(), change.ChildDataShardIds.end());

    const auto& parents = change.ParentDataShardIds;
    const auto& children = change.ChildDataShardIds;
    if (!change.OperationId
        || parents.empty() || parents.front() == 0
        || std::adjacent_find(parents.begin(), parents.end()) != parents.end()
        || children.empty() || children.front() == 0
        || std::adjacent_find(children.begin(), children.end()) != children.end()
        || std::any_of(parents.begin(), parents.end(), [&children](ui64 parentDataShardId) {
            return std::binary_search(children.begin(), children.end(), parentDataShardId);
        })) {
        return {std::nullopt, TStringBuilder() << "Invalid partitioning change " << change.OperationId};
    }

    return {std::move(change), TString()};
}

TString TCdcAggregatorStreamProtoConverter::SerializeTopic(const TCdcAggregatorStreamTopic& topic) const {
    NKikimrCdcAggregator::TCdcAggregatorStreamTopic proto;
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
