#include "cdc_aggregator_stream_proto.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

namespace {

template <typename TProto>
TParseResult<TCdcAggregatorStreamTopic> ParseTopicFields(const TProto& proto) {
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

TParseResult<TCdcAggregatorStreamConfig> TCdcAggregatorStreamProtoConverter::ParseConfig(const NKikimrCdcAggregator::TTrackingConfig& proto) const {
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

TParseResult<TCdcAggregatorStreamTopic> TCdcAggregatorStreamProtoConverter::ParseTopic(const NKikimrCdcAggregator::TCdcAggregatorStreamTopic& proto) const {
    return ParseTopicFields(proto);
}

TParseResult<TCdcAggregatorStreamPartitioningChange> TCdcAggregatorStreamProtoConverter::ParsePartitioningChange(
    const NKikimrCdcAggregator::TEvPartitioningChanged& proto) const
{
    TCdcAggregatorStreamPartitioningChange change{
        .OperationId = proto.GetOperationId(),
        .ParentDataShardId = proto.GetParentDataShardId(),
        .ChildDataShardIds = {proto.GetChildDataShardIds().begin(), proto.GetChildDataShardIds().end()},
    };
    auto& children = change.ChildDataShardIds;
    std::sort(children.begin(), children.end());

    if (!change.OperationId
        || !change.ParentDataShardId
        || children.empty() || children.front() == 0
        || std::adjacent_find(children.begin(), children.end()) != children.end()
        || std::binary_search(children.begin(), children.end(), change.ParentDataShardId))
    {
        return {std::nullopt, TStringBuilder() << "Invalid partitioning change " << change.OperationId
            << " of DataShard " << change.ParentDataShardId};
    }

    return {std::move(change), TString()};
}

TCdcAggregatorStreamHeartbeatsBatch TCdcAggregatorStreamProtoConverter::ParseHeartbeatsBatch(const NKikimrCdcAggregator::TEvWriteHeartbeats& proto) const {
    TCdcAggregatorStreamHeartbeatsBatch batch;
    for (const auto& sourceHeartbeat : proto.GetHeartbeats()) {
        const ui64 sourceDataShardId = sourceHeartbeat.GetSourceDataShardId();
        auto heartbeat = NPQ::THeartbeat::Parse(sourceHeartbeat.GetHeartbeat());
        if (!heartbeat.Version.Step || heartbeat.Data.empty()) {
            batch.InvalidSourceDataShardIds.insert(sourceDataShardId);
            continue;
        }
        const auto [kept, inserted] = batch.HeartbeatsBySourceDataShardId.emplace(sourceDataShardId, heartbeat);
        if (!inserted && kept->second.Version < heartbeat.Version) {
            kept->second = std::move(heartbeat);
        }
    }
    return batch;
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

NKikimrCdcAggregator::TEvWriteHeartbeatsResult TCdcAggregatorStreamProtoConverter::SerializeWriteHeartbeatsResult(
    const TCdcAggregatorStreamHeartbeatsBatch& batch, const TCdcAggregatorStreamHeartbeatsDiff& diff) const
{
    using TResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

    TResult proto;
    proto.SetStatus(TResult::SUCCESS);
    for (const auto& [sourceDataShardId, _] : batch.HeartbeatsBySourceDataShardId) {
        auto& result = *proto.AddResults();
        result.SetSourceDataShardId(sourceDataShardId);
        if (const auto* lastHeartbeat = diff.LastHeartbeatsBySourceDataShardId.FindPtr(sourceDataShardId)) {
            result.SetStatus(TResult::THeartbeatResult::SUCCESS);
            lastHeartbeat->Version.ToProto(result.MutableWrittenVersion());
        } else {
            result.SetStatus(TResult::THeartbeatResult::UNKNOWN_SOURCE);
        }
    }
    for (const auto sourceDataShardId : batch.InvalidSourceDataShardIds) {
        auto& result = *proto.AddResults();
        result.SetSourceDataShardId(sourceDataShardId);
        result.SetStatus(TResult::THeartbeatResult::BAD_REQUEST);
    }
    return proto;
}

NKikimrCdcAggregator::TEvDescribeTrackingResult TCdcAggregatorStreamProtoConverter::SerializeDescribeTrackingResult(const TCdcAggregatorStream& stream) const {
    NKikimrCdcAggregator::TEvDescribeTrackingResult proto;

    TMap<ui64, std::optional<TRowVersion>> lastVersionsBySourceDataShardId;
    for (const auto& [dataShardId, lastHeartbeat] : stream.GetLastHeartbeatsBySourceDataShardId()) {
        lastVersionsBySourceDataShardId.emplace(dataShardId, lastHeartbeat ? std::optional(lastHeartbeat->Version) : std::nullopt);
    }
    for (const auto& [dataShardId, lastVersion] : lastVersionsBySourceDataShardId) {
        auto& source = *proto.AddSources();
        source.SetDataShardId(dataShardId);
        if (lastVersion) {
            lastVersion->ToProto(source.MutableLastHeartbeat());
        }
    }

    if (const auto& currentHeartbeat = stream.GetCurrentHeartbeat()) {
        currentHeartbeat->Version.ToProto(proto.MutableCurrentHeartbeat());
    }

    return proto;
}

} // namespace NKikimr::NCdcAggregator
