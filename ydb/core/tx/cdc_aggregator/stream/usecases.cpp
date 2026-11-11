#include "usecases.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

namespace {

bool ParseTrackingConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TTrackingConfig& config, TString& error) {
    config.StreamPathId = TPathId::FromProto(proto.GetStreamPathId());
    if (!config.StreamPathId || !config.StreamPathId.OwnerId || !config.StreamPathId.LocalPathId) {
        error = "Stream path id is not set";
        return false;
    }
    config.StreamPath = proto.GetStreamPath();

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

    auto& partitions = config.TargetPartitions;
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

bool IsValidHeartbeat(const NKikimrCdcAggregator::TEvWriteHeartbeats::TSourceHeartbeat& proto, const NPQ::THeartbeat& heartbeat) {
    return proto.GetSourceDataShardId()
        && proto.HasHeartbeat()
        && !heartbeat.Version.IsMin()
        && !heartbeat.Version.IsMax()
        && !heartbeat.Data.empty()
        && heartbeat.Data.size() <= MaxHeartbeatDataSize;
}

bool IsValidPartitioningChange(ui64 parent, const TVector<ui64>& children) {
    return parent
        && !children.empty()
        && children.front() != 0
        && std::adjacent_find(children.begin(), children.end()) == children.end()
        && !std::binary_search(children.begin(), children.end(), parent);
}

} // namespace

NKikimrCdcAggregator::TEvConfigureTrackingResult ConfigureTracking(
        TStream& stream, const NKikimrCdcAggregator::TEvConfigureTracking& request, TStreamChanges& changes)
{
    using TResult = NKikimrCdcAggregator::TEvConfigureTrackingResult;

    TResult result;
    TTrackingConfig config;
    TString error;
    if (!ParseTrackingConfig(request.GetConfig(), config, error)) {
        result.SetStatus(TResult::BAD_REQUEST);
        result.SetError(error);
        return result;
    }

    switch (stream.Configure(config, changes)) {
        case EConfigureOutcome::Success:
            result.SetStatus(TResult::SUCCESS);
            break;
        case EConfigureOutcome::Conflict:
            result.SetStatus(TResult::CONFLICT);
            result.SetError(TStringBuilder() << "Tracking of stream " << stream.GetConfig().StreamPathId
                << " is already configured differently");
            break;
    }

    return result;
}

NKikimrCdcAggregator::TEvWriteHeartbeatsResult WriteHeartbeats(
        TStream& stream, const NKikimrCdcAggregator::TEvWriteHeartbeats& request, TStreamChanges& changes)
{
    using TResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;
    using THeartbeatResult = TResult::THeartbeatResult;

    TResult result;
    if (!stream.IsConfigured()) {
        result.SetStatus(TResult::NOT_READY);
        result.SetError("Tracking is not configured");
        return result;
    }
    if (static_cast<size_t>(request.HeartbeatsSize()) > MaxHeartbeatsPerBatch) {
        result.SetStatus(TResult::BAD_REQUEST);
        result.SetError(TStringBuilder() << "Batch has " << request.HeartbeatsSize()
            << " heartbeats, the limit is " << MaxHeartbeatsPerBatch);
        return result;
    }

    result.SetStatus(TResult::SUCCESS);
    for (const auto& proto : request.GetHeartbeats()) {
        const ui64 source = proto.GetSourceDataShardId();
        const auto heartbeat = NPQ::THeartbeat::Parse(proto.GetHeartbeat());

        auto& sourceResult = *result.AddResults();
        sourceResult.SetSourceDataShardId(source);
        if (!IsValidHeartbeat(proto, heartbeat)) {
            sourceResult.SetStatus(THeartbeatResult::BAD_REQUEST);
            continue;
        }

        switch (stream.WriteHeartbeat(source, heartbeat, changes)) {
            case ESourceOutcome::Success:
                sourceResult.SetStatus(THeartbeatResult::SUCCESS);
                stream.GetSources().at(source)->ToProto(sourceResult.MutableWrittenVersion());
                break;
            case ESourceOutcome::UnknownSource:
                sourceResult.SetStatus(THeartbeatResult::UNKNOWN_SOURCE);
                break;
        }
    }

    return result;
}

NKikimrCdcAggregator::TEvPartitioningChangedAck PartitioningChanged(
        TStream& stream, const NKikimrCdcAggregator::TEvPartitioningChanged& request, TStreamChanges& changes)
{
    using TResult = NKikimrCdcAggregator::TEvPartitioningChangedAck;

    TResult result;
    const ui64 parent = request.GetParentDataShardId();
    TVector<ui64> children(request.GetChildDataShardIds().begin(), request.GetChildDataShardIds().end());
    std::sort(children.begin(), children.end());
    if (!IsValidPartitioningChange(parent, children)) {
        result.SetStatus(TResult::BAD_REQUEST);
        result.SetError(TStringBuilder() << "Invalid partitioning change of DataShard " << parent);
        return result;
    }
    if (!stream.IsConfigured()) {
        result.SetStatus(TResult::NOT_READY);
        result.SetError("Tracking is not configured");
        return result;
    }

    switch (stream.ChangePartitioning(parent, children, changes)) {
        case ESourceOutcome::Success:
            result.SetStatus(TResult::SUCCESS);
            break;
        case ESourceOutcome::UnknownSource:
            result.SetStatus(TResult::UNKNOWN_SOURCE);
            result.SetError(TStringBuilder() << "DataShard " << parent << " is not a source");
            break;
    }

    return result;
}

NKikimrCdcAggregator::TEvDescribeTrackingResult DescribeTracking(const TStream& stream) {
    NKikimrCdcAggregator::TEvDescribeTrackingResult result;
    if (const auto& current = stream.GetCurrentHeartbeat()) {
        current->Version.ToProto(result.MutableCurrentHeartbeat());
    }

    TVector<std::pair<ui64, std::optional<TRowVersion>>> sources(stream.GetSources().begin(), stream.GetSources().end());
    std::sort(sources.begin(), sources.end(), [](const auto& left, const auto& right) {
        return left.first < right.first;
    });
    for (const auto& [id, version] : sources) {
        auto& source = *result.AddSources();
        source.SetDataShardId(id);
        if (version) {
            version->ToProto(source.MutableLastHeartbeat());
        }
    }

    return result;
}

} // namespace NKikimr::NCdcAggregator
