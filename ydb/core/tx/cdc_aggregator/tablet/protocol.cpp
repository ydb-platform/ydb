#include "protocol.h"

#include <util/system/yassert.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

namespace {

TPathId ParsePathId(bool has, const NKikimrProto::TPathID& proto) {
    return has ? TPathId::FromProto(proto) : TPathId();
}

TBarrier ParseBarrier(const NKikimrCdcAggregator::TBarrier& proto) {
    return TBarrier{
        .Version = TRowVersion::FromProto(proto.GetVersion()),
        .Payload = proto.GetPayload(),
    };
}

void FillBarrier(const TBarrier& barrier, NKikimrCdcAggregator::TBarrier& proto) {
    barrier.Version.ToProto(proto.MutableVersion());
    proto.SetPayload(barrier.Payload);
}

NKikimrCdcAggregator::TEvReportDeliveredBatchResult::TSourceResult::EStatus MapSourceStatus(
        TReportDeliveredResult::TSourceResult::EStatus status)
{
    using EStatus = TReportDeliveredResult::TSourceResult::EStatus;
    using TProto = NKikimrCdcAggregator::TEvReportDeliveredBatchResult::TSourceResult;
    switch (status) {
        case EStatus::Success:
            return TProto::SUCCESS;
        case EStatus::UnknownSource:
            return TProto::UNKNOWN_SOURCE;
        case EStatus::RetiredSource:
            return TProto::RETIRED_SOURCE;
        case EStatus::BadRequest:
            return TProto::BAD_REQUEST;
    }
    Y_ABORT("Unexpected source status");
}

} // namespace

TStreamConfig ParseStreamConfig(const NKikimrCdcAggregator::TStreamConfig& proto) {
    TStreamConfig config{
        .StreamPathId = ParsePathId(proto.HasStreamPathId(), proto.GetStreamPathId()),
        .SchemeShardId = proto.GetSchemeShardId(),
        .Version = proto.GetConfigVersion(),
        .Sources = TVector<TSourceId>(proto.GetSourceTabletIds().begin(), proto.GetSourceTabletIds().end()),
    };
    for (const auto& partition : proto.GetPartitions()) {
        config.Partitions.push_back({.PartitionId = partition.GetPartitionId(), .TabletId = partition.GetTabletId()});
    }
    return config;
}

void FillStreamConfig(const TStreamConfig& config, NKikimrCdcAggregator::TStreamConfig& proto) {
    config.StreamPathId.ToProto(proto.MutableStreamPathId());
    proto.SetSchemeShardId(config.SchemeShardId);
    proto.SetConfigVersion(config.Version);
    for (const auto source : config.Sources) {
        proto.AddSourceTabletIds(source);
    }
    for (const auto& partition : config.Partitions) {
        auto& partitionProto = *proto.AddPartitions();
        partitionProto.SetPartitionId(partition.PartitionId);
        partitionProto.SetTabletId(partition.TabletId);
    }
}

TConfigureCommand ParseConfigureCommand(const NKikimrCdcAggregator::TEvConfigure& proto) {
    return TConfigureCommand{.Config = ParseStreamConfig(proto.GetConfig())};
}

TActivateCommand ParseActivateCommand(const NKikimrCdcAggregator::TEvActivate& proto) {
    return TActivateCommand{
        .StreamPathId = ParsePathId(proto.HasStreamPathId(), proto.GetStreamPathId()),
        .ConfigVersion = proto.GetConfigVersion(),
    };
}

TReportDeliveredCommand ParseReportDeliveredCommand(const NKikimrCdcAggregator::TEvReportDeliveredBatch& proto) {
    TReportDeliveredCommand command{.StreamPathId = ParsePathId(proto.HasStreamPathId(), proto.GetStreamPathId())};
    command.Reports.reserve(proto.ReportsSize());
    for (const auto& report : proto.GetReports()) {
        command.Reports.push_back({.Source = report.GetSourceTabletId(), .Delivered = ParseBarrier(report.GetDelivered())});
    }
    return command;
}

TReplaceSourceCommand ParseReplaceSourceCommand(const NKikimrCdcAggregator::TEvReplaceSource& proto) {
    return TReplaceSourceCommand{
        .StreamPathId = ParsePathId(proto.HasStreamPathId(), proto.GetStreamPathId()),
        .Replacement = {
            .Parent = proto.GetParentTabletId(),
            .Children = TVector<TSourceId>(proto.GetChildTabletIds().begin(), proto.GetChildTabletIds().end()),
            .OperationId = proto.GetOperationId(),
        },
    };
}

void FillConfigureResult(const TConfigureResult& result, NKikimrCdcAggregator::TEvConfigureResult& proto) {
    using EStatus = TConfigureResult::EStatus;
    using TProto = NKikimrCdcAggregator::TEvConfigureResult;
    switch (result.Status) {
        case EStatus::Success:
            proto.SetStatus(TProto::SUCCESS);
            break;
        case EStatus::BadRequest:
            proto.SetStatus(TProto::BAD_REQUEST);
            break;
        case EStatus::Conflict:
            proto.SetStatus(TProto::CONFLICT);
            break;
    }
    proto.SetError(result.Error);
    if (result.ConfigVersion) {
        proto.SetConfigVersion(result.ConfigVersion);
    }
}

void FillActivateResult(const TActivateResult& result, NKikimrCdcAggregator::TEvActivateResult& proto) {
    using EStatus = TActivateResult::EStatus;
    using TProto = NKikimrCdcAggregator::TEvActivateResult;
    switch (result.Status) {
        case EStatus::Success:
            proto.SetStatus(TProto::SUCCESS);
            break;
        case EStatus::NotReady:
            proto.SetStatus(TProto::NOT_READY);
            break;
        case EStatus::WrongStream:
            proto.SetStatus(TProto::WRONG_STREAM);
            break;
        case EStatus::Conflict:
            proto.SetStatus(TProto::CONFLICT);
            break;
    }
    proto.SetError(result.Error);
}

void FillReportDeliveredResult(const TReportDeliveredResult& result, NKikimrCdcAggregator::TEvReportDeliveredBatchResult& proto) {
    using EStatus = TReportDeliveredResult::EStatus;
    using TSourceStatus = TReportDeliveredResult::TSourceResult::EStatus;
    using TProto = NKikimrCdcAggregator::TEvReportDeliveredBatchResult;
    switch (result.Status) {
        case EStatus::Success:
            proto.SetStatus(TProto::SUCCESS);
            break;
        case EStatus::NotReady:
            proto.SetStatus(TProto::NOT_READY);
            break;
        case EStatus::WrongStream:
            proto.SetStatus(TProto::WRONG_STREAM);
            break;
        case EStatus::BadRequest:
            proto.SetStatus(TProto::BAD_REQUEST);
            break;
    }
    proto.SetError(result.Error);

    for (const auto& sourceResult : result.Results) {
        auto& sourceProto = *proto.AddResults();
        sourceProto.SetSourceTabletId(sourceResult.Source);
        sourceProto.SetStatus(MapSourceStatus(sourceResult.Status));
        if (sourceResult.Status == TSourceStatus::Success) {
            sourceResult.PersistedVersion.ToProto(sourceProto.MutablePersistedVersion());
        }
    }
}

void FillReplaceSourceResult(const TReplaceSourceResult& result, NKikimrCdcAggregator::TEvReplaceSourceResult& proto) {
    using EStatus = TReplaceSourceResult::EStatus;
    using TProto = NKikimrCdcAggregator::TEvReplaceSourceResult;
    switch (result.Status) {
        case EStatus::Success:
            proto.SetStatus(TProto::SUCCESS);
            break;
        case EStatus::NotReady:
            proto.SetStatus(TProto::NOT_READY);
            break;
        case EStatus::WrongStream:
            proto.SetStatus(TProto::WRONG_STREAM);
            break;
        case EStatus::UnknownSource:
            proto.SetStatus(TProto::UNKNOWN_SOURCE);
            break;
        case EStatus::Conflict:
            proto.SetStatus(TProto::CONFLICT);
            break;
        case EStatus::BadRequest:
            proto.SetStatus(TProto::BAD_REQUEST);
            break;
    }
    proto.SetError(result.Error);
}

void FillStreamDescription(const TStream& stream, NKikimrCdcAggregator::TEvDescribeStreamResult& proto) {
    if (!stream.IsConfigured()) {
        return;
    }

    FillStreamConfig(stream.GetConfig(), *proto.MutableConfig());
    proto.SetActive(stream.GetPhase() == EStreamPhase::Active);
    if (const auto& barrier = stream.GetReadyBarrier()) {
        FillBarrier(*barrier, *proto.MutableReadyBarrier());
    }

    TVector<TSourceId> sources;
    sources.reserve(stream.GetSources().size());
    for (const auto& [source, progress] : stream.GetSources()) {
        sources.push_back(source);
    }
    std::sort(sources.begin(), sources.end());
    for (const auto source : sources) {
        auto& sourceProto = *proto.AddSources();
        sourceProto.SetSourceTabletId(source);
        if (const auto& progress = *stream.FindSourceProgress(source)) {
            progress->Version.ToProto(sourceProto.MutableDelivered());
        }
    }

    TVector<TSourceId> retired;
    retired.reserve(stream.GetReplacements().size());
    for (const auto& [parent, replacement] : stream.GetReplacements()) {
        retired.push_back(parent);
    }
    std::sort(retired.begin(), retired.end());
    for (const auto parent : retired) {
        proto.AddRetiredSourceTabletIds(parent);
    }
}

} // namespace NKikimr::NCdcAggregator
