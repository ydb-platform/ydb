#include "stream.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

bool ParseTrackingConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TTrackingConfig& config, TString& error) {
    config.StreamPathId = TPathId::FromProto(proto.GetStreamPathId());
    if (!config.StreamPathId.OwnerId || !config.StreamPathId.LocalPathId) {
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

TStream TStream::Restore(TTrackingConfig config, THashSet<ui64> sources) {
    TStream stream;
    stream.Config = std::move(config);
    stream.Sources = std::move(sources);
    return stream;
}

const TTrackingConfig& TStream::GetConfig() const {
    Y_ENSURE(Config, "Tracking is not configured");
    return *Config;
}

EConfigureOutcome TStream::Configure(const TTrackingConfig& config, TStreamChanges& changes) {
    if (Config) {
        return *Config == config ? EConfigureOutcome::Success : EConfigureOutcome::Conflict;
    }

    Config = config;
    changes.ConfigChanged = true;
    for (const auto sourceDataShardId : config.SourceDataShardIds) {
        Sources.insert(sourceDataShardId);
        changes.AddedSources.insert(sourceDataShardId);
    }

    return EConfigureOutcome::Success;
}

ESourceOutcome TStream::ChangePartitioning(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds, TStreamChanges& changes) {
    Y_ENSURE(Config, "Tracking is not configured");

    if (!Sources.contains(parentDataShardId)) {
        const bool applied = std::all_of(childDataShardIds.begin(), childDataShardIds.end(), [this](ui64 childDataShardId) {
            return Sources.contains(childDataShardId);
        });
        return applied ? ESourceOutcome::Success : ESourceOutcome::UnknownSource;
    }

    Sources.erase(parentDataShardId);
    changes.ErasedSources.insert(parentDataShardId);
    for (const auto childDataShardId : childDataShardIds) {
        if (Sources.insert(childDataShardId).second) {
            changes.AddedSources.insert(childDataShardId);
        }
    }

    return ESourceOutcome::Success;
}

} // namespace NKikimr::NCdcAggregator
