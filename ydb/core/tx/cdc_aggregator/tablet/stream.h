#pragma once

#include <ydb/core/protos/cdc_aggregator.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

struct TTargetPartition {
    ui32 PartitionId = 0;
    ui64 TabletId = 0;

    bool operator==(const TTargetPartition&) const = default;
};

struct TTrackingConfig {
    TPathId StreamPathId;
    TString StreamPath;
    TVector<ui64> SourceDataShardIds;
    TVector<TTargetPartition> TargetPartitions;

    bool operator==(const TTrackingConfig&) const = default;
};

bool ParseTrackingConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TTrackingConfig& config, TString& error);

struct TStreamChanges {
    bool ConfigChanged = false;
    THashSet<ui64> AddedSources;
    THashSet<ui64> ErasedSources;
};

enum class EConfigureOutcome {
    Success,
    Conflict,
};

enum class ESourceOutcome {
    Success,
    UnknownSource,
};

class TStream {
public:
    static TStream Restore(TTrackingConfig config, THashSet<ui64> sources);

    bool IsConfigured() const {
        return Config.has_value();
    }

    const TTrackingConfig& GetConfig() const;

    const THashSet<ui64>& GetSources() const {
        return Sources;
    }

    EConfigureOutcome Configure(const TTrackingConfig& config, TStreamChanges& changes);
    ESourceOutcome ChangePartitioning(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds, TStreamChanges& changes);

private:
    std::optional<TTrackingConfig> Config;
    THashSet<ui64> Sources;
};

} // namespace NKikimr::NCdcAggregator
