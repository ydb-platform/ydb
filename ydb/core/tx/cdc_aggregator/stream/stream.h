#pragma once

#include <ydb/core/persqueue/common/heartbeat.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
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

struct TStreamChanges {
    bool Config = false;
    THashSet<ui64> UpsertedSources;
    THashSet<ui64> ErasedSources;
    TMap<TRowVersion, TString> UpsertedHeartbeats;
    TSet<TRowVersion> ErasedHeartbeats;
    bool CurrentHeartbeat = false;

    bool IsEmpty() const;
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
    static TStream Restore(
        TTrackingConfig config,
        THashMap<ui64, std::optional<TRowVersion>> sources,
        const TMap<TRowVersion, TString>& heartbeats,
        std::optional<NPQ::THeartbeat> currentHeartbeat);

    bool IsConfigured() const {
        return Config.has_value();
    }

    const TTrackingConfig& GetConfig() const;

    const THashMap<ui64, std::optional<TRowVersion>>& GetSources() const {
        return Sources;
    }

    const std::optional<NPQ::THeartbeat>& GetCurrentHeartbeat() const {
        return CurrentHeartbeat;
    }

    EConfigureOutcome Configure(const TTrackingConfig& config, TStreamChanges& changes);
    ESourceOutcome WriteHeartbeat(ui64 source, const NPQ::THeartbeat& heartbeat, TStreamChanges& changes);
    ESourceOutcome ChangePartitioning(ui64 parent, const TVector<ui64>& children, TStreamChanges& changes);

private:
    void AddSource(ui64 source, TStreamChanges& changes);
    void EraseSource(ui64 source, TStreamChanges& changes);
    void HoldHeartbeat(const NPQ::THeartbeat& heartbeat, TStreamChanges& changes);
    void ReleaseHeartbeat(const TRowVersion& version, TStreamChanges& changes);
    void AdvanceCurrentHeartbeat(TStreamChanges& changes);

private:
    struct THeldHeartbeat {
        TString Data;
        ui64 SourceCount = 0;
    };

    std::optional<TTrackingConfig> Config;
    THashMap<ui64, std::optional<TRowVersion>> Sources;
    ui64 SourcesWithHeartbeat = 0;
    TMap<TRowVersion, THeldHeartbeat> Heartbeats;
    std::optional<NPQ::THeartbeat> CurrentHeartbeat;
};

} // namespace NKikimr::NCdcAggregator
