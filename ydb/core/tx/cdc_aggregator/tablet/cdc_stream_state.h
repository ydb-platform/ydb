#pragma once

#include <ydb/core/protos/cdc_aggregator.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NCdcAggregator {

struct TTopicPartition {
    ui32 PartitionId = 0;
    ui64 TabletId = 0;

    bool operator==(const TTopicPartition&) const = default;
};

struct TCdcStreamTopic {
    TPathId StreamPathId;
    TString StreamPath;
    TVector<TTopicPartition> Partitions;

    bool operator==(const TCdcStreamTopic&) const = default;
};

struct TCdcStreamConfig {
    TCdcStreamTopic Topic;
    TVector<ui64> SourceDataShardIds;
};

bool ParseCdcStreamTopic(const NKikimrCdcAggregator::TTrackingConfig& proto, TCdcStreamTopic& topic, TString& error);
bool ParseCdcStreamConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TCdcStreamConfig& config, TString& error);
TString SerializeCdcStreamTopic(const TCdcStreamTopic& topic);

struct TCdcAggregatorStreamChanges {
    THashSet<ui64> AddedSourceDataShardIds;
    THashSet<ui64> ErasedSourceDataShardIds;
};

enum class ESourceOutcome {
    Success,
    UnknownSource,
};

class TCdcAggregatorStreamState {
public:
    explicit TCdcAggregatorStreamState(const TCdcStreamConfig& config);
    TCdcAggregatorStreamState(TCdcStreamTopic topic, THashSet<ui64> sourceDataShardIds);

    const TCdcStreamTopic& GetTopic() const {
        return Topic;
    }

    const THashSet<ui64>& GetSourceDataShardIds() const {
        return SourceDataShardIds;
    }

    bool IsSameConfig(const TCdcStreamConfig& config) const;
    ESourceOutcome ReplaceSourceDataShard(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds, TCdcAggregatorStreamChanges& changes);

private:
    TCdcStreamTopic Topic;
    THashSet<ui64> SourceDataShardIds;
};

} // namespace NKikimr::NCdcAggregator
