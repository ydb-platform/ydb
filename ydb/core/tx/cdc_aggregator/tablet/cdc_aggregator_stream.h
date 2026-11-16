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

struct TCdcAggregatorStreamTopic {
    TPathId StreamPathId;
    TString StreamPath;
    TVector<TTopicPartition> Partitions;

    bool operator==(const TCdcAggregatorStreamTopic&) const = default;
};

struct TCdcAggregatorStreamConfig {
    TCdcAggregatorStreamTopic Topic;
    TVector<ui64> SourceDataShardIds;
};

bool ParseCdcAggregatorStreamTopic(const NKikimrCdcAggregator::TTrackingConfig& proto, TCdcAggregatorStreamTopic& topic, TString& error);
bool ParseCdcAggregatorStreamConfig(const NKikimrCdcAggregator::TTrackingConfig& proto, TCdcAggregatorStreamConfig& config, TString& error);
TString SerializeCdcAggregatorStreamTopic(const TCdcAggregatorStreamTopic& topic);

enum class ESourceOutcome {
    Success,
    UnknownSource,
};

struct TCdcAggregatorStreamSourcesDiff {
    THashSet<ui64> Inserted;
    THashSet<ui64> Deleted;
};

struct TCdcAggregatorStreamUpdateSourcesResult {
    ESourceOutcome Outcome = ESourceOutcome::Success;
    TCdcAggregatorStreamSourcesDiff Diff;
};

class TCdcAggregatorStream {
public:
    explicit TCdcAggregatorStream(const TCdcAggregatorStreamConfig& config);
    TCdcAggregatorStream(TCdcAggregatorStreamTopic topic, THashSet<ui64> sourceDataShardIds);

    const TCdcAggregatorStreamTopic& GetTopic() const {
        return Topic;
    }

    const THashSet<ui64>& GetSourceDataShardIds() const {
        return SourceDataShardIds;
    }

    bool IsSameConfig(const TCdcAggregatorStreamConfig& config) const;
    TCdcAggregatorStreamUpdateSourcesResult UpdateSources(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds);

private:
    TCdcAggregatorStreamTopic Topic;
    THashSet<ui64> SourceDataShardIds;
};

} // namespace NKikimr::NCdcAggregator
