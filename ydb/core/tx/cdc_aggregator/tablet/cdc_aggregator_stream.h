#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>

#include <optional>
#include <utility>

namespace NKikimr::NCdcAggregator {

struct TCdcAggregatorStreamTopicPartition {
    ui32 PartitionId = 0;
    ui64 TabletId = 0;

    bool operator==(const TCdcAggregatorStreamTopicPartition&) const = default;
};

struct TCdcAggregatorStreamTopic {
    TPathId StreamPathId;
    TString StreamPath;
    TVector<TCdcAggregatorStreamTopicPartition> Partitions;

    bool operator==(const TCdcAggregatorStreamTopic&) const = default;
};

struct TCdcAggregatorStreamConfig {
    TCdcAggregatorStreamTopic Topic;
    TVector<ui64> SourceDataShardIds;
};

struct TCdcAggregatorStreamSourcesDiff {
    THashSet<ui64> Inserted;
    THashSet<ui64> Deleted;
};

struct TCdcAggregatorStreamPartitioningChange {
    ui64 OperationId = 0;
    TVector<ui64> ParentDataShardIds;
    TVector<ui64> ChildDataShardIds;
};

class TCdcAggregatorStream {
public:
    using TPartitioningChanges = TMap<std::pair<TVector<ui64>, ui64>, TVector<ui64>>;

    explicit TCdcAggregatorStream(const TCdcAggregatorStreamConfig& config);
    TCdcAggregatorStream(TCdcAggregatorStreamTopic topic, THashSet<ui64> sourceDataShardIds, TPartitioningChanges partitioningChanges);

    const TCdcAggregatorStreamTopic& GetTopic() const {
        return Topic;
    }

    const THashSet<ui64>& GetSourceDataShardIds() const {
        return SourceDataShardIds;
    }

    std::optional<TCdcAggregatorStreamSourcesDiff> ApplyPartitioningChange(const TCdcAggregatorStreamPartitioningChange& change);

private:
    TCdcAggregatorStreamTopic Topic;
    THashSet<ui64> SourceDataShardIds;
    TPartitioningChanges PartitioningChanges;
};

} // namespace NKikimr::NCdcAggregator
