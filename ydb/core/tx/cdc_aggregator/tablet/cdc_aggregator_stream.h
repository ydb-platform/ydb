#pragma once

#include <ydb/core/persqueue/common/heartbeat.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash.h>
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

struct TCdcAggregatorStreamHeartbeatsDiff {
    THashMap<ui64, NPQ::THeartbeat> LastHeartbeatBySourceDataShardId;
    std::optional<NPQ::THeartbeat> CurrentHeartbeat;
};

struct TCdcAggregatorStreamPartitioningChange {
    ui64 OperationId = 0;
    ui64 ParentDataShardId = 0;
    TVector<ui64> ChildDataShardIds;
};

class TCdcAggregatorStream {
public:
    using TLastHeartbeats = THashMap<ui64, std::optional<NPQ::THeartbeat>>;
    using TPartitioningChanges = TMap<std::pair<ui64, ui64>, TVector<ui64>>;

    explicit TCdcAggregatorStream(const TCdcAggregatorStreamConfig& config);
    TCdcAggregatorStream(
        TCdcAggregatorStreamTopic topic,
        TLastHeartbeats lastHeartbeatBySourceDataShardId,
        std::optional<NPQ::THeartbeat> currentHeartbeat,
        TPartitioningChanges partitioningChanges);

    const TCdcAggregatorStreamTopic& GetTopic() const {
        return Topic;
    }

    const TLastHeartbeats& GetLastHeartbeatBySourceDataShardId() const {
        return LastHeartbeatBySourceDataShardId;
    }

    const std::optional<NPQ::THeartbeat>& GetCurrentHeartbeat() const {
        return CurrentHeartbeat;
    }

    TCdcAggregatorStreamHeartbeatsDiff WriteHeartbeats(const THashMap<ui64, NPQ::THeartbeat>& heartbeatBySourceDataShardId);
    std::optional<TCdcAggregatorStreamSourcesDiff> ApplyPartitioningChange(const TCdcAggregatorStreamPartitioningChange& change);

private:
    TCdcAggregatorStreamTopic Topic;
    TLastHeartbeats LastHeartbeatBySourceDataShardId;
    std::optional<NPQ::THeartbeat> CurrentHeartbeat;
    TPartitioningChanges PartitioningChanges;
};

} // namespace NKikimr::NCdcAggregator
