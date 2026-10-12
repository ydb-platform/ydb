#pragma once

#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NKqp {

// Index of the partition of `shardId` inside `partitioning`, or
// `partitioning.Size()` when the shard is not in the partitioning.
size_t FindPartitionIndex(const TPartitioning& partitioning, ui64 shardId);

// Full key range of the index-th partition of `partitioning`.
// The first partition starts at the min key (`keyColumnsCount` null cells); every
// partition ends at the next partition's EndKeyPrefix (inclusive) and starts right
// after the previous partition's EndKeyPrefix (exclusive if that boundary was
// inclusive); the last partition has no upper bound.
// Returns a default (undefined) range for out-of-bounds index, a partition without
// range info (Range == Nothing) or a column-table partitioning where every
// partition has an empty EndKeyPrefix.
TOwnedTableRange GetShardKeyRange(const TPartitioning& partitioning, size_t index, size_t keyColumnsCount);

// Borders/version of `shardId` in `partitioning`. Borders stay undefined for a
// shard missing from `partitioning` or a shard without representable key range
// (e.g. column tables); the version is still recorded when `partitioning` is set.
TShardMeta MakeShardMeta(
    const TTableId& tableId,
    const TPartitioning::TCPtr& partitioning,
    ui64 shardId,
    size_t keyColumnsCount);

// A resolved partitioning with an O(1) ShardId-to-partition map. Built once
// where the partitioning is stored (write controller, buffer lock actor, stream
// lookup actor) and reused for every shard lookup instead of rescanning the
// partitions: FindPartitionIndex is O(n), the map lookup is O(1).
class TShardPartitionIndex {
public:
    TShardPartitionIndex() = default;

    // Builds the map in O(partition count). A null partitioning is allowed and
    // yields an empty index.
    explicit TShardPartitionIndex(TPartitioning::TCPtr partitioning);

    explicit operator bool() const;

    const TPartitioning::TCPtr& GetPartitioning() const;

    // Drops the partitioning and its index.
    void Reset();

    // Index of the partition of `shardId`, or `GetPartitioning()->Size()` when
    // the shard is not in the partitioning (FindPartitionIndex's contract).
    // O(1).
    size_t FindPartitionIndex(ui64 shardId) const;

private:
    TPartitioning::TCPtr Partitioning;
    THashMap<ui64, size_t> ShardIdToPartition;
};

// Borders/version of `shardId` in the partitioning held by `index`: the same
// semantics as the partitioning overload, with an O(1) shard lookup.
TShardMeta MakeShardMeta(
    const TTableId& tableId,
    const TShardPartitionIndex& index,
    ui64 shardId,
    size_t keyColumnsCount);

// Proto wire format of TShardMeta for TEvKqpInputActorResultInfo.
void FillShardPartitioningInfo(NKikimrTxDataShard::TShardPartitioningInfo& proto, ui64 shardId, const TShardMeta& meta);

// Inverse of FillShardPartitioningInfo.
TShardMeta ToShardMeta(const NKikimrTxDataShard::TShardPartitioningInfo& proto, const TTableId& tableId);

} // namespace NKqp
} // namespace NKikimr
