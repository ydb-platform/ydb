#include "kqp_shard_meta.h"

#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr {
namespace NKqp {

// Index of the partition of `shardId` inside `partitioning`, or
// `partitioning.Size()` when the shard is not in the partitioning.
size_t FindPartitionIndex(const TPartitioning& partitioning, ui64 shardId) {
    const auto& partitions = partitioning.GetTablePartitioning();
    for (size_t index = 0; index < partitions.size(); ++index) {
        if (partitions[index].ShardId == shardId) {
            return index;
        }
    }
    return partitions.size();
}

// Full key range of the index-th partition of `partitioning`.
// The first partition starts at the min key (`keyColumnsCount` null cells); every
// partition ends at the next partition's EndKeyPrefix (inclusive) and starts right
// after the previous partition's EndKeyPrefix (exclusive if that boundary was
// inclusive); the last partition has no upper bound.
// Returns a default (undefined) range for out-of-bounds index, a partition without
// range info (Range == Nothing) or a column-table partitioning where every
// partition has an empty EndKeyPrefix.
TOwnedTableRange GetShardKeyRange(const TPartitioning& partitioning, size_t index, size_t keyColumnsCount) {
    const auto& partitions = partitioning.GetTablePartitioning();
    if (index >= partitions.size() || !partitions[index].Range) {
        return TOwnedTableRange();
    }

    const auto& range = *partitions[index].Range;
    const auto& endKey = range.EndKeyPrefix.GetCells();
    // An empty EndKeyPrefix means "+inf": legitimate for the last partition, but
    // for a non-last one (column-table style partitioning without borders) the
    // range is not representable.
    if (endKey.empty() && index + 1 != partitions.size()) {
        return TOwnedTableRange();
    }

    // The first partition starts at the min key: keyColumnsCount null cells.
    TVector<TCell> from(keyColumnsCount);
    bool inclusiveFrom = true;
    if (index > 0) {
        if (!partitions[index - 1].Range) {
            return TOwnedTableRange();
        }
        const auto& prev = *partitions[index - 1].Range;
        const auto& prevEnd = prev.EndKeyPrefix.GetCells();
        if (prevEnd.empty()) {
            return TOwnedTableRange();
        }
        from.assign(prevEnd.begin(), prevEnd.end());
        // A previous boundary that includes its EndKeyPrefix must not be re-included here.
        inclusiveFrom = !prev.IsInclusive;
    }

    // The last partition has no upper bound (empty To, InclusiveTo == false).
    return TOwnedTableRange(from, inclusiveFrom, endKey, range.IsInclusive || range.IsPoint);
}

// Borders/version of `shardId` in `partitioning`. Borders stay undefined for a
// shard missing from `partitioning` or a shard without representable key range
// (e.g. column tables); the version is still recorded when `partitioning` is set.
TShardMeta MakeShardMeta(
        const TTableId& tableId,
        const TPartitioning::TCPtr& partitioning,
        ui64 shardId,
        size_t keyColumnsCount) {
    TShardMeta meta;
    meta.TableId = tableId;
    if (!partitioning) {
        return meta;
    }
    meta.PartitioningVersion = partitioning->GetVersion();
    const auto index = FindPartitionIndex(*partitioning, shardId);
    if (index < partitioning->Size()) {
        meta.Borders = GetShardKeyRange(*partitioning, index, keyColumnsCount);
    }
    return meta;
}

TShardPartitionIndex::TShardPartitionIndex(TPartitioning::TCPtr partitioning)
    : Partitioning(std::move(partitioning)) {
    if (Partitioning) {
        const auto& partitions = Partitioning->GetTablePartitioning();
        for (size_t index = 0; index < partitions.size(); ++index) {
            // emplace keeps the first occurrence, matching the linear scan.
            ShardIdToPartition.emplace(partitions[index].ShardId, index);
        }
    }
}

TShardPartitionIndex::operator bool() const {
    return static_cast<bool>(Partitioning);
}

const TPartitioning::TCPtr& TShardPartitionIndex::GetPartitioning() const {
    return Partitioning;
}

void TShardPartitionIndex::Reset() {
    Partitioning.reset();
    ShardIdToPartition.clear();
}

size_t TShardPartitionIndex::FindPartitionIndex(ui64 shardId) const {
    const auto it = ShardIdToPartition.find(shardId);
    if (it != ShardIdToPartition.end()) {
        return it->second;
    }
    return Partitioning ? Partitioning->Size() : 0;
}

// Borders/version of `shardId` in the partitioning held by `index`.
TShardMeta MakeShardMeta(
        const TTableId& tableId,
        const TShardPartitionIndex& index,
        ui64 shardId,
        size_t keyColumnsCount) {
    TShardMeta meta;
    meta.TableId = tableId;
    if (!index) {
        return meta;
    }
    const auto& partitioning = index.GetPartitioning();
    meta.PartitioningVersion = partitioning->GetVersion();
    const auto shardIndex = index.FindPartitionIndex(shardId);
    if (shardIndex < partitioning->Size()) {
        meta.Borders = GetShardKeyRange(*partitioning, shardIndex, keyColumnsCount);
    }
    return meta;
}

// Proto wire format of TShardMeta for TEvKqpInputActorResultInfo.
void FillShardPartitioningInfo(NKikimrTxDataShard::TShardPartitioningInfo& proto, ui64 shardId, const TShardMeta& meta) {
    proto.SetShardId(shardId);
    proto.SetPartitioningVersion(meta.PartitioningVersion);
    // A defined shard range always starts at the min key or at the previous
    // partition boundary, so From is non-empty; undefined borders stay unset.
    if (!meta.Borders.From.empty()) {
        auto* borders = proto.MutableBorders();
        borders->SetFrom(TSerializedCellVec::Serialize(meta.Borders.From));
        borders->SetTo(TSerializedCellVec::Serialize(meta.Borders.To));
        borders->SetFromInclusive(meta.Borders.InclusiveFrom);
        borders->SetToInclusive(meta.Borders.InclusiveTo);
    }
}

// Inverse of FillShardPartitioningInfo.
TShardMeta ToShardMeta(const NKikimrTxDataShard::TShardPartitioningInfo& proto, const TTableId& tableId) {
    TShardMeta meta;
    meta.TableId = tableId;
    meta.PartitioningVersion = proto.GetPartitioningVersion();
    if (proto.HasBorders()) {
        const auto& borders = proto.GetBorders();
        meta.Borders = TOwnedTableRange(
            TSerializedCellVec(borders.GetFrom()).GetCells(),
            borders.GetFromInclusive(),
            TSerializedCellVec(borders.GetTo()).GetCells(),
            borders.GetToInclusive());
    }
    return meta;
}

} // namespace NKqp
} // namespace NKikimr
