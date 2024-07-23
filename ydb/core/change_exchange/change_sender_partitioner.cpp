#include "change_sender_partitioner.h"

namespace NKikimr::NChangeExchange {

ui64 ResolveSchemaBoundaryPartitionId(const NKikimr::TKeyDesc& keyDesc, TConstArrayRef<TCell> key) {
        const auto& partitions = keyDesc.GetPartitions();
        Y_ABORT_UNLESS(partitions);
        const auto& schema = keyDesc.KeyColumnTypes;

        const auto range = TTableRange(key);
        Y_ABORT_UNLESS(range.Point);

        const auto it = LowerBound(
            partitions.cbegin(), partitions.cend(), true,
            [&](const auto& partition, bool) {
                Y_ABORT_UNLESS(partition.Range);
                const int compares = CompareBorders<true, false>(
                    partition.Range->EndKeyPrefix.GetCells(), range.From,
                    partition.Range->IsInclusive || partition.Range->IsPoint,
                    range.InclusiveFrom || range.Point, schema
                );

                return (compares < 0);
            }
        );

        Y_ABORT_UNLESS(it != partitions.cend());
        return it->ShardId;
}


} // NKikimr::NChangeExchange
