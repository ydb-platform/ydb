#include <ydb/core/kqp/common/kqp_shard_meta.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

TVector<TCell> SingleCell(ui32 value) {
    return { TCell::Make(value) };
}

TVector<TCell> PairCell(ui32 first, ui32 second) {
    return { TCell::Make(first), TCell::Make(second) };
}

// A partitioning of the integer range [0, +inf) split by the given end keys
// (exclusive), with the given partitioning version. `Max<ui32>()` marks the last
// partition (no upper bound).
TPartitioning::TCPtr MakePartitioning(std::initializer_list<std::pair<ui64, ui32>> shardsWithEndKeys, ui64 version) {
    TVector<TKeyDesc::TPartitionInfo> partitions;
    partitions.reserve(shardsWithEndKeys.size());
    for (const auto& [shardId, endKey] : shardsWithEndKeys) {
        TKeyDesc::TPartitionInfo partition(shardId);
        partition.Range.ConstructInPlace();
        if (endKey != Max<ui32>()) {
            partition.Range->EndKeyPrefix = TSerializedCellVec(SingleCell(endKey));
            partition.Range->IsInclusive = false;
        }
        partitions.push_back(std::move(partition));
    }
    return std::make_shared<TPartitioning>(std::move(partitions), version);
}

} // namespace

Y_UNIT_TEST_SUITE(ShardMetaHelpers) {

    Y_UNIT_TEST(FindPartitionIndex) {
        auto partitioning = MakePartitioning({{1, 10}, {2, 20}, {3, Max<ui32>()}}, 1);

        UNIT_ASSERT_VALUES_EQUAL(FindPartitionIndex(*partitioning, 1), 0);
        UNIT_ASSERT_VALUES_EQUAL(FindPartitionIndex(*partitioning, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(FindPartitionIndex(*partitioning, 3), 2);
        UNIT_ASSERT_VALUES_EQUAL(FindPartitionIndex(*partitioning, 4), 3);
    }

    Y_UNIT_TEST(MakeShardMetaSingleShard) {
        auto partitioning = MakePartitioning({{7, Max<ui32>()}}, 5);

        auto meta = MakeShardMeta(TTableId(1, 100), partitioning, 7, 1);
        UNIT_ASSERT(meta.IsKnown());
        UNIT_ASSERT_VALUES_EQUAL(meta.PartitioningVersion, 5);
        UNIT_ASSERT_VALUES_EQUAL(meta.Borders.From.size(), 1);
        UNIT_ASSERT(meta.Borders.From[0].IsNull());
        UNIT_ASSERT(meta.Borders.InclusiveFrom);
        UNIT_ASSERT(meta.Borders.To.empty());
        UNIT_ASSERT(!meta.Borders.InclusiveTo);
        UNIT_ASSERT(!meta.Borders.Point);
    }

    Y_UNIT_TEST(MakeShardMetaBordersMatchNeighbors) {
        auto partitioning = MakePartitioning({{1, 10}, {2, 20}, {3, Max<ui32>()}}, 3);

        auto first = MakeShardMeta(TTableId(1, 100), partitioning, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(first.Borders.From.size(), 1);
        UNIT_ASSERT(first.Borders.From[0].IsNull());
        UNIT_ASSERT(first.Borders.InclusiveFrom);
        UNIT_ASSERT_VALUES_EQUAL(first.Borders.To[0].AsValue<ui32>(), 10);
        UNIT_ASSERT(!first.Borders.InclusiveTo);

        auto middle = MakeShardMeta(TTableId(1, 100), partitioning, 2, 1);
        UNIT_ASSERT_VALUES_EQUAL(middle.Borders.From[0].AsValue<ui32>(), 10);
        UNIT_ASSERT(middle.Borders.InclusiveFrom);
        UNIT_ASSERT_VALUES_EQUAL(middle.Borders.To[0].AsValue<ui32>(), 20);
        UNIT_ASSERT(!middle.Borders.InclusiveTo);

        auto last = MakeShardMeta(TTableId(1, 100), partitioning, 3, 1);
        UNIT_ASSERT_VALUES_EQUAL(last.Borders.From[0].AsValue<ui32>(), 20);
        UNIT_ASSERT(last.Borders.InclusiveFrom);
        UNIT_ASSERT(last.Borders.To.empty());
    }

    Y_UNIT_TEST(MakeShardMetaUnknownShard) {
        auto partitioning = MakePartitioning({{1, 10}, {2, Max<ui32>()}}, 4);

        auto meta = MakeShardMeta(TTableId(1, 100), partitioning, 99, 1);
        // The shard is not in the partitioning: the version is still recorded,
        // borders stay unknown.
        UNIT_ASSERT(meta.IsKnown());
        UNIT_ASSERT_VALUES_EQUAL(meta.PartitioningVersion, 4);
        UNIT_ASSERT(meta.Borders.From.empty() && meta.Borders.To.empty());
    }

    Y_UNIT_TEST(MakeShardMetaNullPartitioning) {
        auto meta = MakeShardMeta(TTableId(1, 100), nullptr, 7, 1);
        UNIT_ASSERT(!meta.IsKnown());
        UNIT_ASSERT(meta.Borders.From.empty() && meta.Borders.To.empty());
    }

    Y_UNIT_TEST(MakeShardMetaMultiColumnKey) {
        TVector<TKeyDesc::TPartitionInfo> partitions;
        partitions.emplace_back(1);
        partitions.back().Range.ConstructInPlace();
        partitions.back().Range->EndKeyPrefix = TSerializedCellVec(PairCell(5, 10));
        partitions.emplace_back(2);
        partitions.back().Range.ConstructInPlace(); // last partition: empty EndKeyPrefix = unbounded
        auto partitioning = std::make_shared<TPartitioning>(std::move(partitions), 9);

        auto meta = MakeShardMeta(TTableId(1, 100), partitioning, 2, 2);
        UNIT_ASSERT(meta.IsKnown());
        UNIT_ASSERT_VALUES_EQUAL(meta.Borders.From.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(meta.Borders.From[0].AsValue<ui32>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(meta.Borders.From[1].AsValue<ui32>(), 10);
    }

    Y_UNIT_TEST(ProtoRoundTrip) {
        auto partitioning = MakePartitioning({{1, 10}, {2, Max<ui32>()}}, 6);
        auto meta = MakeShardMeta(TTableId(1, 100), partitioning, 2, 1);

        // Borders survive the proto round trip used by the stream lookup actor
        // and the data executer.
        NKikimrTxDataShard::TShardPartitioningInfo proto;
        FillShardPartitioningInfo(proto, 2, meta);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetShardId(), 2);
        auto restored = ToShardMeta(proto, TTableId(1, 100));

        UNIT_ASSERT_VALUES_EQUAL(restored.PartitioningVersion, 6);
        UNIT_ASSERT(SameBorders(restored.Borders, meta.Borders));
    }

    Y_UNIT_TEST(ProtoRoundTripUnknownBorders) {
        auto partitioning = MakePartitioning({{1, 10}, {2, Max<ui32>()}}, 6);
        auto unknown = MakeShardMeta(TTableId(1, 100), partitioning, 42, 1);

        NKikimrTxDataShard::TShardPartitioningInfo proto;
        FillShardPartitioningInfo(proto, 42, unknown);
        UNIT_ASSERT(!proto.HasBorders());

        auto restored = ToShardMeta(proto, TTableId(1, 100));
        UNIT_ASSERT(restored.IsKnown());
        UNIT_ASSERT(restored.Borders.From.empty() && restored.Borders.To.empty());
    }

    Y_UNIT_TEST(ShardPartitionIndex) {
        auto partitioning = MakePartitioning({{1, 10}, {2, 20}, {3, Max<ui32>()}}, 1);

        TShardPartitionIndex index(partitioning);
        UNIT_ASSERT(index);
        UNIT_ASSERT(index.GetPartitioning() == partitioning);
        UNIT_ASSERT_VALUES_EQUAL(index.FindPartitionIndex(1), 0u);
        UNIT_ASSERT_VALUES_EQUAL(index.FindPartitionIndex(2), 1u);
        UNIT_ASSERT_VALUES_EQUAL(index.FindPartitionIndex(3), 2u);
        UNIT_ASSERT_VALUES_EQUAL(index.FindPartitionIndex(4), partitioning->Size());

        TShardPartitionIndex null(nullptr);
        UNIT_ASSERT(!null);
        UNIT_ASSERT(!null.GetPartitioning());
        UNIT_ASSERT_VALUES_EQUAL(null.FindPartitionIndex(1), 0u);

        TShardPartitionIndex empty;
        UNIT_ASSERT(!empty);
        UNIT_ASSERT_VALUES_EQUAL(empty.FindPartitionIndex(1), 0u);
    }

    Y_UNIT_TEST(MakeShardMetaViaIndexMatchesRaw) {
        auto partitioning = MakePartitioning({{1, 10}, {2, 20}, {3, Max<ui32>()}}, 7);
        TShardPartitionIndex index(partitioning);

        for (const ui64 shardId : {1ull, 2ull, 3ull, 99ull}) {
            const auto viaIndex = MakeShardMeta(TTableId(1, 100), index, shardId, 1);
            const auto raw = MakeShardMeta(TTableId(1, 100), partitioning, shardId, 1);
            UNIT_ASSERT_VALUES_EQUAL(viaIndex.IsKnown(), raw.IsKnown());
            UNIT_ASSERT_VALUES_EQUAL(viaIndex.PartitioningVersion, raw.PartitioningVersion);
            UNIT_ASSERT(SameBorders(viaIndex.Borders, raw.Borders));
        }

        const auto empty = MakeShardMeta(TTableId(1, 100), TShardPartitionIndex(), 1, 1);
        UNIT_ASSERT(!empty.IsKnown());
    }

} // ShardMetaHelpers

} // namespace NKikimr::NKqp
