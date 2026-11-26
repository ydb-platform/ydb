#include <ydb/core/tx/schemeshard/schemeshard_path_describer.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

using namespace NKikimr;
using namespace NSchemeShard;

namespace {

TTableInfo::TPtr MakeTestTable(ui32 n) {
    auto info = MakeIntrusive<TTableInfo>();

    info->Columns.emplace(1, TTableInfo::TColumn("key", 1, NScheme::TTypeInfo(NScheme::NTypeIds::Int64), "", true));
    info->Columns[1].KeyOrder = 0;
    info->KeyColumnIds = {1};

    TVector<TTableShardInfo> parts;
    for (ui32 i = 0; i < n; ++i) {
        // Ranges [0,1), [1,2), ..., [n-1, +inf)
        TString range = (i + 1 < n)
            ? TSerializedCellVec::Serialize({TCell::Make(ui64(i + 1))})
            : TSerializedCellVec::Serialize({});
        parts.emplace_back(TShardIdx(1, i), std::move(range));
    }
    info->SetPartitioning(std::move(parts));
    return info;
}

// Same as MakeTestTable, but with boundary keys at multiples of `stride`
// so genuine splits have integer keys available between boundaries
TTableInfo::TPtr MakeTestTableStride(ui32 n, ui64 stride) {
    auto info = MakeIntrusive<TTableInfo>();

    info->Columns.emplace(1, TTableInfo::TColumn("key", 1, NScheme::TTypeInfo(NScheme::NTypeIds::Int64), "", true));
    info->Columns[1].KeyOrder = 0;
    info->KeyColumnIds = {1};

    TVector<TTableShardInfo> parts;
    for (ui32 i = 0; i < n; ++i) {
        TString range = (i + 1 < n)
            ? TSerializedCellVec::Serialize({TCell::Make(stride * (i + 1))})
            : TSerializedCellVec::Serialize({});
        parts.emplace_back(TShardIdx(1, i), std::move(range));
    }
    info->SetPartitioning(std::move(parts));
    return info;
}

THashMap<TShardIdx, TShardInfo> MakeShardInfos(ui32 n, ui64 tabletBase = 1000) {
    THashMap<TShardIdx, TShardInfo> shards;
    for (ui32 i = 0; i < n; ++i) {
        TShardInfo shard;
        shard.TabletID = TTabletId(tabletBase + i);
        shards.emplace(TShardIdx(1, i), shard);
    }
    return shards;
}

TString DescribeWireForm(
    TTableInfo& tableInfo,
    const THashMap<TShardIdx, TShardInfo>& shardInfos,
    const TTableDescribeOptions& opts
) {
    TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
    BuildTableDescribe(result, tableInfo, shardInfos, opts);
    // The wire form of TEventPreSerializedPB: PreSerializedData goes first,
    // the serialized Record follows (see TEventPreSerializedPB::SerializeToArcadiaStream)
    return result.PreSerializedData + result.Record.SerializeAsString();
}

TString DescribePreSerialized(
    TTableInfo& tableInfo,
    const THashMap<TShardIdx, TShardInfo>& shardInfos,
    const TTableDescribeOptions& opts
) {
    TEvSchemeShard::TEvDescribeSchemeResultBuilder result("/Root/table", TPathId(1, 10));
    BuildTableDescribe(result, tableInfo, shardInfos, opts);
    return result.PreSerializedData;
}

// Byte-exact reference for PreSerializedData: a conventionally serialized
// TEvDescribeSchemeResult whose PathDescription carries exactly the sections
// BuildTableDescribe emits for these options.
TString MakePreSerializedByteReference(
    const TTableInfo& tableInfo,
    const THashMap<TShardIdx, TShardInfo>& shardInfos,
    const TTableDescribeOptions& opts
) {
    NKikimrScheme::TEvDescribeSchemeResult ref;
    auto* pd = ref.MutablePathDescription();
    const size_t n = tableInfo.GetPartitions().size();
    if (opts.ReturnBoundaries && n >= 2) {
        FillTableBoundaries(pd->MutableTable()->MutableSplitBoundary(), tableInfo);
    }
    if (opts.ReturnPartitioning && n) {
        FillTablePartitions(pd->MutableTablePartitions(), tableInfo, shardInfos, opts.ReturnRangeKey, false);
    }
    if (opts.ReturnPartitionStats && n) {
        for (const auto* p : tableInfo.GetPartitions()) {
            const auto* stats = tableInfo.GetStats().PartitionStats.FindPtr(p->ShardIdx);
            FillTableStats(pd->AddTablePartitionStats(), stats ? *stats : TPartitionStats{});
            FillTableMetrics(pd->AddTablePartitionMetrics(), stats ? *stats : TPartitionStats{});
        }
    }
    return ref.SerializeAsString();
}

NKikimrScheme::TEvDescribeSchemeResult MakeReferenceDescribe(
    const TTableInfo& tableInfo,
    const THashMap<TShardIdx, TShardInfo>& shardInfos,
    bool returnRangeKey,
    bool returnPartitionStats
) {
    NKikimrScheme::TEvDescribeSchemeResult ref;
    auto* pd = ref.MutablePathDescription();
    FillTablePartitions(pd->MutableTablePartitions(), tableInfo, shardInfos, returnRangeKey, false);
    FillTableBoundaries(pd->MutableTable()->MutableSplitBoundary(), tableInfo);
    if (returnPartitionStats) {
        for (const auto* p : tableInfo.GetPartitions()) {
            const auto* stats = tableInfo.GetStats().PartitionStats.FindPtr(p->ShardIdx);
            FillTableStats(pd->AddTablePartitionStats(), stats ? *stats : TPartitionStats{});
            FillTableMetrics(pd->AddTablePartitionMetrics(), stats ? *stats : TPartitionStats{});
        }
    }
    return ref;
}

NKikimrSchemeOp::TPathDescription ParsePathDescription(const TString& wire) {
    NKikimrScheme::TEvDescribeSchemeResult merged;
    UNIT_ASSERT(merged.ParseFromString(wire));
    return merged.GetPathDescription();
}

template <typename TField>
void AssertSameRepeated(const TField& actual, const TField& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    for (int i = 0; i < actual.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(
            actual.Get(i).SerializeAsString(),
            expected.Get(i).SerializeAsString()
        );
    }
}

void AssertSameTableParts(
    const NKikimrSchemeOp::TPathDescription& actual,
    const NKikimrSchemeOp::TPathDescription& expected
) {
    AssertSameRepeated(actual.GetTablePartitions(), expected.GetTablePartitions());
    AssertSameRepeated(actual.GetTable().GetSplitBoundary(), expected.GetTable().GetSplitBoundary());
    AssertSameRepeated(actual.GetTablePartitionStats(), expected.GetTablePartitionStats());
    AssertSameRepeated(actual.GetTablePartitionMetrics(), expected.GetTablePartitionMetrics());
}

TTableDescribeOptions AllOptions(bool returnRangeKey) {
    return TTableDescribeOptions{
        .CreateFinished = true,
        .ReturnBoundaries = true,
        .ReturnPartitioning = true,
        .ReturnPartitionStats = true,
        .ReturnRangeKey = returnRangeKey,
    };
}

} // namespace

// The element-wise pre-serialization caches (per-shard stats entries, per-position
// partitions/boundaries entries) are serialized independently and spliced into
// PreSerializedData. The gates: a merged parse of the concatenation must be
// equal to a message built by a single full fill, and the spliced payload must
// be byte-equal to that single fill's conventional serialization.
Y_UNIT_TEST_SUITE(TTableDescribeWireEquivalence) {
    // Counts cover the varint/tag edge cases of the element encoding
    Y_UNIT_TEST(PartitionsBoundariesAndStats) {
        for (const bool returnRangeKey : {true, false}) {
            for (const ui32 n : {1u, 2u, 3u, 17u, 300u}) {
                auto info = MakeTestTable(n);
                auto shards = MakeShardInfos(n);

                // Cold, cache-filling describe (lazy element builds on the first
                // pass): the wire form must already parse equal to the reference
                {
                    const TString wire = DescribeWireForm(*info, shards, AllOptions(returnRangeKey));
                    const auto actual = ParsePathDescription(wire);
                    const auto expected = MakeReferenceDescribe(
                        *info, shards, returnRangeKey, true
                    ).GetPathDescription();
                    AssertSameTableParts(actual, expected);
                }

                // The wire form must stay identical on the fully-cached (warm) path
                const TString warmWire = DescribeWireForm(*info, shards, AllOptions(returnRangeKey));
                {
                    const auto actual = ParsePathDescription(warmWire);
                    const auto expected = MakeReferenceDescribe(
                        *info, shards, returnRangeKey, true
                    ).GetPathDescription();
                    AssertSameTableParts(actual, expected);
                }

                // ...and byte-exact with a single fill's serialization
                UNIT_ASSERT_VALUES_EQUAL(
                    DescribePreSerialized(*info, shards, AllOptions(returnRangeKey)),
                    MakePreSerializedByteReference(*info, shards, AllOptions(returnRangeKey))
                );

                // ...and after a split step (a window splice of the element caches)
                if (n >= 2) {
                    const TShardIdx removed(1, 0);
                    const ui64 nextShardId = n + 1;
                    TVector<TTableShardInfo> dst = {
                        TTableShardInfo(
                            TShardIdx(1, nextShardId),
                            TSerializedCellVec::Serialize({TCell::Make(ui64(1))})),
                    };
                    TShardInfo newShard;
                    newShard.TabletID = TTabletId(5000);
                    shards.emplace(dst[0].ShardIdx, newShard);
                    info->ApplySplitMerge(std::move(dst), {removed}, 0, TInstant::Now(), false, false);

                    const TString wire = DescribeWireForm(*info, shards, AllOptions(returnRangeKey));
                    const auto actual = ParsePathDescription(wire);
                    const auto expected = MakeReferenceDescribe(
                        *info, shards, returnRangeKey, true
                    ).GetPathDescription();
                    AssertSameTableParts(actual, expected);
                    UNIT_ASSERT_VALUES_EQUAL(
                        DescribePreSerialized(*info, shards, AllOptions(returnRangeKey)),
                        MakePreSerializedByteReference(*info, shards, AllOptions(returnRangeKey))
                    );
                }
            }
        }
    }

    // Per-shard stats blobs are renewed where aggregate reports write the stats;
    // a describe after an update must reflect the new values on every affected shard
    Y_UNIT_TEST(StatsRenewedOnUpdate) {
        const ui32 n = 5;
        auto info = MakeTestTable(n);
        auto shards = MakeShardInfos(n);

        // Warm the caches first: without eager renewal the describe below
        // would lazily serve these zeroed entries no matter what the report
        // contains
        DescribeWireForm(*info, shards, AllOptions(true));

        TPartitionStats s;
        s.SeqNo = TMessageSeqNo{1, 0};
        s.RowCount = 42;
        s.DataSize = 100500;
        s.PartCount = 1;
        s.SetCurrentRawCpuUsage(777, TInstant::Zero());
        TDiskSpaceUsageDelta delta;
        info->UpdateShardStats(&delta, TShardIdx(1, 2), s, TInstant::Zero());

        const auto actual = ParsePathDescription(DescribeWireForm(*info, shards, AllOptions(true)));
        UNIT_ASSERT_VALUES_EQUAL(n, actual.GetTablePartitionStats().size());
        // both the stats and the metrics element must be renewed in place:
        // the warmed slots held zeros, so only the eager renewal can serve these
        UNIT_ASSERT_VALUES_EQUAL(42, actual.GetTablePartitionStats(2).GetRowCount());
        UNIT_ASSERT_VALUES_EQUAL(100500, actual.GetTablePartitionStats(2).GetDataSize());
        UNIT_ASSERT_VALUES_EQUAL(777, actual.GetTablePartitionMetrics(2).GetCPU());
        for (ui32 i = 0; i < n; ++i) {
            if (i == 2) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(0, actual.GetTablePartitionStats(i).GetRowCount());
        }
        // the aggregated part sees the update too
        UNIT_ASSERT_VALUES_EQUAL(42, actual.GetTableStats().GetRowCount());
    }

    // KIKIMR-4337: while a table is being created the pre-serialization caches
    // must self-clear on every describe
    Y_UNIT_TEST(CachesSelfClearWhileCreateInFlux) {
        const ui32 n = 4;
        auto info = MakeTestTable(n);
        auto shards = MakeShardInfos(n);

        TTableDescribeOptions opts = AllOptions(true);
        opts.CreateFinished = false;

        auto actual = ParsePathDescription(DescribeWireForm(*info, shards, opts));
        UNIT_ASSERT_VALUES_EQUAL(n, actual.GetTablePartitions().size());
        // every describe rebuilt everything from scratch — caches are empty again
        UNIT_ASSERT(info->PreserializedTablePartitionsElems.empty());
        UNIT_ASSERT(info->PreserializedTablePartitionsNoKeysElems.empty());
        UNIT_ASSERT(info->PreserializedTableSplitBoundariesElems.empty());
        UNIT_ASSERT(info->PreserializedTablePartitionStatsElems.empty());

        // An in-flux table may have shards without tablet ids yet (create/copy
        // in flight): the describe must serve the InvalidTabletId sentinel
        // like the old code did, not abort, and still not pin anything in
        // the caches
        shards[TShardIdx(1, 0)].TabletID = InvalidTabletId;
        actual = ParsePathDescription(DescribeWireForm(*info, shards, opts));
        UNIT_ASSERT_VALUES_EQUAL(n, actual.GetTablePartitions().size());
        UNIT_ASSERT_VALUES_EQUAL(ui64(InvalidTabletId), actual.GetTablePartitions(0).GetDatashardId());
        UNIT_ASSERT(info->PreserializedTablePartitionsElems.empty());
    }

    // Real 1->2 split (second-to-last partition: the right boundary migrates
    // into the unused slot) and 2->1 merge; bytes must equal a fresh fill.
    Y_UNIT_TEST(SpliceOnSplitAndMerge) {
        auto info = MakeTestTableStride(4, 10); // [0,10) [10,20) [20,30) [30,inf)
        auto shards = MakeShardInfos(4);
        DescribeWireForm(*info, shards, AllOptions(true)); // warm the caches

        // Split the second-to-last partition [20,30) into [20,25) and [25,30)
        {
            TVector<TTableShardInfo> dst = {
                TTableShardInfo(TShardIdx(1, 10), TSerializedCellVec::Serialize({TCell::Make(ui64(25))})),
                TTableShardInfo(TShardIdx(1, 11), TSerializedCellVec::Serialize({TCell::Make(ui64(30))})),
            };
            TShardInfo lo;
            lo.TabletID = TTabletId(5000);
            shards.emplace(dst[0].ShardIdx, lo);
            TShardInfo hi;
            hi.TabletID = TTabletId(5001);
            shards.emplace(dst[1].ShardIdx, hi);
            // splitFirstIdx = the removed partition's position
            info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 2)}, 2, TInstant::Now(), false, false);

            const auto actual = ParsePathDescription(DescribeWireForm(*info, shards, AllOptions(true)));
            AssertSameTableParts(
                actual,
                MakeReferenceDescribe(*info, shards, true, true)
                    .GetPathDescription());
            UNIT_ASSERT_VALUES_EQUAL(
                DescribePreSerialized(*info, shards, AllOptions(true)),
                MakePreSerializedByteReference(*info, shards, AllOptions(true))
            );
            UNIT_ASSERT_VALUES_EQUAL(5, actual.GetTablePartitions().size());
            UNIT_ASSERT_VALUES_EQUAL(4, actual.GetTable().GetSplitBoundary().size());
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                actual.GetTable().GetSplitBoundary(2)
                    .GetKeyPrefix().GetTuple(0).GetOptional().GetInt64()
            );
        }

        // Merge the first two partitions [0,10) [10,20) back into [0,20)
        {
            TVector<TTableShardInfo> dst = {
                TTableShardInfo(TShardIdx(1, 12), TSerializedCellVec::Serialize({TCell::Make(ui64(20))})),
            };
            TShardInfo mergedShard;
            mergedShard.TabletID = TTabletId(5002);
            shards.emplace(dst[0].ShardIdx, mergedShard);
            info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 0), TShardIdx(1, 1)}, 0, TInstant::Now(), false, false);

            const auto actual = ParsePathDescription(DescribeWireForm(*info, shards, AllOptions(true)));
            AssertSameTableParts(
                actual,
                MakeReferenceDescribe(*info, shards, true, true)
                    .GetPathDescription());
            UNIT_ASSERT_VALUES_EQUAL(
                DescribePreSerialized(*info, shards, AllOptions(true)),
                MakePreSerializedByteReference(*info, shards, AllOptions(true))
            );
        }
    }

    // A stats report landing between ApplySplitMerge and the next describe:
    // renewal must find the post-splice position
    Y_UNIT_TEST(RenewAfterSpliceBeforeDescribe) {
        auto info = MakeTestTableStride(3, 10); // [0,10) [10,20) [20,inf)
        auto shards = MakeShardInfos(3);
        DescribeWireForm(*info, shards, AllOptions(true)); // warm the caches

        // Split [0,10) into [0,5) and [5,10): two fresh cache slots at positions 0,1
        TVector<TTableShardInfo> dst = {
            TTableShardInfo(TShardIdx(1, 10), TSerializedCellVec::Serialize({TCell::Make(ui64(5))})),
            TTableShardInfo(TShardIdx(1, 11), TSerializedCellVec::Serialize({TCell::Make(ui64(10))})),
        };
        TShardInfo lo;
        lo.TabletID = TTabletId(5000);
        shards.emplace(dst[0].ShardIdx, lo);
        TShardInfo hi;
        hi.TabletID = TTabletId(5001);
        shards.emplace(dst[1].ShardIdx, hi);
        info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 0)}, 0, TInstant::Now(), false, false);

        // Report for a SURVIVOR shard whose slot is warm at its post-splice
        // position (the old [10,20) partition shifts to position 2): the
        // eager renewal must find the new position and overwrite the warm
        // entry - the lazy fill would keep serving the stale zeros
        TPartitionStats s;
        s.SeqNo = TMessageSeqNo{1, 0};
        s.RowCount = 42;
        s.SetCurrentRawCpuUsage(777, TInstant::Zero());
        TDiskSpaceUsageDelta delta;
        info->UpdateShardStats(&delta, TShardIdx(1, 1), s, TInstant::Zero());

        const auto actual = ParsePathDescription(DescribeWireForm(*info, shards, AllOptions(true)));
        UNIT_ASSERT_VALUES_EQUAL(4, actual.GetTablePartitionStats().size());
        UNIT_ASSERT_VALUES_EQUAL(42, actual.GetTablePartitionStats(2).GetRowCount());
        UNIT_ASSERT_VALUES_EQUAL(777, actual.GetTablePartitionMetrics(2).GetCPU());
        for (ui32 i : {0, 1, 3}) {
            UNIT_ASSERT_VALUES_EQUAL(0, actual.GetTablePartitionStats(i).GetRowCount());
        }
    }

    // A follower report updates only TopCpuUsage, which is not serialized:
    // the response must stay byte-identical and the caches intact
    Y_UNIT_TEST(FollowerReportDoesNotTouchSerializedStats) {
        const ui32 n = 3;
        auto info = MakeTestTable(n);
        auto shards = MakeShardInfos(n);

        TPartitionStats s;
        s.SeqNo = TMessageSeqNo{1, 0};
        s.RowCount = 42;
        TDiskSpaceUsageDelta delta;
        info->UpdateShardStats(&delta, TShardIdx(1, 1), s, TInstant::Zero());

        const TString warm = DescribePreSerialized(*info, shards, AllOptions(true));
        UNIT_ASSERT(!info->PreserializedTablePartitionStatsElems.empty());

        TPartitionStats followerStats;
        followerStats.SeqNo = TMessageSeqNo{1, 5};
        followerStats.RowCount = 999; // must not leak into the serialized elements
        info->UpdateShardStatsForFollower(1, TShardIdx(1, 1), followerStats);

        UNIT_ASSERT(!info->PreserializedTablePartitionStatsElems.empty());
        UNIT_ASSERT_VALUES_EQUAL(warm, DescribePreSerialized(*info, shards, AllOptions(true)));
    }

    // Same-count re-keys and reshuffles must clear the caches even though the
    // size token alone would not notice them
    Y_UNIT_TEST(SameCountRekeyClearsCaches) {
        const ui32 n = 3;

        // MovePartitioning: same shard set, positions reshuffled
        auto info = MakeTestTable(n);
        auto shards = MakeShardInfos(n);
        DescribeWireForm(*info, shards, AllOptions(true));
        UNIT_ASSERT(!info->PreserializedTablePartitionsElems.empty());
        {
            TVector<TTableShardInfo> moved;
            for (const auto* p : info->GetPartitions()) {
                moved.push_back(*p);
            }
            DoSwap(moved[0], moved[1]); // reshuffle, +inf range stays last
            info->MovePartitioning(std::move(moved));
            UNIT_ASSERT(info->PreserializedTablePartitionsElems.empty());
            UNIT_ASSERT(info->PreserializedTablePartitionsNoKeysElems.empty());
            UNIT_ASSERT(info->PreserializedTableSplitBoundariesElems.empty());
            UNIT_ASSERT(info->PreserializedTablePartitionStatsElems.empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            DescribePreSerialized(*info, shards, AllOptions(true)),
            MakePreSerializedByteReference(*info, shards, AllOptions(true))
        );

        // CopyPartitioning: fresh shard set, same count, stats re-keyed
        auto copy = MakeTestTable(n);
        DescribeWireForm(*copy, shards, AllOptions(true));
        UNIT_ASSERT(!copy->PreserializedTablePartitionsElems.empty());
        {
            TVector<TTableShardInfo> rekeyed;
            for (const auto* p : copy->GetPartitions()) {
                rekeyed.emplace_back(TShardIdx(2, p->Position), p->EndOfRange);
            }
            copy->CopyPartitioning(std::move(rekeyed));
            UNIT_ASSERT(copy->PreserializedTablePartitionsElems.empty());
            UNIT_ASSERT(copy->PreserializedTablePartitionsNoKeysElems.empty());
            UNIT_ASSERT(copy->PreserializedTableSplitBoundariesElems.empty());
            UNIT_ASSERT(copy->PreserializedTablePartitionStatsElems.empty());
        }
    }
}

