#include <ydb/core/statistics/aggregator/key_range_predicate.h>
#include <ydb/core/statistics/aggregator/select_builder.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <library/cpp/testing/unittest/registar.h>

#include <optional>

using namespace NKikimr;
using namespace NKikimr::NStat;

Y_UNIT_TEST_SUITE(KeyRangePredicate) {

    TSerializedTableRange MakeRange(
        TVector<TCell> from, bool fromInclusive,
        TVector<TCell> to, bool toInclusive)
    {
        TSerializedTableRange range;
        if (!from.empty()) {
            range.From = TSerializedCellVec(from);
        }
        range.FromInclusive = fromInclusive;
        if (!to.empty()) {
            range.To = TSerializedCellVec(to);
        }
        range.ToInclusive = toInclusive;
        return range;
    }

    TVector<TKeyDesc::TPartitionInfo> MakeUniformPartitions(ui32 shardCount, ui64 endStep) {
        TVector<TKeyDesc::TPartitionInfo> partitions(shardCount);
        for (ui32 i = 0; i < shardCount; ++i) {
            partitions[i].ShardId = i + 1;
            if (i + 1 < shardCount) {
                partitions[i].Range = TKeyDesc::TPartitionRangeInfo{
                    .EndKeyPrefix = TSerializedCellVec(TVector<TCell>{TCell::Make(endStep * (i + 1))}),
                    .IsInclusive = false,
                };
            } else {
                partitions[i].Range = TKeyDesc::TPartitionRangeInfo{};
            }
        }
        return partitions;
    }

    struct TPred {
        TString Where;
        TString Declares;
        TString Error;
        NYdb::TParamsBuilder Params;
    };

    bool BuildPred(
        TConstArrayRef<TString> names,
        TConstArrayRef<NScheme::TTypeInfo> types,
        const TSerializedTableRange& range,
        TPred& p)
    {
        return TryBuildKeyRangePredicate(names, types, range, p.Where, p.Declares, p.Params, p.Error);
    }

    const TVector<TString> Key{"Key"};
    const TVector<NScheme::TTypeInfo> Uint64{NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)};
    const TVector<TString> KeyValue{"Key", "Value"};
    const TVector<NScheme::TTypeInfo> Uint64String{
        NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
        NScheme::TTypeInfo(NScheme::NTypeIds::String),
    };

    Y_UNIT_TEST(FullRangeIsEmptyWhere) {
        TPred p;
        UNIT_ASSERT(BuildPred(Key, Uint64, TSerializedTableRange{}, p));
        UNIT_ASSERT_VALUES_EQUAL(p.Where, "");
        UNIT_ASSERT_VALUES_EQUAL(p.Declares, "");
        UNIT_ASSERT(p.Params.Build().Empty());
    }

    Y_UNIT_TEST(SingleColumnExclusiveInclusive) {
        auto range = MakeRange(
            {TCell::Make(ui64(100))}, false,
            {TCell::Make(ui64(200))}, true);
        TPred p;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, range, p), p.Error);
        UNIT_ASSERT(p.Where.Contains("`Key` > $from_0"));
        UNIT_ASSERT(p.Where.Contains("(`Key` IS NULL OR `Key` <= $to_0)"));
        UNIT_ASSERT(p.Declares.Contains("DECLARE $from_0 AS Uint64;"));
        UNIT_ASSERT(p.Declares.Contains("DECLARE $to_0 AS Uint64;"));
        UNIT_ASSERT(!p.Params.Build().Empty());
    }

    Y_UNIT_TEST(OpenUpperBound) {
        TPred p;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, MakeRange({TCell::Make(ui64(100))}, true, {}, false), p), p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where, "`Key` >= $from_0");
        UNIT_ASSERT(!p.Where.Contains("IS NULL"));
        UNIT_ASSERT(!p.Declares.Contains("$to_"));
    }

    Y_UNIT_TEST(OpenLowerBound) {
        TPred p;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, MakeRange({}, true, {TCell::Make(ui64(200))}, false), p), p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where, "(`Key` IS NULL OR `Key` < $to_0)");
        UNIT_ASSERT(!p.Where.Contains("$from_"));
    }

    Y_UNIT_TEST(CompositePrefixBound) {
        auto range = MakeRange({TCell::Make(ui64(100))}, false, {TCell::Make(ui64(200))}, true);
        TPred p;
        UNIT_ASSERT_C(BuildPred(KeyValue, Uint64String, range, p), p.Error);
        UNIT_ASSERT(p.Where.Contains("`Key` > $from_0"));
        UNIT_ASSERT(p.Where.Contains("(`Key` IS NULL OR `Key` <= $to_0)"));
        UNIT_ASSERT(!p.Where.Contains("AsTuple"));
    }

    Y_UNIT_TEST(CompositeFullKeyBound) {
        TString value("abc");
        auto range = MakeRange(
            {TCell::Make(ui64(100)), TCell(value.data(), value.size())}, true,
            {TCell::Make(ui64(200)), TCell(value.data(), value.size())}, false);
        TPred p;
        UNIT_ASSERT_C(BuildPred(KeyValue, Uint64String, range, p), p.Error);
        UNIT_ASSERT(p.Where.Contains("AsTuple(`Key`,`Value`) >= AsTuple($from_0,$from_1)"));
        UNIT_ASSERT(p.Where.Contains(
            "((`Key` IS NULL OR `Key` < $to_0) OR "
            "(`Key` = $to_0 AND (`Value` IS NULL OR `Value` < $to_1)))"));
        UNIT_ASSERT(p.Declares.Contains("DECLARE $from_1 AS String;"));
    }

    Y_UNIT_TEST(SelectHasWhereAndNoTabletId) {
        auto range = MakeRange({TCell::Make(ui64(100))}, true, {TCell::Make(ui64(200))}, false);
        TPred p;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, range, p), p.Error);

        TSelectBuilder builder(/*isIntermediateAggregation=*/true);
        builder.AddBuiltinAggregation({}, "count");
        const TString sql = builder.Build("Table", /*tabletId=*/{}, p.Where, p.Declares);
        UNIT_ASSERT(sql.Contains("DECLARE $from_0 AS Uint64;"));
        UNIT_ASSERT(sql.Contains("WHERE `Key` >= $from_0 AND (`Key` IS NULL OR `Key` < $to_0)"));
        UNIT_ASSERT(!sql.Contains("TabletId"));
    }

    Y_UNIT_TEST(InclusiveNullLowerBoundIsOpen) {
        TPred p;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, MakeRange({TCell()}, true, {}, false), p), p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where, "");
    }

    Y_UNIT_TEST(TrailingNullPaddingIsPrefixBound) {
        auto range = MakeRange(
            {TCell::Make(ui64(100)), TCell()}, true,
            {TCell::Make(ui64(200)), TCell()}, false);
        TPred p;
        UNIT_ASSERT_C(BuildPred(KeyValue, Uint64String, range, p), p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where, "`Key` >= $from_0 AND (`Key` IS NULL OR `Key` < $to_0)");
        UNIT_ASSERT(!p.Declares.Contains("$from_1"));
        UNIT_ASSERT(!p.Where.Contains("AsTuple"));
    }

    Y_UNIT_TEST(SplitRangesIncludeNullOnlyOnTheLeft) {
        TVector<TKeyDesc::TPartitionInfo> partitions(2);
        partitions[0].ShardId = 1;
        partitions[0].Range = TKeyDesc::TPartitionRangeInfo{
            .EndKeyPrefix = TSerializedCellVec(TVector<TCell>{TCell::Make(ui64(10))}),
            .IsInclusive = false,
        };
        partitions[1].ShardId = 2;
        partitions[1].Range = TKeyDesc::TPartitionRangeInfo{};

        auto subranges = MakeShardSubranges(partitions);
        UNIT_ASSERT_VALUES_EQUAL(subranges.size(), 2);

        TPred left;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, subranges[0].second, left), left.Error);
        UNIT_ASSERT_VALUES_EQUAL(left.Where, "(`Key` IS NULL OR `Key` < $to_0)");

        TPred right;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, subranges[1].second, right), right.Error);
        UNIT_ASSERT_VALUES_EQUAL(right.Where, "`Key` >= $from_0");
        UNIT_ASSERT(!right.Where.Contains("IS NULL"));
    }

    Y_UNIT_TEST(NullPrefixBoundIsEncoded) {
        TString mid("m");
        auto range = MakeRange({TCell(), TCell(mid.data(), mid.size())}, true, {}, false);
        TPred p;
        UNIT_ASSERT_C(BuildPred(KeyValue, Uint64String, range, p), p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where,
            "(`Key` IS NOT NULL OR (`Key` IS NULL AND `Value` >= $from_1))");
        UNIT_ASSERT(p.Declares.Contains("DECLARE $from_1 AS String;"));
        UNIT_ASSERT(!p.Declares.Contains("$from_0"));
        UNIT_ASSERT(!p.Params.Build().Empty());
    }

    Y_UNIT_TEST(NullPrefixExclusiveUpperBound) {
        TString mid("m");
        auto range = MakeRange({}, true, {TCell(), TCell(mid.data(), mid.size())}, false);
        TPred p;
        UNIT_ASSERT_C(BuildPred(KeyValue, Uint64String, range, p), p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where,
            "(`Key` IS NULL AND (`Value` IS NULL OR `Value` < $to_1))");
        UNIT_ASSERT(p.Declares.Contains("DECLARE $to_1 AS String;"));
        UNIT_ASSERT(!p.Declares.Contains("$to_0"));
    }

    Y_UNIT_TEST(SplitAtNullPrefixCoversBothSides) {
        TString mid("m");
        TVector<TKeyDesc::TPartitionInfo> partitions(2);
        partitions[0].ShardId = 1;
        partitions[0].Range = TKeyDesc::TPartitionRangeInfo{
            .EndKeyPrefix = TSerializedCellVec(TVector<TCell>{
                TCell(), TCell(mid.data(), mid.size())}),
            .IsInclusive = false,
        };
        partitions[1].ShardId = 2;
        partitions[1].Range = TKeyDesc::TPartitionRangeInfo{};

        auto subranges = MakeShardSubranges(partitions);
        UNIT_ASSERT_VALUES_EQUAL(subranges.size(), 2);

        TPred left;
        UNIT_ASSERT_C(BuildPred(KeyValue, Uint64String, subranges[0].second, left), left.Error);
        UNIT_ASSERT(left.Where.Contains("`Key` IS NULL"));
        UNIT_ASSERT(left.Where.Contains("`Value` < $to_1"));
        UNIT_ASSERT(!left.Where.Contains("IS NOT NULL"));

        TPred right;
        UNIT_ASSERT_C(BuildPred(KeyValue, Uint64String, subranges[1].second, right), right.Error);
        UNIT_ASSERT(right.Where.Contains("`Key` IS NOT NULL"));
        UNIT_ASSERT(right.Where.Contains("`Value` >= $from_1"));
    }

    Y_UNIT_TEST(ExclusiveNullLowerBound) {
        TPred p;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, MakeRange({TCell()}, false, {}, false), p), p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where, "`Key` IS NOT NULL");
        UNIT_ASSERT_VALUES_EQUAL(p.Declares, "");
    }

    Y_UNIT_TEST(InclusiveNullUpperBound) {
        TPred p;
        UNIT_ASSERT_C(BuildPred(Key, Uint64, MakeRange({}, true, {TCell()}, true), p), p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where, "`Key` IS NULL");
        UNIT_ASSERT_VALUES_EQUAL(p.Declares, "");
    }

    Y_UNIT_TEST(UnsupportedTypeFails) {
        const TVector<NScheme::TTypeInfo> types{NScheme::TTypeInfo(NScheme::NTypeIds::PairUi64Ui64)};
        TPred p;
        UNIT_ASSERT(!BuildPred(Key, types, MakeRange({TCell::Make(ui64(1))}, true, {}, false), p));
        UNIT_ASSERT(p.Error.Contains("unsupported key column type"));
        UNIT_ASSERT(!CanEncodeKeyBoundType(NScheme::NTypeIds::PairUi64Ui64));
        UNIT_ASSERT(!CanEncodeKeyBoundType(NScheme::NTypeIds::Pg));
        UNIT_ASSERT(!CanEncodeKeyRangePredicate(types));
        UNIT_ASSERT(CanEncodeKeyBoundType(NScheme::NTypeIds::DyNumber));
        UNIT_ASSERT(CanEncodeKeyRangePredicate(Uint64));
    }

    Y_UNIT_TEST(PgTypeFailsEncoding) {
        auto* desc = NPg::TypeDescFromPgTypeName("pgint8");
        UNIT_ASSERT(desc);
        const TVector<NScheme::TTypeInfo> types{NScheme::TTypeInfo(desc)};
        TPred p;
        UNIT_ASSERT(!BuildPred(Key, types, MakeRange({TCell::Make(i64(1))}, true, {}, false), p));
        UNIT_ASSERT(p.Error.Contains("unsupported key column type"));
        UNIT_ASSERT(p.Error.Contains("pgint8"));
        UNIT_ASSERT(!CanEncodeKeyRangePredicate(types));
    }

    Y_UNIT_TEST(DyNumberBoundUsesTextParam) {
        const TString text = "10.23";
        auto binary = NDyNumber::ParseDyNumberString(text);
        UNIT_ASSERT(binary);
        UNIT_ASSERT_VALUES_UNEQUAL(*binary, text);

        TPred p;
        const TVector<NScheme::TTypeInfo> types{NScheme::TTypeInfo(NScheme::NTypeIds::DyNumber)};
        UNIT_ASSERT_C(
            BuildPred(Key, types, MakeRange({TCell(binary->data(), binary->size())}, true, {}, false), p),
            p.Error);
        UNIT_ASSERT_VALUES_EQUAL(p.Where, "`Key` >= $from_0");
        UNIT_ASSERT(p.Declares.Contains("DECLARE $from_0 AS DyNumber;"));

        auto built = p.Params.Build();
        auto value = built.GetValue("$from_0");
        UNIT_ASSERT(value);
        NYdb::TValueParser parser(*value);
        const auto& dyNumber = parser.GetDyNumber();
        auto canonical = NDyNumber::DyNumberToString(*binary);
        UNIT_ASSERT(canonical);
        UNIT_ASSERT_VALUES_EQUAL(TString(dyNumber.data(), dyNumber.size()), *canonical);
        UNIT_ASSERT(NDyNumber::IsValidDyNumberString(dyNumber));
        UNIT_ASSERT_VALUES_UNEQUAL(TString(dyNumber.data(), dyNumber.size()), *binary);
    }

    Y_UNIT_TEST(MakeShardSubrangesFromEndPrefixes) {
        TVector<TKeyDesc::TPartitionInfo> partitions(3);
        partitions[0].ShardId = 1;
        partitions[0].Range = TKeyDesc::TPartitionRangeInfo{
            .EndKeyPrefix = TSerializedCellVec(TVector<TCell>{TCell::Make(ui64(100))}),
            .IsInclusive = false,
        };
        partitions[1].ShardId = 2;
        partitions[1].Range = TKeyDesc::TPartitionRangeInfo{
            .EndKeyPrefix = TSerializedCellVec(TVector<TCell>{TCell::Make(ui64(200))}),
            .IsInclusive = false,
        };
        partitions[2].ShardId = 3;
        partitions[2].Range = TKeyDesc::TPartitionRangeInfo{};

        auto subranges = MakeShardSubranges(partitions);
        UNIT_ASSERT_VALUES_EQUAL(subranges.size(), 3);
        UNIT_ASSERT(!subranges[0].second.From);
        UNIT_ASSERT(subranges[0].second.To);
        UNIT_ASSERT(!subranges[0].second.ToInclusive);
        UNIT_ASSERT(subranges[1].second.FromInclusive);
        UNIT_ASSERT(!subranges[2].second.To);
    }

    Y_UNIT_TEST(BudgetedSubrangesMergeTenShardsToFive) {
        auto subranges = MakeBudgetedSubranges(
            MakeUniformPartitions(10, 100), /*tableBytesSize=*/500, /*rangeBudgetBytes=*/100);
        UNIT_ASSERT_VALUES_EQUAL(subranges.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(subranges[0].first, 1);
        UNIT_ASSERT_VALUES_EQUAL(subranges[1].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(subranges[4].first, 9);
        UNIT_ASSERT(!subranges[4].second.To);
    }

    Y_UNIT_TEST(BudgetedSubrangesDoNotSplitShards) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeBudgetedSubranges(MakeUniformPartitions(4, 100), 500, 100).size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(
            MakeBudgetedSubranges(MakeUniformPartitions(4, 100), 500, 0).size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(
            MakeBudgetedSubranges(MakeUniformPartitions(4, 100), std::nullopt, 100).size(), 4);
    }

}
