#include "key_range_split.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NPQ;

namespace {

const TVector<NScheme::TTypeInfo> Uint64Key = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)};

TSerializedCellVec MakeKey(ui64 value) {
    const TVector<TCell> cells = {TCell::Make<ui64>(value)};
    return TSerializedCellVec(TSerializedCellVec::Serialize(cells));
}

TPartitionKeyRange MakeRange(std::optional<ui64> from, std::optional<ui64> to) {
    TPartitionKeyRange range;
    if (from) {
        range.FromBound = MakeKey(*from);
    }
    if (to) {
        range.ToBound = MakeKey(*to);
    }
    return range;
}

TKeyRangeSource MakeSource(const TString& sourceId, std::optional<ui64> from, std::optional<ui64> to) {
    return {.SourceId = sourceId, .KeyRange = MakeRange(from, to)};
}

void AddKeys(TKeyRangeWriteStats& stats, ui64 from, ui64 to, ui64 bytesPerKey) {
    for (ui64 key = from; key < to; ++key) {
        stats.Keys.push_back({.Key = MakeKey(key), .WrittenBytes = bytesPerKey});
    }
}

TVector<TString> GetSourceIds(const TPlannedPartition& partition) {
    TVector<TString> sourceIds;
    for (const auto& source : partition.Sources) {
        sourceIds.push_back(source.SourceId);
    }
    return sourceIds;
}

const TVector<NScheme::TTypeInfo> Uint64PairKey = {
    NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
    NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
};

TSerializedCellVec MakePairKey(std::optional<ui64> first, std::optional<ui64> second) {
    const TVector<TCell> cells = {
        first ? TCell::Make<ui64>(*first) : TCell(),
        second ? TCell::Make<ui64>(*second) : TCell(),
    };
    return TSerializedCellVec(TSerializedCellVec::Serialize(cells));
}

TSerializedCellVec MakePrefixKey(ui64 first) {
    const TVector<TCell> cells = {TCell::Make<ui64>(first)};
    return TSerializedCellVec(TSerializedCellVec::Serialize(cells));
}

void AssertBound(const TMaybe<TSerializedCellVec>& bound, std::optional<ui64> expected) {
    UNIT_ASSERT_VALUES_EQUAL(bound.Defined(), expected.has_value());
    if (expected) {
        UNIT_ASSERT_VALUES_EQUAL(bound->GetBuffer(), MakeKey(*expected).GetBuffer());
    }
}

} // anonymous

Y_UNIT_TEST_SUITE(TPlanKeyRangeSplitTest) {
    Y_UNIT_TEST(ShouldSplitInsideSourceWhenPartitionHasOneSource) {
        // given
        TKeyRangeWriteStats stats;
        stats.Sources = {MakeSource("ds1", std::nullopt, std::nullopt)};
        AddKeys(stats, 1, 101, 1);

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(std::nullopt, std::nullopt), Uint64Key, stats);

        // then
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        AssertBound(plan[0].Range.FromBound, std::nullopt);
        AssertBound(plan[0].Range.ToBound, 51);
        AssertBound(plan[1].Range.FromBound, 51);
        AssertBound(plan[1].Range.ToBound, std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(plan[0].EstimatedBytes, 50);
        UNIT_ASSERT_VALUES_EQUAL(plan[1].EstimatedBytes, 50);
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[0]), TVector<TString>({"ds1"}));
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[1]), TVector<TString>({"ds1"}));
    }

    Y_UNIT_TEST(ShouldSplitInsideSourceWhenUpperBoundIsPresentButEmpty) {
        // given
        TKeyRangeWriteStats stats;
        stats.Sources = {MakeSource("ds1", std::nullopt, std::nullopt)};
        stats.Sources[0].KeyRange.ToBound = TSerializedCellVec(TString());
        AddKeys(stats, 1, 101, 1);
        auto parent = MakeRange(std::nullopt, std::nullopt);
        parent.ToBound = TSerializedCellVec(TString());

        // when
        const auto plan = PlanKeyRangeSplit(parent, Uint64Key, stats);

        // then
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        AssertBound(plan[0].Range.ToBound, 51);
        AssertBound(plan[1].Range.FromBound, 51);
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[0]), TVector<TString>({"ds1"}));
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[1]), TVector<TString>({"ds1"}));
    }

    Y_UNIT_TEST(ShouldSplitBetweenSourcesWhenSourceBoundaryHalvesLoad) {
        // given
        TKeyRangeWriteStats stats;
        stats.Sources = {
            MakeSource("ds1", std::nullopt, 100),
            MakeSource("ds2", 100, 200),
            MakeSource("ds3", 200, std::nullopt),
        };
        AddKeys(stats, 10, 20, 2);
        AddKeys(stats, 110, 120, 3);
        AddKeys(stats, 210, 220, 5);

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(std::nullopt, std::nullopt), Uint64Key, stats);

        // then
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        AssertBound(plan[0].Range.ToBound, 200);
        UNIT_ASSERT_VALUES_EQUAL(plan[0].EstimatedBytes, 50);
        UNIT_ASSERT_VALUES_EQUAL(plan[1].EstimatedBytes, 50);
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[0]), TVector<TString>({"ds1", "ds2"}));
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[1]), TVector<TString>({"ds3"}));
    }

    Y_UNIT_TEST(ShouldSplitInsideHotSourceWhenItWritesMostOfLoad) {
        // given
        TKeyRangeWriteStats stats;
        stats.Sources = {MakeSource("ds1", std::nullopt, 100)};
        for (ui64 i = 1; i < 10; ++i) {
            stats.Sources.push_back(MakeSource("ds" + ToString(i + 1), i * 100, i < 9 ? std::optional<ui64>((i + 1) * 100) : std::nullopt));
        }
        AddKeys(stats, 0, 90, 1);
        stats.Keys.push_back({.Key = MakeKey(100), .WrittenBytes = 2});
        for (ui64 i = 2; i < 10; ++i) {
            stats.Keys.push_back({.Key = MakeKey(i * 100), .WrittenBytes = 1});
        }

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(std::nullopt, std::nullopt), Uint64Key, stats);

        // then
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        AssertBound(plan[0].Range.ToBound, 50);
        UNIT_ASSERT_VALUES_EQUAL(plan[0].EstimatedBytes, 50);
        UNIT_ASSERT_VALUES_EQUAL(plan[1].EstimatedBytes, 50);
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[0]), TVector<TString>({"ds1"}));
        UNIT_ASSERT_VALUES_EQUAL(plan[1].Sources.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(plan[1].Sources.front().SourceId, "ds1");
    }

    Y_UNIT_TEST(ShouldNotSplitWhenAllLoadIsOnOneKey) {
        // given
        TKeyRangeWriteStats stats;
        stats.Sources = {
            MakeSource("ds1", std::nullopt, 100),
            MakeSource("ds2", 100, std::nullopt),
        };
        stats.Keys.push_back({.Key = MakeKey(150), .WrittenBytes = 100});

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(std::nullopt, std::nullopt), Uint64Key, stats);

        // then
        UNIT_ASSERT(plan.empty());
    }

    Y_UNIT_TEST(ShouldIgnoreKeysOutsideParentRange) {
        // given
        TKeyRangeWriteStats stats;
        stats.Sources = {MakeSource("ds1", 100, 300)};
        stats.Keys = {
            {.Key = MakeKey(50), .WrittenBytes = 1000},
            {.Key = MakeKey(150), .WrittenBytes = 10},
            {.Key = MakeKey(250), .WrittenBytes = 10},
            {.Key = MakeKey(300), .WrittenBytes = 1000},
        };

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(100, 300), Uint64Key, stats);

        // then
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        AssertBound(plan[0].Range.FromBound, 100);
        AssertBound(plan[0].Range.ToBound, 250);
        AssertBound(plan[1].Range.FromBound, 250);
        AssertBound(plan[1].Range.ToBound, 300);
        UNIT_ASSERT_VALUES_EQUAL(plan[0].EstimatedBytes, 10);
        UNIT_ASSERT_VALUES_EQUAL(plan[1].EstimatedBytes, 10);
    }

    Y_UNIT_TEST(ShouldNotSplitWhenKeysAreNotFullKeysOfSchema) {
        // given
        const TVector<TCell> cells = {TCell::Make<ui64>(1), TCell::Make<ui64>(2)};
        TKeyRangeWriteStats stats;
        stats.Sources = {MakeSource("ds1", std::nullopt, std::nullopt)};
        stats.Keys = {
            {.Key = TSerializedCellVec(TSerializedCellVec::Serialize(cells)), .WrittenBytes = 10},
            {.Key = TSerializedCellVec(TSerializedCellVec::Serialize(cells)), .WrittenBytes = 10},
        };

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(std::nullopt, std::nullopt), Uint64Key, stats);

        // then
        UNIT_ASSERT(plan.empty());
    }

    Y_UNIT_TEST(ShouldOrderKeysByTypeWhenBytesOrderDiffers) {
        // given
        TKeyRangeWriteStats stats;
        stats.Keys = {
            {.Key = MakeKey(256), .WrittenBytes = 10},
            {.Key = MakeKey(2), .WrittenBytes = 10},
        };

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(std::nullopt, std::nullopt), Uint64Key, stats);

        // then
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        AssertBound(plan[1].Range.FromBound, 256);
        UNIT_ASSERT_VALUES_EQUAL(plan[0].EstimatedBytes, 10);
        UNIT_ASSERT_VALUES_EQUAL(plan[1].EstimatedBytes, 10);
    }

    Y_UNIT_TEST(ShouldPadSourceBoundaryWithNullsWhenItIsKeyPrefix) {
        // given
        TKeyRangeWriteStats stats;
        stats.Sources = {
            {.SourceId = "ds1", .KeyRange = {.FromBound = Nothing(), .ToBound = MakePrefixKey(5)}},
            {.SourceId = "ds2", .KeyRange = {.FromBound = MakePrefixKey(5), .ToBound = Nothing()}},
        };
        stats.Keys = {
            {.Key = MakePairKey(4, 1), .WrittenBytes = 10},
            {.Key = MakePairKey(6, 1), .WrittenBytes = 10},
        };

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(std::nullopt, std::nullopt), Uint64PairKey, stats);

        // then
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        UNIT_ASSERT(plan[1].Range.FromBound);
        UNIT_ASSERT_VALUES_EQUAL(plan[1].Range.FromBound->GetBuffer(), MakePairKey(5, std::nullopt).GetBuffer());
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[0]), TVector<TString>({"ds1"}));
        UNIT_ASSERT_VALUES_EQUAL(GetSourceIds(plan[1]), TVector<TString>({"ds2"}));
    }
}

Y_UNIT_TEST_SUITE(TKeyRangeKeysCollectorTest) {
    Y_UNIT_TEST(ShouldCollectOnlyFullKeysOfSchema) {
        // given
        TKeyRangeKeysCollector collector(Uint64Key, 1, TDuration::Hours(1));
        const TInstant now = TInstant::Seconds(1000);
        const TVector<TCell> pair = {TCell::Make<ui64>(1), TCell::Make<ui64>(2)};

        // when
        const bool fullKey = collector.Add(MakeKey(5).GetBuffer(), 16, now);
        const bool longKey = collector.Add(TSerializedCellVec::Serialize(pair), 16, now);
        const bool garbage = collector.Add("garbage", 16, now);
        const bool emptyKey = collector.Add("", 16, now);

        // then
        UNIT_ASSERT(fullKey);
        UNIT_ASSERT(!longKey);
        UNIT_ASSERT(!garbage);
        UNIT_ASSERT(!emptyKey);
        const auto keys = collector.GetKeys(now);
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(keys[0].Key.GetBuffer(), MakeKey(5).GetBuffer());
        UNIT_ASSERT_VALUES_EQUAL(keys[0].WrittenBytes, 16);
    }

    Y_UNIT_TEST(ShouldForgetKeysWhenWindowExpires) {
        // given
        TKeyRangeKeysCollector collector(Uint64Key, 1, TDuration::Seconds(1));
        const TInstant now = TInstant::Seconds(1000);
        collector.Add(MakeKey(5).GetBuffer(), 16, now);

        // when
        const auto keys = collector.GetKeys(now + TDuration::Seconds(2));

        // then
        UNIT_ASSERT(keys.empty());
    }

    Y_UNIT_TEST(ShouldPlanSplitInsideSourceFromCollectedKeys) {
        // given
        TKeyRangeKeysCollector collector(Uint64Key, 1, TDuration::Hours(1));
        const TInstant now = TInstant::Seconds(1000);
        for (ui64 key = 1; key <= 100; ++key) {
            collector.Add(MakeKey(key).GetBuffer(), 16, now);
        }
        TKeyRangeWriteStats stats;
        stats.Sources = {MakeSource("ds1", std::nullopt, std::nullopt)};
        stats.Keys = collector.GetKeys(now);

        // when
        const auto plan = PlanKeyRangeSplit(MakeRange(std::nullopt, std::nullopt), Uint64Key, stats);

        // then
        UNIT_ASSERT_VALUES_EQUAL(plan.size(), 2);
        AssertBound(plan[1].Range.FromBound, 51);
        UNIT_ASSERT_VALUES_EQUAL(plan[0].EstimatedBytes, 800);
        UNIT_ASSERT_VALUES_EQUAL(plan[1].EstimatedBytes, 800);
    }
}
