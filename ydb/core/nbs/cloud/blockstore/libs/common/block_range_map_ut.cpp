#include "block_range_map.h"

#include <library/cpp/testing/unittest/registar.h>

#include <span>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEmptyType
{
};

using TTestRangeMap = TBlockRangeMap<ui64, TEmptyType>;

void AddRanges(
    const std::span<const TTestRangeMap::TItem>& ranges,
    TTestRangeMap* map)
{
    for (const auto& r: ranges) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            true,
            map->AddRange(r.Key, r.Range),
            TStringBuilder() << r.Key << r.Range.Print());
    }
}

void RemoveRanges(
    const std::span<const TTestRangeMap::TItem>& ranges,
    TTestRangeMap* map)
{
    for (const auto& r: ranges) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            true,
            map->ExtractRange(r.Key).has_value(),
            TStringBuilder() << r.Key << r.Range.Print());
    }
}

void TestOverlaps(
    const TTestRangeMap& map,
    const std::span<const TTestRangeMap::TItem>& ranges,
    TBlockRange64 rangeToCheck)
{
    TStringBuilder sb;
    sb << "List: " << map.DebugPrint() << ", ranges: ";
    for (const auto& r: ranges) {
        sb << r.Key << r.Range.Print();
    }
    sb << " failed: ";

    auto cmp = [](TBlockRange64 lhs, TBlockRange64 rhs)
    {
        return std::tie(lhs.End, lhs.Start) < std::tie(rhs.End, rhs.Start);
    };

    auto expected =
        [&](TBlockRange64 range) -> std::optional<TTestRangeMap::TItem>
    {
        std::optional<TTestRangeMap::TItem> result = std::nullopt;
        for (auto r: ranges) {
            if (r.Range.Overlaps(range)) {
                if (!result) {
                    result = r;
                } else if (cmp(r.Range, result->Range)) {
                    result = r;
                }
            }
        }
        return result;
    };

    for (ui64 i = rangeToCheck.Start; i <= rangeToCheck.End; ++i) {
        for (ui64 j = i; j <= rangeToCheck.End; ++j) {
            auto r = TBlockRange64::MakeClosedInterval(i, j);
            auto expectedResult = expected(r);
            const auto* result = map.FindFirstOverlapping(r);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedResult.has_value(),
                result != nullptr,
                sb + r.Print());

            if (expectedResult) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expectedResult->Key,
                    result->Key,
                    sb + r.Print());
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expectedResult->Range,
                    result->Range,
                    sb + r.Print());
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeMapTest)
{
    Y_UNIT_TEST(Empty)
    {
        const TVector<TTestRangeMap::TItem> ranges = {};

        TTestRangeMap map;
        UNIT_ASSERT_VALUES_EQUAL(0, map.Size());
        UNIT_ASSERT_VALUES_EQUAL(true, map.Empty());

        UNIT_ASSERT_VALUES_EQUAL("", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(AddOnce)
    {
        TTestRangeMap map;
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            map.AddRange(111, TBlockRange64::WithLength(10, 20)));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            map.AddRange(111, TBlockRange64::WithLength(10, 20)));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            map.AddRange(111, TBlockRange64::WithLength(20, 10)));

        UNIT_ASSERT_VALUES_EQUAL("111[10..29]", map.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(1, map.Size());
        UNIT_ASSERT_VALUES_EQUAL(false, map.Empty());

        UNIT_ASSERT_VALUES_EQUAL(true, map.RemoveRange(111));
        UNIT_ASSERT_VALUES_EQUAL(0, map.Size());
        UNIT_ASSERT_VALUES_EQUAL(true, map.Empty());
        UNIT_ASSERT_VALUES_EQUAL(false, map.ExtractRange(111).has_value());
    }

    Y_UNIT_TEST(OneBlock)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeOneBlock(2)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[2..2]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OneBlockOnLeft)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 10, .Range = TBlockRange64::MakeOneBlock(0)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("10[0..0]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 3));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(RangeOnLeft)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 10, .Range = TBlockRange64::WithLength(0, 3)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("10[0..2]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 5));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OneRange)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 10, .Range = TBlockRange64::WithLength(2, 3)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("10[2..4]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(WithGaps)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 1, .Range = TBlockRange64::WithLength(2, 3)},
            {.Key = 2, .Range = TBlockRange64::MakeOneBlock(6)},
            {.Key = 3, .Range = TBlockRange64::WithLength(10, 3)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[2..4]2[6..6]3[10..12]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 15));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(SideBySide)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 1, .Range = TBlockRange64::WithLength(2, 3)},
            {.Key = 2, .Range = TBlockRange64::MakeOneBlock(5)},
            {.Key = 3, .Range = TBlockRange64::WithLength(6, 3)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[2..4]2[5..5]3[6..8]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(SameOverlappedRanges)
    {
        const TVector<TTestRangeMap::TItem> ranges1 = {
            {.Key = 1, .Range = TBlockRange64::WithLength(3, 3)}};
        const TVector<TTestRangeMap::TItem> ranges2 = {
            {.Key = 2, .Range = TBlockRange64::WithLength(3, 3)}};

        TTestRangeMap map;
        AddRanges(ranges1, &map);
        AddRanges(ranges2, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[3..5]2[3..5]", map.DebugPrint());
        TestOverlaps(map, ranges1, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges1, &map);
        UNIT_ASSERT_VALUES_EQUAL("2[3..5]", map.DebugPrint());
        TestOverlaps(map, ranges2, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges2, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRangesWithSameStart)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(3, 3)},
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(3, 4)},
            {.Key = 3, .Range = TBlockRange64::MakeClosedInterval(3, 5)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[3..3]2[3..4]3[3..5]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRangesWithSameEnd)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(3, 5)},
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(4, 5)},
            {.Key = 3, .Range = TBlockRange64::MakeClosedInterval(5, 5)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[3..5]2[4..5]3[5..5]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRanges)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(3, 9)},
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(4, 8)},
            {.Key = 3, .Range = TBlockRange64::MakeClosedInterval(5, 7)},
            {.Key = 4, .Range = TBlockRange64::MakeClosedInterval(6, 6)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL(
            "4[6..6]3[5..7]2[4..8]1[3..9]",
            map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 12));
    }

    Y_UNIT_TEST(BigGap)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(2, 4)},
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(9, 9)},
            {.Key = 3, .Range = TBlockRange64::MakeClosedInterval(8, 10)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[2..4]2[9..9]3[8..10]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 12));
    }

    Y_UNIT_TEST(HugeGap)
    {
        const TVector<TTestRangeMap::TItem> ranges = {
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(2, 4)},
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(1002, 1004)}};

        TTestRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("2[2..4]1[1002..1004]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));
        TestOverlaps(map, ranges, TBlockRange64::WithLength(500, 10));
        TestOverlaps(map, ranges, TBlockRange64::WithLength(1000, 10));
    }

    Y_UNIT_TEST(Enumerate)
    {
        using TTestRangeMap = TBlockRangeMap<TString, ui64>;
        using TItem = TTestRangeMap::TItem;

        TTestRangeMap map;

        const TVector<TItem> ranges = {
            {.Key = "key-1",
             .Range = TBlockRange64::MakeClosedInterval(2, 4),
             .Value = 100},
            {.Key = "key-2",
             .Range = TBlockRange64::MakeClosedInterval(3, 5),
             .Value = 200},
            {.Key = "key-3",
             .Range = TBlockRange64::MakeClosedInterval(5, 7),
             .Value = 300},
        };
        for (const auto& item: ranges) {
            map.AddRange(item.Key, item.Range, item.Value);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            "key-1[2..4]key-2[3..5]key-3[5..7]",
            map.DebugPrint());

        {
            TSet<TString> enumerated;
            map.Enumerate(
                [&](const TItem& item)
                {
                    enumerated.insert(item.Key);
                    return TTestRangeMap::EEnumerateContinuation::Continue;
                });
            UNIT_ASSERT_VALUES_EQUAL(3, enumerated.size());
        }
        {
            TSet<TString> enumerated;
            map.EnumerateOverlapping(
                TBlockRange64::MakeClosedInterval(1, 1),
                [&](const TItem& item)
                {
                    enumerated.insert(item.Key);
                    return TTestRangeMap::EEnumerateContinuation::Continue;
                });
            UNIT_ASSERT_VALUES_EQUAL(0, enumerated.size());
        }
        {
            TSet<TString> enumerated;
            map.EnumerateOverlapping(
                TBlockRange64::MakeClosedInterval(2, 4),
                [&](const TItem& item)
                {
                    enumerated.insert(item.Key);
                    return TTestRangeMap::EEnumerateContinuation::Continue;
                });
            UNIT_ASSERT_VALUES_EQUAL(2, enumerated.size());
            UNIT_ASSERT_VALUES_EQUAL(true, enumerated.contains("key-1"));
            UNIT_ASSERT_VALUES_EQUAL(true, enumerated.contains("key-2"));
        }
        {
            TSet<TString> enumerated;
            map.EnumerateOverlapping(
                TBlockRange64::MakeClosedInterval(3, 5),
                [&](const TItem& item)
                {
                    enumerated.insert(item.Key);
                    return TTestRangeMap::EEnumerateContinuation::Continue;
                });
            UNIT_ASSERT_VALUES_EQUAL(3, enumerated.size());
            UNIT_ASSERT_VALUES_EQUAL(true, enumerated.contains("key-1"));
            UNIT_ASSERT_VALUES_EQUAL(true, enumerated.contains("key-2"));
            UNIT_ASSERT_VALUES_EQUAL(true, enumerated.contains("key-3"));
        }
        {
            TSet<TString> enumerated;
            map.EnumerateOverlapping(
                TBlockRange64::MakeClosedInterval(5, 5),
                [&](const TItem& item)
                {
                    enumerated.insert(item.Key);
                    return TTestRangeMap::EEnumerateContinuation::Continue;
                });
            UNIT_ASSERT_VALUES_EQUAL(2, enumerated.size());
            UNIT_ASSERT_VALUES_EQUAL(true, enumerated.contains("key-2"));
            UNIT_ASSERT_VALUES_EQUAL(true, enumerated.contains("key-3"));
        }
        {
            TSet<TString> enumerated;
            map.EnumerateOverlapping(
                TBlockRange64::MakeClosedInterval(6, 6),
                [&](const TItem& item)
                {
                    enumerated.insert(item.Key);
                    return TTestRangeMap::EEnumerateContinuation::Continue;
                });
            UNIT_ASSERT_VALUES_EQUAL(1, enumerated.size());
            UNIT_ASSERT_VALUES_EQUAL(true, enumerated.contains("key-3"));
        }
    }

    Y_UNIT_TEST(MoveOnlyType)
    {
        TBlockRangeMap<ui64, std::unique_ptr<int>> map;
        map.AddRange(1, TBlockRange64::MakeOneBlock(1), nullptr);
        auto extracted = map.ExtractRange(1);
        UNIT_ASSERT_VALUES_EQUAL(true, extracted.has_value());
        UNIT_ASSERT_EQUAL(nullptr, extracted->Value);
    }
}

}   // namespace NYdb::NBS::NBlockStore
