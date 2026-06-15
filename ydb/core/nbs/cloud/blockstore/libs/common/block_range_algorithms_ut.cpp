#include "block_range_algorithms.h"

#include <library/cpp/testing/unittest/registar.h>

#include <span>

namespace NYdb::NBS::NBlockStore {

namespace {

[[nodiscard]] TString DebugPrintResult(const std::span<TWeightedRange> result)
{
    TStringBuilder resultStr;
    for (const auto& item: result) {
        resultStr << item.Key << item.Range.Print() << ";";
    }
    return resultStr;
}

}   // namespace

Y_UNIT_TEST_SUITE(TSplitOnNonOverlappingContinuousRanges)
{
    Y_UNIT_TEST(EmptyRanges)
    {
        TVector<TWeightedRange> ranges;
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(0, 100),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL("0[0..100];", DebugPrintResult(result));
    }

    Y_UNIT_TEST(RangeWithEdgesOfRequest)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 19)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(10, 19),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL("100[10..19];", DebugPrintResult(result));
    }

    Y_UNIT_TEST(CutEdges)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 50)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(20, 60)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(15, 55),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[15..19];"
            "200[20..55];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(CutEdges2)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 50)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(20, 40)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(15, 45),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[15..19];"
            "200[20..40];"
            "100[41..45];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(TwoSequentialNonOverlappingRanges)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 19)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(30, 39)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(0, 49),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "0[0..9];"
            "100[10..19];"
            "0[20..29];"
            "200[30..39];"
            "0[40..49];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(TwoFullyOverlappingRanges)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 50)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(20, 30)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(10, 50),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[10..19];"
            "200[20..30];"
            "100[31..50];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(TwoPartiallyOverlappingRanges)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 30)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(25, 45)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(10, 45),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[10..24];"
            "200[25..45];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(ThreeOverlappingRanges)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 50)},
            {.Key = 150, .Range = TBlockRange64::MakeClosedInterval(20, 40)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(30, 35)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(10, 50),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[10..19];"
            "150[20..29];"
            "200[30..35];"
            "150[36..40];"
            "100[41..50];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(RangeWithSameStart)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 109)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(10, 49)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(0, 99),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "0[0..9];"
            "200[10..49];"
            "100[50..99];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(ManyConsecutiveRanges)
    {
        TVector<TWeightedRange> ranges;
        const int keysCount = 100;
        for (ui64 i = 1; i <= keysCount; ++i) {
            ranges.push_back(
                {.Key = i, .Range = TBlockRange64::MakeClosedInterval(i, i)});
        }
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(0, keysCount),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(keysCount + 1, result.size());

        for (size_t i = 0; i < result.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, result[i].Key);
            UNIT_ASSERT_VALUES_EQUAL(i, result[i].Range.Start);
            UNIT_ASSERT_VALUES_EQUAL(i, result[i].Range.Start);
        }
    }

    Y_UNIT_TEST(StaircaseWithOverlappedRanges)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 30)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(25, 45)},
            {.Key = 300, .Range = TBlockRange64::MakeClosedInterval(40, 60)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(10, 60),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[10..24];"
            "200[25..39];"
            "300[40..60];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(InsideOfBaseRange)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 15)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(25, 30)},
            {.Key = 300, .Range = TBlockRange64::MakeClosedInterval(45, 50)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(0, 60),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "0[0..9];"
            "100[10..15];"
            "0[16..24];"
            "200[25..30];"
            "0[31..44];"
            "300[45..50];"
            "0[51..60];",
            DebugPrintResult(result));
    }

    Y_UNIT_TEST(FewBiggerKeysInsideOfOneSmaller)
    {
        TVector<TWeightedRange> ranges = {
            {.Key = 100, .Range = TBlockRange64::MakeClosedInterval(10, 100)},
            {.Key = 200, .Range = TBlockRange64::MakeClosedInterval(20, 25)},
            {.Key = 300, .Range = TBlockRange64::MakeClosedInterval(40, 45)},
            {.Key = 400, .Range = TBlockRange64::MakeClosedInterval(70, 75)}};
        auto result = SplitOnNonOverlappingContinuousRanges(
            TBlockRange64::MakeClosedInterval(0, 100),
            ranges);

        UNIT_ASSERT_VALUES_EQUAL(
            "0[0..9];"
            "100[10..19];"
            "200[20..25];"
            "100[26..39];"
            "300[40..45];"
            "100[46..69];"
            "400[70..75];"
            "100[76..100];",
            DebugPrintResult(result));
    }
}
}   // namespace NYdb::NBS::NBlockStore
