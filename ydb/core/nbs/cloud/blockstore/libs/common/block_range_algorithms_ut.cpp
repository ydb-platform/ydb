#include "block_range_algorithms.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

namespace {
// help structure to work with 'SplitExecOnNonOverlappingRanges' algorithm
struct TOverlappingItem
{
    ui64 Key{};
    TBlockRange64 Range;

    TOverlappingItem(ui64 key, TBlockRange64 range)
        : Key(key)
        , Range(range)
    {}

    [[nodiscard]] TString DebugPrint() const
    {
        TStringBuilder result;
        result << Key << Range.Print();
        return result;
    }
};
}   // namespace

Y_UNIT_TEST_SUITE(TSplitExecOnNonOverlappingRanges)
{
    // one global result to avoid copy-paste of lamda's to store result
    TVector<TOverlappingItem> result;

    bool AddResultItemCallback(ui64 key, TBlockRange64 range)
    {
        result.push_back(TOverlappingItem(key, range));
        return true;
    }

    [[nodiscard]] TString DebugPrintResult()
    {
        TStringBuilder resultStr;
        for (const auto& item: result) {
            resultStr << item.DebugPrint() << ";";
        }
        return resultStr;
    }

    Y_UNIT_TEST(EmptyRanges)
    {
        result.clear();
        TVector<TOverlappingItem> ranges;
        SplitExecOnNonOverlappingRanges(0, 100, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(result.size(), 0);
    }

    Y_UNIT_TEST(RangeWithEdgesOfRequest)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 19)}};
        SplitExecOnNonOverlappingRanges(10, 19, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL("100[10..19];", DebugPrintResult());
    }

    Y_UNIT_TEST(CutEdges)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 50)},
            {200, TBlockRange64::MakeClosedInterval(20, 60)}};
        SplitExecOnNonOverlappingRanges(15, 55, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[15..19];"
            "200[20..55];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(CutEdges2)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 50)},
            {200, TBlockRange64::MakeClosedInterval(20, 40)}};
        SplitExecOnNonOverlappingRanges(15, 45, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[15..19];"
            "200[20..40];"
            "100[41..45];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(TwoSequentialNonOverlappingRanges)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 19)},
            {200, TBlockRange64::MakeClosedInterval(30, 39)}};
        SplitExecOnNonOverlappingRanges(0, 49, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "0[0..9];"
            "100[10..19];"
            "0[20..29];"
            "200[30..39];"
            "0[40..49];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(TwoFullyOverlappingRanges)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 50)},
            {200, TBlockRange64::MakeClosedInterval(20, 30)}};
        SplitExecOnNonOverlappingRanges(10, 50, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[10..19];"
            "200[20..30];"
            "100[31..50];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(TwoPartiallyOverlappingRanges)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 30)},
            {200, TBlockRange64::MakeClosedInterval(25, 45)}};
        SplitExecOnNonOverlappingRanges(10, 45, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[10..24];"
            "200[25..45];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(ThreeOverlappingRanges)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 50)},
            {150, TBlockRange64::MakeClosedInterval(20, 40)},
            {200, TBlockRange64::MakeClosedInterval(30, 35)}};
        SplitExecOnNonOverlappingRanges(10, 50, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[10..19];"
            "150[20..29];"
            "200[30..35];"
            "150[36..40];"
            "100[41..50];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(RangeWithSameStart)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 109)},
            {200, TBlockRange64::MakeClosedInterval(10, 49)}};
        SplitExecOnNonOverlappingRanges(0, 99, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "0[0..9];"
            "200[10..49];"
            "100[50..99];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(ManyConsecutiveRanges)
    {
        result.clear();
        TVector<TOverlappingItem> ranges;
        const int keysCount = 100;
        for (ui64 i = 1; i <= keysCount; ++i) {
            ranges.push_back({i, TBlockRange64::MakeClosedInterval(i, i)});
        }
        SplitExecOnNonOverlappingRanges(
            0,
            keysCount,
            ranges,
            AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(keysCount + 1, result.size());

        for (size_t i = 0; i < result.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, result[i].Key);
            UNIT_ASSERT_VALUES_EQUAL(i, result[i].Range.Start);
            UNIT_ASSERT_VALUES_EQUAL(i, result[i].Range.Start);
        }
    }

    Y_UNIT_TEST(StaircaseWithOverlappedRanges)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 30)},
            {200, TBlockRange64::MakeClosedInterval(25, 45)},
            {300, TBlockRange64::MakeClosedInterval(40, 60)}};
        SplitExecOnNonOverlappingRanges(10, 60, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "100[10..24];"
            "200[25..39];"
            "300[40..60];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(InsideOfBaseRange)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 15)},
            {200, TBlockRange64::MakeClosedInterval(25, 30)},
            {300, TBlockRange64::MakeClosedInterval(45, 50)}};
        SplitExecOnNonOverlappingRanges(0, 60, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "0[0..9];"
            "100[10..15];"
            "0[16..24];"
            "200[25..30];"
            "0[31..44];"
            "300[45..50];"
            "0[51..60];",
            DebugPrintResult());
    }

    Y_UNIT_TEST(FewBiggerKeysInsideOfOneSmaller)
    {
        result.clear();
        TVector<TOverlappingItem> ranges = {
            {100, TBlockRange64::MakeClosedInterval(10, 100)},
            {200, TBlockRange64::MakeClosedInterval(20, 25)},
            {300, TBlockRange64::MakeClosedInterval(40, 45)},
            {400, TBlockRange64::MakeClosedInterval(70, 75)}};
        SplitExecOnNonOverlappingRanges(0, 100, ranges, AddResultItemCallback);

        UNIT_ASSERT_VALUES_EQUAL(
            "0[0..9];"
            "100[10..19];"
            "200[20..25];"
            "100[26..39];"
            "300[40..45];"
            "100[46..69];"
            "400[70..75];"
            "100[76..100];",
            DebugPrintResult());
    }
}
}   // namespace NYdb::NBS::NBlockStore
