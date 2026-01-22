#include "window_frame_bounds.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>
#include <util/random/random.h>
#include <util/string/printf.h>

using namespace NYql::NWindow;

Y_UNIT_TEST_SUITE(CoreWinFrameCollectorBoundsTest) {

Y_UNIT_TEST(DifferentEntries_ReturnDifferentHandles) {
    TCoreWinFrameCollectorBounds<i64> bounds(/*dedup=*/true);

    // AddRange
    auto rangeHandle1 = bounds.AddRange(TInputRangeWindowFrame<i64>(TInputRange<i64>(10, EDirection::Preceding), TInputRange<i64>(5, EDirection::Following)));
    auto rangeHandle2 = bounds.AddRange(TInputRangeWindowFrame<i64>(TInputRange<i64>(20, EDirection::Preceding), TInputRange<i64>(10, EDirection::Following)));
    UNIT_ASSERT_UNEQUAL(rangeHandle1.Index(), rangeHandle2.Index());

    // AddRow
    auto rowHandle1 = bounds.AddRow(TInputRowWindowFrame(TInputRow(10, EDirection::Preceding), TInputRow(5, EDirection::Following)));
    auto rowHandle2 = bounds.AddRow(TInputRowWindowFrame(TInputRow(20, EDirection::Preceding), TInputRow(10, EDirection::Following)));
    UNIT_ASSERT_UNEQUAL(rowHandle1.Index(), rowHandle2.Index());

    // AddRangeIncremental
    auto rangeIncHandle1 = bounds.AddRangeIncremental(TInputRange<i64>(10, EDirection::Preceding));
    auto rangeIncHandle2 = bounds.AddRangeIncremental(TInputRange<i64>(20, EDirection::Following));
    UNIT_ASSERT_UNEQUAL(rangeIncHandle1.Index(), rangeIncHandle2.Index());

    // AddRowIncremental
    auto rowIncHandle1 = bounds.AddRowIncremental(TInputRow(10, EDirection::Preceding));
    auto rowIncHandle2 = bounds.AddRowIncremental(TInputRow(20, EDirection::Following));
    UNIT_ASSERT_UNEQUAL(rowIncHandle1.Index(), rowIncHandle2.Index());
}

Y_UNIT_TEST(DuplicateEntries_ReturnCachedHandles) {
    TCoreWinFrameCollectorBounds<i64> bounds(/*dedup=*/true);

    // AddRange
    auto rangeFrame = TInputRangeWindowFrame<i64>(TInputRange<i64>(10, EDirection::Preceding), TInputRange<i64>(5, EDirection::Following));
    auto rangeHandle1 = bounds.AddRange(rangeFrame);
    auto rangeHandle2 = bounds.AddRange(rangeFrame);
    UNIT_ASSERT_EQUAL(rangeHandle1.Index(), rangeHandle2.Index());
    UNIT_ASSERT_EQUAL(bounds.RangeIntervals().size(), 1);

    // AddRow
    auto rowFrame = TInputRowWindowFrame(TInputRow(10, EDirection::Preceding), TInputRow(5, EDirection::Following));
    auto rowHandle1 = bounds.AddRow(rowFrame);
    auto rowHandle2 = bounds.AddRow(rowFrame);
    UNIT_ASSERT_EQUAL(rowHandle1.Index(), rowHandle2.Index());
    UNIT_ASSERT_EQUAL(bounds.RowIntervals().size(), 1);

    // AddRangeIncremental
    auto rangeDelta = TInputRange<i64>(10, EDirection::Preceding);
    auto rangeIncHandle1 = bounds.AddRangeIncremental(rangeDelta);
    auto rangeIncHandle2 = bounds.AddRangeIncremental(rangeDelta);
    UNIT_ASSERT_EQUAL(rangeIncHandle1.Index(), rangeIncHandle2.Index());
    UNIT_ASSERT_EQUAL(bounds.RangeIncrementals().size(), 1);

    // AddRowIncremental
    auto rowDelta = TInputRow(10, EDirection::Preceding);
    auto rowIncHandle1 = bounds.AddRowIncremental(rowDelta);
    auto rowIncHandle2 = bounds.AddRowIncremental(rowDelta);
    UNIT_ASSERT_EQUAL(rowIncHandle1.Index(), rowIncHandle2.Index());
    UNIT_ASSERT_EQUAL(bounds.RowIncrementals().size(), 1);
}

Y_UNIT_TEST(DuplicateEntries_NoDedupFlag_NoCaching) {
    TCoreWinFrameCollectorBounds<i64> bounds(/*dedup=*/false);

    // AddRange - duplicates should create new entries
    auto rangeFrame = TInputRangeWindowFrame<i64>(TInputRange<i64>(10, EDirection::Preceding), TInputRange<i64>(5, EDirection::Following));
    auto rangeHandle1 = bounds.AddRange(rangeFrame);
    auto rangeHandle2 = bounds.AddRange(rangeFrame);
    UNIT_ASSERT_UNEQUAL(rangeHandle1.Index(), rangeHandle2.Index());
    UNIT_ASSERT_EQUAL(bounds.RangeIntervals().size(), 2);

    // AddRow - duplicates should create new entries
    auto rowFrame = TInputRowWindowFrame(TInputRow(10, EDirection::Preceding), TInputRow(5, EDirection::Following));
    auto rowHandle1 = bounds.AddRow(rowFrame);
    auto rowHandle2 = bounds.AddRow(rowFrame);
    UNIT_ASSERT_UNEQUAL(rowHandle1.Index(), rowHandle2.Index());
    UNIT_ASSERT_EQUAL(bounds.RowIntervals().size(), 2);

    // AddRangeIncremental - duplicates should create new entries
    auto rangeDelta = TInputRange<i64>(10, EDirection::Preceding);
    auto rangeIncHandle1 = bounds.AddRangeIncremental(rangeDelta);
    auto rangeIncHandle2 = bounds.AddRangeIncremental(rangeDelta);
    UNIT_ASSERT_UNEQUAL(rangeIncHandle1.Index(), rangeIncHandle2.Index());
    UNIT_ASSERT_EQUAL(bounds.RangeIncrementals().size(), 2);

    // AddRowIncremental - duplicates should create new entries
    auto rowDelta = TInputRow(10, EDirection::Preceding);
    auto rowIncHandle1 = bounds.AddRowIncremental(rowDelta);
    auto rowIncHandle2 = bounds.AddRowIncremental(rowDelta);
    UNIT_ASSERT_UNEQUAL(rowIncHandle1.Index(), rowIncHandle2.Index());
    UNIT_ASSERT_EQUAL(bounds.RowIncrementals().size(), 2);
}

} // Y_UNIT_TEST_SUITE(CoreWinFrameCollectorBoundsTest)
