#include <yql/essentials/core/yql_range_frame_collector_bounds.h>
#include <yql/essentials/ast/yql_expr.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NWindow {

namespace {

TExprNode::TPtr MakeAtomNode(TExprContext& ctx, const TString& value) {
    return ctx.Builder(TPositionHandle())
        .Atom(value)
        .Build();
}

TWindowFrameSettingWithOffset MakeOffset(TExprNode::TPtr node, TMaybe<TNodeTransform> caster = Nothing(), TMaybe<ui32> procId = Nothing()) {
    return TWindowFrameSettingWithOffset(std::move(node), std::move(caster), Nothing(), std::move(procId));
}

} // namespace

Y_UNIT_TEST_SUITE(RangeFrameCollectorBoundsDedup) {

Y_UNIT_TEST(ReturnsSameHandleForIdenticalRangeFrames) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    TRangeFrameCollectorBounds::TRangeBound bound1(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound bound2(MakeOffset(node), EDirection::Following);
    TRangeFrameCollectorBounds::TRangeFrame frame1(bound1, bound2);
    TRangeFrameCollectorBounds::TRangeFrame frame2(bound1, bound2);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledDifferentDirectionsDifferentHandles) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    TRangeFrameCollectorBounds::TRangeBound boundPreceding(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound boundFollowing(MakeOffset(node), EDirection::Following);

    TRangeFrameCollectorBounds::TRangeFrame frame1(boundPreceding, boundFollowing);
    TRangeFrameCollectorBounds::TRangeFrame frame2(boundFollowing, boundPreceding);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledSameDirectionDifferentNodesDifferentHandles) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node1 = MakeAtomNode(ctx, "10");
    auto node2 = MakeAtomNode(ctx, "20");

    TRangeFrameCollectorBounds::TRangeBound bound1(MakeOffset(node1), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound bound2(MakeOffset(node2), EDirection::Preceding);

    TRangeFrameCollectorBounds::TRangeFrame frame1(bound1, bound1);
    TRangeFrameCollectorBounds::TRangeFrame frame2(bound2, bound2);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledUnboundedValuesSameHandle) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    auto unboundedPreceding = TRangeFrameCollectorBounds::TRangeBound::Inf(EDirection::Preceding);
    auto unboundedFollowing = TRangeFrameCollectorBounds::TRangeBound::Inf(EDirection::Following);
    TRangeFrameCollectorBounds::TRangeBound finiteBound(MakeOffset(node), EDirection::Preceding);

    TRangeFrameCollectorBounds::TRangeFrame frame1(unboundedPreceding, unboundedFollowing);
    TRangeFrameCollectorBounds::TRangeFrame frame2(unboundedPreceding, unboundedFollowing);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledUnboundedDifferentDirectionsDifferentHandles) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto unboundedPreceding = TRangeFrameCollectorBounds::TRangeBound::Inf(EDirection::Preceding);
    auto unboundedFollowing = TRangeFrameCollectorBounds::TRangeBound::Inf(EDirection::Following);

    TRangeFrameCollectorBounds::TRangeFrame frame1(unboundedPreceding, unboundedFollowing);
    TRangeFrameCollectorBounds::TRangeFrame frame2(unboundedFollowing, unboundedPreceding);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledZeroValuesSameHandle) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto zero1 = TRangeFrameCollectorBounds::TRangeBound::Zero();
    auto zero2 = TRangeFrameCollectorBounds::TRangeBound::Zero();

    TRangeFrameCollectorBounds::TRangeFrame frame1(zero1, zero2);
    TRangeFrameCollectorBounds::TRangeFrame frame2(zero1, zero2);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledZeroAndFiniteDifferentHandles) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "0");
    auto zero = TRangeFrameCollectorBounds::TRangeBound::Zero();
    TRangeFrameCollectorBounds::TRangeBound finiteBound(MakeOffset(node), EDirection::Following);

    TRangeFrameCollectorBounds::TRangeFrame frame1(zero, zero);
    TRangeFrameCollectorBounds::TRangeFrame frame2(finiteBound, finiteBound);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledMixedBoundsCorrectDedup) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    auto unbounded = TRangeFrameCollectorBounds::TRangeBound::Inf(EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound finiteBound(MakeOffset(node), EDirection::Following);

    TRangeFrameCollectorBounds::TRangeFrame frame1(unbounded, finiteBound);
    TRangeFrameCollectorBounds::TRangeFrame frame2(unbounded, finiteBound);
    TRangeFrameCollectorBounds::TRangeFrame frame3(finiteBound, unbounded);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});
    auto handle3 = collector.AddRange(frame3, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle3.Index());
}

Y_UNIT_TEST(DedupEnabledRowIntervalsDedup) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    TInputRow row1(5, EDirection::Preceding);
    TInputRow row2(10, EDirection::Following);
    TInputRowWindowFrame frame1(row1, row2);
    TInputRowWindowFrame frame2(row1, row2);
    TInputRowWindowFrame frame3(row2, row1);

    auto handle1 = collector.AddRow(frame1);
    auto handle2 = collector.AddRow(frame2);
    auto handle3 = collector.AddRow(frame3);

    UNIT_ASSERT_VALUES_EQUAL(collector.RowIntervals().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle3.Index());
}

Y_UNIT_TEST(DedupEnabledRangeIncrementalsDedup) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    TRangeFrameCollectorBounds::TRangeBound delta1(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound delta2(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound delta3(MakeOffset(node), EDirection::Following);

    auto handle1 = collector.AddRangeIncremental(delta1, {});
    auto handle2 = collector.AddRangeIncremental(delta2, {});
    auto handle3 = collector.AddRangeIncremental(delta3, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIncrementals().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle3.Index());
}

Y_UNIT_TEST(DedupEnabledRowIncrementalsDedup) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    TInputRow delta1(5, EDirection::Preceding);
    TInputRow delta2(5, EDirection::Preceding);
    TInputRow delta3(5, EDirection::Following);

    auto handle1 = collector.AddRowIncremental(delta1);
    auto handle2 = collector.AddRowIncremental(delta2);
    auto handle3 = collector.AddRowIncremental(delta3);

    UNIT_ASSERT_VALUES_EQUAL(collector.RowIncrementals().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle3.Index());
}

Y_UNIT_TEST(DedupEnabledRowIncrementalsZeroNormalization) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    TInputRow zeroPreceding(0, EDirection::Preceding);
    TInputRow zeroFollowing(0, EDirection::Following);

    auto handle1 = collector.AddRowIncremental(zeroPreceding);
    auto handle2 = collector.AddRowIncremental(zeroFollowing);

    UNIT_ASSERT_VALUES_EQUAL(collector.RowIncrementals().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledRowIntervalsZeroNormalization) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    TInputRow zeroPreceding(0, EDirection::Preceding);
    TInputRow zeroFollowing(0, EDirection::Following);
    TInputRow nonZero(5, EDirection::Following);

    TInputRowWindowFrame frame1(zeroPreceding, nonZero);
    TInputRowWindowFrame frame2(zeroFollowing, nonZero);

    auto handle1 = collector.AddRow(frame1);
    auto handle2 = collector.AddRow(frame2);

    UNIT_ASSERT_VALUES_EQUAL(collector.RowIntervals().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledRowUnboundedDedup) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    TInputRow unboundedPreceding = TInputRow::Inf(EDirection::Preceding);
    TInputRow unboundedFollowing = TInputRow::Inf(EDirection::Following);
    TInputRow finite(5, EDirection::Following);

    TInputRowWindowFrame frame1(unboundedPreceding, unboundedFollowing);
    TInputRowWindowFrame frame2(unboundedPreceding, unboundedFollowing);
    TInputRowWindowFrame frame3(unboundedPreceding, finite);

    auto handle1 = collector.AddRow(frame1);
    auto handle2 = collector.AddRow(frame2);
    auto handle3 = collector.AddRow(frame3);

    UNIT_ASSERT_VALUES_EQUAL(collector.RowIntervals().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle3.Index());
}

Y_UNIT_TEST(EmptyCollector) {
    TRangeFrameCollectorBounds collector;

    UNIT_ASSERT(collector.Empty());
    UNIT_ASSERT(collector.RangeIntervals().empty());
    UNIT_ASSERT(collector.RowIntervals().empty());
    UNIT_ASSERT(collector.RangeIncrementals().empty());
    UNIT_ASSERT(collector.RowIncrementals().empty());
}

Y_UNIT_TEST(NonEmptyAfterAdding) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    UNIT_ASSERT(collector.Empty());

    auto node = MakeAtomNode(ctx, "10");
    TRangeFrameCollectorBounds::TRangeBound bound(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeFrame frame(bound, bound);

    collector.AddRange(frame, {});
    UNIT_ASSERT(!collector.Empty());
}

Y_UNIT_TEST(HandlePropertiesCorrect) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    TRangeFrameCollectorBounds::TRangeBound rangeBound(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeFrame rangeFrame(rangeBound, rangeBound);
    TInputRow rowBound(5, EDirection::Preceding);
    TInputRowWindowFrame rowFrame(rowBound, rowBound);

    auto rangeHandle = collector.AddRange(rangeFrame, {});
    auto rowHandle = collector.AddRow(rowFrame);
    auto rangeIncrHandle = collector.AddRangeIncremental(rangeBound, {});
    auto rowIncrHandle = collector.AddRowIncremental(rowBound);

    UNIT_ASSERT(rangeHandle.IsRange());
    UNIT_ASSERT(!rangeHandle.IsIncremental());

    UNIT_ASSERT(!rowHandle.IsRange());
    UNIT_ASSERT(!rowHandle.IsIncremental());

    UNIT_ASSERT(rangeIncrHandle.IsRange());
    UNIT_ASSERT(rangeIncrHandle.IsIncremental());

    UNIT_ASSERT(!rowIncrHandle.IsRange());
    UNIT_ASSERT(rowIncrHandle.IsIncremental());
}

Y_UNIT_TEST(AsBaseReturnsCorrectReference) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    TRangeFrameCollectorBounds::TRangeBound bound(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeFrame frame(bound, bound);

    collector.AddRange(frame, {});

    const auto& base = collector.AsBase();
    UNIT_ASSERT_VALUES_EQUAL(base.RangeIntervals().size(), 1);
    UNIT_ASSERT(!base.Empty());
}

Y_UNIT_TEST(DedupEnabledMultipleAdditionsOfSameFrame) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    TRangeFrameCollectorBounds::TRangeBound bound(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeFrame frame(bound, bound);

    auto handle1 = collector.AddRange(frame, {});
    auto handle2 = collector.AddRange(frame, {});
    auto handle3 = collector.AddRange(frame, {});
    auto handle4 = collector.AddRange(frame, {});
    auto handle5 = collector.AddRange(frame, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
    UNIT_ASSERT_VALUES_EQUAL(handle2.Index(), handle3.Index());
    UNIT_ASSERT_VALUES_EQUAL(handle3.Index(), handle4.Index());
    UNIT_ASSERT_VALUES_EQUAL(handle4.Index(), handle5.Index());
}

Y_UNIT_TEST(DedupEnabledDifferentMinMaxSameValues) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node1 = MakeAtomNode(ctx, "10");
    auto node2 = MakeAtomNode(ctx, "20");

    TRangeFrameCollectorBounds::TRangeBound bound1(MakeOffset(node1), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound bound2(MakeOffset(node2), EDirection::Following);

    TRangeFrameCollectorBounds::TRangeFrame frame1(bound1, bound2);
    TRangeFrameCollectorBounds::TRangeFrame frame2(bound2, bound1);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledAllEqualButAtomNodeDiffers) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node1 = MakeAtomNode(ctx, "10");
    auto node2 = MakeAtomNode(ctx, "10");
    auto node3 = MakeAtomNode(ctx, "11");

    TRangeFrameCollectorBounds::TRangeBound minBound1(MakeOffset(node1), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound maxBound1(MakeOffset(node2), EDirection::Following);
    TRangeFrameCollectorBounds::TRangeBound minBound2(MakeOffset(node1), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound maxBound2(MakeOffset(node3), EDirection::Following);

    TRangeFrameCollectorBounds::TRangeFrame frame1(minBound1, maxBound1);
    TRangeFrameCollectorBounds::TRangeFrame frame2(minBound2, maxBound2);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledAllEqualButDirectionDiffers) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");

    TRangeFrameCollectorBounds::TRangeBound minBound(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound maxBoundFollowing(MakeOffset(node), EDirection::Following);
    TRangeFrameCollectorBounds::TRangeBound maxBoundPreceding(MakeOffset(node), EDirection::Preceding);

    TRangeFrameCollectorBounds::TRangeFrame frame1(minBound, maxBoundFollowing);
    TRangeFrameCollectorBounds::TRangeFrame frame2(minBound, maxBoundPreceding);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledOneZeroOtherNonZero) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto zero = TRangeFrameCollectorBounds::TRangeBound::Zero();
    auto node = MakeAtomNode(ctx, "10");
    TRangeFrameCollectorBounds::TRangeBound nonZeroBound(MakeOffset(node), EDirection::Following);

    TRangeFrameCollectorBounds::TRangeFrame frameWithZeroMin(zero, nonZeroBound);
    TRangeFrameCollectorBounds::TRangeFrame frameWithNonZeroMin(nonZeroBound, nonZeroBound);

    auto handle1 = collector.AddRange(frameWithZeroMin, {});
    auto handle2 = collector.AddRange(frameWithNonZeroMin, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledRowOneZeroOtherNonZero) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    TInputRow zero(0, EDirection::Following);
    TInputRow nonZero(10, EDirection::Following);

    TInputRowWindowFrame frameWithZeroMin(zero, nonZero);
    TInputRowWindowFrame frameWithNonZeroMin(nonZero, nonZero);

    auto handle1 = collector.AddRow(frameWithZeroMin);
    auto handle2 = collector.AddRow(frameWithNonZeroMin);

    UNIT_ASSERT_VALUES_EQUAL(collector.RowIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledRowAllEqualButDirectionDiffers) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    TInputRow minBound(5, EDirection::Preceding);
    TInputRow maxBoundFollowing(10, EDirection::Following);
    TInputRow maxBoundPreceding(10, EDirection::Preceding);

    TInputRowWindowFrame frame1(minBound, maxBoundFollowing);
    TInputRowWindowFrame frame2(minBound, maxBoundPreceding);

    auto handle1 = collector.AddRow(frame1);
    auto handle2 = collector.AddRow(frame2);

    UNIT_ASSERT_VALUES_EQUAL(collector.RowIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledRowAllEqualButValueDiffers) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    TInputRow minBound(5, EDirection::Preceding);
    TInputRow maxBound1(10, EDirection::Following);
    TInputRow maxBound2(11, EDirection::Following);

    TInputRowWindowFrame frame1(minBound, maxBound1);
    TInputRowWindowFrame frame2(minBound, maxBound2);

    auto handle1 = collector.AddRow(frame1);
    auto handle2 = collector.AddRow(frame2);

    UNIT_ASSERT_VALUES_EQUAL(collector.RowIntervals().size(), 2);
    UNIT_ASSERT_VALUES_UNEQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledDifferentCasterDifferentHandles) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");
    auto casterLambda = MakeAtomNode(ctx, "caster");

    TRangeFrameCollectorBounds::TRangeBound bound1(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound bound2(MakeOffset(node, TNodeTransform(casterLambda)), EDirection::Preceding);

    TRangeFrameCollectorBounds::TRangeFrame frame1(bound1, bound1);
    TRangeFrameCollectorBounds::TRangeFrame frame2(bound2, bound2);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
}

Y_UNIT_TEST(DedupEnabledDifferentProcIdDifferentHandles) {
    TExprContext ctx;
    TRangeFrameCollectorBounds collector;

    auto node = MakeAtomNode(ctx, "10");

    TRangeFrameCollectorBounds::TRangeBound bound1(MakeOffset(node), EDirection::Preceding);
    TRangeFrameCollectorBounds::TRangeBound bound2(MakeOffset(node, Nothing(), 42u), EDirection::Preceding);

    TRangeFrameCollectorBounds::TRangeFrame frame1(bound1, bound1);
    TRangeFrameCollectorBounds::TRangeFrame frame2(bound2, bound2);

    auto handle1 = collector.AddRange(frame1, {});
    auto handle2 = collector.AddRange(frame2, {});

    UNIT_ASSERT_VALUES_EQUAL(collector.RangeIntervals().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(handle1.Index(), handle2.Index());
}

} // Y_UNIT_TEST_SUITE(RangeFrameCollectorBoundsDedup)

} // namespace NYql::NWindow
