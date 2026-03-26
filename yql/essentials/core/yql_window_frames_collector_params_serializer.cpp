#include <yql/essentials/core/yql_window_frames_collector_params_serializer.h>

namespace NYql::NWindow {

namespace {

constexpr TStringBuf KeyMin = "Min";
constexpr TStringBuf KeyMax = "Max";
constexpr TStringBuf KeyRangeIntervals = "RangeIntervals";
constexpr TStringBuf KeyRowIntervals = "RowIntervals";
constexpr TStringBuf KeyRangeIncrementals = "RangeIncrementals";
constexpr TStringBuf KeyRowIncrementals = "RowIncrementals";
constexpr TStringBuf KeySortOrder = "SortOrder";
constexpr TStringBuf KeyBounds = "Bounds";
constexpr TStringBuf KeySortColumnName = "SortColumnName";
constexpr TStringBuf KeyDirection = "Direction";
constexpr TStringBuf KeyNumber = "Number";

TNumberAndDirection<TExprNode::TPtr> ConvertToExprNode(
    TPositionHandle pos,
    const TNumberAndDirection<ui64>& value,
    TExprContext& ctx)
{
    if (value.IsInf()) {
        return TNumberAndDirection<TExprNode::TPtr>::Inf(value.GetDirection());
    }
    auto node = ctx.Builder(pos)
        .Callable("Uint64")
            .Atom(0, ToString(value.GetUnderlyingValue()))
        .Seal()
        .Build();
    return TNumberAndDirection<TExprNode::TPtr>(node, value.GetDirection());
}

TWindowFrame<TNumberAndDirection<TExprNode::TPtr>> ConvertToExprNode(
    TPositionHandle pos,
    const TWindowFrame<TNumberAndDirection<ui64>>& frame,
    TExprContext& ctx)
{
    return TWindowFrame<TNumberAndDirection<TExprNode::TPtr>>(
        ConvertToExprNode(pos, frame.Min(), ctx),
        ConvertToExprNode(pos, frame.Max(), ctx)
    );
}

TExprNode::TPtr SerializeNumberAndDirection(
    TPositionHandle pos,
    const TNumberAndDirection<TExprNode::TPtr>& value,
    TExprContext& ctx)
{
    TExprNode::TPtr numberAndDirection;
    if (value.IsInf()) {
        numberAndDirection = ctx.Builder(pos)
                                .Callable("Void")
                                .Seal()
                             .Build();
    } else {
        YQL_ENSURE(!value.IsZero(), "Unexpected zero tag here.");
        numberAndDirection = value.GetUnderlyingValue();
    }

    return ctx.Builder(pos)
        .Callable("AsStruct")
            .List(0)
                .Atom(0, KeyDirection)
                .Callable(1, "String")
                    .Atom(0, DirectionToString(value.GetDirection()))
                .Seal()
            .Seal()
            .List(1)
                .Atom(0, KeyNumber)
                .Add(1, numberAndDirection)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr SerializeWindowFrame(
    TPositionHandle pos,
    const TWindowFrame<TNumberAndDirection<TExprNode::TPtr>>& frame,
    TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Callable("AsStruct")
            .List(0)
                .Atom(0, KeyMin)
                .Add(1, SerializeNumberAndDirection(pos, frame.Min(), ctx))
            .Seal()
            .List(1)
                .Atom(0, KeyMax)
                .Add(1, SerializeNumberAndDirection(pos, frame.Max(), ctx))
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr SerializeWindowFrameAggregatedBounds(TPositionHandle pos, const TCoreWinFrameCollectorBounds<TExprNode::TPtr>& bounds, TExprContext& ctx) {
    TExprNodeList rangeIntervalItems;
    for (const auto& frame : bounds.RangeIntervals()) {
        rangeIntervalItems.push_back(SerializeWindowFrame(pos, frame, ctx));
    }

    TExprNodeList rowIntervalItems;
    for (const auto& frame : bounds.RowIntervals()) {
        rowIntervalItems.push_back(SerializeWindowFrame(pos, ConvertToExprNode(pos, frame, ctx), ctx));
    }

    TExprNodeList rangeIncrementalItems;
    for (const auto& item : bounds.RangeIncrementals()) {
        rangeIncrementalItems.push_back(SerializeNumberAndDirection(pos, item, ctx));
    }

    TExprNodeList rowIncrementalItems;
    for (const auto& item : bounds.RowIncrementals()) {
        rowIncrementalItems.push_back(SerializeNumberAndDirection(pos, ConvertToExprNode(pos, item, ctx), ctx));
    }

    return ctx.Builder(pos)
        .Callable("AsStruct")
            .List(0)
                .Atom(0, KeyRangeIntervals)
                .List(1)
                    .Add(std::move(rangeIntervalItems))
                .Seal()
            .Seal()
            .List(1)
                .Atom(0, KeyRowIntervals)
                .List(1)
                    .Add(std::move(rowIntervalItems))
                .Seal()
            .Seal()
            .List(2)
                .Atom(0, KeyRangeIncrementals)
                .List(1)
                    .Add(std::move(rangeIncrementalItems))
                .Seal()
            .Seal()
            .List(3)
                .Atom(0, KeyRowIncrementals)
                .List(1)
                    .Add(std::move(rowIncrementalItems))
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

} // anonymous namespace

TExprNode::TPtr SerializeWindowAggregatorParamsToExpr(
    const TExprNodeCoreWinFrameCollectorParams& params,
    TPositionHandle pos,
    TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Callable("AsStruct")
            .List(0)
                .Atom(0, KeySortOrder)
                .Callable(1, "String")
                    .Atom(0, ToString(params.GetSortOrder()))
                .Seal()
            .Seal()
            .List(1)
                .Atom(0, KeyBounds)
                .Add(1, SerializeWindowFrameAggregatedBounds(pos, params.GetBounds(), ctx))
            .Seal()
            .List(2)
                .Atom(0, KeySortColumnName)
                .Callable(1, "String")
                    .Atom(0, params.GetSortColumnName())
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

} // namespace NYql::NWindow
