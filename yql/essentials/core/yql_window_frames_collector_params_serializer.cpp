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
constexpr TStringBuf KeyDirection = "Direction";
constexpr TStringBuf KeyNumber = "Number";
constexpr TStringBuf KeySortedColumn = "SortedColumn";
constexpr TStringBuf KeyFiniteValue = "FiniteValue";
constexpr TStringBuf KeyProcId = "ProcId";

using TCoreWinFrameCollectorBounds = TCoreWinFrameCollectorBounds<TWindowFrameSettingWithOffset, /*WithSortedColumnNames=*/true>;

constexpr TCoreWinFrameCollectorBounds::TSortColumnNamesView SortedColumnsNotRequired = {};
constexpr TCoreWinFrameCollectorBounds::TSortColumnNameView SortedColumnNotRequired = {};

TWindowFrameSettingBound ConvertToFrameBound(
    TPositionHandle pos,
    const TNumberAndDirection<ui64>& value,
    TExprContext& ctx)
{
    if (value.IsInf()) {
        return TWindowFrameSettingBound::Inf(value.GetDirection());
    }
    // clang-format off
    auto node = ctx.Builder(pos)
        .Callable("Uint64")
            .Atom(0, ToString(value.GetUnderlyingValue()))
        .Seal()
        .Build();
    // clang-format on
    return TWindowFrameSettingBound({node,  Nothing(), Nothing(), Nothing()}, value.GetDirection());
}

TWindowFrame<TWindowFrameSettingBound> ConvertToFrameBound(
    TPositionHandle pos,
    const TWindowFrame<TNumberAndDirection<ui64>>& frame,
    TExprContext& ctx)
{
    return TWindowFrame<TWindowFrameSettingBound>(
        ConvertToFrameBound(pos, frame.Min(), ctx),
        ConvertToFrameBound(pos, frame.Max(), ctx));
}

TExprNode::TPtr SerializeNumberAndDirection(
    TPositionHandle pos,
    const TWindowFrameSettingBound& value,
    TCoreWinFrameCollectorBounds::TSortColumnNameView sortColumnName,
    TExprContext& ctx)
{
    auto visitor = TOverloaded{
        [&](TWindowFrameSettingBound::TUnbounded) {
            // clang-format off
            return ctx.Builder(pos)
                .Callable("AsTagged")
                    .Callable(0, "Void")
                    .Seal()
                    .Atom(1, "inf")
                .Seal()
                .Build();
            // clang-format on
        },
        [&](const TWindowFrameSettingWithOffset& value) {
            auto frameBound = value.GetFrameBound();
            if (const auto& boundCast = value.GetBoundCast()) {
                frameBound = (*boundCast)(frameBound, ctx);
            }
            // clang-format off
            return ctx.Builder(pos)
                .Callable("AsStruct")
                    .List(0)
                        .Atom(0, KeyFiniteValue)
                        .Add(1, frameBound)
                    .Seal()
                    .Do([&](auto& builder) -> auto& {
                        if (const auto& procId = value.GetProcId()) {
                            builder
                                .List(1)
                                    .Atom(0, KeyProcId)
                                    .Callable(1, "Uint32")
                                        .Atom(0, ToString(*procId))
                                    .Seal()
                                .Seal();
                        }
                        return builder;
                    })
                .Seal()
                .Build();
            // clang-format on
        },
        [&](const TWindowFrameSettingBound::TZero) {
            // clang-format off
            return ctx.Builder(pos)
                .Callable("AsTagged")
                    .Callable(0, "Void")
                    .Seal()
                    .Atom(1, "zero")
                .Seal()
                .Build();
            // clang-format on
        },
    };

    TExprNode::TPtr numberAndDirection = std::visit(visitor, value.GetValue());

    // clang-format off
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
            .List(2)
                .Atom(0, KeySortedColumn)
                .Callable(1, "String")
                    .Atom(0, sortColumnName)
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr SerializeWindowFrame(
    TPositionHandle pos,
    const TWindowFrame<TWindowFrameSettingBound>& frame,
    const TCoreWinFrameCollectorBounds::TSortColumnNamesView& sortColumnNames,
    TExprContext& ctx)
{
    // clang-format off
    return ctx.Builder(pos)
        .Callable("AsStruct")
            .List(0)
                .Atom(0, KeyMin)
                .Add(1, SerializeNumberAndDirection(pos, frame.Min(), sortColumnNames.first, ctx))
            .Seal()
            .List(1)
                .Atom(0, KeyMax)
                .Add(1, SerializeNumberAndDirection(pos, frame.Max(), sortColumnNames.second, ctx))
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr SerializeWindowFrameAggregatedBounds(TPositionHandle pos, const TCoreWinFrameCollectorBounds& bounds, TExprContext& ctx) {
    TExprNodeList rangeIntervalItems;
    for (const auto& frame : bounds.RangeIntervals()) {
        rangeIntervalItems.push_back(SerializeWindowFrame(pos, frame.first, frame.second, ctx));
    }

    TExprNodeList rowIntervalItems;
    for (const auto& frame : bounds.RowIntervals()) {
        rowIntervalItems.push_back(SerializeWindowFrame(pos, ConvertToFrameBound(pos, frame, ctx), SortedColumnsNotRequired, ctx));
    }

    TExprNodeList rangeIncrementalItems;
    for (const auto& item : bounds.RangeIncrementals()) {
        rangeIncrementalItems.push_back(SerializeNumberAndDirection(pos, item.first, item.second, ctx));
    }

    TExprNodeList rowIncrementalItems;
    for (const auto& item : bounds.RowIncrementals()) {
        rowIncrementalItems.push_back(SerializeNumberAndDirection(pos, ConvertToFrameBound(pos, item, ctx), SortedColumnNotRequired, ctx));
    }

    // clang-format off
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
    // clang-format on
}

} // anonymous namespace

TExprNode::TPtr SerializeWindowAggregatorParamsToExpr(
    const TExprNodeCoreWinFrameCollectorParams& params,
    TPositionHandle pos,
    TExprContext& ctx)
{
    // clang-format off
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
        .Seal()
        .Build();
    // clang-format on
}

} // namespace NYql::NWindow
