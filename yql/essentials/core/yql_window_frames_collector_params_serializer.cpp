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

TExprNode::TPtr BuildNumberVariantType(TPositionHandle pos, TStringBuf dataTypeName, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("VariantType")
            .Callable(0, "StructType")
                .List(0)
                    .Atom(0, "Unbounded")
                    .Callable(1, "VoidType")
                    .Seal()
                .Seal()
                .List(1)
                    .Atom(0, "Bounded")
                    .Callable(1, "DataType")
                        .Atom(0, dataTypeName)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

template <typename T>
TExprNode::TPtr SerializeNumberAndDirection(
    TPositionHandle pos,
    const TNumberAndDirection<T>& value,
    TStringBuf dataTypeName,
    TStringBuf callableName,
    TExprContext& ctx)
{
    auto variantType = BuildNumberVariantType(pos, dataTypeName, ctx);

    TExprNode::TPtr numberVariant;
    if (value.IsInf()) {
        numberVariant = ctx.Builder(pos)
            .Callable("Variant")
                .Callable(0, "Void")
                .Seal()
                .Atom(1, "Unbounded")
                .Add(2, variantType)
            .Seal()
            .Build();
    } else {
        numberVariant = ctx.Builder(pos)
            .Callable("Variant")
                .Callable(0, callableName)
                    .Atom(0, ToString(value.GetUnderlyingValue()))
                .Seal()
                .Atom(1, "Bounded")
                .Add(2, variantType)
            .Seal()
            .Build();
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
                .Add(1, numberVariant)
            .Seal()
        .Seal()
        .Build();
}

template <typename T>
TExprNode::TPtr SerializeWindowFrame(
    TPositionHandle pos,
    const TWindowFrame<TNumberAndDirection<T>>& frame,
    TStringBuf dataTypeName,
    TStringBuf callableName,
    TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Callable("AsStruct")
            .List(0)
                .Atom(0, KeyMin)
                .Add(1, SerializeNumberAndDirection(pos, frame.Min(), dataTypeName, callableName, ctx))
            .Seal()
            .List(1)
                .Atom(0, KeyMax)
                .Add(1, SerializeNumberAndDirection(pos, frame.Max(), dataTypeName, callableName, ctx))
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr SerializeWindowFrameAggregatedBounds(TPositionHandle pos, const TCoreWinFrameCollectorBounds<TString>& bounds, TStringBuf rangeCallableName, TExprContext& ctx) {
    TExprNodeList rangeIntervalItems;
    for (const auto& frame : bounds.RangeIntervals()) {
        rangeIntervalItems.push_back(SerializeWindowFrame(pos, frame, rangeCallableName, rangeCallableName, ctx));
    }

    TExprNodeList rowIntervalItems;
    for (const auto& frame : bounds.RowIntervals()) {
        rowIntervalItems.push_back(SerializeWindowFrame(pos, frame, "Uint64", "Uint64", ctx));
    }

    TExprNodeList rangeIncrementalItems;
    for (const auto& item : bounds.RangeIncrementals()) {
        rangeIncrementalItems.push_back(SerializeNumberAndDirection(pos, item, rangeCallableName, rangeCallableName, ctx));
    }

    TExprNodeList rowIncrementalItems;
    for (const auto& item : bounds.RowIncrementals()) {
        rowIncrementalItems.push_back(SerializeNumberAndDirection(pos, item, "Uint64", "Uint64", ctx));
    }

    return ctx.Builder(pos)
        .Callable("AsStruct")
            .List(0)
                .Atom(0, KeyRangeIntervals)
                .Callable(1, "AsList")
                    .Add(std::move(rangeIntervalItems))
                .Seal()
            .Seal()
            .List(1)
                .Atom(0, KeyRowIntervals)
                .Callable(1, "AsList")
                    .Add(std::move(rowIntervalItems))
                .Seal()
            .Seal()
            .List(2)
                .Atom(0, KeyRangeIncrementals)
                .Callable(1, "AsList")
                    .Add(std::move(rangeIncrementalItems))
                .Seal()
            .Seal()
            .List(3)
                .Atom(0, KeyRowIncrementals)
                .Callable(1, "AsList")
                    .Add(std::move(rowIncrementalItems))
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

} // anonymous namespace

TExprNode::TPtr SerializeWindowAggregatorParamsToExpr(
    const TStringCoreWinFramesCollectorParams& params,
    TPositionHandle pos,
    TStringBuf rangeCallableName,
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
                .Add(1, SerializeWindowFrameAggregatedBounds(pos, params.GetBounds(), rangeCallableName, ctx))
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
