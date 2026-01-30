#pragma once

#include <yql/essentials/core/sql_types/sort_order.h>
#include <yql/essentials/core/sql_types/window_direction.h>
#include <yql/essentials/minikql/mkql_window_comparator_bounds.h>
#include <yql/essentials/minikql/mkql_saturated_math.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_window_defs.h>

namespace NKikimr::NMiniKQL {

namespace NDetail {

// Creates a comparator functor that wraps IsBelongToInterval.
template <typename TStreamElement, typename TRangeType>
TRangeComparator<TStreamElement> CreateComparator(
    EDirection direction,
    TRangeType delta,
    EInfBoundary infBoundary,
    ESortOrder sortOrder)
{
    EDirection adjustedDirection = (sortOrder == ESortOrder::Asc) ? direction : InvertDirection(direction);

    if (infBoundary == EInfBoundary::Left) {
        return [adjustedDirection, delta](TStreamElement from, TStreamElement to) {
            return IsBelongToInterval<EInfBoundary::Left>(adjustedDirection, from, delta, to);
        };
    } else {
        return [adjustedDirection, delta](TStreamElement from, TStreamElement to) {
            return IsBelongToInterval<EInfBoundary::Right>(adjustedDirection, from, delta, to);
        };
    }
}

template <typename TStreamElement>
TNumberAndDirection<TRangeComparator<TStreamElement>> ConvertBoundToComparator(
    const TVariantBound& variantBound,
    EInfBoundary infBoundary,
    ESortOrder sortOrder)
{
    using TComparator = TRangeComparator<TStreamElement>;
    using TResult = TNumberAndDirection<TComparator>;

    EDirection direction = variantBound.GetDirection();

    if (variantBound.IsInf()) {
        return TResult::Inf(direction);
    }
    MKQL_ENSURE(variantBound.IsFinite(), "");
    const TRangeVariant& value = variantBound.GetUnderlyingValue();

    return std::visit([&](auto delta) -> TResult {
        TComparator comparator = CreateComparator<TStreamElement>(direction, delta, infBoundary, sortOrder);
        return TResult(std::move(comparator), direction);
    }, value);
}

template <typename TStreamElement>
TWindowFrame<TNumberAndDirection<TRangeComparator<TStreamElement>>> ConvertFrameToComparator(
    const TWindowFrame<TVariantBound>& variantFrame,
    ESortOrder sortOrder)
{
    using TComparator = TRangeComparator<TStreamElement>;
    using TComparatorBound = TNumberAndDirection<TComparator>;

    // Determine InfBoundary based on sort order
    // For Asc: Add uses Left, Remove uses Right
    // For Desc: Add uses Right, Remove uses Left
    EInfBoundary addBoundary = (sortOrder == ESortOrder::Asc) ? EInfBoundary::Left : EInfBoundary::Right;
    EInfBoundary removeBoundary = (sortOrder == ESortOrder::Asc) ? EInfBoundary::Right : EInfBoundary::Left;

    // Min bound is used for removing elements (left side of window)
    // Max bound is used for adding elements (right side of window)
    TComparatorBound minComparator = ConvertBoundToComparator<TStreamElement>(variantFrame.Min(), removeBoundary, sortOrder);
    TComparatorBound maxComparator = ConvertBoundToComparator<TStreamElement>(variantFrame.Max(), addBoundary, sortOrder);

    return {std::move(minComparator), std::move(maxComparator)};
}

} // namespace NDetail

template <typename TStreamElement>
TComparatorBounds<TStreamElement> ConvertBoundsToComparators(
    const TVariantBounds& variantBounds,
    ESortOrder sortOrder)
{
    TComparatorBounds<TStreamElement> bounds;

    // Determine InfBoundary based on sort order for incrementals
    EInfBoundary addBoundary = (sortOrder == ESortOrder::Asc) ? EInfBoundary::Left : EInfBoundary::Right;

    // Convert range intervals
    for (const auto& frame : variantBounds.RangeIntervals()) {
        bounds.AddRange(NDetail::ConvertFrameToComparator<TStreamElement>(frame, sortOrder));
    }

    // Row intervals pass through as-is
    for (const auto& frame : variantBounds.RowIntervals()) {
        bounds.AddRow(frame);
    }

    // Convert range incrementals
    for (const auto& delta : variantBounds.RangeIncrementals()) {
        bounds.AddRangeIncremental(NDetail::ConvertBoundToComparator<TStreamElement>(delta, addBoundary, sortOrder));
    }

    // Row incrementals pass through as-is
    for (const auto& delta : variantBounds.RowIncrementals()) {
        bounds.AddRowIncremental(delta);
    }

    return bounds;
}

} // namespace NKikimr::NMiniKQL
