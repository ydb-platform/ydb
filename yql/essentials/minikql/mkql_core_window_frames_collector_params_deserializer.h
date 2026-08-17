#pragma once

#include <yql/essentials/core/sql_types/sort_order.h>
#include <yql/essentials/core/sql_types/window_direction.h>
#include <yql/essentials/minikql/mkql_window_comparator_bounds.h>
#include <yql/essentials/minikql/mkql_saturated_math.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_window_defs.h>

namespace NKikimr::NMiniKQL {

namespace NDetail {

template <typename T>
constexpr static bool IsNan(T value) {
    if constexpr (std::is_floating_point_v<T>) {
        return std::isnan(value);
    } else if constexpr (std::is_same_v<T, NYql::NDecimal::TInt128>) {
        return NYql::NDecimal::IsNan(value);
    } else {
        return false;
    }
}

constexpr decltype(auto) Scale(auto&& value Y_LIFETIME_BOUND, auto Scaler) {
    if constexpr (std::is_same_v<decltype(Scaler), TPlainNumericTag>) {
        return std::forward<decltype(value)>(value);
    } else {
        return std::invoke(Scaler, std::forward<decltype(value)>(value));
    }
};

// Creates a comparator functor that wraps IsBelongToInterval.
template <typename TStreamElement,
          typename TElement,
          typename TContext,
          typename TDeserializerContext,
          auto TRangeScaler,
          typename TRangeType,
          auto TRangeColumnScaler,
          typename TRangeColumnType>
TRangeComparator<TStreamElement, TContext> CreateComparator(
    EDirection direction,
    TWithScale<TRangeScaler, TRangeType> delta,
    TWithScale<TRangeColumnScaler, TRangeColumnType> column,
    const TDeserializerContext& deserializerContext,
    ESortOrder sortOrder,
    ui32 memberIndex,
    bool isAdding,
    EInfBoundary infBoundary)
{
    if constexpr (std::is_same_v<TRangeType, NYql::NDecimal::TInt128>) {
        MKQL_ENSURE(column.Precision == delta.Precision, "Precision must be the same for column and delta");
    }
    EDirection adjustedDirection = (sortOrder == ESortOrder::Asc) ? direction : InvertDirection(direction);
    return [adjustedDirection, delta = std::move(delta), isAdding, deserializerContext, memberIndex, infBoundary](TStreamElement from, TStreamElement to, bool currentRighter, TContext& context) {
        auto fromValue = deserializerContext.ExtractMember(from, memberIndex);
        auto toValue = deserializerContext.ExtractMember(to, memberIndex);
        bool fromNull = deserializerContext.CheckNull(fromValue);
        bool toNull = deserializerContext.CheckNull(toValue);
        bool returnOnBound = isAdding ? currentRighter : !currentRighter;
        if (fromNull != toNull) {
            return returnOnBound;
        }
        // Two empty optionals are inside same interval always. Since they are equal.
        if (fromNull && toNull) {
            return true;
        }

        if constexpr (std::is_same_v<typename TBlackboxTypeData<TContext, TElement>::TPtr, TRangeColumnType>) {
            static_assert(std::is_same_v<typename TBlackboxTypeData<TContext, TElement>::TPtr, TRangeType>, "TBlackboxTypeData::TPtr is not the same as TRangeColumnType");
            return delta.Value->IsBelongToInterval(infBoundary,
                                                   adjustedDirection,
                                                   std::move(fromValue),
                                                   std::move(toValue),
                                                   context);
        } else {
            auto fromValueUnpacked = deserializerContext.template Extract<TRangeColumnType>(fromValue);
            auto toValueUnpacked = deserializerContext.template Extract<TRangeColumnType>(toValue);
            if (IsNan(fromValueUnpacked) != IsNan(toValueUnpacked)) {
                return returnOnBound;
            }

            if (IsNan(fromValueUnpacked) && IsNan(toValueUnpacked)) {
                return true;
            }

            if constexpr (std::is_same_v<TRangeType, NYql::NDecimal::TInt128>) {
                return IsBelongToInterval(infBoundary, adjustedDirection,
                                          Scale(std::move(fromValueUnpacked), TRangeColumnScaler),
                                          Scale(delta.Value, TRangeScaler),
                                          Scale(std::move(toValueUnpacked), TRangeColumnScaler),
                                          delta.Precision);
            } else {
                return IsBelongToInterval(infBoundary, adjustedDirection,
                                          Scale(std::move(fromValueUnpacked), TRangeColumnScaler),
                                          Scale(delta.Value, TRangeScaler),
                                          Scale(std::move(toValueUnpacked), TRangeColumnScaler));
            }
        }
    };
}

template <typename TStreamElement, typename TElement, typename TContext, typename TDeserializerContext>
TNumberAndDirection<TRangeComparator<TStreamElement, TContext>> ConvertBoundToComparator(
    const TVariantBound<TContext, TElement>& variantBound,
    EInfBoundary infBoundary,
    ESortOrder sortOrder,
    const TDeserializerContext& deserializerContext,
    bool isAdding)
{
    using TComparator = TRangeComparator<TStreamElement, TContext>;
    using TResult = TNumberAndDirection<TComparator>;

    EDirection direction = variantBound.GetDirection();

    if (variantBound.IsInf()) {
        return TResult::Inf(direction);
    }
    MKQL_ENSURE(variantBound.IsFinite(), "");
    TRangeBound value(variantBound.GetUnderlyingValue());

    return VisitColumnTypeWithScale<TContext, TElement>([&](auto&& delta, auto&& column) -> TResult {
        TComparator comparator = CreateComparator<TStreamElement, TElement, TContext>(direction, std::forward<decltype(delta)>(delta), std::forward<decltype(column)>(column), deserializerContext, sortOrder, value.GetIndex(), isAdding, infBoundary);
        return TResult(std::move(comparator), direction);
    }, value.GetBound(), value.GetColumn());
}

template <typename TStreamElement, typename TElement, typename TContext, typename TDeserializerContext>
TWindowFrame<TNumberAndDirection<TRangeComparator<TStreamElement, TContext>>> ConvertFrameToComparator(
    const TWindowFrame<TVariantBound<TContext, TElement>>& variantFrame,
    ESortOrder sortOrder,
    const TDeserializerContext& deserializerContext)
{
    using TComparator = TRangeComparator<TStreamElement, TContext>;
    using TComparatorBound = TNumberAndDirection<TComparator>;

    // Determine InfBoundary based on sort order
    // For Asc: Add uses Left, Remove uses Right
    // For Desc: Add uses Right, Remove uses Left
    EInfBoundary addBoundary = (sortOrder == ESortOrder::Asc) ? EInfBoundary::Left : EInfBoundary::Right;
    EInfBoundary removeBoundary = (sortOrder == ESortOrder::Asc) ? EInfBoundary::Right : EInfBoundary::Left;

    // Min bound is used for removing elements (left side of window)
    // Max bound is used for adding elements (right side of window)
    TComparatorBound minComparator = ConvertBoundToComparator<TStreamElement, TElement, TContext, TDeserializerContext>(std::move(variantFrame.Min()), removeBoundary, sortOrder, deserializerContext, /*isAdding=*/false);
    TComparatorBound maxComparator = ConvertBoundToComparator<TStreamElement, TElement, TContext, TDeserializerContext>(std::move(variantFrame.Max()), addBoundary, sortOrder, deserializerContext, /*isAdding=*/true);

    return {std::move(minComparator), std::move(maxComparator)};
}

} // namespace NDetail

template <typename TMemberExtractor, typename TNullChecker, typename TElementExtractor>
class TDeserializerContext {
public:
    TDeserializerContext(TMemberExtractor memberExtractor, TNullChecker nullChecker, TElementExtractor elementExtractor)
        : MemberExtractor_(std::move(memberExtractor))
        , NullChecker_(std::move(nullChecker))
        , ElementExtractor_(std::move(elementExtractor))
    {
    }

    auto ExtractMember(const auto& value, ui32 memberIndex) const {
        return MemberExtractor_(value, memberIndex);
    }

    bool CheckNull(const auto& value) const {
        return NullChecker_(value);
    }

    template <typename T>
    auto Extract(const auto& value) const {
        return ElementExtractor_.template operator()<T>(value);
    }

private:
    TMemberExtractor MemberExtractor_;
    TNullChecker NullChecker_;
    TElementExtractor ElementExtractor_;
};

struct TNoopDeserializerContext {};

template <typename TStreamElement, typename TElement, typename TContext, typename TDeserializerContext>
TComparatorBounds<TStreamElement, TContext> ConvertBoundsToComparators(
    const TVariantBounds<TContext, TElement>& variantBounds,
    ESortOrder sortOrder,
    const TDeserializerContext& deserializerContext)
{
    TComparatorBounds<TStreamElement, TContext> bounds;

    // Determine InfBoundary based on sort order for incrementals
    EInfBoundary addBoundary = (sortOrder == ESortOrder::Asc) ? EInfBoundary::Left : EInfBoundary::Right;

    if constexpr (!std::is_same_v<TDeserializerContext, TNoopDeserializerContext>) {
        // Convert range intervals
        for (const auto& frame : variantBounds.RangeIntervals()) {
            bounds.AddRange(NDetail::ConvertFrameToComparator<TStreamElement, TElement, TContext, TDeserializerContext>(std::move(frame), sortOrder, deserializerContext));
        }
    }

    // Row intervals pass through as-is
    for (const auto& frame : variantBounds.RowIntervals()) {
        bounds.AddRow(frame);
    }

    // Convert range incrementals
    if constexpr (!std::is_same_v<TDeserializerContext, TNoopDeserializerContext>) {
        for (const auto& delta : variantBounds.RangeIncrementals()) {
            bounds.AddRangeIncremental(NDetail::ConvertBoundToComparator<TStreamElement, TElement, TContext, TDeserializerContext>(std::move(delta), addBoundary, sortOrder, deserializerContext, /*isAdding=*/true));
        }
    }

    // Row incrementals pass through as-is
    for (const auto& delta : variantBounds.RowIncrementals()) {
        bounds.AddRowIncremental(delta);
    }

    return bounds;
}

} // namespace NKikimr::NMiniKQL
