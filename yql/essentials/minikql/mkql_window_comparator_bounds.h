#pragma once

#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_date_scaler.h>
#include <yql/essentials/minikql/mkql_saturated_math.h>

#include <util/system/types.h>

#include <functional>
#include <type_traits>
#include <variant>

namespace NKikimr::NMiniKQL {

class TPlainNumericTag {};

template <auto TScaler, typename TValue>
struct TWithScale {
    TValue Value = {};
};

template <>
struct TWithScale<TPlainNumericTag{}, NYql::NDecimal::TInt128> {
    NYql::NDecimal::TInt128 Value = {};
    ui8 Precision = {};
};

template <typename T>
using TNoScaledType = TWithScale<TPlainNumericTag{}, T>;

template <typename T>
using TScaledDateType = TWithScale<ToScaledDate<NUdf::TDataType<T>>, typename NUdf::TDataType<T>::TLayout>;

using TNumericTypeWithScale =
    std::variant<
        TNoScaledType<i8>,
        TNoScaledType<ui8>,
        TNoScaledType<i16>,
        TNoScaledType<ui16>,
        TNoScaledType<i32>,
        TNoScaledType<ui32>,
        TNoScaledType<i64>,
        TNoScaledType<ui64>,
        TNoScaledType<float>,
        TNoScaledType<double>>;

using TDateTypeWithScale = std::variant<
    TScaledDateType<NUdf::TDate>,
    TScaledDateType<NUdf::TDatetime>,
    TScaledDateType<NUdf::TTimestamp>,
    TScaledDateType<NUdf::TInterval>,
    TScaledDateType<NUdf::TTzDate>,
    TScaledDateType<NUdf::TTzDatetime>,
    TScaledDateType<NUdf::TTzTimestamp>,
    TScaledDateType<NUdf::TDate32>,
    TScaledDateType<NUdf::TDatetime64>,
    TScaledDateType<NUdf::TTimestamp64>,
    TScaledDateType<NUdf::TInterval64>,
    TScaledDateType<NUdf::TTzDate32>,
    TScaledDateType<NUdf::TTzDatetime64>,
    TScaledDateType<NUdf::TTzTimestamp64>>;

using TRangeNumericTypeWithScale = std::variant<
    TNoScaledType<ui32>,
    TNoScaledType<ui64>,
    TNoScaledType<float>,
    TNoScaledType<double>>;

using TRangeIntervalTypeWithScale = std::variant<
    TScaledDateType<NUdf::TInterval64>,
    TScaledDateType<NUdf::TInterval>>;

template <typename TContext, typename TElement>
class TBlackboxTypeData: public TAtomicRefCount<TBlackboxTypeData<TContext, TElement>> {
public:
    using TPtr = TIntrusivePtr<TBlackboxTypeData>;
    virtual bool IsBelongToInterval(EInfBoundary infBoundary, EDirection direction, TElement from, TElement to, TContext& ctx) const = 0;
    virtual ~TBlackboxTypeData() = default;
};

template <typename TContext, typename TElement>
using TBlackboxType = std::variant<TNoScaledType<typename TBlackboxTypeData<TContext, TElement>::TPtr>>;

using TNumericDecimalWithScale = std::variant<TNoScaledType<NYql::NDecimal::TInt128>>;

template <typename TContext, typename TElement>
using TColumnTypeWithScale = std::variant<TNumericTypeWithScale, TDateTypeWithScale, TBlackboxType<TContext, TElement>, TNumericDecimalWithScale>;

template <typename TContext, typename TElement>
using TRangeTypeWithScale = std::variant<TRangeNumericTypeWithScale, TRangeIntervalTypeWithScale, TBlackboxType<TContext, TElement>, TNumericDecimalWithScale>;

// Custom visit function need only to reduce code bloat due to dead branches of std::visit.
template <typename TContext, typename TElement>
decltype(auto) VisitColumnTypeWithScale(const auto& lambda, TRangeTypeWithScale<TContext, TElement> range, TColumnTypeWithScale<TContext, TElement> column) {
    using TResult = decltype(lambda(TNoScaledType<i8>(), TNoScaledType<i8>()));
    using TBlackbox = TBlackboxType<TContext, TElement>;

    return std::visit([&lambda](auto& rangeInner, auto& columnInner) -> TResult {
        using TRange = std::decay_t<decltype(rangeInner)>;
        using TColumn = std::decay_t<decltype(columnInner)>;

        if constexpr (std::is_same_v<TRange, TRangeNumericTypeWithScale> && std::is_same_v<TColumn, TNumericTypeWithScale>) {
            return std::visit(lambda, rangeInner, columnInner);
        } else if constexpr (std::is_same_v<TRange, TRangeIntervalTypeWithScale> && std::is_same_v<TColumn, TDateTypeWithScale>) {
            return std::visit(lambda, rangeInner, columnInner);
        } else if constexpr (std::is_same_v<TRange, TBlackbox> && std::is_same_v<TColumn, TBlackbox>) {
            return std::visit(lambda, rangeInner, columnInner);
        } else if constexpr (std::is_same_v<TRange, TNumericDecimalWithScale> && std::is_same_v<TColumn, TNumericDecimalWithScale>) {
            return std::visit(lambda, rangeInner, columnInner);
        } else {
            ythrow yexception() << "Not appropriate visiting for window range and column types";
        }
    }, range, column);
}

template <typename TContext, typename TElement>
class TRangeBound {
public:
    TRangeBound(TRangeTypeWithScale<TContext, TElement> bound, TColumnTypeWithScale<TContext, TElement> column, ui32 index)
        : Bound_(std::move(bound))
        , Column_(std::move(column))
        , Index_(index)
    {
    }

    const TRangeTypeWithScale<TContext, TElement>& GetBound() const {
        return Bound_;
    }

    // Should be used only for type extraction, not for value.
    const TColumnTypeWithScale<TContext, TElement>& GetColumn() const {
        return Column_;
    }

    ui32 GetIndex() const {
        return Index_;
    }

private:
    TRangeTypeWithScale<TContext, TElement> Bound_;
    TColumnTypeWithScale<TContext, TElement> Column_;
    ui32 Index_;
};

template <typename TContext, typename TElement>
using TVariantBound = NYql::NWindow::TNumberAndDirection<TRangeBound<TContext, TElement>>;

template <typename TContext, typename TElement>
using TVariantBounds = NYql::NWindow::TCoreWinFrameCollectorBounds<TRangeBound<TContext, TElement>, /*WithSortedColumnNames=*/false>;

template <typename TStreamElement, typename TContext>
using TRangeComparator = std::function<bool(TStreamElement from, TStreamElement to, bool currentRighter, TContext& context)>;

template <typename TStreamElement, typename TContext>
using TComparatorBound = NYql::NWindow::TNumberAndDirection<TRangeComparator<TStreamElement, TContext>>;

template <typename TStreamElement, typename TContext>
using TComparatorWindowFrame = NYql::NWindow::TWindowFrame<TComparatorBound<TStreamElement, TContext>>;

template <typename TStreamElement, typename TContext>
using TComparatorBounds = NYql::NWindow::TCoreWinFrameCollectorBounds<TRangeComparator<TStreamElement, TContext>, /*WithSortedColumnNames=*/false>;

} // namespace NKikimr::NMiniKQL
