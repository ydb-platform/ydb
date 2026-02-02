#pragma once

#include <yql/essentials/core/sql_types/window_frame_bounds.h>

#include <util/system/types.h>

#include <functional>
#include <variant>

namespace NKikimr::NMiniKQL {

// Variant type for all supported numeric range types.
using TRangeVariant = std::variant<i8, ui8, i16, ui16, i32, ui32, i64, ui64, float, double>;

using TVariantBound = NYql::NWindow::TNumberAndDirection<TRangeVariant>;
using TVariantBounds = NYql::NWindow::TCoreWinFrameCollectorBounds<TRangeVariant>;

template <typename TStreamElement>
using TRangeComparator = std::function<bool(TStreamElement from, TStreamElement to)>;

template <typename TStreamElement>
using TComparatorBound = NYql::NWindow::TNumberAndDirection<TRangeComparator<TStreamElement>>;

template <typename TStreamElement>
using TComparatorWindowFrame = NYql::NWindow::TWindowFrame<TComparatorBound<TStreamElement>>;

template <typename TStreamElement>
using TComparatorBounds = NYql::NWindow::TCoreWinFrameCollectorBounds<TRangeComparator<TStreamElement>>;

} // namespace NKikimr::NMiniKQL
