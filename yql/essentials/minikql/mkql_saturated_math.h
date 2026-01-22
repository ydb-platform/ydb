#pragma once

#include <yql/essentials/core/sql_types/window_direction.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <cmath>
#include <limits>
#include <type_traits>

namespace NKikimr::NMiniKQL {

using NYql::NWindow::EDirection;

// clang-format off
template <class T>
using TNextWiderSigned =
    std::conditional_t<sizeof(T) == sizeof(i8), i16,
    std::conditional_t<sizeof(T) == sizeof(i16), i32,
    std::conditional_t<sizeof(T) == sizeof(i32), i64,
                       NYql::NDecimal::TInt128>>>;
// clang-format on

enum class EInfBoundary {
    Left,
    Right,
};

// InfBoundary == Right -> [boundary, +inf)
// InfBoundary == Left  -> (-inf,  boundary]
template <EInfBoundary InfBoundary, class T>
Y_FORCE_INLINE constexpr bool IsBelongToInterval(EDirection dir, T from, T delta, T x) {
    if constexpr (std::is_floating_point_v<T>) {
        Y_DEBUG_ABORT_UNLESS(!std::isnan(delta));
        Y_DEBUG_ABORT_UNLESS(!std::isnan(from));
        Y_DEBUG_ABORT_UNLESS(!std::isnan(x));
        const T b = (dir == EDirection::Following) ? (from + delta) : (from - delta);
        if constexpr (InfBoundary == EInfBoundary::Right) {
            return x >= b;
        } else {
            return x <= b;
        }
    } else {
        static_assert(std::is_integral_v<T>, "T must be integral or floating");
        static_assert(!std::is_same_v<T, bool>, "bool is not supported");
        static_assert(sizeof(T) == sizeof(i8) || sizeof(T) == sizeof(i16) ||
                          sizeof(T) == sizeof(i32) || sizeof(T) == sizeof(i64),
                      "Only 8/16/32/64-bit integers are supported");

        using W = TNextWiderSigned<T>;
        const W wf = static_cast<W>(from);
        const W wd = static_cast<W>(delta);
        const W wx = static_cast<W>(x);
        const W b = (dir == EDirection::Following) ? (wf + wd) : (wf - wd);

        if constexpr (InfBoundary == EInfBoundary::Right) {
            return wx >= b; // [b, +inf)
        } else {
            return wx <= b; // (-inf, b]
        }
    }
}

} // namespace NKikimr::NMiniKQL
