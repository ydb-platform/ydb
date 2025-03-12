#pragma once

#include <util/system/compiler.h>
#include <util/system/types.h>

#include <cstring>
#include <limits>
#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// See: |std::bit_cast| from header <bit> from C++2a.
// Remove this implementation and use the standard one when it becomes available.
template <class TTo, class TFrom>
TTo BitCast(const TFrom &src) noexcept
{
    static_assert(sizeof(TTo) == sizeof(TFrom));
    static_assert(std::is_trivially_copyable_v<TFrom>);
    static_assert(std::is_trivial_v<TTo>);

    TTo dst;
    std::memcpy(&dst, &src, sizeof(TTo));
    return dst;
}

// See: |std::midpoint| from header <numeric> from C++2a.
// Remove this implementation and use the standard one when it becomes available.
template <class TInt>
TInt Midpoint(TInt a, TInt b) noexcept
{
    static_assert(std::is_integral_v<TInt>);
    static_assert(std::is_same_v<std::remove_cv_t<TInt>, TInt>);
    static_assert(!std::is_same_v<TInt, bool>);

    using TUInt = std::make_unsigned_t<TInt>;

    int k = 1;
    TUInt mn = a;
    TUInt mx = b;
    if (a > b) {
        k = -1;
        mn = b;
        mx = a;
    }

    return a + k * TInt(TUInt(mx - mn) >> 1);
}

Y_FORCE_INLINE i64 SignedSaturationArithmeticMultiply(i64 lhs, i64 rhs)
{
    if (lhs == 0 || rhs == 0) {
        return 0;
    }

    i64 sign = 1;
    if (lhs < 0) {
        sign = -sign;
    }
    if (rhs < 0) {
        sign = -sign;
    }

    i64 result;
    if (__builtin_mul_overflow(lhs, rhs, &result)) {
        if (sign < 0) {
            return std::numeric_limits<i64>::min();
        } else {
            return std::numeric_limits<i64>::max();
        }
    } else {
        return result;
    }
}

Y_FORCE_INLINE i64 UnsignedSaturationArithmeticMultiply(i64 lhs, i64 rhs)
{
    i64 result;
    if (__builtin_mul_overflow(lhs, rhs, &result)) {
        return std::numeric_limits<i64>::max();
    } else {
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
