#ifndef UTIL_INL_H_
#error "Direct inclusion of this file is not allowed, include util.h"
// For the sake of sane code completion.
#include "util.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

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

Y_FORCE_INLINE i64 UnsignedSaturationArithmeticMultiply(i64 lhs, i64 rhs, i64 max)
{
    i64 result;
    if (__builtin_mul_overflow(lhs, rhs, &result) || result > max) {
        return max;
    } else {
        return result;
    }
}

Y_FORCE_INLINE i64 SignedSaturationArithmeticAdd(i64 lhs, i64 rhs)
{
    // NB: If operands have different signs, no overflow is possible.
    i64 sign = 1;
    if (lhs < 0 && rhs < 0) {
        sign = -sign;
    }

    i64 result;
    if (__builtin_add_overflow(lhs, rhs, &result)) {
        if (sign < 0) {
            return std::numeric_limits<i64>::min();
        } else {
            return std::numeric_limits<i64>::max();
        }
    } else {
        return result;
    }
}

Y_FORCE_INLINE i64 UnsignedSaturationArithmeticAdd(i64 lhs, i64 rhs, i64 max)
{
    i64 result;
    if (__builtin_add_overflow(lhs, rhs, &result) || result > max) {
        return max;
    } else {
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
