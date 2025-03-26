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
TTo BitCast(const TFrom &src) noexcept;

////////////////////////////////////////////////////////////////////////////////

// See: |std::midpoint| from header <numeric> from C++2a.
// Remove this implementation and use the standard one when it becomes available.
template <class TInt>
TInt Midpoint(TInt a, TInt b) noexcept;

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE i64 SignedSaturationArithmeticMultiply(i64 lhs, i64 rhs);
Y_FORCE_INLINE i64 UnsignedSaturationArithmeticMultiply(i64 lhs, i64 rhs, i64 max = std::numeric_limits<i64>::max());

Y_FORCE_INLINE i64 SignedSaturationArithmeticAdd(i64 lhs, i64 rhs);
Y_FORCE_INLINE i64 UnsignedSaturationArithmeticAdd(i64 lhs, i64 rhs, i64 max = std::numeric_limits<i64>::max());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define UTIL_INL_H_
#include "util-inl.h"
#undef UTIL_INL_H_
