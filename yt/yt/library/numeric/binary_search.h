#pragma once

#include "util.h"

#include <util/system/yassert.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static_assert(std::numeric_limits<double>::is_iec559, "We assume IEEE 754 floating point implementation");

////////////////////////////////////////////////////////////////////////////////

// Returns the lowest integer value in [|lo|, |hi|] satisfying the predicate.
// |lo| must not be greater than |hi|.
// |predicate| must be monotonous and deterministic, and |predicate(hi)| must be satisfied.
// |predicate(FloatingPointLowerBound(lo, hi, predicate))| is guaranteed to be satisfied.
// Note that if |predicate(lo)| is satisfied, |lo| is returned.
// The behaviour is undefined if |predicate| is not monotonous or not deterministic.
template <class TInt, class TPredicate>
constexpr TInt IntegerLowerBound(TInt lo, TInt hi, TPredicate&& predicate);

// Return the highest integer value in [|lo|, |hi|] satisfying the predicate.
// |lo| must not be greater than |hi|.
// |predicate| must be monotonous and deterministic, and |predicate(lo)| must be satisfied.
// |predicate(IntegerInverseLowerBound(lo, hi, predicate))| is guaranteed to be satisfied.
// Note that if |predicate(hi)| is satisfied, |hi| is returned.
// The behaviour is undefined if |predicate| is not monotonous or not deterministic.
template <class TInt, class TPredicate>
constexpr TInt IntegerInverseLowerBound(TInt lo, TInt hi, TPredicate&& predicate);

// Returns the lowest representable value in [|lo|, |hi|] satisfying the predicate.
// |lo| an |hi| must not be NaN (as for |std::isnan|), and |lo| must not be greater than |hi|.
// Notice that |lo| and / or |hi| can be infinite.
// |predicate| must be monotonous and deterministic, and |predicate(hi)| must be satisfied.
// |predicate(FloatingPointLowerBound(lo, hi, predicate))| is guaranteed to be satisfied.
// The behaviour is undefined if |predicate| is not monotonous or not deterministic.
// The function makes no more than 70 calls to |predicate|
// (64 for the binary search, and 6 are reserved for corner case handling).
template <class TPredicate>
double FloatingPointLowerBound(double lo, double hi, TPredicate&& predicate);

// Returns the highest representable value in [|lo|, |hi|] satisfying the predicate.
// |lo| an |hi| must not be NaN (as for |std::isnan|), and |lo| must not be greater than |hi|.
// Notice that |lo| and / or |hi| can be infinite.
// |predicate| must be monotonous and deterministic, and |predicate(lo)| must be satisfied.
// |predicate(FloatingPointInverseLowerBound(lo, hi, predicate))| is guaranteed to be satisfied.
// The behaviour is undefined if |predicate| is not monotonous or not deterministic.
// The function makes no more than 70 calls to |predicate|
// (64 for the binary search, and 6 are reserved for corner case handling).
template <class TPredicate>
double FloatingPointInverseLowerBound(double lo, double hi, TPredicate&& predicate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define BINARY_SEARCH_INL_H_
#include "binary_search-inl.h"
#undef BINARY_SEARCH_INL_H_
