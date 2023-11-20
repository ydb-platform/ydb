#ifndef BINARY_SEARCH_INL_H_
#error "Direct inclusion of this file is not allowed, include binary_search.h"
// For the sake of sane code completion.
#include "binary_search.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

static constexpr uint64_t Uint64FirstBit = 1ull << 63ull;

// In IEEE 754 the bit representation of non-negative finite doubles is monotonous,
// i.e. |BitCast<uint64_t>(x1) < BitCast<uin64_t>(x2)| iff |x1 < x2|.
// Negative numbers are represented by setting the first bit (called "sign bit") to 1.
//
// In order to create monotonous one-to-one mapping between |uint64_t| and |double|,
// we flip all bits in the binary representation of negative values (map them to the numbers less than 2^63),
// and flip the sign bit in the binary representation of positive values
// (map them to the numbers larger than or equal to 2^63).
//
// Examples:
//     0.0 is represented by  0x8000000000000000 (2^63).
//     -0.0 is represented by 0x7FFFFFFFFFFFFFFF (2^63 - 1).
//     +inf is represented by 0xFFF0000000000000. Any (lexicographically) larger bit pattern represents NaN.
//     -inf is represented by 0x000FFFFFFFFFFFFF. Any (lexicographically) smaller bit pattern represents NaN.
inline uint64_t DoubleToBitset(double value) noexcept
{
    auto bitset = BitCast<uint64_t>(value);
    // If the sign bit is 1, mask=0xFFFFFFFFFFFFFFFF. Otherwise, mask=0x8000000000000000.
    // We use arithmetic right shift to achieve this.
    uint64_t mask = (static_cast<int64_t>(bitset) >> 63) | Uint64FirstBit; // NOLINT(hicpp-signed-bitwise)
    // For negative values (first bit is 1), we flip all bits.
    // For positive values (first bit is 0), we flip only the first bit.
    return bitset ^ mask;
}

inline double BitsetToDouble(uint64_t bitset) noexcept
{
    // If the first bit is 0, mask=0xFFFFFFFFFFFFFFFF. Otherwise, mask=0x8000000000000000.
    // We use arithmetic right shift to achieve this.
    uint64_t mask = (static_cast<int64_t>(~bitset) >> 63) | Uint64FirstBit; // NOLINT(hicpp-signed-bitwise)
    // To restore a negative value (first bit is 0), we flip all bits.
    // To restore a positive value (first bit is 1), we flip only the first bit.
    return BitCast<double>(bitset ^ mask);
}

} // namespace NDetail

template <class TInt, class TPredicate>
constexpr TInt IntegerLowerBound(TInt lo, TInt hi, TPredicate&& predicate)
{
    static_assert(std::is_integral_v<TInt>);

    using TUInt = std::make_unsigned_t<TInt>;

    YT_VERIFY(lo <= hi);

    YT_VERIFY(predicate(hi));
    if (predicate(lo)) {
        return lo;
    }

    // Notice that lo < hi and hence does not overflow.
    while (lo + 1 < hi) {
        // NB(antonkikh): std::midpoint is slow because it also handles the case when lo > hi.
        TInt mid = lo + static_cast<TInt>(static_cast<TUInt>(hi - lo) >> 1);

        if (predicate(mid)) {
            hi = mid;
        } else {
            lo = mid;
        }
    }

    return hi;
}

template <class TInt, class TPredicate>
constexpr TInt IntegerInverseLowerBound(TInt lo, TInt hi, TPredicate&& predicate)
{
    static_assert(std::is_integral_v<TInt>);

    using TUInt = std::make_unsigned_t<TInt>;

    YT_VERIFY(lo <= hi);

    YT_VERIFY(predicate(lo));
    if (predicate(hi)) {
        return hi;
    }

    // Notice that lo < hi and hence does not overflow.
    while (lo + 1 < hi) {
        // NB(antonkikh): std::midpoint is slow because it also handles the case when lo > hi.
        TInt mid = lo + static_cast<TInt>(static_cast<TUInt>(hi - lo) >> 1);

        if (predicate(mid)) {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    return lo;
}

template <class TPredicate>
double FloatingPointLowerBound(double lo, double hi, TPredicate&& predicate)
{
    YT_VERIFY(!std::isnan(lo));
    YT_VERIFY(!std::isnan(hi));
    YT_VERIFY(lo <= hi);

    // NB(antonkikh): Note that this handles the case when |hi == -0.0| and |lo == 0.0|.
    if (lo == hi) {
        YT_VERIFY(predicate(hi));
        return hi;
    }

    uint64_t resultBitset = IntegerLowerBound(
        NDetail::DoubleToBitset(lo),
        NDetail::DoubleToBitset(hi),
        [&predicate] (uint64_t bitset) { return predicate(NDetail::BitsetToDouble(bitset)); });
    return NDetail::BitsetToDouble(resultBitset);
}

template <class TPredicate>
double FloatingPointInverseLowerBound(double lo, double hi, TPredicate&& predicate)
{
    YT_VERIFY(!std::isnan(lo));
    YT_VERIFY(!std::isnan(hi));
    YT_VERIFY(lo <= hi);

    // NB(antonkikh): Note that this handles the case when |hi == -0.0| and |lo == 0.0|.
    if (lo == hi) {
        YT_VERIFY(predicate(lo));
        return lo;
    }

    uint64_t resultBitset = IntegerInverseLowerBound(
        NDetail::DoubleToBitset(lo),
        NDetail::DoubleToBitset(hi),
        [&predicate] (uint64_t bitset) { return predicate(NDetail::BitsetToDouble(bitset)); });
    return NDetail::BitsetToDouble(resultBitset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
