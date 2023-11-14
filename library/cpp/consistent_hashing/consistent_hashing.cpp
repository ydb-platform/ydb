#include "consistent_hashing.h"

#include <library/cpp/pop_count/popcount.h>

#include <util/generic/bitops.h>

/*
 * (all numbers are written in big-endian manner: the least significant digit on the right)
 * (only bit representations are used - no hex or octal, leading zeroes are ommited)
 *
 * Consistent hashing scheme:
 *
 *      (sizeof(TValue) * 8, y]  (y, 0]
 * a =             *             ablock
 * b =             *             cblock
 *
 *      (sizeof(TValue) * 8, k]  (k, 0]
 * c =             *             cblock
 *
 * d =             *
 *
 * k - is determined by 2^(k-1) < n <= 2^k inequality
 * z - is number of ones in cblock
 * y - number of digits after first one in cblock
 *
 * The cblock determines logic of using a- and b- blocks:
 *
 *  bits of cblock | result of a function
 *              0  :   0
 *              1  :   1 (optimization, the next case includes this one)
 *          1?..?  :   1ablock (z is even) or 1bblock (z is odd) if possible (<n)
 *
 * If last case is not possible (>=n), than smooth moving from n=2^(k-1) to n=2^k is applied.
 * Using "*" bits of a-,b-,c-,d- blocks ui64 value is combined, modulo of which determines
 * if the value should be greather than 2^(k-1) or ConsistentHashing(x, 2^(k-1)) should be used.
 * The last case is optimized according to previous checks.
 */

namespace {
    ui64 PowerOf2(size_t k) {
        return (ui64)0x1 << k;
    }

    template <class TValue>
    TValue SelectAOrBBlock(TValue a, TValue b, TValue cBlock) {
        size_t z = PopCount<unsigned long long>(cBlock);
        bool useABlock = z % 2 == 0;
        return useABlock ? a : b;
    }

    // Gets the exact result for n = k2 = 2 ^ k
    template <class TValue>
    size_t ConsistentHashingForPowersOf2(TValue a, TValue b, TValue c, ui64 k2) {
        TValue cBlock = c & (k2 - 1); // (k, 0] bits of c
        // Zero and one cases
        if (cBlock < 2) {
            // First two cases of result function table: 0 if cblock is 0, 1 if cblock is 1.
            return cBlock;
        }
        size_t y = GetValueBitCount<unsigned long long>(cBlock) - 1; // cblock = 0..01?..? (y = number of digits after 1), y > 0
        ui64 y2 = PowerOf2(y);                                       // y2 = 2^y
        TValue abBlock = SelectAOrBBlock(a, b, cBlock) & (y2 - 1);
        return y2 + abBlock;
    }

    template <class TValue>
    ui64 GetAsteriskBits(TValue a, TValue b, TValue c, TValue d, size_t k) {
        size_t shift = sizeof(TValue) * 8 - k;
        ui64 res = (d << shift) | (c >> k);
        ++shift;
        res <<= shift;
        res |= b >> (k - 1);
        res <<= shift;
        res |= a >> (k - 1);

        return res;
    }

    template <class TValue>
    size_t ConsistentHashingImpl(TValue a, TValue b, TValue c, TValue d, size_t n) {
        Y_ABORT_UNLESS(n > 0, "Can't map consistently to a zero values.");
        // Uninteresting case
        if (n == 1) {
            return 0;
        }
        size_t k = GetValueBitCount(n - 1); // 2^(k-1) < n <= 2^k, k >= 1
        ui64 k2 = PowerOf2(k);              // k2 = 2^k
        size_t largeValue;
        {
            // Bit determined variant. Large scheme.
            largeValue = ConsistentHashingForPowersOf2(a, b, c, k2);
            if (largeValue < n) {
                return largeValue;
            }
        }
        // Since largeValue is not assigned yet
        // Smooth moving from one bit scheme to another
        ui64 k21 = PowerOf2(k - 1);
        {
            size_t s = GetAsteriskBits(a, b, c, d, k) % (largeValue * (largeValue + 1));
            size_t largeValue2 = s / k2 + k21;
            if (largeValue2 < n) {
                return largeValue2;
            }
        }
        // Bit determined variant. Short scheme.
        return ConsistentHashingForPowersOf2(a, b, c, k21); // Do not apply checks. It is always less than k21 = 2^(k-1)
    }

}

size_t ConsistentHashing(ui64 x, size_t n) {
    ui32 lo = Lo32(x);
    ui32 hi = Hi32(x);
    return ConsistentHashingImpl<ui16>(Lo16(lo), Hi16(lo), Lo16(hi), Hi16(hi), n);
}
size_t ConsistentHashing(ui64 lo, ui64 hi, size_t n) {
    return ConsistentHashingImpl<ui32>(Lo32(lo), Hi32(lo), Lo32(hi), Hi32(hi), n);
}
