#ifndef __BIGNUM_C
#define __BIGNUM_C

#include "common.h"
#include "multiply.h"

#if defined(USE_SSE2)
#if defined(HAVE_INTRIN_H)
#include <intrin.h>
#elif defined(HAVE_X86INTRIN_H)
#include <x86intrin.h>
#elif defined(HAVE_EMMINTRIN_H)
#include <xmmintrin.h>
#include <emmintrin.h>
#endif
#endif

static inline unsigned is_odd(uint64_t x)
{
    return 1 == (x & 1);
}

static inline unsigned is_even(uint64_t x)
{
    return !is_odd(x);
}

/**
 * Check if a multi-word integer x is greater than or equal to y.
 *
 * @param x     The first term
 * @param y     The second term
 * @param nw    The number of words that make up x and y
 * @return      1 if x>=y, 0 if x<y
 */
STATIC int ge(const uint64_t *x, const uint64_t *y, size_t nw)
{
    unsigned mask = (unsigned)-1;
    unsigned result = 0;
    size_t i, j;

    i = nw - 1;
    for (j=0; j<nw; j++, i--) {
        unsigned greater, lower;

        greater = x[i] > y[i];
        lower = x[i] < y[i];
        result |= mask & (greater | (lower << 1));
        mask &= (greater ^ lower) - 1;
    }

    return result<2;
}

/*
 * Subtract a multi-word integer b from a.
 *
 * @param out   The location where the multi-word result is stored
 * @param a     Number to subtract from
 * @param b     Number to subtract
 * @param nw    The number of words of both a and b
 * @result      0 if there is no borrow, 1 otherwise
 */
STATIC unsigned sub(uint64_t *out, const uint64_t *a, const uint64_t *b, size_t nw)
{
    size_t i;
    unsigned borrow1 , borrow2;

    borrow2 = 0;
    for (i=0; i<nw; i++) {
        borrow1 = b[i] > a[i];
        out[i] = a[i] - b[i];

        borrow1 |= borrow2 > out[i];
        out[i] -= borrow2;

        borrow2 = borrow1;
    }

    return borrow2;
}

/*
 * Multiply a multi-word integer a by a 64-bit scalar k and
 * then add the result to the multi-word integer t.
 *
 * @param t     The multi-word integer accumulator
 * @param tw    The number of words of t
 * @param a     The multi-word integer to multiply with the scalar
 * @param aw    The number of words of a
 * @param k     The 64-bit scalar multiplier
 */
STATIC void addmul(uint64_t *t, size_t tw, const uint64_t *a, size_t aw, uint64_t k)
{
    size_t i;
    uint64_t carry;

    carry = 0;
    for (i=0; i<aw; i++) {
        uint64_t prod_lo, prod_hi;

        DP_MULT(a[i], k, prod_lo, prod_hi);
    
        prod_lo += carry;
        prod_hi += prod_lo < carry;

        t[i] += prod_lo;
        prod_hi += t[i] < prod_lo;

        carry = prod_hi;
    }

    for (; carry; i++) {
        t[i] += carry;
        carry = t[i] < carry;
    }

    assert(i <= tw);
}

/**
 * Multiply two multi-word integers.
 *
 * @param t          The location where the result is stored. It is twice as big as
 *                   either a (or b). It is an array of  2*nw words).
 * @param scratchpad Temporary area. It is an array of 3*nw words.
 * @param a          The first term, array of nw words.
 * @param b          The second term, array of nw words.
 * @param nw         The number of words of both a and b.
 *
 */
STATIC void product(uint64_t *t, uint64_t *scratchpad, const uint64_t *a, const uint64_t *b, size_t nw)
{
    size_t i;

    memset(t, 0, 2*sizeof(uint64_t)*nw);
    
    for (i=0; i<(nw ^ (nw & 1)); i+=2) {
        addmul128(&t[i], scratchpad, a, b[i], b[i+1], 2*nw-i, nw);
    }

    if (is_odd(nw)) {
        addmul(&t[nw-1], nw+2, a, nw, b[nw-1]);
    }
}

/*
 * Select a number out of two, in constant time.
 *
 * @param out   The location where the multi-word result is stored
 * @param a     The first choice, selected if cond is true (non-zero)
 * @param b     The second choice, selected if cond is false (zero)
 * @param cond  The flag that drives the selection
 * @param words The number of words of a, b, and out
 * @return      0 for success, the appropriate code otherwise.
 */
STATIC int mod_select(uint64_t *out, const uint64_t *a, const uint64_t *b, unsigned cond, size_t words)
{
    uint64_t mask;
#if defined(USE_SSE2)
    unsigned pairs, i;
    __m128i r0, r1, r2, r3, r4, r5;

    pairs = (unsigned)words / 2;
    mask = (uint64_t)((cond != 0) - 1); /* 0 for a, 1s for b */

#if SYS_BITS == 64
    r0 = _mm_set1_epi64x((int64_t)mask);
#else
    r0 = _mm_loadl_epi64((__m128i*)&mask);
    r0 = _mm_unpacklo_epi64(r0, r0);
#endif
    for (i=0; i<pairs; i++, a+=2, b+=2, out+=2) {
        r1 = _mm_loadu_si128((__m128i const*)b);
        r2 = _mm_loadu_si128((__m128i const*)a);
        r3 = _mm_and_si128(r0, r1);
        r4 = _mm_andnot_si128(r0, r2);
        r5 = _mm_or_si128(r3, r4);
        _mm_storeu_si128((__m128i*)out, r5);
    }

    if (words & 1) {
        *out = (*b & mask) ^ (*a & ~mask);
    }
#else
    unsigned i;

    mask = (uint64_t)((cond != 0) - 1);
    for (i=0; i<words; i++) {
        *out++ = (*b++ & mask) ^ (*a++ & ~mask);
    }
#endif

    return 0;
}

/*
 * Add two multi-word numbers with modulo arithmetic.
 *
 * @param out       The locaton where the multi-word result (nw words) is stored
 * @param a         The first term (nw words)
 * @param b         The second term (nw words)
 * @param modulus   The modulus (nw words)
 * @param tmp1      A temporary area (nw words)
 * @param tmp2      A temporary area (nw words)
 * @param nw        The number of 64-bit words in all parameters
 */
STATIC void add_mod(uint64_t* out, const uint64_t* a, const uint64_t* b, const uint64_t *modulus, uint64_t *tmp1, uint64_t *tmp2, size_t nw)
{
    unsigned i;
    unsigned carry, borrow1, borrow2;

    /*
     * Compute sum in tmp1[], and subtract modulus[]
     * from tmp1[] into tmp2[].
     */
    borrow2 = 0;
    for (i=0, carry=0; i<nw; i++) {
        tmp1[i] = a[i] + carry;
        carry = tmp1[i] < carry;
        tmp1[i] += b[i];
        carry += tmp1[i] < b[i];

        borrow1 = modulus[i] > tmp1[i];
        tmp2[i] = tmp1[i] - modulus[i];
        borrow1 |= borrow2 > tmp2[i];
        tmp2[i] -= borrow2;
        borrow2 = borrow1;
    }

    /*
     * If there is no borrow or if there is carry,
     * tmp1[] is larger than modulus, so we must return tmp2[].
     */
    mod_select(out, tmp2, tmp1, carry | (borrow2 ^ 1), nw);
}

/*
 * Subtract two multi-word numbers with modulo arithmetic.
 *
 * @param out       The location where the multi-word result (nw words) is stored
 * @param a         The number it will be subtracted from (nw words)
 * @param b         The number to subtract (nw wordS)
 * @param modulus   The modulus (nw words)
 * @param tmp1      A temporary area (nw words)
 * @param tmp2      A temporary area (nw words)
 * @param nw        The number of 64-bit words in all parameters
 * @return          0 for success, the relevant error code otherwise
 */
STATIC int sub_mod(uint64_t *out, const uint64_t *a, const uint64_t *b, const uint64_t *modulus, uint64_t *tmp1, uint64_t *tmp2, size_t nw)
{
    unsigned i;
    unsigned carry, borrow1 , borrow2;

    /*
     * Compute difference in tmp1[], and add modulus[]
     * to tmp1[] into tmp2[].
     */
    borrow2 = 0;
    carry = 0;
    for (i=0; i<nw; i++) {
        borrow1 = b[i] > a[i];
        tmp1[i] = a[i] - b[i];
        borrow1 |= borrow2 > tmp1[i];
        tmp1[i] -= borrow2;
        borrow2 = borrow1;

        tmp2[i] = tmp1[i] + carry;
        carry = tmp2[i] < carry;
        tmp2[i] += modulus[i];
        carry += tmp2[i] < modulus[i];
    }

    /*
     * If there is no borrow, tmp[] is smaller than modulus.
     */
    mod_select(out, tmp2, tmp1, borrow2, nw);

    return 0;
}

#endif  /* __BIGNUM_C **/
