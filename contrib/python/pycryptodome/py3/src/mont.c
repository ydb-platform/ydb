/* ===================================================================
 *
 * Copyright (c) 2018, Helder Eijs <helderijs@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * ===================================================================
 */

#include <assert.h>

#include "common.h"
#include "endianess.h"
#include "multiply.h"
#include "mont.h"

#if SYS_BITS == 32
#include "multiply_32.c"
#else
#if SYS_BITS == 64
#include "multiply_64.c"
#else
#error You must define the macro SYS_BITS
#endif
#endif

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

#include "modexp_utils.h"
#include "bignum.c"

void mont_printf(const char *prefix, const uint64_t *mont_number, const MontContext *ctx)
{
    uint8_t* number;
    unsigned i;

    number = (uint8_t*)calloc(1, ctx->modulus_len);

    if (NULL == mont_number || NULL == ctx || NULL == number)
        return;

    mont_to_bytes(number, ctx->modulus_len, mont_number, ctx);
    printf("%s", prefix);
    for (i=0; i<ctx->modulus_len; i++) {
        printf("%02X", number[i]);
    }
    printf("\n");
    free(number);
}

/**
 * Compute the inverse modulo 2⁶⁴ of a 64-bit odd integer.
 *
 * See https://crypto.stackexchange.com/questions/47493/how-to-determine-the-multiplicative-inverse-modulo-64-or-other-power-of-two
 */
STATIC uint64_t inverse64(uint64_t a)
{
    uint64_t x;

    assert(1 & a);
    x = ((a << 1 ^ a) & 4) << 1 ^ a;
    x += x - a*x*x;
    x += x - a*x*x;
    x += x - a*x*x;
    x += x - a*x*x;
    assert((x*a & 0xFFFFFFFFFFFFFFFFULL) == 1);

    return x;
}

/*
 * Compute R² mod N, where R is the smallest power of 2⁶⁴ larger than N.
 *
 * @param r2_mod_n  The location where the result is stored at
 * @param n         The modulus N
 * @param nw        The number of 64-bit words of both r2_mod_n and n
 */
STATIC void rsquare(uint64_t *r2_mod_n, uint64_t *n, size_t nw)
{
    size_t i;
    size_t R_bits;

    memset(r2_mod_n, 0, sizeof(uint64_t)*nw);

    /**
     * Start with R2=1, double 2*bitlen(R) times,
     * and reduce it as soon as it exceeds n
     */
    r2_mod_n[0] = 1;
    R_bits = nw * sizeof(uint64_t) * 8;
    for (i=0; i<R_bits*2; i++) {
        unsigned overflow;
        size_t j;
        
        /** Double, by shifting left by one bit **/
        overflow = (unsigned)(r2_mod_n[nw-1] >> 63);
        for (j=nw-1; j>0; j--) {
            r2_mod_n[j] = (r2_mod_n[j] << 1) + (r2_mod_n[j-1] >> 63);
        }
        /** Fill-in with zeroes **/
        r2_mod_n[0] <<= 1;
        
        /** Subtract n if the result exceeds it **/
        while (overflow || ge(r2_mod_n, n, nw)) {
            sub(r2_mod_n, r2_mod_n, n, nw);
            overflow = 0;
        }
    }
}

/*
 * Montgomery modular multiplication, that is a*b*R mod N.
 *
 * @param out   The location where the result is stored
 * @param a     The first term (already in Montgomery form, a*R mod N)
 * @param b     The second term (already in Montgomery form, b*R mod N)
 * @param n     The modulus (in normal form), such that R>N
 * @param m0    Least-significant word of the opposite of the inverse of n modulo R, that is, -n[0]⁻¹ mod R
 * @param t     Temporary, internal result; it must have been created with mont_new_number(&p,SCRATCHPAD_NR,ctx).
 * @param nw    Number of words making up the 3 integers: out, a, and b.
 *              It also defines R as 2^(64*nw).
 *
 * Useful read:
 * https://web.archive.org/web/20190917203334/https://alicebob.cryptoland.net/understanding-the-montgomery-reduction-algorithm/
 */
#if SCRATCHPAD_NR < 7
#error Scratchpad is too small
#endif
STATIC void mont_mult_generic(uint64_t *out, const uint64_t *a, const uint64_t *b, const uint64_t *n, uint64_t m0, uint64_t *tmp, size_t nw)
{
    size_t i;
    uint64_t *t, *scratchpad, *t2;
    unsigned cond;

    /*
     * tmp is an array of SCRATCHPAD*nw words
     * We carve out 3 values in it:
     * - 3*nw words, the value a*b + m*n (we only use 2*nw+1 words)
     * - 3*nw words, temporary area for computing the product
     * - nw words, the reduced value with a final subtraction by n
     */
    t = tmp;
    scratchpad = tmp + 3*nw;
    t2 = scratchpad + 3*nw;

    if (a == b) {
        square(t, scratchpad, a, nw);
    } else {
        product(t, scratchpad, a, b, nw);
    }

    t[2*nw] = 0; /** MSW **/

    /** Clear lower words (two at a time) **/
    for (i=0; i<(nw ^ (nw & 1)); i+=2) {
        uint64_t k0, k1, ti1, prod_lo, prod_hi;

        /** Multiplier for n that will make t[i+0] go 0 **/
        k0 = t[i] * m0;
        
        /** Simulate Muladd for digit 0 **/
        DP_MULT(k0, n[0], prod_lo, prod_hi);
        prod_lo += t[i];
        prod_hi += prod_lo < t[i];

        /** Expected digit 1 **/
        ti1 = t[i+1] + n[1]*k0 + prod_hi;
        
        /** Multiplier for n that will make t[i+1] go 0 **/
        k1 = ti1 * m0;
        
        addmul128(&t[i], scratchpad, n, k0, k1, 2*nw+1-i, nw);
    }

    /** One left for odd number of words **/
    if (is_odd(nw)) {
        addmul(&t[nw-1], nw+2, n, nw, t[nw-1]*m0);
    }
    
    assert(t[2*nw] <= 1); /** MSW **/

    /** t[0..nw-1] == 0 **/
    
    /** Divide by R and possibly subtract n **/
    sub(t2, &t[nw], n, nw);
    cond = (unsigned)(t[2*nw] | (uint64_t)ge(&t[nw], n, nw));
    mod_select(out, t2, &t[nw], cond, (unsigned)nw);
}

STATIC void mont_mult_p256(uint64_t *out, const uint64_t *a, const uint64_t *b, const uint64_t *n, uint64_t m0, uint64_t *tmp, size_t nw)
{
    unsigned i;
    uint64_t *t, *scratchpad, *t2;
    unsigned cond;
#define WORDS_64        4U
#define PREDIV_WORDS_64 (2*WORDS_64+1)      /** Size of the number to divide by R **/
#define WORDS_32        (WORDS_64*2)
#define PREDIV_WORDS_32 (2*PREDIV_WORDS_64)

#if SYS_BITS == 32
    uint32_t t32[18];
#endif

    assert(nw == 4);
    assert(m0 == 1);

    t = tmp;
    scratchpad = tmp + 3*nw;
    t2 = scratchpad + 3*nw;

    if (a == b) {
        square(t, scratchpad, a, WORDS_64);
    } else {
        product(t, scratchpad, a, b, WORDS_64);
    }

    t[PREDIV_WORDS_64-1] = 0; /** MSW **/

#if SYS_BITS == 32
    for (i=0; i<PREDIV_WORDS_64; i++) {
        t32[2*i] = (uint32_t)t[i];
        t32[2*i+1] = (uint32_t)(t[i] >> 32);
    }

    for (i=0; i<WORDS_32; i++) {
        uint32_t k, carry;
        uint64_t prod, k2;
        unsigned j;

        k = t32[i];
        k2 = ((uint64_t)k<<32) - k;

        /* p[0] = 2³²-1 */
        prod = k2 + t32[i+0];
        t32[i+0] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* p[1] = 2³²-1 */
        prod = k2 + t32[i+1] + carry;
        t32[i+1] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* p[2] = 2³²-1 */
        prod = k2 + t32[i+2] + carry;
        t32[i+2] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* p[3] = 0 */
        t32[i+3] += carry;
        carry = t32[i+3] < carry;
        /* p[4] = 0 */
        t32[i+4] += carry;
        carry = t32[i+4] < carry;
        /* p[5] = 0 */
        t32[i+5] += carry;
        carry = t32[i+5] < carry;
        /* p[6] = 1 */
        t32[i+6] += carry;
        carry = t32[i+6] < carry;
        t32[i+6] += k;
        carry |= t32[i+6] < k;
        /* p[7] = 2³²-1 */
        prod = k2 + t32[i+7] + carry;
        t32[i+7] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);

        for (j=WORDS_32; carry; j++) {
            t32[i+j] += carry;
            carry = t32[i+j] < carry;
        }
    }

    for (i=0; i<PREDIV_WORDS_64; i++) {
        t[i] = ((uint64_t)t32[2*i+1]<<32) + t32[2*i];
    }

#elif SYS_BITS == 64

    for (i=0; i<WORDS_64; i++) {
        unsigned j;
        uint64_t carry, k;
        uint64_t prod_lo, prod_hi;

        k = t[i];

        /* n[0] = 2⁶⁴ - 1 */
        prod_lo = (uint64_t)(0 - k);
        prod_hi = k - (k!=0);
        t[i+0] += prod_lo;
        prod_hi += t[i+0] < prod_lo;
        carry = prod_hi;

        /* n[1] = 2³² - 1 */
        DP_MULT(n[1], k, prod_lo, prod_hi);
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+1] += prod_lo;
        prod_hi += t[i+1] < prod_lo;
        carry = prod_hi;

        /* n[2] = 0 */
        t[i+2] += carry;
        carry = t[i+2] < carry;

        /* n[3] = 2⁶⁴ - 2³² + 1 */
        DP_MULT(n[3], k, prod_lo, prod_hi);
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+3] += prod_lo;
        prod_hi += t[i+3] < prod_lo;
        carry = prod_hi;

        for (j=WORDS_64; carry; j++) {
            t[i+j] += carry;
            carry = t[i+j] < carry;
        }
    }
#else
#error You must define the SYS_BITS macro
#endif

    assert(t[PREDIV_WORDS_64-1] <= 1); /** MSW **/

    /** t[0..nw-1] == 0 **/

    /** Divide by R and possibly subtract n **/
    sub(t2, &t[nw], n, WORDS_64);
    cond = (unsigned)(t[PREDIV_WORDS_64-1] | (uint64_t)ge(&t[WORDS_64], n, WORDS_64));
    mod_select(out, t2, &t[WORDS_64], cond, WORDS_64);

#undef WORDS_64
#undef PREDIV_WORDS_64
#undef WORDS_32
#undef PREDIV_WORDS_32
}

STATIC void mont_mult_p384(uint64_t *out, const uint64_t *a, const uint64_t *b, const uint64_t *n, uint64_t m0, uint64_t *tmp, size_t nw)
{
    size_t i;
    uint64_t *t, *scratchpad, *t2;
    unsigned cond;
#define WORDS_64        6U
#define PREDIV_WORDS_64 (2*WORDS_64+1)      /** Size of the number to divide by R **/
#define WORDS_32        (WORDS_64*2)
#define PREDIV_WORDS_32 (2*PREDIV_WORDS_64)

#if SYS_BITS == 32
    uint32_t t32[PREDIV_WORDS_32];
#endif

    assert(nw == WORDS_64);
    assert(m0 == 0x0000000100000001ULL);

    t = tmp;
    scratchpad = tmp + 3*nw;
    t2 = scratchpad + 3*nw;

    if (a == b) {
        square(t, scratchpad, a, WORDS_64);
    } else {
        product(t, scratchpad, a, b, WORDS_64);
    }

    t[PREDIV_WORDS_64-1] = 0; /** MSW **/

#if SYS_BITS == 32
    for (i=0; i<PREDIV_WORDS_64; i++) {
        t32[2*i] = (uint32_t)t[i];
        t32[2*i+1] = (uint32_t)(t[i] >> 32);
    }

    for (i=0; i<WORDS_32; i++) {
        uint32_t k, carry;
        uint64_t prod, k2, k3;
        unsigned j;

        k = t32[i];
        k2 = ((uint64_t)k<<32) - k;
        k3 = k2 - k;

        /* n32[0] = 2³² - 1 */
        prod = k2 + t32[i+0];
        t32[i+0] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[1] = 0 */
        prod = (uint64_t)t32[i+1] + carry;
        t32[i+1] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[2] = 0 */
        prod = (uint64_t)t32[i+2] + carry;
        t32[i+2] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[3] = 2³² - 1 */
        prod = k2 + t32[i+3] + carry;
        t32[i+3] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[4] = 2³² - 2 */
        prod = k3 + t32[i+4] + carry;
        t32[i+4] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[5] = 2³² - 1 */
        prod = k2 + t32[i+5] + carry;
        t32[i+5] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[6] = 2³² - 1 */
        prod = k2 + t32[i+6] + carry;
        t32[i+6] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[7] = 2³² - 1 */
        prod = k2 + t32[i+7] + carry;
        t32[i+7] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[8] = 2³² - 1 */
        prod = k2 + t32[i+8] + carry;
        t32[i+8] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[9] = 2³² - 1 */
        prod = k2 + t32[i+9] + carry;
        t32[i+9] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[10] = 2³² - 1 */
        prod = k2 + t32[i+10] + carry;
        t32[i+10] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);
        /* n32[11] = 2³² - 1 */
        prod = k2 + t32[i+11] + carry;
        t32[i+11] = (uint32_t)prod;
        carry = (uint32_t)(prod >> 32);

        for (j=WORDS_32; carry; j++) {
            t32[i+j] += carry;
            carry = t32[i+j] < carry;
        }
    }

    for (i=0; i<PREDIV_WORDS_64; i++) {
        t[i] = ((uint64_t)t32[2*i+1]<<32) + t32[2*i];
    }

#elif SYS_BITS == 64

    for (i=0; i<WORDS_64; i++) {
        unsigned j;
        uint64_t carry;
        uint64_t k, k2_lo, k2_hi;
        uint64_t prod_lo, prod_hi;

        k = t[i] + (t[i] << 32);
        k2_lo = (uint64_t)(0 - k);
        k2_hi = k - (k!=0);

        /* n[0] = 2³² - 1 */
        DP_MULT(n[0], k, prod_lo, prod_hi);
        t[i+0] += prod_lo;
        prod_hi += t[i+0] < prod_lo;
        carry = prod_hi;
        /* n[1] = 2⁶⁴ - 2³² */
        DP_MULT(n[1], k, prod_lo, prod_hi);
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+1] += prod_lo;
        prod_hi += t[i+1] < prod_lo;
        carry = prod_hi;
        /* n[2] = 2⁶⁴ - 2 */
        DP_MULT(n[2], k, prod_lo, prod_hi);
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+2] += prod_lo;
        prod_hi += t[i+2] < prod_lo;
        carry = prod_hi;
        /* n[3] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+3] += prod_lo;
        prod_hi += t[i+3] < prod_lo;
        carry = prod_hi;
        /* n[4] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+4] += prod_lo;
        prod_hi += t[i+4] < prod_lo;
        carry = prod_hi;
        /* n[5] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+5] += prod_lo;
        prod_hi += t[i+5] < prod_lo;
        carry = prod_hi;

        for (j=WORDS_64; carry; j++) {
            t[i+j] += carry;
            carry = t[i+j] < carry;
        }
    }
#else
#error You must define the SYS_BITS macro
#endif

    assert(t[PREDIV_WORDS_64-1] <= 1); /** MSW **/

    /** Words t[0..WORDS_64-1] have all been set to zero **/

    /** Divide by R and possibly subtract n **/
    sub(t2, &t[WORDS_64], n, WORDS_64);
    cond = (unsigned)(t[PREDIV_WORDS_64-1] | (uint64_t)ge(&t[WORDS_64], n, WORDS_64));
    mod_select(out, t2, &t[WORDS_64], cond, WORDS_64);

#undef WORDS_64
#undef PREDIV_WORDS_64
#undef WORDS_32
#undef PREDIV_WORDS_32
}

STATIC void mont_mult_p521(uint64_t *out, const uint64_t *a, const uint64_t *b, const uint64_t *n, uint64_t m0, uint64_t *tmp, size_t nw)
{
    uint64_t *t, *scratchpad, *s, *tmp1, *tmp2;

    assert(nw == 9);
    assert(m0 == 1);

    /*
     * A number in the form:
     *      x*2⁵²¹ + y
     * is congruent modulo 2⁵²¹-1 to:
     *      x + y
     */

    /*
     * tmp is an array of SCRATCHPAD*nw words
     * We carve out 3 values in it:
     * - 2*nw words, the value a*b
     * - 3*nw words, temporary area for computing the product
     * - nw words, the second term of the addition
     */

    t = tmp;
    scratchpad = t + 2*nw;
    s = scratchpad + 3*nw;
    tmp1 = scratchpad;
    tmp2 = scratchpad + nw;

    if (a == b) {
        square(t, scratchpad, a, 9);
    } else {
        product(t, scratchpad, a, b, 9);
    }

    /* t is a 1042-bit number, occupying 17 words (of the total 18); the MSW (t[16]) only has 18 bits */
    s[0] = (t[8] >> 9)  | (t[9] << 55);     t[8] &= 0x1FF;
    s[1] = (t[9] >> 9)  | (t[10] << 55);
    s[2] = (t[10] >> 9) | (t[11] << 55);
    s[3] = (t[11] >> 9) | (t[12] << 55);
    s[4] = (t[12] >> 9) | (t[13] << 55);
    s[5] = (t[13] >> 9) | (t[14] << 55);
    s[6] = (t[14] >> 9) | (t[15] << 55);
    s[7] = (t[15] >> 9) | (t[16] << 55);
    s[8] = t[16] >> 9;

    add_mod(out, t, s, n, tmp1, tmp2, nw);
}

STATIC void mont_mult_ed448(uint64_t *out, const uint64_t *a, const uint64_t *b, const uint64_t *n, uint64_t m0, uint64_t *tmp, size_t nw)
{
    size_t i;
    uint64_t *t, *scratchpad, *t2;
    unsigned cond;

    assert(nw == 7);
    assert(m0 == 1);

    /*
     * tmp is an array of SCRATCHPAD*nw words
     * We carve out 3 values in it:
     * - 3*nw words, the value a*b + m*n (we only use 2*nw+1 words)
     * - 3*nw words, temporary area for computing the product
     * - nw words, the reduced value with a final subtraction by n
     */
    t = tmp;
    scratchpad = tmp + 3*nw;
    t2 = scratchpad + 3*nw;

    if (a == b) {
        square(t, scratchpad, a, nw);
    } else {
        product(t, scratchpad, a, b, nw);
    }

    t[2*nw] = 0; /** MSW **/

    /** Clear lower words **/
    for (i=0; i<7; i++) {
        uint64_t k, k2_lo, k2_hi;
        uint64_t carry, j;
        uint64_t prod_lo, prod_hi;

        k = t[i];
        k2_lo = (uint64_t)(0 - k);
        k2_hi = k - (k!=0);

        /* n[0] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        t[i+0] += prod_lo;
        prod_hi += t[i+0] < prod_lo;
        carry = prod_hi;

        /* n[1] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+1] += prod_lo;
        prod_hi += t[i+1] < prod_lo;
        carry = prod_hi;

        /* n[2] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+2] += prod_lo;
        prod_hi += t[i+2] < prod_lo;
        carry = prod_hi;

        /* n[3] = 2⁶⁴ - 2³² - 1 */
        DP_MULT(n[3], k, prod_lo, prod_hi);
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+3] += prod_lo;
        prod_hi += t[i+3] < prod_lo;
        carry = prod_hi;

        /* n[4] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+4] += prod_lo;
        prod_hi += t[i+4] < prod_lo;
        carry = prod_hi;

        /* n[5] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+5] += prod_lo;
        prod_hi += t[i+5] < prod_lo;
        carry = prod_hi;

        /* n[6] = 2⁶⁴ - 1 */
        prod_lo = k2_lo;
        prod_hi = k2_hi;
        prod_lo += carry;
        prod_hi += prod_lo < carry;
        t[i+6] += prod_lo;
        prod_hi += t[i+6] < prod_lo;
        carry = prod_hi;

        for (j=7; carry; j++) {
            t[i+j] += carry;
            carry = t[i+j] < carry;
        }

        assert(j <= (15-i));
    }

    assert(t[2*nw] <= 1); /** MSW **/

    /** t[0..nw-1] == 0 **/

    /** Divide by R and possibly subtract n **/
    sub(t2, &t[nw], n, nw);
    cond = (unsigned)(t[2*nw] | (uint64_t)ge(&t[nw], n, nw));
    mod_select(out, t2, &t[nw], cond, (unsigned)nw);
}


/* ---- PUBLIC FUNCTIONS ---- */

void mont_context_free(MontContext *ctx)
{
    if (NULL == ctx)
        return;
    free(ctx->one);
    free(ctx->r2_mod_n);
    free(ctx->r_mod_n);
    free(ctx->modulus);
    free(ctx->modulus_min_2);
    free(ctx);
}

/*
 * Return how many bytes a big endian multi-word number takes in memory.
 */
size_t mont_bytes(const MontContext *ctx)
{
    if (NULL == ctx)
        return 0;
    return ctx->bytes;
}

/*
 * Allocate memory for an array of numbers in Montgomery form
 * and initialize it to 0.
 *
 * @param out   The location where the address of the newly allocated
 *              array will be placed in.
 *              The caller is responsible for deallocating the memory
 *              using free().
 * @param count How many numbers the array contains.
 * @param ctx   The Montgomery context.
 * @return      0 if successful, the relevant error code otherwise.
 *
 */
int mont_new_number(uint64_t **out, unsigned count, const MontContext *ctx)
{
    if (NULL == out || NULL == ctx)
        return ERR_NULL;

    *out = (uint64_t*)calloc(count * ctx->words, sizeof(uint64_t));
    if (NULL == *out)
        return ERR_MEMORY;

    return 0;
}

int mont_new_from_uint64(uint64_t **out, uint64_t x, const MontContext *ctx)
{
    int res;

    res = mont_new_number(out, 1, ctx);
    if (res) return res;

    return mont_set(*out, x, ctx);
}

int mont_new_random_number(uint64_t **out, unsigned count, uint64_t seed, const MontContext *ctx)
{
    int res;
    unsigned i;
    uint64_t *number;

    res = mont_new_number(out, count, ctx);
    if (res)
        return res;

    number = *out;
    expand_seed(seed, (uint8_t*)number, count * ctx->bytes);
    for (i=0; i<count; i++, number += ctx->words) {
        number[ctx->words-1] = 0;
    }
    return 0;
}

/*
 * Transform a big endian-encoded number into Montgomery form, by performing memory allocation.
 *
 * @param out       The location where the pointer to the newly allocated memory will be put in.
 *                  The memory will contain the number encoded in Montgomery form.
 *                  The caller is responsible for deallocating the memory.
 * @param ctx       Montgomery context, as created by mont_context_init().
 * @param number    The big endian-encoded number to transform.
 * @param len       The length of the big-endian number in bytes (this may be
 *                  smaller than the output of mont_bytes(ctx)).
 * @return          0 in case of success, the relevant error code otherwise.
 */
int mont_new_from_bytes(uint64_t **out, const uint8_t *number, size_t len, const MontContext *ctx)
{
    uint64_t *encoded = NULL;
    uint64_t *tmp1 = NULL;
    uint64_t *scratchpad = NULL;
    int res = 0;

    if (NULL == out || NULL == ctx || NULL == number)
        return ERR_NULL;

    *out = NULL;

    /** Removing leading zeroes but avoid a zero-length string **/
    if (0 == len)
        return ERR_NOT_ENOUGH_DATA;
    while (len>1 && *number==0) {
        len--;
        number++;
    }

    if (ctx->bytes < len)
        return ERR_VALUE;

    /** The caller will deallocate this memory **/
    *out = encoded = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == encoded)
        return ERR_MEMORY;

    /** Input number, loaded in words **/
    tmp1 = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == tmp1) {
        res = ERR_MEMORY;
        goto cleanup;
    }
    bytes_to_words(tmp1, ctx->words, number, len);

    /** Scratchpad **/
    scratchpad = (uint64_t*)calloc(SCRATCHPAD_NR, ctx->words*sizeof(uint64_t));
    if (NULL == scratchpad) {
        res = ERR_MEMORY;
        goto cleanup;
    }

    if (ctx->modulus_type != ModulusP521) {
        mont_mult_generic(encoded, tmp1, ctx->r2_mod_n, ctx->modulus, ctx->m0, scratchpad, ctx->words);
    } else {
        while (ge(tmp1, ctx->modulus, ctx->words)) {
            res = (int)sub(tmp1, tmp1, ctx->modulus, ctx->words);
            if (res) goto cleanup;
        }
        res = mont_copy(encoded, tmp1, ctx);
        if (res) goto cleanup;
    }

    res = 0;

cleanup:
    free(scratchpad);
    free(tmp1);
    if (res != 0) {
        free(encoded);
        *out = NULL;
    }
    return res;
}

/*
 * Transform a number from Montgomery representation to big endian-encoding.
 *
 * @param number        The location where the number will be put in, encoded
 *                      in big-endian form and with zero padding on the left.
 * @param len           Space allocate at number, at least ctx->modulus_len bytes.
 * @param ctx           The address of the Montgomery context.
 * @param mont_number   The number in Montgomery form to transform.
 * @return              0 if successful, the relevant error code otherwise.
 */
int mont_to_bytes(uint8_t *number, size_t len, const uint64_t* mont_number, const MontContext *ctx)
{
    uint64_t *tmp1 = NULL;
    uint64_t *scratchpad = NULL;
    int res;

    if (NULL == number || NULL == ctx || NULL == mont_number)
        return ERR_NULL;

    if (len < ctx->modulus_len)
        return ERR_NOT_ENOUGH_DATA;

    /** Number in normal form, but still in words **/
    tmp1 = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == tmp1)
        return ERR_MEMORY;

    /** Scratchpad **/
    scratchpad = (uint64_t*)calloc(SCRATCHPAD_NR, ctx->words*sizeof(uint64_t));
    if (NULL == scratchpad) {
        free(tmp1);
        return ERR_MEMORY;
    }

    if (ctx->modulus_type != ModulusP521)
        mont_mult_generic(tmp1, mont_number, ctx->one, ctx->modulus, ctx->m0, scratchpad, ctx->words);
    else
        mont_copy(tmp1, mont_number, ctx);
    res = words_to_bytes(number, len, tmp1, ctx->words);

    free(scratchpad);
    free(tmp1);
    return res;
}

/*
 * Add two numbers in Montgomery representation.
 *
 * @param out   The location where the result will be stored; it must have been created with mont_new_number(&p,1,ctx).
 * @param a     The first term.
 * @param b     The second term.
 * @param tmp   Temporary, internal result; it must have been created with mont_new_number(&p,SCRATCHPAD_NR,ctx).
 * @param ctx   The Montgomery context.
 * @return      0 for success, the relevant error code otherwise.
 */
int mont_add(uint64_t* out, const uint64_t* a, const uint64_t* b, uint64_t *tmp, const MontContext *ctx)
{
    if (NULL == out || NULL == a || NULL == b || NULL == tmp || NULL == ctx)
        return ERR_NULL;
    add_mod(out, a, b, ctx->modulus, tmp, tmp + ctx->words, ctx->words);
    return 0;
}

/*
 * Multiply two numbers in Montgomery representation.
 *
 * @param out   The location where the result will be stored at; it must have been created with mont_new_number(&p,1,ctx)
 * @param a     The first term.
 * @param b     The second term.
 * @param tmp   Temporary, internal result; it must have been created with mont_new_number(&p,SCRATCHPAD_NR,ctx).
 * @param ctx   The Montgomery context.
 * @return      0 for success, the relevant error code otherwise.
 */
int mont_mult(uint64_t* out, const uint64_t* a, const uint64_t *b, uint64_t *tmp, const MontContext *ctx)
{
    if (NULL == out || NULL == a || NULL == b || NULL == tmp || NULL == ctx)
        return ERR_NULL;

    switch (ctx->modulus_type) {
        case ModulusP256:
            mont_mult_p256(out, a, b, ctx->modulus, ctx->m0, tmp, ctx->words);
            break;
        case ModulusP384:
            mont_mult_p384(out, a, b, ctx->modulus, ctx->m0, tmp, ctx->words);
            break;
        case ModulusP521:
            mont_mult_p521(out, a, b, ctx->modulus, ctx->m0, tmp, ctx->words);
            break;
        case ModulusEd448:
            mont_mult_ed448(out, a, b, ctx->modulus, ctx->m0, tmp, ctx->words);
            break;
        case ModulusGeneric:
            mont_mult_generic(out, a, b, ctx->modulus, ctx->m0, tmp, ctx->words);
            break;
        default:
            return ERR_MODULUS;
    }

    return 0;
}

/*
 * Subtract integer b from a.
 *
 * @param out   The location where the result is stored at; it must have been created with mont_new_number(&p,1,ctx).
 *              It can be the same as either a or b.
 * @param a     The number it will be subtracted from.
 * @param b     The number to subtract.
 * @param tmp   Temporary, internal result; it must have been created with mont_new_number(&p,2,ctx).
 * @param ctx   The Montgomery context.
 * @return      0 for success, the relevant error code otherwise.
 */
int mont_sub(uint64_t *out, const uint64_t *a, const uint64_t *b, uint64_t *tmp, const MontContext *ctx)
{
    if (NULL == out || NULL == a || NULL == b || NULL == tmp || NULL == ctx)
        return ERR_NULL;

    return sub_mod(out, a, b, ctx->modulus, tmp, tmp + ctx->words, ctx->words);
}

STATIC void curve448_invert(uint64_t *z,
                            uint64_t *t0,
                            uint64_t *t1,
                            const uint64_t *x,
                            uint64_t *scratch,
                            const MontContext *ctx)
{
    #define DOUBLE(out, in)         mont_mult(out, in, in, scratch, ctx)
    #define SHIFTL(out, in, times)  do { unsigned i; mont_mult(out, in, in, scratch, ctx); \
                                         for (i=0; i<times-1; i++) \
                                            mont_mult(out, out, out, scratch, ctx); \
                                    } while (0)
    #define ADD(out, in1, in2)      mont_mult(out, in1, in2, scratch, ctx)

    /** Generated with addchain (https://github.com/mmcloughlin/addchain) **/
    DOUBLE(z, x);
    ADD(z, x, z);
    DOUBLE(z, z);
    ADD(z, x, z);
    SHIFTL(t0, z, 3);
    ADD(z, z, t0);
    SHIFTL(t0, z, 6);
    ADD(t0, z, t0);
    SHIFTL(t1, t0, 12);
    ADD(t0, t0, t1);
    SHIFTL(t1, t0, 6);
    ADD(z, z, t1);
    SHIFTL(t1, t1, 18);
    ADD(t0, t0, t1);
    SHIFTL(t1, t0, 48);
    ADD(t0, t0, t1);
    SHIFTL(t1, t0, 96);
    ADD(t0, t0, t1);
    SHIFTL(t0, t0, 30);
    ADD(z, z, t0);
    DOUBLE(t0, z);
    ADD(t0, x, t0);
    SHIFTL(t0, t0, 223);
    ADD(z, z, t0);
    SHIFTL(z, z, 2);
    ADD(z, x, z);

    #undef DOUBLE
    #undef SHIFTL
    #undef ADD
}

void mont_inv_prime_generic(uint64_t *out,
                           uint64_t *tmp1,
                           const uint64_t *a,
                           uint64_t *scratchpad,
                           const MontContext *ctx)
{
    unsigned idx_word;
    uint64_t bit;
    uint64_t *exponent = NULL;

    /** Exponent is guaranteed to be >0 **/
    exponent = ctx->modulus_min_2;

    /* Find most significant bit */
    idx_word = ctx->words-1;
    for (;;) {
        if (exponent[idx_word] != 0)
            break;
        if (idx_word-- == 0)
            break;
    }
    for (bit = (uint64_t)1U << 63; 0 == (exponent[idx_word] & bit); bit>>=1);

    /* Start from 1 (in Montgomery form, which is R mod N) */
    memcpy(out, ctx->r_mod_n, ctx->bytes);

    /** Left-to-right exponentiation **/
    for (;;) {
        while (bit > 0) {
            mont_mult(tmp1, out, out, scratchpad, ctx);
            if (exponent[idx_word] & bit) {
                mont_mult(out, tmp1, a, scratchpad, ctx);
            } else {
                memcpy(out, tmp1, ctx->bytes);
            }
            bit >>= 1;
        }
        if (idx_word-- == 0)
            break;
        bit = (uint64_t)1 << 63;
    }
}

/*
 * Compute the modular inverse of an integer in Montgomery form.
 *
 * Condition: the modulus defining the Montgomery context MUST BE a non-secret prime number.
 *
 * @param out   The location where the result will be stored at; it must have
 *              been allocated with mont_new_number(&p, 1, ctx).
 * @param a     The number to compute the modular inverse of, already in Montgomery form.
 * @param ctx   The Montgomery context.
 * @return      0 for success, the relevant error code otherwise.
 */
int mont_inv_prime(uint64_t *out, uint64_t *a, const MontContext *ctx)
{
    uint64_t *tmp1 = NULL;
    uint64_t *tmp2 = NULL;
    uint64_t *scratchpad = NULL;
    int res;

    if (NULL == out || NULL == a || NULL == ctx)
        return ERR_NULL;

    tmp1 = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == tmp1)
        return ERR_MEMORY;

    tmp2 = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == tmp2) {
        res = ERR_MEMORY;
        goto cleanup;
    }

    scratchpad = (uint64_t*)calloc(SCRATCHPAD_NR, ctx->words*sizeof(uint64_t));
    if (NULL == scratchpad) {
        res = ERR_MEMORY;
        goto cleanup;
    }

    switch (ctx->modulus_type) {
        case ModulusEd448:
            curve448_invert(out, tmp1, tmp2, a, scratchpad, ctx);
            break;
        default:
            mont_inv_prime_generic(out, tmp1, a, scratchpad, ctx);
            break;
    }
    res = 0;

cleanup:
    free(tmp1);
    free(tmp2);
    free(scratchpad);
    return res;
}

/*
 * Assign a value to a number in Montgomery form.
 *
 * @param out   The location where the result is stored at; it must have been created with mont_new_number(&p,1,ctx).
 * @param x     The value to set.
 * @param ctx   The Montgomery context.
 * @return      0 for success, the relevant error code otherwise.
 */
int mont_set(uint64_t *out, uint64_t x, const MontContext *ctx)
{
    uint64_t *tmp, *scratchpad;

    if (NULL == out || NULL == ctx)
        return ERR_NULL;

    if (x == 0) {
        memset(out, 0, ctx->bytes);
        return 0;
    }
    if (x == 1) {
        mont_copy(out, ctx->r_mod_n, ctx);
        return 0;
    }

    tmp = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == tmp)
        return ERR_MEMORY;
    tmp[0] = x;

    scratchpad = (uint64_t*)calloc(SCRATCHPAD_NR, ctx->words*sizeof(uint64_t));
    if (NULL == scratchpad) {
        free(tmp);
        return ERR_MEMORY;
    }

    if (ctx->modulus_type != ModulusP521)
        mont_mult_generic(out, tmp, ctx->r2_mod_n, ctx->modulus, ctx->m0, scratchpad, ctx->words);
    else
        mont_copy(out, tmp, ctx);

    free(tmp);
    free(scratchpad);

    return 0;
}

static int cmp_modulus(const uint8_t *mod1, size_t mod1_len, const uint8_t *mod2, size_t mod2_len)
{
    size_t diff;

    if (mod1_len > mod2_len) {
        diff = mod1_len - mod2_len;
        if (0 != memcmp(mod1+diff, mod2, mod2_len))
            return -1;
        if (NULL != memchr_not(mod1, 0, diff))
            return -1;
    } else {
        diff = mod2_len - mod1_len;
        if (0 != memcmp(mod2+diff, mod1, mod1_len))
            return -1;
        if (NULL != memchr_not(mod2, 0, diff))
            return -1;
    }
    return 0;
}

/*
 * Create a new context for the Montgomery and the given odd modulus.
 *
 * @param out       The locate where the pointer to the newly allocated data will be stored at.
 *                  The memory will contain the new Montgomery context.
 * @param modulus   The modulus encoded in big endian form.
 * @param mod_len   The length of the modulus in bytes.
 * @return          0 for success, the appropriate code otherwise.
 */
int mont_context_init(MontContext **out, const uint8_t *modulus, size_t mod_len)
{
    const uint8_t p256_mod[32] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x01,
                                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                  0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    const uint8_t p384_mod[48] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
                                  0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF};
    const uint8_t p521_mod[66] = {0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF};
    const uint8_t ed448_mod[56] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    uint64_t *scratchpad = NULL;
    MontContext *ctx;
    int res;

    if (NULL == out || NULL == modulus)
        return ERR_NULL;

    /** Consume leading zeros **/
    while (mod_len>0 && *modulus==0) {
        modulus++;
        mod_len--;
    }
    if (0 == mod_len)
        return ERR_MODULUS;

    /** Ensure modulus is odd and at least 3, otherwise we can't compute its inverse over B **/
    if (is_even(modulus[mod_len-1]))
        return ERR_MODULUS;
    if (mod_len==1 && modulus[0]==1)
        return ERR_MODULUS;

    *out = ctx = (MontContext*)calloc(1, sizeof(MontContext));
    if (NULL == ctx)
        return ERR_MEMORY;

    /* Check if the modulus has a special form */
    /* For P-521, modulo reduction is very simple so the Montgomery
     * representation is not actually used.
     */
    ctx->modulus_type = ModulusGeneric;
    switch (mod_len) {
        case sizeof(p256_mod):
            if (0 == cmp_modulus(modulus, mod_len, p256_mod, sizeof(p256_mod))) {
                ctx->modulus_type = ModulusP256;
            }
            break;
        case sizeof(p384_mod):
            if (0 == cmp_modulus(modulus, mod_len, p384_mod, sizeof(p384_mod))) {
                ctx->modulus_type = ModulusP384;
            }
            break;
        case sizeof(p521_mod):
            if (0 == cmp_modulus(modulus, mod_len, p521_mod, sizeof(p521_mod))) {
                ctx->modulus_type = ModulusP521;
            }
            break;
        case sizeof(ed448_mod):
            if (0 == cmp_modulus(modulus, mod_len, ed448_mod, sizeof(ed448_mod))) {
                ctx->modulus_type = ModulusEd448;
            }
            break;
    }

    ctx->words = ((unsigned)mod_len + 7) / 8;
    ctx->bytes = (unsigned)(ctx->words * sizeof(uint64_t));
    ctx->modulus_len = (unsigned)mod_len;

    /** Load modulus N **/
    ctx->modulus = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (0 == ctx->modulus) {
        res = ERR_MEMORY;
        goto cleanup;
    }
    bytes_to_words(ctx->modulus, ctx->words, modulus, mod_len);

    /** Prepare 1 **/
    ctx->one = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == ctx->one) {
        res = ERR_MEMORY;
        goto cleanup;
    }
    ctx->one[0] = 1;

    /** Pre-compute R² mod N **/
    /** Pre-compute -n[0]⁻¹ mod R **/
    ctx->r2_mod_n = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (0 == ctx->r2_mod_n) {
        res = ERR_MEMORY;
        goto cleanup;
    }
    if (ctx->modulus_type != ModulusP521) {
        rsquare(ctx->r2_mod_n, ctx->modulus, ctx->words);
        ctx->m0 = inverse64(~ctx->modulus[0]+1);
    } else {
        memcpy(ctx->r2_mod_n, ctx->one, ctx->words * sizeof(uint64_t));
        ctx->m0 = 1U;
    }

    /** Pre-compute R mod N **/
    ctx->r_mod_n = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == ctx->r_mod_n) {
        res = ERR_MEMORY;
        goto cleanup;
    }
    scratchpad = (uint64_t*)calloc(SCRATCHPAD_NR, ctx->words*sizeof(uint64_t));
    if (NULL == scratchpad) {
        res = ERR_MEMORY;
        goto cleanup;
    }
    if (ctx->modulus_type != ModulusP521)
        mont_mult_generic(ctx->r_mod_n, ctx->one, ctx->r2_mod_n, ctx->modulus, ctx->m0, scratchpad, ctx->words);
    else
        memcpy(ctx->r_mod_n, ctx->one, ctx->words * sizeof(uint64_t));

    /** Pre-compute modulus - 2 **/
    /** Modulus is guaranteed to be >= 3 **/
    ctx->modulus_min_2 = (uint64_t*)calloc(ctx->words, sizeof(uint64_t));
    if (NULL == ctx->modulus_min_2) {
        res = ERR_MEMORY;
        goto cleanup;
    }
    sub(ctx->modulus_min_2, ctx->modulus, ctx->one, ctx->words);
    sub(ctx->modulus_min_2, ctx->modulus_min_2, ctx->one, ctx->words);

    res = 0;

cleanup:
    free(scratchpad);
    if (res != 0) {
        mont_context_free(ctx);
    }
    return res;
}

int mont_is_zero(const uint64_t *a, const MontContext *ctx)
{
    unsigned i;
    uint64_t sum = 0;

    if (NULL == a || NULL == ctx)
        return -1;

    for (i=0; i<ctx->words; i++) {
        sum |= *a++;
    }

    return (sum == 0);
}

int mont_is_one(const uint64_t *a, const MontContext *ctx)
{
    unsigned i;
    uint64_t sum = 0;

    if (NULL == a || NULL == ctx)
        return -1;

    for (i=0; i<ctx->words; i++) {
        sum |= a[i] ^ ctx->r_mod_n[i];
    }

    return (sum == 0);
}

int mont_is_equal(const uint64_t *a, const uint64_t *b, const MontContext *ctx)
{
    unsigned i;
    uint64_t result = 0;

    if (NULL == a || NULL == b || NULL == ctx)
        return -1;

    for (i=0; i<ctx->words; i++) {
        result |= *a++ ^ *b++;
    }

    return (result == 0);
}

int mont_copy(uint64_t *out, const uint64_t *a, const MontContext *ctx)
{
    unsigned i;

    if (NULL == out || NULL == a || NULL == ctx)
        return ERR_NULL;

    for (i=0; i<ctx->words; i++) {
        *out++ = *a++;
    }

    return 0;
}
