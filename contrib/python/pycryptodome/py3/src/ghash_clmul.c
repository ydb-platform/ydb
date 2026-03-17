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

#include "common.h"

FAKE_INIT(ghash_clmul)

#if defined(HAVE_INTRIN_H)
#include <intrin.h>
#elif defined(HAVE_X86INTRIN_H)
#include <x86intrin.h>
#elif defined(HAVE_EMMINTRIN_H)
#include <xmmintrin.h>
#include <emmintrin.h>
#else
#error No SSE2 headers available
#endif

#if defined(HAVE_WMMINTRIN_H) && defined(HAVE_TMMINTRIN_H)
#include <wmmintrin.h>
#include <tmmintrin.h>
#else
#error No CLMUL headers available
#endif

/**
 * This module implement the basic GHASH multiplication, as described in
 * NIST SP 800-38D.
 *
 * At the core, we perform a binary polynomial multiplication (carry-less)
 * modulo P(x) = x^128 + x^7 + x + 1, that is a finite field multiplication in
 * GF(2^128).
 *
 * The GCM standard requires that coefficients of the two polynomials are encoded
 * little-endian byte-wise but also (oddly enough) bit-wise (i.e. within a byte).
 *
 * In other words, the unity element x is encoded in memory as:
 *
 * 0x80 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 = (binary) 100000.....0
 *
 * Note how the least significant bit (LSB) is the leftmost one in the first byte.
 *
 * In order to use the native CPU instructions though, the conventional representation of
 * bits within a byte (with leftmost bit being the most significant) must be in place.
 *
 * To that end, instead of performing expensive bit-swapping, computing the
 * multiplication, and then bit-swapping again, we can use an equivalent
 * expression (see [2][3]) that operates directly on *bit-reflected* data.
 * Such expression interprets the original encoding of the factors as if the
 * rightmost bit was the most significant.
 *
 * The new expression A * (B * x) * x^{-128} modulo p(x) = x^128 + x^127 + x^126 + x^121 + 1
 *
 * For instance, what used to be the unity element x for the original multiplication,
 * is now the value x^127 for the equivalent expression. Within each byte, the
 * leftmost bit is the most significant as desired.
 *
 * However, this also means that bytes end up encoded big-endian in memory. In
 * order to use x86 arithmetic (and the XMM registers), data must be
 * byte-swapped when loaded from or stored into memory.
 *
 * References:
 * [1] http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.447.379&rep=rep1&type=pdf
 * [2] https://crypto.stanford.edu/RealWorldCrypto/slides/gueron.pdf
 * [3] https://blog.quarkslab.com/reversing-a-finite-field-multiplication-optimization.html
 */

struct exp_key {
    /**
     *  Powers of H (already swapped in byte endianness, and pre-multiplied by X)
     *
     *  h[0] is x*H   mod p(x)
     *  h[1] is x*H^2 mod p(x)
     *  h[2] is x*H^3 mod p(x)
     *  h[3] is x*H^4 mod p(x)
     */
    __m128i h[4];
};

/**
 * Perform the Montgomery reduction on a polynomial of degree 255,
 * using basis x^128 and modulus p(x) = x^128 + x^127 + x^126 + x^121 + 1.
 *
 * See at the bottom for an explanation.
 */
STATIC __m128i reduce(__m128i prod_high, __m128i prod_low)
{
    const uint64_t c2 = (uint64_t)0xc2 << 56;
    __m128i t1, t2, t3, t4, t7;
   
    t1 = prod_high;                                     /* U3:U2 */
    t7 = prod_low;                                      /* U1:U0 */
    t3 = _mm_loadl_epi64((__m128i*)&c2);
    t2 = _mm_clmulepi64_si128(t3, t7, 0x00);            /* A */
    t4 = _mm_shuffle_epi32(t7, _MM_SHUFFLE(1,0,3,2));   /* U0:U1 */
    t4 = _mm_xor_si128(t4, t2);                         /* B */
    t2 = _mm_clmulepi64_si128(t3, t4, 0x00);            /* C */
    t4 = _mm_shuffle_epi32(t4, _MM_SHUFFLE(1,0,3,2));   /* B0:B1 */
    t4 = _mm_xor_si128(t4, t2);                         /* D */
    t1 = _mm_xor_si128(t1, t4);                         /* T */

    return t1;
}

/**
 * Perform the carry-less multiplication of two polynomials of degree 127.
 */
STATIC void clmult(__m128i *prod_high, __m128i *prod_low, __m128i a, __m128i b)
{
    __m128i c, d, e, f, g, h, i;

    c = _mm_clmulepi64_si128(a, b, 0x00);   /* A0*B0 */
    d = _mm_clmulepi64_si128(a, b, 0x11);   /* A1*B1 */
    e = _mm_clmulepi64_si128(a, b, 0x10);   /* A0*B1 */
    f = _mm_clmulepi64_si128(a, b, 0x01);   /* A1*B0 */
    g = _mm_xor_si128(e, f);                /* E1+F1:E0+F0 */
    h = _mm_slli_si128(g, 8);               /* E0+F0:0 */
    i = _mm_srli_si128(g, 8);               /* 0:E1+F1 */
    *prod_high = _mm_xor_si128(d, i);
    *prod_low  = _mm_xor_si128(c, h);
}

/**
 * Multiply a polynomial of degree 127 by x, modulo p(x) = x^128 + x^127 + x^126 + x^121 + 1
 */
STATIC __m128i multx(__m128i a)
{
    int msb;
    int64_t r;
    uint64_t p0, p1;
    __m128i t0, t1, t2, t3, t4, t5, t6, t7;

    msb = _mm_movemask_epi8(a) >> 15;           /* Bit 0 is a[127] */
    r = (msb ^ 1) - 1;                          /* MSB is copied into all 64 positions */
    p0 = (uint64_t)r & 0x0000000000000001U;     /* Zero or XOR mask (low) */
    p1 = (uint64_t)r & ((uint64_t)0xc2 << 56);  /* Zero or XOR mask (high) */
    t0 = _mm_loadl_epi64((__m128i*)&p0);
    t1 = _mm_loadl_epi64((__m128i*)&p1);
    t2 = _mm_unpacklo_epi64(t0, t1);        /* Zero or XOR mask */

    /* Shift value a left by 1 bit */
    t3 = _mm_slli_si128(a, 8);      /* Shift a left by 64 bits (lower 64 bits are zero) */
    t4 = _mm_srli_epi64(t3, 63);    /* Bit 64 is now a[63], all other bits are 0 */
    t5 = _mm_slli_epi64(a, 1);      /* Shift left by 1 bit, but bit 64 is zero, not a[63] */
    t6 = _mm_or_si128(t4, t5);      /* Actual result of shift left by 1 bit */

    /* XOR conditional mask */
    t7 = _mm_xor_si128(t2, t6);
    
    return t7;
}

/** Swap bytes in an XMM register **/
STATIC __m128i swap(__m128i a)
{
    __m128i mask;

    mask = _mm_set_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    return _mm_shuffle_epi8(a, mask);
}

EXPORT_SYM int ghash_expand_clmul(const uint8_t h[16], struct exp_key **expanded)
{
    __m128i h128;
    unsigned i;

    if (NULL==h || NULL==expanded)
        return ERR_NULL;

    *expanded = align_alloc(sizeof(struct exp_key), 16);
    if (NULL == *expanded)
        return ERR_MEMORY;

    h128 = swap(_mm_loadu_si128((__m128i*)h));
    (*expanded)->h[0] = multx(h128);    /** x*H **/
    
    for (i=1; i<4; i++) {
        __m128i r0, r1;
        
        clmult(&r0, &r1, (*expanded)->h[i-1], (*expanded)->h[0]);
        (*expanded)->h[i] = reduce(r0, r1);
    }

    return 0;
}

EXPORT_SYM int ghash_destroy_clmul(struct exp_key *expanded)
{
    align_free(expanded);
    return 0;
}

EXPORT_SYM int ghash_clmul(
        uint8_t y_out[16],
        const uint8_t block_data[],
        size_t len,
        const uint8_t y_in[16],
        struct exp_key *expanded
        )
{
    size_t i;
    __m128i y_temp;
    size_t len16;

    if (NULL==y_out || NULL==block_data || NULL==y_in || NULL==expanded)
        return ERR_NULL;

    if (len % 16)
        return ERR_NOT_ENOUGH_DATA;

    y_temp = swap(_mm_loadu_si128((__m128i*)y_in));

    /** Authenticate 64 bytes per cycle **/
    len16 = len ^ (len & 0x3F);
    for (i=0; i<len16; i+=16*4) {
        __m128i sum0, sum1;
        __m128i xm3, xm2, xm1, x;
        __m128i r0, r1, r2, r3, r4, r5, r6;

        xm3 = swap(_mm_loadu_si128((__m128i*)&block_data[i]));
        xm2 = swap(_mm_loadu_si128((__m128i*)&block_data[i+16]));
        xm1 = swap(_mm_loadu_si128((__m128i*)&block_data[i+16*2]));
        x = swap(_mm_loadu_si128((__m128i*)&block_data[i+16*3]));

        /** (X_{i-3} + Y_{i-4}) * H^4 **/
        r0 = _mm_xor_si128(xm3, y_temp);
        clmult(&sum0, &sum1, r0, expanded->h[3]);
        
        /** X_{i-2} * H^3 **/
        clmult(&r1, &r2, xm2, expanded->h[2]);
        sum0 = _mm_xor_si128(sum0, r1);
        sum1 = _mm_xor_si128(sum1, r2);
        
        /** X_{i-1} * H^2 **/
        clmult(&r3, &r4, xm1, expanded->h[1]);
        sum0 = _mm_xor_si128(sum0, r3);
        sum1 = _mm_xor_si128(sum1, r4);

        /** X_{i} * H **/
        clmult(&r5, &r6, x, expanded->h[0]);
        sum0 = _mm_xor_si128(sum0, r5);
        sum1 = _mm_xor_si128(sum1, r6);

        /** mod P **/
        y_temp = reduce(sum0, sum1);
    }

    /** Y_i = (X_i + Y_{i-1}) * H mod P **/
    for (; i<len; i+=16) {
        __m128i z, xi;
        __m128i prod_hi, prod_lo;

        xi = swap(_mm_loadu_si128((__m128i*)&block_data[i]));
        z = _mm_xor_si128(y_temp, xi);
        clmult(&prod_hi, &prod_lo, z, expanded->h[0]);
        y_temp = reduce(prod_hi, prod_lo);
    }

    y_temp = swap(y_temp);
    _mm_storeu_si128((__m128i*)y_out, y_temp);
    return 0;
}

/**
 * The function reduce() computes the Montgomery reduction
 * of U (input, 256 bits) with FastREDC algorithm:
 *
 *  Q = ((U mod X^128) * p' mod X^128
 *  T = (U + Q*p) div X^128
 *
 * where:
 *  p = 1:C200000000000000:1 = 1:c2:1
 *  p' = p^{-1} mod X^128 = C200000000000000:1 = c2:1
 *
 * U3:U2 : U1:U0 (256 bit)
 * Q1:Q0 (128 bit)
 * T1:T0 (128 bit)
 *
 * Q = (U mod X^128) * p' mod X^128
 *   = (U1:U0) * p' mod X^128 = (U1:U0) * (c2:1) mod X^128 = Q1:Q0
 *   Q0 = U0
 *   Q1 = L(U0*c2) + U1
 *
 * T = (U + Q*p) div X^128 = T1:T0
 *
 * Q*p = S = Q1:Q0 * 1:c2:1
 *   S0 = Q0 (dropped)
 *   S1 = L(c2*Q0) + Q1 (dropped)
 *   S2 = Q0 + L(c2*Q1) + H(c2*Q0)
 *   S3 = Q1 + H(c2*Q1)
 *
 * T0 = S2 + U2
 * T1 = S3 + U3
 *
 * Q1 = L(U0*c2) + U1
 * T0 = U0 + L(c2*Q1) + H(c2*U0) + U2
 * T1 = Q1 + H(c2*Q1) + U3
 *
 * A = c2*U0
 * Q1 = A0 + U1
 * T0 = U0 + L(c2*Q1) + A1 + U2
 * T1 = Q1 + H(c2*Q1) + U3
 *
 * A = c2*U0
 * B = A + U0:U1 = B1:Q1
 * T0 = L(c2*B0) + B1 + U2
 * T1 = B0 + H(c2*B0) + U3
 *
 * A = c2*U0
 * B = A + U0:U1
 * C = c2*B0
 * T0 = C0 + B1 + U2
 * T1 = B0 + C1 + U3
 *
 * A = c2*U0
 * B = A + U0:U1
 * C = c2*B0
 * D = C + B0:B1
 * T0 = D0 + U2
 * T1 = D1 + U3
 */



