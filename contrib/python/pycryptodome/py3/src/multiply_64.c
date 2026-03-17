/* ===================================================================
 *
 * Copyright (c) 2019, Helder Eijs <helderijs@gmail.com>
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

#include <stddef.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include "multiply.h"

/**
 * Add a 64-bit value x to y/sum_mid/sum_hi
 */
#if defined(_MSC_VER) && (_MSC_VER>=1900) && (defined(_M_X64) || defined(__x86_64__))

#include <intrin.h>
#define ADD192(y, x) do {           \
    unsigned char c = 0;            \
    c = _addcarry_u64(c, x, y, &y); \
    c = _addcarry_u64(c, 0, sum_mid, &sum_mid); \
    _addcarry_u64(c, 0, sum_hi, &sum_hi);   \
    } while (0)

#else

#define ADD192(y, x) do {       \
    uint64_t c;                 \
    y += x;                     \
    sum_mid += (c = (y < x));   \
    sum_hi += sum_mid < c;      \
    } while (0)

#endif

void addmul128(uint64_t *t, uint64_t *scratchpad, const uint64_t *a, uint64_t b0, uint64_t b1, size_t t_words, size_t a_nw)
{
    uint64_t sum_low, sum_mid, sum_hi;
    uint64_t pr_low, pr_high, aim1;
    size_t i;

    assert(t_words >= a_nw + 2);

    if (a_nw == 0) {
        return;
    }

    /** LSW **/
    DP_MULT(a[0], b0, sum_low, sum_mid);
    sum_hi = 0;
    
    ADD192(t[0], sum_low);

    sum_low = sum_mid;
    sum_mid = sum_hi;
    sum_hi = 0;

    aim1 = a[0];
    for (i=1; i<(a_nw-1)/4*4+1;) {
        /** I **/
        DP_MULT(aim1, b1, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        DP_MULT(a[i], b0, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        ADD192(t[i], sum_low);
        sum_low = sum_mid;
        sum_mid = sum_hi;
        sum_hi = 0; 
        aim1 = a[i];
        i++;
        /** II **/
        DP_MULT(aim1, b1, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        DP_MULT(a[i], b0, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        ADD192(t[i], sum_low);
        sum_low = sum_mid;
        sum_mid = sum_hi;
        sum_hi = 0;
        aim1 = a[i];
        i++;
        /** III **/
        DP_MULT(aim1, b1, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        DP_MULT(a[i], b0, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        ADD192(t[i], sum_low);
        sum_low = sum_mid;
        sum_mid = sum_hi;
        sum_hi = 0;
        aim1 = a[i];
        i++;
        /** IV **/
        DP_MULT(aim1, b1, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        DP_MULT(a[i], b0, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        ADD192(t[i], sum_low);
        sum_low = sum_mid;
        sum_mid = sum_hi;
        sum_hi = 0;
        aim1 = a[i];
        i++;
    }
   
    /** Execute 0 to 3 times **/
    for (; i<a_nw; i++) {

        DP_MULT(aim1, b1, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        DP_MULT(a[i], b0, pr_low, pr_high);
        ADD192(sum_low, pr_low);
        sum_mid += pr_high;
        sum_hi += sum_mid < pr_high;
        ADD192(t[i], sum_low);
        sum_low = sum_mid;
        sum_mid = sum_hi;
        sum_hi = 0;
        aim1 = a[i];
    }

    /** MSW - 1 **/
    DP_MULT(a[i-1], b1, pr_low, pr_high);
    ADD192(sum_low, pr_low);
    sum_mid += pr_high;
    sum_hi += sum_mid < pr_high;
    ADD192(t[i], sum_low);

    sum_low = sum_mid;
    sum_mid = sum_hi;
    sum_hi = 0;
    i++;

    /** MSW **/
    ADD192(t[i], sum_low);
    sum_low = sum_mid;
    sum_mid = sum_hi;
    sum_hi = 0;
    i++;

    /* i == a_nw + 2 */

    /** Extend carry indefinetly **/
    for (; i<t_words; i++) {
        ADD192(t[i], sum_low);

        sum_low = sum_mid;
        sum_mid = sum_hi;
        sum_hi = 0;
    }
}


void square(uint64_t *t, uint64_t *scratchpad, const uint64_t *a, size_t nw)
{
    size_t i, j;
    uint64_t carry;

    if (nw == 0) {
        return;
    }

    memset(t, 0, 2*sizeof(uint64_t)*nw);

    /** Compute all mix-products without doubling **/
    for (i=0; i<nw; i++) {
        carry = 0;

        for (j=i+1; j<nw; j++) {
            uint64_t sum_lo, sum_hi;

            DP_MULT(a[j], a[i], sum_lo, sum_hi);

            sum_lo += carry;
            sum_hi += sum_lo < carry;

            t[i+j] += sum_lo;
            carry = sum_hi + (t[i+j] < sum_lo);
        }

        /** Propagate carry **/
        for (j=i+nw; carry>0; j++) {
            t[j] += (uint64_t)carry;
            carry = t[j] < carry;
        }
    }

    /** Double mix-products and add squares **/
    carry = 0;
    for (i=0, j=0; i<nw; i++, j+=2) {
        uint64_t sum_lo, sum_hi, tmp, tmp2;

        DP_MULT(a[i], a[i], sum_lo, sum_hi);

        sum_lo += carry;
        sum_hi += sum_lo < carry;

        sum_hi += (tmp = ((t[j+1] << 1) + (t[j] >> 63)));
        carry = (t[j+1] >> 63) + (sum_hi < tmp);

        sum_lo += (tmp = (t[j] << 1));
        sum_hi += (tmp2 = (sum_lo < tmp));
        carry += sum_hi < tmp2;

        t[j] = sum_lo;
        t[j+1] = sum_hi;
    }
    assert(carry == 0);
}

