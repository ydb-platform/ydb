/* This file implements the core float parsing routines used in msgspec.
 *
 * It contains an implementation of the Eisel-Lemire algorithm, as described in
 * https://nigeltao.github.io/blog/2020/eisel-lemire.html. Much of the
 * implementation is based on the one available in Wuffs
 * (https://github.com/google/wuffs/blob/c104ae296c3557f946e4bd5ee8b85511f12c141c/internal/cgen/base/floatconv-submodule-code.c#L989).
 *
 * It also contains a fallback implementation using a High Precision Double
 * (HPD). This method is based on the following blogpost by Nigel Tao (
 * https://nigeltao.github.io/blog/2020/parse-number-f64-simple.html), as well
 * as the implementation in Wuffs
 * (https://github.com/google/wuffs/blob/c104ae296c3557f946e4bd5ee8b85511f12c141c/internal/cgen/base/floatconv-submodule-code.c#L1307-L1308).
 *
 * The Wuffs license is copied below:
 *
 * """ Copyright 2020 The Wuffs Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.  """
 *
 * */

#ifndef MS_ATOF_H
#define MS_ATOF_H

#include <stdint.h>
#include <math.h>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

#include "atof_consts.h"

typedef struct ms_uint128 {
    uint64_t lo;
    uint64_t hi;
} ms_uint128;

static inline ms_uint128
ms_mulu64(uint64_t x, uint64_t y) {
#if defined(__SIZEOF_INT128__)
    ms_uint128 out;
    __uint128_t z = ((__uint128_t)x) * ((__uint128_t)y);
    out.lo = (uint64_t)z;
    out.hi = (uint64_t)(z >> 64);
    return out;
#else
    ms_uint128 out;
    uint64_t x0 = x & 0xFFFFFFFF;
    uint64_t x1 = x >> 32;
    uint64_t y0 = y & 0xFFFFFFFF;
    uint64_t y1 = y >> 32;
    uint64_t w0 = x0 * y0;
    uint64_t t = (x1 * y0) + (w0 >> 32);
    uint64_t w1 = t & 0xFFFFFFFF;
    uint64_t w2 = t >> 32;
    w1 += x0 * y1;
    out.lo = x * y;
    out.hi = (x1 * y1) + w2 + (w1 >> 32);
    return out;
#endif
}

static inline uint32_t
ms_clzll(uint64_t x) {
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_ARM64) || defined(_M_IA64))
  uint32_t index = 0;
  _BitScanReverse64(&index, x);
  return (int)(63 - index);
#elif defined(__GNUC__)
  return (uint32_t)__builtin_clzll(x);
#else
    uint32_t out;
    if ((x >> 32) == 0) {
        out |= 32;
        x <<= 32;
    }
    if ((x >> 48) == 0) {
        out |= 16;
        x <<= 16;
    }
    if ((x >> 56) == 0) {
        out |= 8;
        x <<= 8;
    }
    if ((x >> 60) == 0) {
        out |= 4;
        x <<= 4;
    }
    if ((x >> 62) == 0) {
        out |= 2;
        x <<= 2;
    }
    if ((x >> 63) == 0) {
        out |= 1;
        x <<= 1;
    }
    return out;
#endif
}

static inline int64_t
eisel_lemire(uint64_t man, int32_t exp) {
    /* The short comment headers below correspond to section titles in Nigel
     * Tao's blogpost. See
     * https://nigeltao.github.io/blog/2020/eisel-lemire.html for a more
     * in-depth description of the algorithm */

    /* Normalization */
    const uint64_t* po10 = ms_atof_powers_of_10[exp + 307];
    uint32_t clz = ms_clzll(man);
    man <<= clz;
    uint64_t ret_exp2 = ((uint64_t)(((217706 * exp) >> 16) + 1087)) - ((uint64_t)clz);

    /* Multiplication */
    ms_uint128 x = ms_mulu64(man, po10[1]);
    uint64_t x_hi = x.hi;
    uint64_t x_lo = x.lo;

    /* Apply a wider Approximation if needed */
	if (((x_hi & 0x1FF) == 0x1FF) && ((x_lo + man) < man)) {
		ms_uint128 y = ms_mulu64(man, po10[0]);
		uint64_t y_hi = y.hi;
		uint64_t y_lo = y.lo;

		uint64_t merged_hi = x_hi;
		uint64_t merged_lo = x_lo + y_hi;
		if (merged_lo < x_lo) {
            merged_hi++;
		}

        /* If the result is still ambiguous at this approximation, abort */
		if (((merged_hi & 0x1FF) == 0x1FF) && ((merged_lo + 1) == 0) && (y_lo + man < man)) {
            return -1;
		}

		x_hi = merged_hi;
		x_lo = merged_lo;
	}

    /* Shift to 54 bits */
	uint64_t msb = x_hi >> 63;
	uint64_t ret_mantissa = x_hi >> (msb + 9);
	ret_exp2 -= 1 ^ msb;

    /* Check for a half-way ambiguity, and abort if present */
	if ((x_lo == 0) && ((x_hi & 0x1FF) == 0) && ((ret_mantissa & 3) == 1)) {
		return -1;
	}

    /* From 54 to 53 bits */
	ret_mantissa += ret_mantissa & 1;
	ret_mantissa >>= 1;
	if ((ret_mantissa >> 53) > 0) {
		ret_mantissa >>= 1;
		ret_exp2++;
	}

    /* Construct final output */
	ret_mantissa &= 0x000FFFFFFFFFFFFF;
    return ((int64_t)(ret_mantissa | (ret_exp2 << 52)));
}

/* Fallback parsing method using a High Precision Double (HPD) */
#define MS_HPD_MAX_DIGITS 800
#define MS_HPD_DP_RANGE 2047
#define MS_HPD_MAX_SHIFT 60

typedef struct ms_hpd
{
    uint32_t num_digits;
    int32_t decimal_point;
    bool negative;
    bool truncated;
    uint8_t digits[800];
} ms_hpd;


static inline void
ms_hpd_trim(ms_hpd *dec) {
    while ((dec->num_digits > 0) && (dec->digits[dec->num_digits - 1] == 0)) {
        dec->num_digits--;
    }
    if (dec->num_digits == 0) {
        dec->decimal_point = 0;
    }
}

static uint32_t
ms_hpd_lshift_num_new_digits(ms_hpd *hpd, uint32_t shift) {
    shift &= 63;

    uint32_t x_a = ms_atof_left_shift[shift];
    uint32_t x_b = ms_atof_left_shift[shift + 1];
    uint32_t num_new_digits = x_a >> 11;
    uint32_t pow5_a = 0x7FF & x_a;
    uint32_t pow5_b = 0x7FF & x_b;

    const uint8_t* pow5 = &ms_atof_powers_of_5[pow5_a];
    uint32_t i = 0;
    uint32_t n = pow5_b - pow5_a;
    for (; i < n; i++) {
        if (i >= hpd->num_digits) {
            return num_new_digits - 1;
        } else if (hpd->digits[i] == pow5[i]) {
            continue;
        } else if (hpd->digits[i] < pow5[i]) {
            return num_new_digits - 1;
        } else {
            return num_new_digits;
        }
    }
    return num_new_digits;
}

static uint64_t
ms_hpd_rounded_integer(ms_hpd *hpd) {
    if ((hpd->num_digits == 0) || (hpd->decimal_point < 0)) {
        return 0;
    } else if (hpd->decimal_point > 18) {
        return UINT64_MAX;
    }

    uint32_t dp = (uint32_t)(hpd->decimal_point);
    uint64_t n = 0;
    uint32_t i = 0;
    for (; i < dp; i++) {
        n = (10 * n) + ((i < hpd->num_digits) ? hpd->digits[i] : 0);
    }

    bool round_up = false;
    if (dp < hpd->num_digits) {
        round_up = hpd->digits[dp] >= 5;
        if ((hpd->digits[dp] == 5) && (dp + 1 == hpd->num_digits)) {
            round_up = hpd->truncated || ((dp > 0) && (1 & hpd->digits[dp - 1]));
        }
    }
    if (round_up) {
        n++;
    }

    return n;
}

static void
ms_hpd_small_lshift(ms_hpd *hpd, uint32_t shift) {
    if (hpd->num_digits == 0) {
        return;
    }
    uint32_t num_new_digits = ms_hpd_lshift_num_new_digits(hpd, shift);
    uint32_t rx = hpd->num_digits - 1;                   // Read  index.
    uint32_t wx = hpd->num_digits - 1 + num_new_digits;  // Write index.
    uint64_t n = 0;

    while (((int32_t)rx) >= 0) {
        n += ((uint64_t)(hpd->digits[rx])) << shift;
        uint64_t quo = n / 10;
        uint64_t rem = n - (10 * quo);
        if (wx < MS_HPD_MAX_DIGITS) {
            hpd->digits[wx] = (uint8_t)rem;
        } else if (rem > 0) {
            hpd->truncated = true;
        }
        n = quo;
        wx--;
        rx--;
    }

    while (n > 0) {
        uint64_t quo = n / 10;
        uint64_t rem = n - (10 * quo);
        if (wx < MS_HPD_MAX_DIGITS) {
            hpd->digits[wx] = (uint8_t)rem;
        } else if (rem > 0) {
            hpd->truncated = true;
        }
        n = quo;
        wx--;
    }

    hpd->num_digits += num_new_digits;
    if (hpd->num_digits > MS_HPD_MAX_DIGITS) {
        hpd->num_digits = MS_HPD_MAX_DIGITS;
    }
    hpd->decimal_point += (int32_t)num_new_digits;
    ms_hpd_trim(hpd);
}

static void
ms_hpd_small_rshift(ms_hpd *hpd, uint32_t shift) {
    uint32_t rx = 0;
    uint32_t wx = 0;
    uint64_t n = 0;

    while ((n >> shift) == 0) {
        if (rx < hpd->num_digits) {
            n = (10 * n) + hpd->digits[rx++];
        } else if (n == 0) {
            return;
        } else {
            while ((n >> shift) == 0) {
                n = 10 * n;
                rx++;
            }
            break;
        }
    }
    hpd->decimal_point -= ((int32_t)(rx - 1));
    if (hpd->decimal_point < -MS_HPD_DP_RANGE) {
        hpd->num_digits = 0;
        hpd->decimal_point = 0;
        hpd->truncated = false;
        return;
    }

    uint64_t mask = (((uint64_t)(1)) << shift) - 1;
    while (rx < hpd->num_digits) {
        uint8_t new_digit = ((uint8_t)(n >> shift));
        n = (10 * (n & mask)) + hpd->digits[rx++];
        hpd->digits[wx++] = new_digit;
    }

    while (n > 0) {
        uint8_t new_digit = ((uint8_t)(n >> shift));
        n = 10 * (n & mask);
        if (wx < MS_HPD_MAX_DIGITS) {
            hpd->digits[wx++] = new_digit;
        } else if (new_digit > 0) {
            hpd->truncated = true;
        }
    }

    hpd->num_digits = wx;
    ms_hpd_trim(hpd);
}

static double
ms_hpd_to_double(ms_hpd *hpd) {
    static const uint32_t num_powers = 19;
    static const uint8_t powers[19] = {
        0,  3,  6,  9,  13, 16, 19, 23, 26, 29,
        33, 36, 39, 43, 46, 49, 53, 56, 59,
    };

    if ((hpd->num_digits == 0) || (hpd->decimal_point < -326)) {
        goto zero;
    } else if (hpd->decimal_point > 310) {
        goto infinity;
    }

    const int32_t f64_bias = -1023;
    int32_t exp2 = 0;
    while (hpd->decimal_point > 0) {
        uint32_t n = (uint32_t)(+hpd->decimal_point);
        uint32_t shift = (n < num_powers) ? powers[n] : MS_HPD_MAX_SHIFT;
        ms_hpd_small_rshift(hpd, shift);
        if (hpd->decimal_point < -MS_HPD_DP_RANGE) {
            goto zero;
        }
        exp2 += (int32_t)shift;
    }
    while (hpd->decimal_point <= 0) {
        uint32_t shift;
        if (hpd->decimal_point == 0) {
            if (hpd->digits[0] >= 5) {
                break;
            }
            shift = (hpd->digits[0] < 2) ? 2 : 1;
        } else {
            uint32_t n = (uint32_t)(-hpd->decimal_point);
            shift = (n < num_powers) ? powers[n] : MS_HPD_MAX_SHIFT;
        }

        ms_hpd_small_lshift(hpd, shift);
        if (hpd->decimal_point > +MS_HPD_DP_RANGE) {
            goto infinity;
        }
        exp2 -= (int32_t)shift;
    }

    exp2--;

    while ((f64_bias + 1) > exp2) {
        uint32_t n = (uint32_t)((f64_bias + 1) - exp2);
        if (n > MS_HPD_MAX_SHIFT) {
            n = MS_HPD_MAX_SHIFT;
        }
        ms_hpd_small_rshift(hpd, n);
        exp2 += (int32_t)n;
    }

    if ((exp2 - f64_bias) >= 0x07FF) {
        goto infinity;
    }

    ms_hpd_small_lshift(hpd, 53);
    uint64_t man2 = ms_hpd_rounded_integer(hpd);

    if ((man2 >> 53) != 0) {
        man2 >>= 1;
        exp2++;
        if ((exp2 - f64_bias) >= 0x07FF) {
            goto infinity;
        }
    }

    if ((man2 >> 52) == 0) {
        exp2 = f64_bias;
    }

    uint64_t exp2_bits = (uint64_t)((exp2 - f64_bias) & 0x07FF);
    uint64_t bits = (
        (man2 & 0x000FFFFFFFFFFFFF) |
        (exp2_bits << 52) |
        (hpd->negative ? 0x8000000000000000 : 0)
    );

    double ret;
    memcpy(&ret, &bits, sizeof(double));
    return ret;

zero:
    return hpd->negative ? -0.0 : 0.0;

infinity:
    return hpd->negative ? -INFINITY : INFINITY;
}

#endif
