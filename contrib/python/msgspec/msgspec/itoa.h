/* This code is a modified and condensed version of the int -> str routines
 * found in yyjson (https://github.com/ibireme/yyjson). The yyjson license
 * is copied below:
 *
 * Copyright (c) 2020 YaoYuan <ibireme@gmail.com>

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef MS_ITOA_H
#define MS_ITOA_H

#include <stdlib.h>

#include "common.h"

static const char DIGIT_TABLE[200] = {
  '0','0','0','1','0','2','0','3','0','4','0','5','0','6','0','7','0','8','0','9',
  '1','0','1','1','1','2','1','3','1','4','1','5','1','6','1','7','1','8','1','9',
  '2','0','2','1','2','2','2','3','2','4','2','5','2','6','2','7','2','8','2','9',
  '3','0','3','1','3','2','3','3','3','4','3','5','3','6','3','7','3','8','3','9',
  '4','0','4','1','4','2','4','3','4','4','4','5','4','6','4','7','4','8','4','9',
  '5','0','5','1','5','2','5','3','5','4','5','5','5','6','5','7','5','8','5','9',
  '6','0','6','1','6','2','6','3','6','4','6','5','6','6','6','7','6','8','6','9',
  '7','0','7','1','7','2','7','3','7','4','7','5','7','6','7','7','7','8','7','9',
  '8','0','8','1','8','2','8','3','8','4','8','5','8','6','8','7','8','8','8','9',
  '9','0','9','1','9','2','9','3','9','4','9','5','9','6','9','7','9','8','9','9'
};

static MS_INLINE void
write_u32_8_digits(uint32_t x, char *buf) {
    uint32_t aabb, ccdd, aa, bb, cc, dd;
    aabb = (uint32_t)(((uint64_t)x * 109951163) >> 40);  /* (x / 10000) */
    ccdd = x - aabb * 10000;                             /* (x % 10000) */
    aa = (aabb * 5243) >> 19;                            /* (aabb / 100) */
    bb = aabb - aa * 100;                                /* (aabb % 100) */
    cc = (ccdd * 5243) >> 19;                            /* (ccdd / 100) */
    dd = ccdd - cc * 100;                                /* (ccdd % 100) */
    memcpy(buf + 0, DIGIT_TABLE + aa * 2, 2);
    memcpy(buf + 2, DIGIT_TABLE + bb * 2, 2);
    memcpy(buf + 4, DIGIT_TABLE + cc * 2, 2);
    memcpy(buf + 6, DIGIT_TABLE + dd * 2, 2);
}

static MS_INLINE void
write_u32_6_digits(uint32_t x, char *buf) {
    uint32_t aa, bbcc, bb, cc;
    aa = (uint32_t)(((uint64_t)x * 109951163) >> 40);  /* (x / 10000) */
    bbcc = x - aa * 10000;                             /* (x % 10000) */
    bb = (bbcc * 5243) >> 19;                          /* (bbcc / 100) */
    cc = bbcc - bb * 100;                              /* (bbcc % 100) */
    memcpy(buf + 0, DIGIT_TABLE + aa * 2, 2);
    memcpy(buf + 2, DIGIT_TABLE + bb * 2, 2);
    memcpy(buf + 4, DIGIT_TABLE + cc * 2, 2);
}

static MS_INLINE void
write_u32_4_digits(uint32_t x, char *buf) {
    uint32_t aa, bb;
    aa = (x * 5243) >> 19;  /* (x / 100) */
    bb = x - aa * 100;      /* (x % 100) */
    memcpy(buf + 0, DIGIT_TABLE + aa * 2, 2);
    memcpy(buf + 2, DIGIT_TABLE + bb * 2, 2);
}

static MS_INLINE void
write_u32_2_digits(uint32_t x, char *buf) {
    memcpy(buf, DIGIT_TABLE + x * 2, 2);
}

static MS_INLINE char *
write_u32_1_to_8_digits(uint32_t x, char *buf) {
    uint32_t aa, bb, cc, dd, aabb, bbcc, ccdd, lz;

    if (x < 100) {                                /* 1-2 digits */
        lz = x < 10;
        memcpy(buf + 0, DIGIT_TABLE + x * 2 + lz, 2);
        buf -= lz;
        return buf + 2;
    } else if (x < 10000) {                       /* 3-4 digits */
        aa = (x * 5243) >> 19;                    /* (x / 100) */
        bb = x - aa * 100;                        /* (x % 100) */
        lz = aa < 10;
        memcpy(buf + 0, DIGIT_TABLE + aa * 2 + lz, 2);
        buf -= lz;
        memcpy(buf + 2, DIGIT_TABLE + bb * 2, 2);
        return buf + 4;
    } else if (x < 1000000) {                           /* 5-6 digits */
        aa = (uint32_t)(((uint64_t)x * 429497) >> 32);  /* (x / 10000) */
        bbcc = x - aa * 10000;                          /* (x % 10000) */
        bb = (bbcc * 5243) >> 19;                       /* (bbcc / 100) */
        cc = bbcc - bb * 100;                           /* (bbcc % 100) */
        lz = aa < 10;
        memcpy(buf + 0, DIGIT_TABLE + aa * 2 + lz, 2);
        buf -= lz;
        memcpy(buf + 2, DIGIT_TABLE + bb * 2, 2);
        memcpy(buf + 4, DIGIT_TABLE + cc * 2, 2);
        return buf + 6;
    } else {                                                 /* 7-8 digits */
        aabb = (uint32_t)(((uint64_t)x * 109951163) >> 40);  /* (x / 10000) */
        ccdd = x - aabb * 10000;                             /* (x % 10000) */
        aa = (aabb * 5243) >> 19;                            /* (aabb / 100) */
        bb = aabb - aa * 100;                                /* (aabb % 100) */
        cc = (ccdd * 5243) >> 19;                            /* (ccdd / 100) */
        dd = ccdd - cc * 100;                                /* (ccdd % 100) */
        lz = aa < 10;
        memcpy(buf + 0, DIGIT_TABLE + aa * 2 + lz, 2);
        buf -= lz;
        memcpy(buf + 2, DIGIT_TABLE + bb * 2, 2);
        memcpy(buf + 4, DIGIT_TABLE + cc * 2, 2);
        memcpy(buf + 6, DIGIT_TABLE + dd * 2, 2);
        return buf + 8;
    }
}

static MS_INLINE char *
write_u64_5_to_8_digits(uint32_t x, char *buf) {
    uint32_t aa, bb, cc, dd, aabb, bbcc, ccdd, lz;

    if (x < 1000000) {                                  /* 5-6 digits */
        aa = (uint32_t)(((uint64_t)x * 429497) >> 32);  /* (x / 10000) */
        bbcc = x - aa * 10000;                          /* (x % 10000) */
        bb = (bbcc * 5243) >> 19;                       /* (bbcc / 100) */
        cc = bbcc - bb * 100;                           /* (bbcc % 100) */
        lz = aa < 10;
        memcpy(buf + 0, DIGIT_TABLE + aa * 2 + lz, 2);
        buf -= lz;
        memcpy(buf + 2, DIGIT_TABLE + bb * 2, 2);
        memcpy(buf + 4, DIGIT_TABLE + cc * 2, 2);
        return buf + 6;
    } else {                                                 /* 7-8 digits */
        aabb = (uint32_t)(((uint64_t)x * 109951163) >> 40);  /* (x / 10000) */
        ccdd = x - aabb * 10000;                             /* (x % 10000) */
        aa = (aabb * 5243) >> 19;                            /* (aabb / 100) */
        bb = aabb - aa * 100;                                /* (aabb % 100) */
        cc = (ccdd * 5243) >> 19;                            /* (ccdd / 100) */
        dd = ccdd - cc * 100;                                /* (ccdd % 100) */
        lz = aa < 10;
        memcpy(buf + 0, DIGIT_TABLE + aa * 2 + lz, 2);
        buf -= lz;
        memcpy(buf + 2, DIGIT_TABLE + bb * 2, 2);
        memcpy(buf + 4, DIGIT_TABLE + cc * 2, 2);
        memcpy(buf + 6, DIGIT_TABLE + dd * 2, 2);
        return buf + 8;
    }
}

/* Write a uint64 to buf, requires 20 bytes of space */
static inline char *
write_u64(uint64_t x, char *buf) {
    uint64_t tmp, hgh;
    uint32_t mid, low;

    if (x < 100000000) {  /* 1-8 digits */
        return write_u32_1_to_8_digits((uint32_t)x, buf);
    } else if (x < (uint64_t)100000000 * 100000000) {  /* 9-16 digits */
        hgh = x / 100000000;                           /* (x / 100000000) */
        low = (uint32_t)(x - hgh * 100000000);         /* (x % 100000000) */
        char *cur = write_u32_1_to_8_digits((uint32_t)hgh, buf);
        write_u32_8_digits(low, cur);
        return cur + 8;
    } else {                                    /* 17-20 digits */
        tmp = x / 100000000;                    /* (x / 100000000) */
        low = (uint32_t)(x - tmp * 100000000);  /* (x % 100000000) */
        hgh = (uint32_t)(tmp / 10000);          /* (tmp / 10000) */
        mid = (uint32_t)(tmp - hgh * 10000);    /* (tmp % 10000) */
        char *cur = write_u64_5_to_8_digits((uint32_t)hgh, buf);
        write_u32_4_digits(mid, cur);
        write_u32_8_digits(low, cur + 4);
        return cur + 12;
    }
}

#endif // MS_ITOA_H
