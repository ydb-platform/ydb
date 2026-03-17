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

#ifndef ENDIANESS_H
#define ENDIANESS_H

#include "common.h"

static inline void u32to8_little(uint8_t *p, const uint32_t *w)
{
#ifdef PYCRYPTO_LITTLE_ENDIAN
    memcpy(p, w, 4);
#else
    p[0] = (uint8_t)*w;
    p[1] = (uint8_t)(*w >> 8);
    p[2] = (uint8_t)(*w >> 16);
    p[3] = (uint8_t)(*w >> 24);
#endif
}

static inline void u8to32_little(uint32_t *w, const uint8_t *p)
{
#ifdef PYCRYPTO_LITTLE_ENDIAN
    memcpy(w, p, 4);
#else
    *w = (uint32_t)p[0] | (uint32_t)p[1]<<8 | (uint32_t)p[2]<<16 | (uint32_t)p[3]<<24;
#endif
}

static inline void u32to8_big(uint8_t *p, const uint32_t *w)
{
#ifdef PYCRYPTO_BIG_ENDIAN
    memcpy(p, w, 4);
#else
    p[0] = (uint8_t)(*w >> 24);
    p[1] = (uint8_t)(*w >> 16);
    p[2] = (uint8_t)(*w >> 8);
    p[3] = (uint8_t)*w;
#endif
}

static inline void u8to32_big(uint32_t *w, const uint8_t *p)
{
#ifdef PYCRYPTO_BIG_ENDIAN
    memcpy(w, p, 4);
#else
    *w = (uint32_t)p[3] | (uint32_t)p[2]<<8 | (uint32_t)p[1]<<16 | (uint32_t)p[0]<<24;
#endif
}

static inline uint32_t load_u8to32_little(const uint8_t *p)
{
    uint32_t w;

    u8to32_little(&w, p);
    return w;
}

static inline uint32_t load_u8to32_big(const uint8_t *p)
{
    uint32_t w;

    u8to32_big(&w, p);
    return w;
}

#define LOAD_U32_LITTLE(p) load_u8to32_little(p)
#define LOAD_U32_BIG(p) load_u8to32_big(p)

#define STORE_U32_LITTLE(p, w) u32to8_little((p), &(w))
#define STORE_U32_BIG(p, w) u32to8_big((p), &(w))

static inline void u64to8_little(uint8_t *p, const uint64_t *w)
{
#ifdef PYCRYPTO_LITTLE_ENDIAN
    memcpy(p, w, 8);
#else
    p[0] = (uint8_t)*w;
    p[1] = (uint8_t)(*w >> 8);
    p[2] = (uint8_t)(*w >> 16);
    p[3] = (uint8_t)(*w >> 24);
    p[4] = (uint8_t)(*w >> 32);
    p[5] = (uint8_t)(*w >> 40);
    p[6] = (uint8_t)(*w >> 48);
    p[7] = (uint8_t)(*w >> 56);
#endif
}

static inline void u8to64_little(uint64_t *w, const uint8_t *p)
{
#ifdef PYCRYPTO_LITTLE_ENDIAN
    memcpy(w, p, 8);
#else
    *w = (uint64_t)p[0]       |
         (uint64_t)p[1] << 8  |
         (uint64_t)p[2] << 16 |
         (uint64_t)p[3] << 24 |
         (uint64_t)p[4] << 32 |
         (uint64_t)p[5] << 40 |
         (uint64_t)p[6] << 48 |
         (uint64_t)p[7] << 56;
#endif
}

static inline void u64to8_big(uint8_t *p, const uint64_t *w)
{
#ifdef PYCRYPTO_BIG_ENDIAN
    memcpy(p, w, 8);
#else
    p[0] = (uint8_t)(*w >> 56);
    p[1] = (uint8_t)(*w >> 48);
    p[2] = (uint8_t)(*w >> 40);
    p[3] = (uint8_t)(*w >> 32);
    p[4] = (uint8_t)(*w >> 24);
    p[5] = (uint8_t)(*w >> 16);
    p[6] = (uint8_t)(*w >> 8);
    p[7] = (uint8_t)*w;
#endif
}

static inline void u8to64_big(uint64_t *w, const uint8_t *p)
{
#ifdef PYCRYPTO_BIG_ENDIAN
    memcpy(w, p, 8);
#else
    *w = (uint64_t)p[0] << 56 |
         (uint64_t)p[1] << 48 |
         (uint64_t)p[2] << 40 |
         (uint64_t)p[3] << 32 |
         (uint64_t)p[4] << 24 |
         (uint64_t)p[5] << 16 |
         (uint64_t)p[6] << 8  |
         (uint64_t)p[7];
#endif
}

static inline uint64_t load_u8to64_little(const uint8_t *p)
{
    uint64_t w;

    u8to64_little(&w, p);
    return w;
}

static inline uint64_t load_u8to64_big(const uint8_t *p)
{
    uint64_t w;

    u8to64_big(&w, p);
    return w;
}

#define LOAD_U64_LITTLE(p) load_u8to64_little(p)
#define LOAD_U64_BIG(p) load_u8to64_big(p)

#define STORE_U64_LITTLE(p, w) u64to8_little((p), &(w))
#define STORE_U64_BIG(p, w) u64to8_big((p), &(w))

/**
 * Convert a big endian-encoded number in[] into a little-endian
 * 64-bit word array x[]. There must be enough words to contain the entire
 * number.
 */
static inline int bytes_to_words(uint64_t *x, size_t words, const uint8_t *in, size_t len)
{
    uint8_t buf8[8];
    size_t words_used, bytes_in_msw, i;
    uint64_t *xp;

    if (0 == words || 0 == len)
        return ERR_NOT_ENOUGH_DATA;
    if (NULL == x || NULL == in)
        return ERR_NULL;

    memset(x, 0, words*sizeof(uint64_t));

    /** Shorten the input **/
    for (; len > 0 && 0 == *in; in++, len--);
    if (0 == len)
        return 0;

    /** How many words we actually need **/
    words_used = (len + 7) / 8;
    if (words_used > words)
        return ERR_MAX_DATA;

    /** Not all bytes in the most-significant words are used **/
    bytes_in_msw = len % 8;
    if (bytes_in_msw == 0)
        bytes_in_msw = 8;

    /** Do most significant word **/
    memset(buf8, 0, 8);
    memcpy(buf8 + (8 - bytes_in_msw), in, bytes_in_msw);
    xp = &x[words_used-1];
    *xp = LOAD_U64_BIG(buf8);
    in += bytes_in_msw;

    /** Do the other words **/
    for (i=0; i<words_used-1; i++, in += 8) {
        xp--;
        *xp = LOAD_U64_BIG(in);
    }
    return 0;
}

/**
 * Convert a little-endian 64-bit word array x[] into a big endian-encoded
 * number out[]. The number is left-padded with zeroes if required.
 */
static inline int words_to_bytes(uint8_t *out, size_t len, const uint64_t *x, size_t words)
{
    size_t i;
    const uint64_t *msw;
    uint8_t buf8[8];
    size_t partial, real_len;

    if (0 == words || 0 == len)
        return ERR_NOT_ENOUGH_DATA;
    if (NULL == x || NULL == out)
        return ERR_NULL;

    memset(out, 0, len);

    /* Shorten the input, so that the rightmost word is
     * the most significant one (and non-zero)
     */
    for (; words>0 && x[words-1]==0; words--);
    if (words == 0)
        return 0;
    msw = &x[words-1];

    /* Find how many non-zero bytes there are in the most-significant word */
    STORE_U64_BIG(buf8, *msw);
    for (partial=8; partial>0 && buf8[8-partial] == 0; partial--);
    assert(partial > 0);
    
    /** Check if there is enough room **/
    real_len = partial + 8*(words-1);
    if (real_len > len)
        return ERR_MAX_DATA;

    /** Pad **/
    out += len - real_len;

    /** Most significant word **/
    memcpy(out, buf8+(8-partial), partial);
    out += partial;
    msw--;

    /** Any remaining full word **/
    for (i=0; i<words-1; i++, out += 8, msw--)
        STORE_U64_BIG(out, *msw);

    return 0;
}

#endif
