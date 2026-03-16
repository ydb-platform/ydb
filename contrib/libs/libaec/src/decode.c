/**
 * @file decode.c
 *
 * @section LICENSE
 * Copyright 2026 Mathis Rosenhauer, Moritz Hanke, Joerg Behrens, Luis Kornblueh
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials provided
 *    with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @section DESCRIPTION
 *
 * Adaptive Entropy Decoder
 * Based on the CCSDS recommended standard 121.0-B-3
 *
 */

#include "config.h"
#include "decode.h"
#include "libaec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_BSR64
#include <intrin.h>
#endif

#define ROS 5
#define RSI_USED_SIZE(state) ((size_t)(state->rsip - state->rsi_buffer))
#define BUFFERSPACE(strm) (strm->avail_in >= strm->state->in_blklen      \
                           && strm->avail_out >= strm->state->out_blklen)

#define FLUSH(KIND)                                                      \
    static void flush_##KIND(struct aec_stream *strm)                    \
    {                                                                    \
        uint32_t *flush_end, *bp, half_d;                                \
        uint32_t xmax, d, data, m;                                       \
        struct internal_state *state = strm->state;                      \
                                                                         \
        flush_end = state->rsip;                                         \
        if (state->pp) {                                                 \
            if (state->flush_start == state->rsi_buffer                  \
                && state->rsip > state->rsi_buffer) {                    \
                state->last_out = *state->rsi_buffer;                    \
                                                                         \
                if (strm->flags & AEC_DATA_SIGNED) {                     \
                    m = UINT32_C(1) << (strm->bits_per_sample - 1);      \
                    /* Reference samples have to be sign extended */     \
                    state->last_out = (state->last_out ^ m) - m;         \
                }                                                        \
                put_##KIND(strm, (uint32_t)state->last_out);             \
                state->flush_start++;                                    \
            }                                                            \
                                                                         \
            data = state->last_out;                                      \
            xmax = state->xmax;                                          \
                                                                         \
            if (state->xmin == 0) {                                      \
                uint32_t med;                                            \
                med = state->xmax / 2 + 1;                               \
                                                                         \
                for (bp = state->flush_start; bp < flush_end; bp++) {    \
                    uint32_t mask;                                       \
                    d = *bp;                                             \
                    half_d = (d >> 1) + (d & 1);                         \
                    /*in this case: data >= med == data & med */         \
                    mask = (data & med)?xmax:0;                          \
                                                                         \
                    /*in this case: xmax - data == xmax ^ data */        \
                    if (half_d <= (mask ^ data)) {                       \
                        data += (d >> 1)^(~((d & 1) - 1));               \
                    } else {                                             \
                        data = mask ^ d;                                 \
                    }                                                    \
                    put_##KIND(strm, data);                              \
                }                                                        \
                state->last_out = data;                                  \
            } else {                                                     \
                for (bp = state->flush_start; bp < flush_end; bp++) {    \
                    d = *bp;                                             \
                    half_d = (d >> 1) + (d & 1);                         \
                                                                         \
                    if ((int32_t)data < 0) {                             \
                        if (half_d <= xmax + data + 1) {                 \
                            data += (d >> 1)^(~((d & 1) - 1));           \
                        } else {                                         \
                            data = d - xmax - 1;                         \
                        }                                                \
                    } else {                                             \
                        if (half_d <= xmax - data) {                     \
                            data += (d >> 1)^(~((d & 1) - 1));           \
                        } else {                                         \
                            data = xmax - d;                             \
                        }                                                \
                    }                                                    \
                    put_##KIND(strm, data);                              \
                }                                                        \
                state->last_out = data;                                  \
            }                                                            \
        } else {                                                         \
            for (bp = state->flush_start; bp < flush_end; bp++)          \
                put_##KIND(strm, *bp);                                   \
        }                                                                \
        state->flush_start = state->rsip;                                \
    }


static inline void put_msb_32(struct aec_stream *strm, uint32_t data)
{
    *strm->next_out++ = (unsigned char)(data >> 24);
    *strm->next_out++ = (unsigned char)(data >> 16);
    *strm->next_out++ = (unsigned char)(data >> 8);
    *strm->next_out++ = (unsigned char)data;
}

static inline void put_msb_24(struct aec_stream *strm, uint32_t data)
{
    *strm->next_out++ = (unsigned char)(data >> 16);
    *strm->next_out++ = (unsigned char)(data >> 8);
    *strm->next_out++ = (unsigned char)data;
}

static inline void put_msb_16(struct aec_stream *strm, uint32_t data)
{
    *strm->next_out++ = (unsigned char)(data >> 8);
    *strm->next_out++ = (unsigned char)data;
}

static inline void put_lsb_32(struct aec_stream *strm, uint32_t data)
{
    *strm->next_out++ = (unsigned char)data;
    *strm->next_out++ = (unsigned char)(data >> 8);
    *strm->next_out++ = (unsigned char)(data >> 16);
    *strm->next_out++ = (unsigned char)(data >> 24);
}

static inline void put_lsb_24(struct aec_stream *strm, uint32_t data)
{
    *strm->next_out++ = (unsigned char)data;
    *strm->next_out++ = (unsigned char)(data >> 8);
    *strm->next_out++ = (unsigned char)(data >> 16);
}

static inline void put_lsb_16(struct aec_stream *strm, uint32_t data)
{
    *strm->next_out++ = (unsigned char)data;
    *strm->next_out++ = (unsigned char)(data >> 8);
}

static inline void put_8(struct aec_stream *strm, uint32_t data)
{
    *strm->next_out++ = (unsigned char)data;
}

FLUSH(msb_32)
FLUSH(msb_24)
FLUSH(msb_16)
FLUSH(lsb_32)
FLUSH(lsb_24)
FLUSH(lsb_16)
FLUSH(8)

static inline void put_sample(struct aec_stream *strm, uint32_t s)
{
    struct internal_state *state = strm->state;

    *state->rsip++ = s;
    strm->avail_out -= state->bytes_per_sample;
}

static inline uint32_t direct_get(struct aec_stream *strm, int n)
{
    /**
       Get n bit from input stream

       No checking whatsoever. Read bits are dumped.
     */

    struct internal_state *state = strm->state;
    if (state->bitp < n)
    {
        int b = (63 - state->bitp) >> 3;
        if (b == 6) {
            state->acc = (state->acc << 48)
                | ((uint64_t)strm->next_in[0] << 40)
                | ((uint64_t)strm->next_in[1] << 32)
                | ((uint64_t)strm->next_in[2] << 24)
                | ((uint64_t)strm->next_in[3] << 16)
                | ((uint64_t)strm->next_in[4] << 8)
                | (uint64_t)strm->next_in[5];
        } else if (b == 7) {
            state->acc = (state->acc << 56)
                | ((uint64_t)strm->next_in[0] << 48)
                | ((uint64_t)strm->next_in[1] << 40)
                | ((uint64_t)strm->next_in[2] << 32)
                | ((uint64_t)strm->next_in[3] << 24)
                | ((uint64_t)strm->next_in[4] << 16)
                | ((uint64_t)strm->next_in[5] << 8)
                | (uint64_t)strm->next_in[6];
        } else if (b == 5) {
            state->acc = (state->acc << 40)
                | ((uint64_t)strm->next_in[0] << 32)
                | ((uint64_t)strm->next_in[1] << 24)
                | ((uint64_t)strm->next_in[2] << 16)
                | ((uint64_t)strm->next_in[3] << 8)
                | (uint64_t)strm->next_in[4];
        } else if (b == 4) {
            state->acc = (state->acc << 32)
                | ((uint64_t)strm->next_in[0] << 24)
                | ((uint64_t)strm->next_in[1] << 16)
                | ((uint64_t)strm->next_in[2] << 8)
                | (uint64_t)strm->next_in[3];
        } else if (b == 3) {
            state->acc = (state->acc << 24)
                | ((uint64_t)strm->next_in[0] << 16)
                | ((uint64_t)strm->next_in[1] << 8)
                | (uint64_t)strm->next_in[2];
        } else if (b == 2) {
            state->acc = (state->acc << 16)
                | ((uint64_t)strm->next_in[0] << 8)
                | (uint64_t)strm->next_in[1];
        } else if (b == 1) {
            state->acc = (state->acc << 8)
                | (uint64_t)strm->next_in[0];
        }
        strm->next_in += b;
        strm->avail_in -= b;
        state->bitp += b << 3;
    }

    state->bitp -= n;
    return (state->acc >> state->bitp) & (UINT64_MAX >> (64 - n));
}

static inline uint32_t direct_get_fs(struct aec_stream *strm)
{
    /**
       Interpret a Fundamental Sequence from the input buffer.

       Essentially counts the number of 0 bits until a 1 is
       encountered.
     */

    uint32_t fs = 0;
    struct internal_state *state = strm->state;

    if (state->bitp)
        state->acc &= UINT64_MAX >> (64 - state->bitp);
    else
        state->acc = 0;

    while (state->acc == 0) {
        if (strm->avail_in < 7)
            return 0;

        state->acc = (state->acc << 56)
            | ((uint64_t)strm->next_in[0] << 48)
            | ((uint64_t)strm->next_in[1] << 40)
            | ((uint64_t)strm->next_in[2] << 32)
            | ((uint64_t)strm->next_in[3] << 24)
            | ((uint64_t)strm->next_in[4] << 16)
            | ((uint64_t)strm->next_in[5] << 8)
            | (uint64_t)strm->next_in[6];
        strm->next_in += 7;
        strm->avail_in -= 7;
        fs += state->bitp;
        state->bitp = 56;
    }

    {
#ifndef __has_builtin
#define __has_builtin(x) 0  /* Compatibility with non-clang compilers. */
#endif
#if HAVE_DECL___BUILTIN_CLZLL || __has_builtin(__builtin_clzll)
        int i = 63 - __builtin_clzll(state->acc);
#elif defined HAVE_BSR64
        unsigned long i;
        _BitScanReverse64(&i, state->acc);
#else
        int i = state->bitp - 1;
        while ((state->acc & (UINT64_C(1) << i)) == 0)
            i--;
#endif
        fs += state->bitp - i - 1;
        state->bitp = i;
    }
    return fs;
}

static inline uint32_t bits_ask(struct aec_stream *strm, int n)
{
    while (strm->state->bitp < n) {
        if (strm->avail_in == 0)
            return 0;
        strm->avail_in--;
        strm->state->acc <<= 8;
        strm->state->acc |= *strm->next_in++;
        strm->state->bitp += 8;
    }
    return 1;
}

static inline uint32_t bits_get(struct aec_stream *strm, int n)
{
    return (strm->state->acc >> (strm->state->bitp - n))
        & (UINT64_MAX >> (64 - n));
}

static inline void bits_drop(struct aec_stream *strm, int n)
{
    strm->state->bitp -= n;
}

static inline uint32_t fs_ask(struct aec_stream *strm)
{
    if (bits_ask(strm, 1) == 0)
        return 0;
    while ((strm->state->acc & (UINT64_C(1) << (strm->state->bitp - 1))) == 0) {
        if (strm->state->bitp == 1) {
            if (strm->avail_in == 0)
                return 0;
            strm->avail_in--;
            strm->state->acc <<= 8;
            strm->state->acc |= *strm->next_in++;
            strm->state->bitp += 8;
        }
        strm->state->fs++;
        strm->state->bitp--;
    }
    return 1;
}

static inline void fs_drop(struct aec_stream *strm)
{
    strm->state->fs = 0;
    strm->state->bitp--;
}

static inline uint32_t copysample(struct aec_stream *strm)
{
    if (bits_ask(strm, strm->bits_per_sample) == 0
        || strm->avail_out < strm->state->bytes_per_sample)
        return 0;

    put_sample(strm, bits_get(strm, strm->bits_per_sample));
    bits_drop(strm, strm->bits_per_sample);
    return 1;
}

static inline int m_id(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;
    if (strm->avail_in >= strm->state->in_blklen) {
        state->id = direct_get(strm, state->id_len);
    } else {
        if (bits_ask(strm, state->id_len) == 0) {
            state->mode = m_id;
            return M_EXIT;
        }
        state->id = bits_get(strm, state->id_len);
        bits_drop(strm, state->id_len);
    }
    state->mode = state->id_table[state->id];
    return(state->mode(strm));
}

static int m_next_cds(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    if ((state->offsets != NULL) && (state->rsi_size == RSI_USED_SIZE(state)))
        vector_push_back(
            state->offsets,
            strm->total_in * 8 - (strm->avail_in * 8 + state->bitp));

    if (state->rsi_size == RSI_USED_SIZE(state)) {
        state->flush_output(strm);
        state->flush_start = state->rsi_buffer;
        state->rsip = state->rsi_buffer;
        if (state->pp) {
            state->ref = 1;
            state->encoded_block_size = strm->block_size - 1;
        }
        if (strm->flags & AEC_PAD_RSI)
            state->bitp -= state->bitp % 8;
    } else {
        state->ref = 0;
        state->encoded_block_size = strm->block_size;
    }
    return m_id(strm);
}

static int m_split_output(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;
    int k = state->id - 1;

    do {
        if (bits_ask(strm, k) == 0 || strm->avail_out < state->bytes_per_sample)
            return M_EXIT;
        if (k)
            *state->rsip++ += bits_get(strm, k);
        else
            state->rsip++;
        strm->avail_out -= state->bytes_per_sample;
        bits_drop(strm, k);
    } while(++state->sample_counter < state->encoded_block_size);

    state->mode = m_next_cds;
    return M_CONTINUE;
}

static int m_split_fs(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;
    int k = state->id - 1;

    do {
        if (fs_ask(strm) == 0)
            return M_EXIT;
        state->rsip[state->sample_counter] = state->fs << k;
        fs_drop(strm);
    } while(++state->sample_counter < state->encoded_block_size);

    state->sample_counter = 0;
    state->mode = m_split_output;

    return M_CONTINUE;
}

static int m_split(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    if (BUFFERSPACE(strm)) {
        int k = state->id - 1;
        size_t binary_part = (k * state->encoded_block_size) / 8 + 9;

        if (state->ref)
            *state->rsip++ = direct_get(strm, strm->bits_per_sample);

        for (size_t i = 0; i < state->encoded_block_size; i++)
            state->rsip[i] = direct_get_fs(strm) << k;

        if (k) {
            if (strm->avail_in < binary_part)
                return M_ERROR;

            for (size_t i = 0; i < state->encoded_block_size; i++)
                *state->rsip++ += direct_get(strm, k);
        } else {
            state->rsip += state->encoded_block_size;
        }

        strm->avail_out -= state->out_blklen;
        state->mode = m_next_cds;
    } else {
        if (state->ref && (copysample(strm) == 0))
            return M_EXIT;
        state->sample_counter = 0;
        state->mode = m_split_fs;
    }
    return M_CONTINUE;
}

static int m_zero_output(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    do {
        if (strm->avail_out < state->bytes_per_sample)
            return M_EXIT;
        put_sample(strm, 0);
    } while(--state->sample_counter);

    state->mode = m_next_cds;
    return M_CONTINUE;
}

static int m_zero_block(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;
    uint32_t zero_blocks;
    uint32_t zero_samples;
    uint32_t zero_bytes;

    if (fs_ask(strm) == 0)
        return M_EXIT;

    zero_blocks = state->fs + 1;
    fs_drop(strm);

    if (zero_blocks == ROS) {
        int b = (int)RSI_USED_SIZE(state) / strm->block_size;
        zero_blocks = MIN((int)(strm->rsi - b), 64 - (b % 64));
    } else if (zero_blocks > ROS) {
        zero_blocks--;
    }

    zero_samples = zero_blocks * strm->block_size - state->ref;
    if (state->rsi_size - RSI_USED_SIZE(state) < zero_samples)
        return M_ERROR;

    zero_bytes = zero_samples * state->bytes_per_sample;
    if (strm->avail_out >= zero_bytes) {
        memset(state->rsip, 0, zero_samples * sizeof(uint32_t));
        state->rsip += zero_samples;
        strm->avail_out -= zero_bytes;
        state->mode = m_next_cds;
    } else {
        state->sample_counter = zero_samples;
        state->mode = m_zero_output;
    }
    return M_CONTINUE;
}

static int m_se_decode(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    while(state->sample_counter < strm->block_size) {
        int32_t m;
        int32_t d1;
        if (fs_ask(strm) == 0)
            return M_EXIT;
        m = state->fs;
        if (m > SE_TABLE_SIZE)
            return M_ERROR;
        d1 = m - state->se_table[2 * m + 1];

        if ((state->sample_counter & 1) == 0) {
            if (strm->avail_out < state->bytes_per_sample)
                return M_EXIT;
            put_sample(strm, state->se_table[2 * m] - d1);
            state->sample_counter++;
        }

        if (strm->avail_out < state->bytes_per_sample)
            return M_EXIT;
        put_sample(strm, d1);
        state->sample_counter++;
        fs_drop(strm);
    }

    state->mode = m_next_cds;
    return M_CONTINUE;
}

static int m_se(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    if (BUFFERSPACE(strm)) {
        uint32_t i = state->ref;

        while (i < strm->block_size) {
            uint32_t m = direct_get_fs(strm);
            int32_t d1;

            if (m > SE_TABLE_SIZE)
                return M_ERROR;

            d1 = m - state->se_table[2 * m + 1];

            if ((i & 1) == 0) {
                put_sample(strm, state->se_table[2 * m] - d1);
                i++;
            }
            put_sample(strm, d1);
            i++;
        }
        state->mode = m_next_cds;
    } else {
        state->mode = m_se_decode;
        state->sample_counter = state->ref;
    }
    return M_CONTINUE;
}

static int m_low_entropy_ref(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    if (state->ref && copysample(strm) == 0)
        return M_EXIT;

    if (state->id == 1)
        state->mode = m_se;
    else
        state->mode = m_zero_block;
    return M_CONTINUE;
}

static int m_low_entropy(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    if (bits_ask(strm, 1) == 0)
        return M_EXIT;
    state->id = bits_get(strm, 1);
    bits_drop(strm, 1);
    state->mode = m_low_entropy_ref;
    return M_CONTINUE;
}

static int m_uncomp_copy(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    do {
        if (copysample(strm) == 0)
            return M_EXIT;
    } while(--state->sample_counter);

    state->mode = m_next_cds;
    return M_CONTINUE;
}

static int m_uncomp(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    if (BUFFERSPACE(strm)) {
        for (size_t i = 0; i < strm->block_size; i++)
            *state->rsip++ = direct_get(strm, strm->bits_per_sample);
        strm->avail_out -= state->out_blklen;
        state->mode = m_next_cds;
    } else {
        state->sample_counter = strm->block_size;
        state->mode = m_uncomp_copy;
    }
    return M_CONTINUE;
}

static void create_se_table(int *table)
{
    int k = 0;
    for (int i = 0; i < 13; i++) {
        int ms = k;
        for (int j = 0; j <= i; j++) {
            table[2 * k] = i;
            table[2 * k + 1] = ms;
            k++;
        }
    }
}

int aec_decode_init(struct aec_stream *strm)
{
    struct internal_state *state;
    int modi;

    if (strm->bits_per_sample > 32
        || strm->bits_per_sample == 0
        || strm->rsi == 0
        || strm->block_size & 1
        || strm->block_size == 0)
        return AEC_CONF_ERROR;

    state = malloc(sizeof(struct internal_state));
    if (state == NULL)
        return AEC_MEM_ERROR;
    memset(state, 0, sizeof(struct internal_state));

    create_se_table(state->se_table);

    strm->state = state;

    if (strm->bits_per_sample > 16) {
        state->id_len = 5;

        if (strm->bits_per_sample <= 24 && strm->flags & AEC_DATA_3BYTE) {
            state->bytes_per_sample = 3;
            if (strm->flags & AEC_DATA_MSB)
                state->flush_output = flush_msb_24;
            else
                state->flush_output = flush_lsb_24;
        } else {
            state->bytes_per_sample = 4;
            if (strm->flags & AEC_DATA_MSB)
                state->flush_output = flush_msb_32;
            else
                state->flush_output = flush_lsb_32;
        }
        state->out_blklen = strm->block_size * state->bytes_per_sample;
    }
    else if (strm->bits_per_sample > 8) {
        state->bytes_per_sample = 2;
        state->id_len = 4;
        state->out_blklen = strm->block_size * 2;
        if (strm->flags & AEC_DATA_MSB)
            state->flush_output = flush_msb_16;
        else
            state->flush_output = flush_lsb_16;
    } else {
        if (strm->flags & AEC_RESTRICTED) {
            if (strm->bits_per_sample <= 4) {
                if (strm->bits_per_sample <= 2)
                    state->id_len = 1;
                else
                    state->id_len = 2;
            } else {
                free(state);
                return AEC_CONF_ERROR;
            }
        } else {
            state->id_len = 3;
        }

        state->bytes_per_sample = 1;
        state->out_blklen = strm->block_size;
        state->flush_output = flush_8;
    }

    if (strm->flags & AEC_DATA_SIGNED) {
        state->xmax = (INT64_C(1) << (strm->bits_per_sample - 1)) - 1;
        state->xmin = ~state->xmax;
    } else {
        state->xmin = 0;
        state->xmax = (UINT64_C(1) << strm->bits_per_sample) - 1;
    }

    state->in_blklen = (strm->block_size * strm->bits_per_sample
                        + state->id_len) / 8 + 16;

    modi = 1UL << state->id_len;
    state->id_table = malloc(modi * sizeof(int (*)(struct aec_stream *)));
    if (state->id_table == NULL) {
        free(state);
        return AEC_MEM_ERROR;
    }

    state->id_table[0] = m_low_entropy;
    for (int i = 1; i < modi - 1; i++) {
        state->id_table[i] = m_split;
    }
    state->id_table[modi - 1] = m_uncomp;

    state->rsi_size = strm->rsi * strm->block_size;
    state->rsi_buffer = malloc(state->rsi_size * sizeof(uint32_t));
    if (state->rsi_buffer == NULL) {
        free(state->id_table);
        free(state);
        return AEC_MEM_ERROR;
    }

    state->pp = strm->flags & AEC_DATA_PREPROCESS;
    if (state->pp) {
        state->ref = 1;
        state->encoded_block_size = strm->block_size - 1;
    } else {
        state->ref = 0;
        state->encoded_block_size = strm->block_size;
    }
    strm->total_in = 0;
    strm->total_out = 0;

    state->rsip = state->rsi_buffer;
    state->flush_start = state->rsi_buffer;
    state->bitp = 0;
    state->fs = 0;
    state->mode = m_id;
    state->offsets = NULL;

    return AEC_OK;
}

int aec_decode(struct aec_stream *strm, int flush)
{
    /**
       Finite-state machine implementation of the adaptive entropy
       decoder.

       Can work with one byte input und one sample output buffers. If
       enough buffer space is available, then faster implementations
       of the states are called. Inspired by zlib.
    */

    struct internal_state *state = strm->state;
    int status;

    strm->total_in += strm->avail_in;
    strm->total_out += strm->avail_out;

    do {
        status = state->mode(strm);
    } while (status == M_CONTINUE);

    if (status == M_ERROR)
        return AEC_DATA_ERROR;

    if (status == M_EXIT && strm->avail_out > 0 &&
        strm->avail_out < state->bytes_per_sample)
        return AEC_MEM_ERROR;

    state->flush_output(strm);

    strm->total_in -= strm->avail_in;
    strm->total_out -= strm->avail_out;

    return AEC_OK;
}

int aec_decode_end(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;
    if (state->offsets != NULL)
        vector_destroy(state->offsets);

    free(state->id_table);
    free(state->rsi_buffer);
    free(state);

    return AEC_OK;
}

int aec_buffer_decode(struct aec_stream *strm)
{
    int status = aec_decode_init(strm);
    if (status != AEC_OK)
        return status;

    status = aec_decode(strm, AEC_FLUSH);
    aec_decode_end(strm);
    return status;
}

int aec_buffer_seek(struct aec_stream *strm, size_t offset)
{
    struct internal_state *state = strm->state;

    size_t byte_offset = offset / 8;
    unsigned char bit_offset = offset % 8;

    if (strm->avail_in < byte_offset)
        return AEC_MEM_ERROR;

    strm->next_in += byte_offset;
    strm->avail_in -= byte_offset;

    if (bit_offset > 0) {
        if (strm->avail_in < 1)
            return AEC_MEM_ERROR;

        state->acc = (uint64_t)strm->next_in[0];
        state->bitp = 8 - bit_offset;
        strm->next_in++;
        strm->avail_in--;
    }
    return AEC_OK;
}

int aec_decode_range(struct aec_stream *strm, const size_t *rsi_offsets, size_t rsi_offsets_count, size_t pos, size_t size)
{
    struct internal_state *state = strm->state;
    int status;
    size_t rsi_size;
    size_t rsi_n;
    unsigned char *out_tmp;
    struct aec_stream strm_tmp = *strm;

    if (state->pp) {
        state->ref = 1;
        state->encoded_block_size = strm->block_size - 1;
    } else {
        state->ref = 0;
        state->encoded_block_size = strm->block_size;
    }

    state->rsip = state->rsi_buffer;
    state->flush_start = state->rsi_buffer;
    state->bitp = 0;
    state->fs = 0;
    state->mode = m_id;

    rsi_size = strm->rsi * strm->block_size * state->bytes_per_sample;
    rsi_n = pos / rsi_size;
    if (rsi_n >= rsi_offsets_count)
        return AEC_DATA_ERROR;

    /* resize and align to bytes_per_sample */
    strm_tmp.total_out = 0;
    strm_tmp.avail_out = size + pos % rsi_size + 1;
    strm_tmp.avail_out += state->bytes_per_sample - strm_tmp.avail_out % state->bytes_per_sample;
    if ((out_tmp = malloc(strm_tmp.avail_out)) == NULL)
        return AEC_MEM_ERROR;
    strm_tmp.next_out = out_tmp;

    if ((status = aec_buffer_seek(&strm_tmp, rsi_offsets[rsi_n])) != AEC_OK)
        return status;

    if ((status = aec_decode(&strm_tmp, AEC_FLUSH)) != 0)
        return status;

    memcpy(strm->next_out, out_tmp + (pos - rsi_n * rsi_size), size);

    strm->next_out += size;
    strm->avail_out -= size;
    strm->total_out += size;
    free(out_tmp);

    return AEC_OK;
}

int aec_decode_count_offsets(struct aec_stream *strm, size_t *count)
{
    struct internal_state *state = strm->state;
    if (state->offsets == NULL) {
        *count = 0;
        return AEC_RSI_OFFSETS_ERROR;
    } else {
        *count = vector_size(state->offsets);
    }
    return AEC_OK;
}

int aec_decode_get_offsets(struct aec_stream *strm, size_t *offsets,
                           size_t offsets_count)
{
    struct internal_state *state = strm->state;
    if (state->offsets == NULL) {
        return AEC_RSI_OFFSETS_ERROR;
    }
    if (offsets_count < vector_size(state->offsets)) {
        return AEC_MEM_ERROR;
    }
    memcpy(offsets, vector_data(state->offsets),
           vector_size(state->offsets) * sizeof(size_t));
    return AEC_OK;
}

int aec_decode_enable_offsets(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;
    if (state->offsets != NULL) {
        return AEC_RSI_OFFSETS_ERROR;
    }
    state->offsets = vector_create();
    vector_push_back(state->offsets, 0);
    return AEC_OK;
}
