/**
 * @file encode.c
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
 * Adaptive Entropy Encoder
 * Based on the CCSDS recommended standard 121.0-B-3
 *
 */

#include "config.h"
#include "encode.h"
#include "encode_accessors.h"
#include "libaec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int m_get_block(struct aec_stream *strm);

static inline void emit(struct internal_state *state,
                        uint32_t data, int bits)
{
    /**
       Emit sequence of bits.
     */

    if (bits <= state->bits) {
        state->bits -= bits;
        *state->cds += (uint8_t)(data << state->bits);
    } else {
        bits -= state->bits;
        *state->cds++ += (uint8_t)((uint64_t)data >> bits);

        while (bits > 8) {
            bits -= 8;
            *state->cds++ = (uint8_t)(data >> bits);
        }

        state->bits = 8 - bits;
        *state->cds = (uint8_t)(data << state->bits);
    }
}

static inline void emitfs(struct internal_state *state, int fs)
{
    /**
       Emits a fundamental sequence.

       fs zero bits followed by one 1 bit.
     */

    for(;;) {
        if (fs < state->bits) {
            state->bits -= fs + 1;
            *state->cds += 1U << state->bits;
            break;
        } else {
            fs -= state->bits;
            *++state->cds = 0;
            state->bits = 8;
        }
    }
}

static inline void copy64(uint8_t *dst, uint64_t src)
{
    dst[0] = (uint8_t)(src >> 56);
    dst[1] = (uint8_t)(src >> 48);
    dst[2] = (uint8_t)(src >> 40);
    dst[3] = (uint8_t)(src >> 32);
    dst[4] = (uint8_t)(src >> 24);
    dst[5] = (uint8_t)(src >> 16);
    dst[6] = (uint8_t)(src >> 8);
    dst[7] = (uint8_t)src;
}

static inline void emitblock_fs(struct aec_stream *strm, int k, int ref)
{
    struct internal_state *state = strm->state;

    uint64_t acc = (uint64_t)*state->cds << 56;
    uint32_t used = 7 - state->bits; /* used bits in 64 bit accumulator */

    for (size_t i = ref; i < strm->block_size; i++) {
        used += (state->block[i] >> k) + 1;
        while (used > 63) {
            copy64(state->cds, acc);
            state->cds += 8;
            acc = 0;
            used -= 64;
        }
        acc |= UINT64_C(1) << (63 - used);
    }

    copy64(state->cds, acc);
    state->cds += used >> 3;
    state->bits = 7 - (used & 7);
}

static inline void emitblock(struct aec_stream *strm, int k, int ref)
{
    /**
       Emit the k LSB of a whole block of input data.
    */

    struct internal_state *state = strm->state;
    uint32_t *in = state->block + ref;
    uint32_t *in_end = state->block + strm->block_size;
    uint64_t mask = (UINT64_C(1) << k) - 1;
    uint8_t *o = state->cds;
    uint64_t a = *o;
    int p = state->bits;

    while(in < in_end) {
        a <<= 56;
        p = (p % 8) + 56;

        while (p > k && in < in_end) {
            p -= k;
            a += ((uint64_t)(*in++) & mask) << p;
        }

        switch (p & ~7) {
        case 0:
            o[0] = (uint8_t)(a >> 56);
            o[1] = (uint8_t)(a >> 48);
            o[2] = (uint8_t)(a >> 40);
            o[3] = (uint8_t)(a >> 32);
            o[4] = (uint8_t)(a >> 24);
            o[5] = (uint8_t)(a >> 16);
            o[6] = (uint8_t)(a >> 8);
            o += 7;
            break;
        case 8:
            o[0] = (uint8_t)(a >> 56);
            o[1] = (uint8_t)(a >> 48);
            o[2] = (uint8_t)(a >> 40);
            o[3] = (uint8_t)(a >> 32);
            o[4] = (uint8_t)(a >> 24);
            o[5] = (uint8_t)(a >> 16);
            a >>= 8;
            o += 6;
            break;
        case 16:
            o[0] = (uint8_t)(a >> 56);
            o[1] = (uint8_t)(a >> 48);
            o[2] = (uint8_t)(a >> 40);
            o[3] = (uint8_t)(a >> 32);
            o[4] = (uint8_t)(a >> 24);
            a >>= 16;
            o += 5;
            break;
        case 24:
            o[0] = (uint8_t)(a >> 56);
            o[1] = (uint8_t)(a >> 48);
            o[2] = (uint8_t)(a >> 40);
            o[3] = (uint8_t)(a >> 32);
            a >>= 24;
            o += 4;
            break;
        case 32:
            o[0] = (uint8_t)(a >> 56);
            o[1] = (uint8_t)(a >> 48);
            o[2] = (uint8_t)(a >> 40);
            a >>= 32;
            o += 3;
            break;
        case 40:
            o[0] = (uint8_t)(a >> 56);
            o[1] = (uint8_t)(a >> 48);
            a >>= 40;
            o += 2;
            break;
        case 48:
            *o++ = (uint8_t)(a >> 56);
            a >>= 48;
            break;
        default:
            a >>= 56;
            break;
        }
    }

    *o = (uint8_t)a;
    state->cds = o;
    state->bits = p % 8;
}

static void preprocess_unsigned(struct aec_stream *strm)
{
    /**
       Preprocess RSI of unsigned samples.

       Combining preprocessing and converting to uint32_t in one loop
       is slower due to the data dependence on x_i-1.
    */

    uint32_t D;
    struct internal_state *state = strm->state;
    const uint32_t *x = state->data_raw;
    uint32_t *d = state->data_pp;
    uint32_t xmax = state->xmax;
    uint32_t rsi = strm->rsi * strm->block_size - 1;

    state->ref = 1;
    state->ref_sample = x[0];
    d[0] = 0;
    for (size_t i = 0; i < rsi; i++) {
        if (x[i + 1] >= x[i]) {
            D = x[i + 1] - x[i];
            if (D <= x[i])
                d[i + 1] = 2 * D;
            else
                d[i + 1] = x[i + 1];
        } else {
            D = x[i] - x[i + 1];
            if (D <= xmax - x[i])
                d[i + 1] = 2 * D - 1;
            else
                d[i + 1] = xmax - x[i + 1];
        }
    }
    state->uncomp_len = (strm->block_size - 1) * strm->bits_per_sample;
}

static void preprocess_signed(struct aec_stream *strm)
{
    /**
       Preprocess RSI of signed samples.
    */

    uint32_t D;
    struct internal_state *state = strm->state;
    uint32_t *x = state->data_raw;
    uint32_t *d = state->data_pp;
    uint32_t xmax = state->xmax;
    uint32_t xmin = state->xmin;
    uint32_t rsi = strm->rsi * strm->block_size - 1;
    uint32_t m = UINT32_C(1) << (strm->bits_per_sample - 1);

    state->ref = 1;
    state->ref_sample = x[0];
    d[0] = 0;
    /* Sign extension */
    x[0] = (x[0] ^ m) - m;

    for (size_t i = 0; i < rsi; i++) {
        x[i + 1] = (x[i + 1] ^ m) - m;
        if ((int32_t)x[i + 1] < (int32_t)x[i]) {
            D = x[i] - x[i + 1];
            if (D <= xmax - x[i])
                d[i + 1] = 2 * D - 1;
            else
                d[i + 1] = xmax - x[i + 1];
        } else {
            D = x[i + 1] - x[i];
            if (D <= x[i] - xmin)
                d[i + 1] = 2 * D;
            else
                d[i + 1] = x[i + 1] - xmin;
        }
    }
    state->uncomp_len = (strm->block_size - 1) * strm->bits_per_sample;
}

static inline uint64_t block_fs(struct aec_stream *strm, int k)
{
    /**
       Sum FS of all samples in block for given splitting position.
    */

    struct internal_state *state = strm->state;
    uint64_t fs = 0;

    for (size_t i = 0; i < strm->block_size; i++)
        fs += (uint64_t)(state->block[i] >> k);

    return fs;
}

static uint32_t assess_splitting_option(struct aec_stream *strm)
{
    /**
       Length of CDS encoded with splitting option and optimal k.

       In Rice coding each sample in a block of samples is split at
       the same position into k LSB and bits_per_sample - k MSB. The
       LSB part is left binary and the MSB part is coded as a
       fundamental sequence a.k.a. unary (see CCSDS 121.0-B-3). The
       function of the length of the Coded Data Set (CDS) depending on
       k has exactly one minimum (see A. Kiely, IPN Progress Report
       42-159).

       To find that minimum with only a few costly evaluations of the
       CDS length, we start with the k of the previous CDS. K is
       increased and the CDS length evaluated. If the CDS length gets
       smaller, then we are moving towards the minimum. If the length
       increases, then the minimum will be found with smaller k.

       For increasing k we know that we will gain block_size bits in
       length through the larger binary part. If the FS length is less
       than the block size then a reduced FS part can't compensate the
       larger binary part. So we know that the CDS for k+1 will be
       larger than for k without actually computing the length. An
       analogue check can be done for decreasing k.
     */

    struct internal_state *state = strm->state;

    /* Block size of current block */
    uint64_t this_bs = strm->block_size - state->ref;

    /* CDS length minimum so far */
    uint64_t len_min = UINT64_MAX;

    int k = state->k;
    int k_min = k;

    /* 1 if we shouldn't reverse */
    int no_turn = (k == 0);

    /* Direction, 1 means increasing k, 0 decreasing k */
    int dir = 1;

    for (;;) {
        /* Length of FS part (not including 1s) */
        uint64_t fs_len = block_fs(strm, k);

        /* CDS length for current k */
        uint64_t len = fs_len + this_bs * (k + 1);

        if (len < len_min) {
            if (len_min < UINT64_MAX)
                no_turn = 1;

            len_min = len;
            k_min = k;

            if (dir) {
                if (fs_len < this_bs || k >= state->kmax) {
                    if (no_turn)
                        break;
                    k = state->k - 1;
                    dir = 0;
                    no_turn = 1;
                } else {
                    k++;
                }
            } else {
                if (fs_len >= this_bs || k == 0)
                    break;
                k--;
            }
        } else {
            if (no_turn)
                break;
            k = state->k - 1;
            dir = 0;
            no_turn = 1;
        }
    }
    state->k = k_min;

    return (uint32_t)len_min;
}

static uint32_t assess_se_option(struct aec_stream *strm)
{
    /**
       Length of CDS encoded with Second Extension option.

       If length is above limit just return UINT32_MAX.
    */

    struct internal_state *state = strm->state;
    uint32_t *block = state->block;
    uint64_t len = 1;

    for (size_t i = 0; i < strm->block_size; i += 2) {
        uint64_t d = (uint64_t)block[i] + (uint64_t)block[i + 1];
        len += d * (d + 1) / 2 + block[i + 1] + 1;
        if (len > state->uncomp_len)
            return UINT32_MAX;
    }

    return (uint32_t)len;
}

static void init_output(struct aec_stream *strm)
{
    /**
       Direct output to next_out if next_out can hold a Coded Data
       Set, use internal buffer otherwise.
    */

    struct internal_state *state = strm->state;

    if (strm->avail_out > state->cds_len) {
        if (!state->direct_out) {
            state->direct_out = 1;
            *strm->next_out = *state->cds;
            state->cds = strm->next_out;
        }
    } else {
        if (state->zero_blocks == 0 || state->direct_out) {
            /* copy leftover from last block */
            *state->cds_buf = *state->cds;
            state->cds = state->cds_buf;
        }
        state->direct_out = 0;
    }
}

/*
 *
 * FSM functions
 *
 */

static int m_flush_block_resumable(struct aec_stream *strm)
{
    /**
       Slow and restartable flushing
    */
    struct internal_state *state = strm->state;

    int n = (int)MIN((size_t)(state->cds - state->cds_buf - state->i),
                     strm->avail_out);
    memcpy(strm->next_out, state->cds_buf + state->i, n);
    strm->next_out += n;
    strm->avail_out -= n;
    state->i += n;

    if (strm->avail_out == 0) {
        return M_EXIT;
    } else {
        state->mode = m_get_block;
        return M_CONTINUE;
    }
}

static int m_flush_block(struct aec_stream *strm)
{
    /**
       Flush block in direct_out mode by updating counters.

       Fall back to slow flushing if in buffered mode.
    */
    struct internal_state *state = strm->state;

#ifdef ENABLE_RSI_PADDING
    if (state->blocks_avail == 0 &&
        strm->flags & AEC_PAD_RSI &&
        state->block_nonzero == 0)
            emit(state, 0, state->bits % 8);
#endif

    if (state->direct_out) {
        int n = (int)(state->cds - strm->next_out);
        strm->next_out += n;
        strm->avail_out -= n;
        state->mode = m_get_block;

        if (state->ready_to_capture_rsi &&
            state->blocks_avail == 0 &&
            state->offsets != NULL) {
                vector_push_back(state->offsets, (strm->total_out - strm->avail_out) * 8 + (8 - state->bits));
                state->ready_to_capture_rsi = 0;
        }

        return M_CONTINUE;
    }

    state->i = 0;
    state->mode = m_flush_block_resumable;
    return M_CONTINUE;
}

static int m_encode_splitting(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;
    int k = state->k;

    emit(state, k + 1, state->id_len);
    if (state->ref)
        emit(state, state->ref_sample, strm->bits_per_sample);

    emitblock_fs(strm, k, state->ref);
    if (k)
        emitblock(strm, k, state->ref);

    return m_flush_block(strm);
}

static int m_encode_uncomp(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    emit(state, (1U << state->id_len) - 1, state->id_len);
    if (state->ref)
        state->block[0] = state->ref_sample;
    emitblock(strm, strm->bits_per_sample, 0);
    return m_flush_block(strm);
}

static int m_encode_se(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    emit(state, 1, state->id_len + 1);
    if (state->ref)
        emit(state, state->ref_sample, strm->bits_per_sample);

    for (size_t i = 0; i < strm->block_size; i+= 2) {
        uint32_t d = state->block[i] + state->block[i + 1];
        emitfs(state, d * (d + 1) / 2 + state->block[i + 1]);
    }

    return m_flush_block(strm);
}

static int m_encode_zero(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    emit(state, 0, state->id_len + 1);

    if (state->zero_ref)
        emit(state, state->zero_ref_sample, strm->bits_per_sample);

    if (state->zero_blocks == ROS)
        emitfs(state, 4);
    else if (state->zero_blocks >= 5)
        emitfs(state, state->zero_blocks);
    else
        emitfs(state, state->zero_blocks - 1);

    state->zero_blocks = 0;
    return m_flush_block(strm);
}

static int m_select_code_option(struct aec_stream *strm)
{
    /**
       Decide which code option to use.
    */

    struct internal_state *state = strm->state;

    uint32_t split_len;
    uint32_t se_len;
    if (state->id_len > 1)
        split_len = assess_splitting_option(strm);
    else
        split_len = UINT32_MAX;
    se_len = assess_se_option(strm);

    if (split_len < state->uncomp_len) {
        if (split_len < se_len)
            return m_encode_splitting(strm);
        else
            return m_encode_se(strm);
    } else {
        if (state->uncomp_len <= se_len)
            return m_encode_uncomp(strm);
        else
            return m_encode_se(strm);
    }
}

static int m_check_zero_block(struct aec_stream *strm)
{
    /**
       Check if input block is all zero.

       Aggregate consecutive zero blocks until we find !0 or reach the
       end of a segment or RSI.
    */

    struct internal_state *state = strm->state;
    uint32_t *p = state->block;

    size_t i;
    for (i = 0; i < strm->block_size; i++)
        if (p[i] != 0)
            break;

    if (i < strm->block_size) {
        if (state->zero_blocks) {
            /* The current block isn't zero but we have to emit a
             * previous zero block first. The current block will be
             * flagged and handled later.
             */
            state->block_nonzero = 1;
            state->mode = m_encode_zero;
            return M_CONTINUE;
        }
        state->mode = m_select_code_option;
        return M_CONTINUE;
    } else {
        state->zero_blocks++;
        if (state->zero_blocks == 1) {
            state->zero_ref = state->ref;
            state->zero_ref_sample = state->ref_sample;
        }
        if (state->blocks_avail == 0 || state->blocks_dispensed % 64 == 0) {
            if (state->zero_blocks > 4)
                state->zero_blocks = ROS;

            state->mode = m_encode_zero;
            return M_CONTINUE;
        }
        state->mode = m_get_block;
        return M_CONTINUE;
    }
}

static int m_get_rsi_resumable(struct aec_stream *strm)
{
    /**
       Get RSI while input buffer is short.

       Let user provide more input. Once we got all input, pad buffer
       to full RSI.
    */

    struct internal_state *state = strm->state;

    do {
        if (strm->avail_in >= state->bytes_per_sample) {
            state->data_raw[state->i] = state->get_sample(strm);
        } else {
            if (state->flush == AEC_FLUSH) {
                if (state->i > 0) {
                    state->blocks_avail = state->i / strm->block_size - 1;
                    if (state->i % strm->block_size)
                        state->blocks_avail++;
                    /* Pad raw buffer with last sample. Only encode
                     * blocks_avail will be encoded later. */
                    do
                        state->data_raw[state->i] =
                            state->data_raw[state->i - 1];
                    while(++state->i < strm->rsi * strm->block_size);
                } else {
                    /* Finish encoding by padding the last byte with
                     * zero bits. */
                    emit(state, 0, state->bits);
                    if (strm->avail_out > 0) {
                        if (!state->direct_out)
                            *strm->next_out++ = *state->cds;
                        strm->avail_out--;
                        state->flushed = 1;
                    }
                    return M_EXIT;
                }
            } else {
                return M_EXIT;
            }
        }
    } while (++state->i < strm->rsi * strm->block_size);

    if (strm->flags & AEC_DATA_PREPROCESS)
        state->preprocess(strm);

    return m_check_zero_block(strm);
}

static int m_get_block(struct aec_stream *strm)
{
    /**
       Provide the next block of preprocessed input data.

       Pull in a whole Reference Sample Interval (RSI) of data if
       block buffer is empty.
    */

    struct internal_state *state = strm->state;

    init_output(strm);

    if (state->block_nonzero) {
        state->block_nonzero = 0;
        state->mode = m_select_code_option;
        return M_CONTINUE;
    }

    if (state->blocks_avail == 0) {
        state->blocks_avail = strm->rsi - 1;
        state->block = state->data_pp;
        state->blocks_dispensed = 1;
        if (strm->avail_in >= state->rsi_len) {
            state->ready_to_capture_rsi = 1;
            state->get_rsi(strm);
            if (strm->flags & AEC_DATA_PREPROCESS)
                state->preprocess(strm);

            return m_check_zero_block(strm);
        } else {
            state->i = 0;
            state->mode = m_get_rsi_resumable;
        }
    } else {
        if (state->ref) {
            state->ref = 0;
            state->uncomp_len = strm->block_size * strm->bits_per_sample;
        }
        state->block += strm->block_size;
        state->blocks_dispensed++;
        state->blocks_avail--;
        return m_check_zero_block(strm);
    }
    return M_CONTINUE;
}

static void cleanup(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    if (strm->flags & AEC_DATA_PREPROCESS && state->data_raw)
        free(state->data_raw);
    if (state->data_pp)
        free(state->data_pp);
    if (state->cds_buf)
        free(state->cds_buf);
    free(state);
}

/*
 *
 * API functions
 *
 */

int aec_encode_init(struct aec_stream *strm)
{
    struct internal_state *state;

    if (strm->bits_per_sample > 32
        || strm->bits_per_sample == 0
        || strm->rsi > 4096
        || strm->rsi == 0)
        return AEC_CONF_ERROR;

    if (strm->flags & AEC_NOT_ENFORCE) {
        /* Allow non-standard block sizes */
        if (strm->block_size & 1
            || strm->block_size == 0
            || strm->block_size > 256)
            return AEC_CONF_ERROR;
    } else {
        /* Only allow standard conforming block sizes */
        if (strm->block_size != 8
            && strm->block_size != 16
            && strm->block_size != 32
            && strm->block_size != 64)
            return AEC_CONF_ERROR;
    }

    state = malloc(sizeof(struct internal_state));
    if (state == NULL)
        return AEC_MEM_ERROR;

    memset(state, 0, sizeof(struct internal_state));
    strm->state = state;
    state->uncomp_len = strm->block_size * strm->bits_per_sample;

    if (strm->bits_per_sample > 16) {
        /* 24/32 input bit settings */
        state->id_len = 5;

        if (strm->bits_per_sample <= 24
            && strm->flags & AEC_DATA_3BYTE) {
            state->bytes_per_sample = 3;
            if (strm->flags & AEC_DATA_MSB) {
                state->get_sample = aec_get_msb_24;
                state->get_rsi = aec_get_rsi_msb_24;
            } else {
                state->get_sample = aec_get_lsb_24;
                state->get_rsi = aec_get_rsi_lsb_24;
            }
        } else {
            state->bytes_per_sample = 4;
            if (strm->flags & AEC_DATA_MSB) {
                state->get_sample = aec_get_msb_32;
                state->get_rsi = aec_get_rsi_msb_32;
            } else {
                state->get_sample = aec_get_lsb_32;
                state->get_rsi = aec_get_rsi_lsb_32;
            }
        }
    }
    else if (strm->bits_per_sample > 8) {
        /* 16 bit settings */
        state->id_len = 4;
        state->bytes_per_sample = 2;

        if (strm->flags & AEC_DATA_MSB) {
            state->get_sample = aec_get_msb_16;
            state->get_rsi = aec_get_rsi_msb_16;
        } else {
            state->get_sample = aec_get_lsb_16;
            state->get_rsi = aec_get_rsi_lsb_16;
        }
    } else {
        /* 8 bit settings */
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

        state->get_sample = aec_get_8;
        state->get_rsi = aec_get_rsi_8;
    }
    state->rsi_len = strm->rsi * strm->block_size * state->bytes_per_sample;

    if (strm->flags & AEC_DATA_SIGNED) {
        state->xmax = (INT64_C(1) << (strm->bits_per_sample - 1)) - 1;
        state->xmin = ~state->xmax;
        state->preprocess = preprocess_signed;
    } else {
        state->xmax = (UINT64_C(1) << strm->bits_per_sample) - 1;
        state->xmin = 0;
        state->preprocess = preprocess_unsigned;
    }

    state->kmax = (1U << state->id_len) - 3;

    /* Maximum CDS length. We need extra 8 bytes for copy64() */
    state->cds_len = (state->id_len + strm->block_size
                      * strm->bits_per_sample) / 8 + 1 + 8;
    state->cds_buf = malloc(state->cds_len);
    if (state->cds_buf == NULL) {
        cleanup(strm);
        return AEC_MEM_ERROR;
    }
    state->data_pp = malloc(strm->rsi
                            * strm->block_size
                            * sizeof(uint32_t));
    if (state->data_pp == NULL) {
        cleanup(strm);
        return AEC_MEM_ERROR;
    }

    if (strm->flags & AEC_DATA_PREPROCESS) {
        state->data_raw = malloc(strm->rsi
                                 * strm->block_size
                                 * sizeof(uint32_t));
        if (state->data_raw == NULL) {
            cleanup(strm);
            return AEC_MEM_ERROR;
        }
    } else {
        state->data_raw = state->data_pp;
    }

    state->block = state->data_pp;

    state->ref = 0;
    strm->total_in = 0;
    strm->total_out = 0;
    state->flushed = 0;

    state->cds = state->cds_buf;
    *state->cds = 0;
    state->bits = 8;
    state->mode = m_get_block;
    state->ready_to_capture_rsi = 0;
    return AEC_OK;
}

int aec_encode(struct aec_stream *strm, int flush)
{
    /**
       Finite-state machine implementation of the adaptive entropy
       encoder.
    */
    struct internal_state *state = strm->state;

    state->flush = flush;
    strm->total_in += strm->avail_in;
    strm->total_out += strm->avail_out;

    while (state->mode(strm) == M_CONTINUE);

    if (state->direct_out) {
        int n = (int)(state->cds - strm->next_out);
        strm->next_out += n;
        strm->avail_out -= n;

        *state->cds_buf = *state->cds;
        state->cds = state->cds_buf;
        state->direct_out = 0;
    }
    strm->total_in -= strm->avail_in;
    strm->total_out -= strm->avail_out;
    return AEC_OK;
}

int aec_encode_end(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    int status = AEC_OK;
    if (state->flush == AEC_FLUSH && state->flushed == 0)
        status = AEC_STREAM_ERROR;
    if (state->offsets != NULL) {
        vector_destroy(state->offsets);
        state->offsets = NULL;
    }
    cleanup(strm);
    return status;
}

int aec_encode_count_offsets(struct aec_stream *strm, size_t *count)
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

int aec_encode_get_offsets(struct aec_stream *strm, size_t *offsets, size_t offsets_count)
{
    struct internal_state *state = strm->state;
    if (state->offsets == NULL) {
        return AEC_RSI_OFFSETS_ERROR;
    }
    if (offsets_count < vector_size(state->offsets)) {
        return AEC_MEM_ERROR;
    }
    memcpy(offsets, vector_data(state->offsets), offsets_count * sizeof(size_t));
    return AEC_OK;
}

int aec_encode_enable_offsets(struct aec_stream *strm)
{
    struct internal_state *state = strm->state;

    if (state->offsets != NULL)
        return AEC_RSI_OFFSETS_ERROR;

    state->offsets = vector_create();
    vector_push_back(state->offsets, 0);
    return AEC_OK;
}

int aec_buffer_encode(struct aec_stream *strm)
{
    int status = aec_encode_init(strm);
    if (status != AEC_OK)
        return status;
    status = aec_encode(strm, AEC_FLUSH);
    if (status != AEC_OK) {
        cleanup(strm);
        return status;
    }
    return aec_encode_end(strm);
}
