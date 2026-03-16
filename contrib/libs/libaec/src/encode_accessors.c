/**
 * @file encode_accessors.c
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
 * Read various data types from input stream
 *
 */

#include "config.h"
#include "encode_accessors.h"
#include "encode.h"
#include "libaec.h"
#include <stdint.h>
#include <string.h>

uint32_t aec_get_8(struct aec_stream *strm)
{
    strm->avail_in--;
    return *strm->next_in++;
}

uint32_t aec_get_lsb_16(struct aec_stream *strm)
{
    uint32_t data = ((uint32_t)strm->next_in[1] << 8)
        | (uint32_t)strm->next_in[0];

    strm->next_in += 2;
    strm->avail_in -= 2;
    return data;
}

uint32_t aec_get_msb_16(struct aec_stream *strm)
{
    uint32_t data = ((uint32_t)strm->next_in[0] << 8)
        | (uint32_t)strm->next_in[1];

    strm->next_in += 2;
    strm->avail_in -= 2;
    return data;
}

uint32_t aec_get_lsb_24(struct aec_stream *strm)
{
    uint32_t data = ((uint32_t)strm->next_in[2] << 16)
        | ((uint32_t)strm->next_in[1] << 8)
        | (uint32_t)strm->next_in[0];

    strm->next_in += 3;
    strm->avail_in -= 3;
    return data;
}

uint32_t aec_get_msb_24(struct aec_stream *strm)
{
    uint32_t data = ((uint32_t)strm->next_in[0] << 16)
        | ((uint32_t)strm->next_in[1] << 8)
        | (uint32_t)strm->next_in[2];

    strm->next_in += 3;
    strm->avail_in -= 3;
    return data;
}

uint32_t aec_get_lsb_32(struct aec_stream *strm)
{
    uint32_t data = ((uint32_t)strm->next_in[3] << 24)
        | ((uint32_t)strm->next_in[2] << 16)
        | ((uint32_t)strm->next_in[1] << 8)
        | (uint32_t)strm->next_in[0];

    strm->next_in += 4;
    strm->avail_in -= 4;
    return data;
}

uint32_t aec_get_msb_32(struct aec_stream *strm)
{
    uint32_t data = ((uint32_t)strm->next_in[0] << 24)
        | ((uint32_t)strm->next_in[1] << 16)
        | ((uint32_t)strm->next_in[2] << 8)
        | (uint32_t)strm->next_in[3];

    strm->next_in += 4;
    strm->avail_in -= 4;
    return data;
}

void aec_get_rsi_8(struct aec_stream *strm)
{
    uint32_t *out = strm->state->data_raw;
    unsigned const char *in = strm->next_in;
    int rsi = strm->rsi *strm->block_size;

    for (int i = 0; i < rsi; i++)
        out[i] = (uint32_t)in[i];

    strm->next_in += rsi;
    strm->avail_in -= rsi;
}

void aec_get_rsi_lsb_16(struct aec_stream *strm)
{
    uint32_t *out = strm->state->data_raw;
    const unsigned char *in = strm->next_in;
    int rsi = strm->rsi *strm->block_size;

    for (int i = 0; i < rsi; i++)
        out[i] = (uint32_t)in[2 * i] | ((uint32_t)in[2 * i + 1] << 8);

    strm->next_in += 2 * rsi;
    strm->avail_in -= 2 * rsi;
}

void aec_get_rsi_msb_16(struct aec_stream *strm)
{
    uint32_t *out = strm->state->data_raw;
    const unsigned char *in = strm->next_in;
    int rsi = strm->rsi *strm->block_size;

    for (int i = 0; i < rsi; i++)
        out[i] = ((uint32_t)in[2 * i] << 8) | (uint32_t)in[2 * i + 1];

    strm->next_in += 2 * rsi;
    strm->avail_in -= 2 * rsi;
}

void aec_get_rsi_lsb_24(struct aec_stream *strm)
{
    uint32_t *out = strm->state->data_raw;
    const unsigned char *in = strm->next_in;
    int rsi = strm->rsi *strm->block_size;

    for (int i = 0; i < rsi; i++)
        out[i] = (uint32_t)in[3 * i]
            | ((uint32_t)in[3 * i + 1] << 8)
            | ((uint32_t)in[3 * i + 2] << 16);

    strm->next_in += 3 * rsi;
    strm->avail_in -= 3 * rsi;
}

void aec_get_rsi_msb_24(struct aec_stream *strm)
{
    uint32_t *out = strm->state->data_raw;
    const unsigned char *in = strm->next_in;
    int rsi = strm->rsi *strm->block_size;

    for (int i = 0; i < rsi; i++)
        out[i] = ((uint32_t)in[3 * i] << 16)
            | ((uint32_t)in[3 * i + 1] << 8)
            | (uint32_t)in[3 * i + 2];

    strm->next_in += 3 * rsi;
    strm->avail_in -= 3 * rsi;
}

#define AEC_GET_RSI_NATIVE_32(BO)                       \
    void aec_get_rsi_##BO##_32(struct aec_stream *strm) \
    {                                               \
        int rsi = strm->rsi *strm->block_size;     \
        memcpy(strm->state->data_raw,               \
               strm->next_in, 4 * rsi);             \
        strm->next_in += 4 * rsi;                   \
        strm->avail_in -= 4 * rsi;                  \
    }

#ifdef WORDS_BIGENDIAN
void aec_get_rsi_lsb_32(struct aec_stream *strm)
{
    uint32_t *out = strm->state->data_raw;
    const unsigned char *in = strm->next_in;
    int rsi = strm->rsi *strm->block_size;

    for (int i = 0; i < rsi; i++)
        out[i] = (uint32_t)in[4 * i]
            | ((uint32_t)in[4 * i + 1] << 8)
            | ((uint32_t)in[4 * i + 2] << 16)
            | ((uint32_t)in[4 * i + 3] << 24);

    strm->next_in += 4 * rsi;
    strm->avail_in -= 4 * rsi;
}

AEC_GET_RSI_NATIVE_32(msb);

#else /* !WORDS_BIGENDIAN */
void aec_get_rsi_msb_32(struct aec_stream *strm)
{
    uint32_t *out = strm->state->data_raw;
    const unsigned char *in = strm->next_in;
    int rsi = strm->rsi *strm->block_size;

    strm->next_in += 4 * rsi;
    strm->avail_in -= 4 * rsi;

    for (int i = 0; i < rsi; i++)
        out[i] = ((uint32_t)in[4 * i] << 24)
            | ((uint32_t)in[4 * i + 1] << 16)
            | ((uint32_t)in[4 * i + 2] << 8)
            | (uint32_t)in[4 * i + 3];
}

AEC_GET_RSI_NATIVE_32(lsb)

#endif /* !WORDS_BIGENDIAN */
