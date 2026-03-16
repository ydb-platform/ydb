/**
 * @file decode.h
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

#ifndef DECODE_H
#define DECODE_H 1

#include "config.h"
#include <stdint.h>
#include <stddef.h>
#include "vector.h"

#define M_CONTINUE 1
#define M_EXIT 0
#define M_ERROR (-1)

#define MIN(a, b) (((a) < (b))? (a): (b))

#define SE_TABLE_SIZE 90

struct aec_stream;

struct internal_state {
    int (*mode)(struct aec_stream *);

    /* option ID */
    int id;

    /* bit length of code option identification key */
    int id_len;

    /* table maps IDs to states */
    int (**id_table)(struct aec_stream *);

    void (*flush_output)(struct aec_stream *);

    /* previous output for post-processing */
    int32_t last_out;

    /* minimum integer for post-processing */
    uint32_t xmin;

    /* maximum integer for post-processing */
    uint32_t xmax;

     /* length of uncompressed input block should be the longest
        legal block */
    uint32_t in_blklen;

    /* length of output block in bytes */
    uint32_t out_blklen;

    uint32_t sample_counter;

    /* accumulator for currently used bit sequence */
    uint64_t acc;

    /* bit pointer to the next unused bit in accumulator */
    int bitp;

    /* last fundamental sequence in accumulator */
    uint32_t fs;

    /* 1 if current block has reference sample */
    int ref;

    /* block_size minus reference sample if present */
    uint32_t encoded_block_size;

    /* 1 if postprocessor has to be used */
    int pp;

    /* storage size of samples in bytes */
    uint32_t bytes_per_sample;

    /* output buffer holding one reference sample interval */
    uint32_t *rsi_buffer;

    /* current position of output in rsi_buffer */
    uint32_t *rsip;

    /* rsi in bytes */
    size_t rsi_size;

    /* first not yet flushed byte in rsi_buffer */
    uint32_t *flush_start;

    /* table for decoding second extension option */
    int se_table[2 * (SE_TABLE_SIZE + 1)];

    /* RSI table */
    struct vector_t *offsets;
};

#endif /* DECODE_H */
