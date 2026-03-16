/* ===================================================================
 *
 * Copyright (c) 2014, Legrandin <helderijs@gmail.com>
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

FAKE_INIT(raw_ocb)

#include "block_base.h"
#include <assert.h>
#include <stdio.h>

#define BLOCK_SIZE 16

typedef uint8_t DataBlock[BLOCK_SIZE];

typedef struct {
    BlockBase   *cipher;

    DataBlock   L_star;
    DataBlock   L_dollar;
    DataBlock   L[65];  /** 0..64 **/

    /** Associated data **/
    uint64_t    counter_A;
    DataBlock   offset_A;
    DataBlock   sum;

    /** Ciphertext/plaintext **/
    uint64_t    counter_P;
    DataBlock   offset_P;
    DataBlock   checksum;
} OcbModeState;

static void double_L(DataBlock *out, DataBlock *in)
{
    unsigned carry;
    int i;

    carry = 0;
    for (i=BLOCK_SIZE-1; i>=0; i--) {
        unsigned t;

        t = ((unsigned)(*in)[i] << 1) | carry;
        carry = t >> 8;
        (*out)[i] = (uint8_t)t;
    }
    carry |= 0x100;
    carry |= carry << 1;
    carry |= carry << 2;
    carry |= carry << 4;
    (*out)[BLOCK_SIZE-1] = (uint8_t)((*out)[BLOCK_SIZE-1] ^ (carry & 0x87));
}

static unsigned ntz(uint64_t counter)
{
    unsigned i;
    for (i=0; i<65; i++) {
        if (counter & 1)
            return i;
        counter >>= 1;
    }
    return 64;
}

EXPORT_SYM int OCB_start_operation(BlockBase *cipher,
                                   const uint8_t *offset_0,
                                   size_t offset_0_len,
                                   OcbModeState **pState)
{

    OcbModeState *state;
    int result;
    unsigned i;

    if ((NULL == cipher) || (NULL == pState)) {
        return ERR_NULL;
    }

    if ((BLOCK_SIZE != cipher->block_len) || (BLOCK_SIZE != offset_0_len)) {
        return ERR_BLOCK_SIZE;
    }

    *pState = state = calloc(1, sizeof(OcbModeState));
    if (NULL == state) {
        return ERR_MEMORY;
    }

    state->cipher = cipher;

    result = state->cipher->encrypt(state->cipher, state->checksum, state->L_star, BLOCK_SIZE);
    if (result)
        return result;

    double_L(&state->L_dollar, &state->L_star);
    double_L(&state->L[0], &state->L_dollar);
    for (i=1; i<=64; i++)
        double_L(&state->L[i], &state->L[i-1]);

    memcpy(state->offset_P, offset_0, BLOCK_SIZE);

    state->counter_A = state->counter_P = 1;

    return 0;
}

enum OcbDirection { OCB_ENCRYPT, OCB_DECRYPT };

EXPORT_SYM int OCB_transcrypt(OcbModeState *state,
                              const uint8_t *in,
                              uint8_t *out,
                              size_t in_len,
                              enum OcbDirection direction)
{
    CipherOperation process = NULL;
    const uint8_t *checksummed = NULL;
    int result;
    unsigned i;

    if ((NULL == state) || (NULL == out) || (NULL == in))
        return ERR_NULL;

    assert(OCB_ENCRYPT==direction || OCB_DECRYPT==direction);
    checksummed = OCB_ENCRYPT==direction ? in : out;
    process = OCB_ENCRYPT==direction ? state->cipher->encrypt : state->cipher->decrypt;

    for (;in_len>=BLOCK_SIZE; in_len-=BLOCK_SIZE) {
        unsigned idx;
        DataBlock pre;

        idx = ntz(state->counter_P);
        for (i=0; i<BLOCK_SIZE; i++) {
            state->offset_P[i] ^= state->L[idx][i];
            pre[i] = in[i] ^ state->offset_P[i];
        }
        if (++state->counter_P == 0)
            return ERR_MAX_DATA;

        result = process(state->cipher, pre, out, BLOCK_SIZE);
        if (result)
            return result;

        for (i=0; i<BLOCK_SIZE; i++) {
            out[i] ^= state->offset_P[i];
            state->checksum[i] ^= checksummed[i];
        }

        in += BLOCK_SIZE;
        checksummed += BLOCK_SIZE;
        out += BLOCK_SIZE;
    }

    /** Process last piece (if any) **/
    if (in_len>0) {
        DataBlock pad;

        for (i=0; i<BLOCK_SIZE; i++)
            state->offset_P[i] ^= state->L_star[i];

        result = state->cipher->encrypt(state->cipher, state->offset_P, pad, BLOCK_SIZE);
        if (result)
            return result;

        for (i=0; i<in_len; i++) {
            out[i] = in[i] ^ pad[i];
            state->checksum[i] ^= checksummed[i];
        }
        state->checksum[in_len] ^= 0x80;
    }

    return 0;
}

/**
 * Encrypt a piece of plaintext.
 *
 * @state   The block cipher state.
 * @in      A pointer to the plaintext. It is aligned to the 16 byte boundary
 *          unless it is the last block.
 * @out     A pointer to an output buffer, that will hold the ciphertext.
 *          The caller must allocate an area of memory as big as the plaintext.
 * @in_len  The size of the plaintext pointed to by @in.
 *
 * @return  0 in case of success, otherwise the relevant error code.
 */
EXPORT_SYM int OCB_encrypt(OcbModeState *state,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t in_len)
{
    return OCB_transcrypt(state, in, out, in_len, OCB_ENCRYPT);
}

/**
 * Decrypt a piece of ciphertext.
 *
 * @state   The block cipher state.
 * @in      A pointer to the ciphertext. It is aligned to the 16 byte boundary
 *          unless it is the last block.
 * @out     A pointer to an output buffer, that will hold the plaintext.
 *          The caller must allocate an area of memory as big as the ciphertext.
 * @in_len  The size of the ciphertext pointed to by @in.
 *
 * @return  0 in case of success, otherwise the relevant error code.
 */
EXPORT_SYM int OCB_decrypt(OcbModeState *state,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t in_len)
{
    return OCB_transcrypt(state, in, out, in_len, OCB_DECRYPT);
}

/**
 * Process a piece of authenticated data.
 *
 * @state   The block cipher state.
 * @in      A pointer to the authenticated data.
 *          It must be aligned to the 16 byte boundary, unless it is
 *          the last piece.
 * @in_len  The size of the authenticated data pointed to by @in.
 */
EXPORT_SYM int OCB_update(OcbModeState *state,
                          const uint8_t *in,
                          size_t in_len)
{
    int result;
    unsigned i;
    DataBlock pt;
    DataBlock ct;

    if ((NULL == state) || (NULL == in))
        return ERR_NULL;

    for (;in_len>=BLOCK_SIZE; in_len-=BLOCK_SIZE) {
        unsigned idx;

        idx = ntz(state->counter_A);
        for (i=0; i<BLOCK_SIZE; i++) {
            state->offset_A[i] ^= state->L[idx][i];
            pt[i] = in[i] ^ state->offset_A[i];
        }
        if (++state->counter_A == 0)
            return ERR_MAX_DATA;

        result = state->cipher->encrypt(state->cipher, pt, ct, BLOCK_SIZE);
        if (result)
            return result;

        for (i=0; i<BLOCK_SIZE; i++)
            state->sum[i] ^= ct[i];

        in += BLOCK_SIZE;
    }

    /** Process last piece (if any) **/
    if (in_len>0) {
        memset(pt, 0, sizeof pt);
        memcpy(pt, in, in_len);
        pt[in_len] = 0x80;

        for (i=0; i<BLOCK_SIZE; i++)
            pt[i] ^= state->offset_A[i] ^ state->L_star[i];

        result = state->cipher->encrypt(state->cipher, pt, ct, BLOCK_SIZE);
        if (result)
            return result;

        for (i=0; i<BLOCK_SIZE; i++)
            state->sum[i] ^= ct[i];
    }

    return 0;
}

EXPORT_SYM int OCB_digest(OcbModeState *state,
                          uint8_t *tag,
                          size_t tag_len)
{
    DataBlock pt;
    unsigned i;
    int result;

    if ((NULL == state) || (NULL == tag))
        return ERR_NULL;

    if (BLOCK_SIZE != tag_len)
        return ERR_TAG_SIZE;

    for (i=0; i<BLOCK_SIZE; i++)
        pt[i] = state->checksum[i] ^ state->offset_P[i] ^ state->L_dollar[i];

    result = state->cipher->encrypt(state->cipher, pt, tag, BLOCK_SIZE);
    if (result)
        return result;

    /** state->sum is HASH(K, A) **/
    for (i=0; i<BLOCK_SIZE; i++)
        tag[i] ^= state->sum[i];

    return 0;
}

EXPORT_SYM int OCB_stop_operation(OcbModeState *state)
{
    if (NULL == state)
        return ERR_NULL;
    state->cipher->destructor(state->cipher);
    free(state);
    return 0;
}
