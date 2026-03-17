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

FAKE_INIT(raw_cbc)

#include "block_base.h"

#define ERR_CBC_IV_LEN  ((1 << 16) | 1)

#define MAX_BLOCK_LEN   16

typedef struct {
    BlockBase *cipher;
    uint8_t iv[MAX_BLOCK_LEN];
} CbcModeState;

EXPORT_SYM int CBC_start_operation(BlockBase *cipher,
                                   const uint8_t iv[],
                                   size_t iv_len,
                                   CbcModeState **pResult)
{
    if ((NULL == cipher) || (NULL == iv) || (NULL == pResult))
        return ERR_NULL;

    if (cipher->block_len > MAX_BLOCK_LEN)
        return ERR_BLOCK_SIZE;

    if (cipher->block_len != iv_len)
        return ERR_CBC_IV_LEN;
    
    *pResult = calloc(1, sizeof(CbcModeState));
    if (NULL == *pResult) {
        return ERR_MEMORY;
    }

    (*pResult)->cipher = cipher;
    memcpy((*pResult)->iv, iv, iv_len);

    return 0;
}

EXPORT_SYM int CBC_encrypt(CbcModeState *cbcState,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    uint8_t pt[MAX_BLOCK_LEN],
            iv[MAX_BLOCK_LEN];
    size_t block_len;

    if ((NULL == cbcState) || (NULL == in) || (NULL == out))
        return ERR_NULL;

    block_len = cbcState->cipher->block_len;
    if (block_len > MAX_BLOCK_LEN)
        return ERR_BLOCK_SIZE;

    memcpy(iv, cbcState->iv, block_len);
    while (data_len >= block_len) {
        unsigned i;
        int result;

        for (i=0; i<block_len; i++)
            pt[i] = in[i] ^ iv[i];

        result = cbcState->cipher->encrypt(cbcState->cipher, pt, out, block_len);
        if (result)
            return result;

        memcpy(iv, out, block_len);

        data_len -= block_len;
        in += block_len;
        out += block_len;
    }
    memcpy(cbcState->iv, iv, block_len);

    if (data_len > 0)
        return ERR_NOT_ENOUGH_DATA;

    return 0;
}

EXPORT_SYM int CBC_decrypt(CbcModeState *cbcState,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    uint8_t pt[MAX_BLOCK_LEN],
            iv[MAX_BLOCK_LEN];
    size_t block_len;

    if ((NULL == cbcState) || (NULL == in) || (NULL == out))
        return ERR_NULL;

    block_len = cbcState->cipher->block_len;
    if (block_len > MAX_BLOCK_LEN)
        return ERR_BLOCK_SIZE;

    memcpy(iv, cbcState->iv, block_len);
    while (data_len >= block_len) {
        unsigned i;
        int result;

        result = cbcState->cipher->decrypt(cbcState->cipher, in, pt, block_len);
        if (result)
            return result;

        for (i=0; i<block_len; i++)
            pt[i] = pt[i] ^ iv[i];

        memcpy(iv, in, block_len);
        memcpy(out, pt, block_len);

        data_len -= block_len;
        in += block_len;
        out += block_len;
    }
    memcpy(cbcState->iv, iv, block_len);

    if (data_len > 0)
        return ERR_NOT_ENOUGH_DATA;

    return 0;
}


EXPORT_SYM int CBC_stop_operation(CbcModeState *state)
{
    if (NULL == state)
        return ERR_NULL;

    state->cipher->destructor(state->cipher);
    free(state);
    return 0;
}
