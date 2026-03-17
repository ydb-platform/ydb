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

#include <stdlib.h>

#include "block_base.h"

#define CIPHER_STATE_TYPE       _PASTE2(MODULE_NAME, _State)
#define CIPHER_ENCRYPT          _PASTE2(MODULE_NAME, _encrypt)
#define CIPHER_DECRYPT          _PASTE2(MODULE_NAME, _decrypt)
#define CIPHER_STOP_OPERATION   _PASTE2(MODULE_NAME, _stop_operation)
#define CIPHER_START_OPERATION  _PASTE2(MODULE_NAME, _start_operation)

typedef struct {
    BlockBase  base_state;
    struct block_state algo_state;
} CIPHER_STATE_TYPE;

int CIPHER_ENCRYPT
           (const BlockBase *state, const uint8_t *in, uint8_t *out, size_t data_len)
{
    size_t block_len;

    if ((state == NULL) || (in == NULL) || (out == NULL))
        return ERR_NULL;

    block_len = state->block_len;

    for (; data_len>=block_len; data_len-=block_len) {
        block_encrypt(&((CIPHER_STATE_TYPE*)state)->algo_state, (uint8_t*)in, out);
        in += block_len;
        out += block_len;
    }

    if (data_len)
        return ERR_NOT_ENOUGH_DATA;

    return 0;
}

int CIPHER_DECRYPT
           (const BlockBase *state, const uint8_t *in, uint8_t *out, size_t data_len)
{
    size_t block_len;

    if ((state == NULL) || (in == NULL) || (out == NULL))
        return ERR_NULL;

    block_len = state->block_len;

    for (; data_len>=block_len; data_len-=block_len) {
        block_decrypt(&((CIPHER_STATE_TYPE*)state)->algo_state, (uint8_t*)in, out);
        in += block_len;
        out += block_len;
    }

    if (data_len)
        return ERR_NOT_ENOUGH_DATA;
    
    return 0;
}

EXPORT_SYM int CIPHER_STOP_OPERATION(BlockBase *state)
{
    if (NULL == state)
        return ERR_NULL;

    block_finalize(&((CIPHER_STATE_TYPE*)state)->algo_state);
    free(state);
    return 0;
}

#ifndef NON_STANDARD_START_OPERATION
EXPORT_SYM int CIPHER_START_OPERATION(const uint8_t key[], size_t key_len, CIPHER_STATE_TYPE **pResult)
{
    BlockBase *block_base;
    int res;

    if ((key == NULL) || (pResult == NULL))
        return ERR_NULL;

    *pResult = calloc(1, sizeof(CIPHER_STATE_TYPE));
    if (NULL == *pResult)
        return ERR_MEMORY;

    block_base = &((*pResult)->base_state);
    block_base->encrypt = &CIPHER_ENCRYPT;
    block_base->decrypt = &CIPHER_DECRYPT;
    block_base->destructor = &CIPHER_STOP_OPERATION;
    block_base->block_len = BLOCK_SIZE;

    res = block_init(&(*pResult)->algo_state, (unsigned char*)key, key_len);
    if (res) {
        free(*pResult);
        *pResult = NULL;
    }

    return res;
}
#endif
