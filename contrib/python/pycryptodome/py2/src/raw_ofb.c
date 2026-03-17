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

FAKE_INIT(raw_ofb)

#include "block_base.h"

#define ERR_OFB_IV_LEN  ((3 << 16) | 1)

#define MAX_BLOCK_LEN   16

typedef struct {
    BlockBase *cipher;

    /** How many bytes at the beginning of the key stream
      * have already been used.
      */
    size_t usedKeyStream;

    uint8_t keyStream[MAX_BLOCK_LEN];
} OfbModeState;

EXPORT_SYM int OFB_start_operation(BlockBase *cipher,
                                   const uint8_t iv[],
                                   size_t iv_len,
                                   OfbModeState **pResult)
{
    if ((NULL == cipher) || (NULL == iv) || (NULL == pResult))
        return ERR_NULL;

    if (cipher->block_len > MAX_BLOCK_LEN)
        return ERR_BLOCK_SIZE;

    if (cipher->block_len != iv_len)
        return ERR_OFB_IV_LEN;

    *pResult = calloc(1, sizeof(OfbModeState));
    if (NULL == *pResult)
        return ERR_MEMORY;

    (*pResult)->cipher = cipher;
    (*pResult)->usedKeyStream = cipher->block_len;
    memcpy((*pResult)->keyStream, iv, iv_len);

    return 0;
}

EXPORT_SYM int OFB_encrypt(OfbModeState *ofbState,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    size_t block_len;
    uint8_t oldKeyStream[MAX_BLOCK_LEN];

    if ((NULL == ofbState) || (NULL == in) || (NULL == out))
        return ERR_NULL;

    block_len = ofbState->cipher->block_len;
    if (block_len > MAX_BLOCK_LEN)
        return ERR_BLOCK_SIZE;

    while (data_len > 0) {
        size_t i;
        size_t keyStreamToUse;

        if (ofbState->usedKeyStream == block_len) {
            int result;

            memcpy(oldKeyStream, ofbState->keyStream, block_len);
            result = ofbState->cipher->encrypt(ofbState->cipher,
                                               oldKeyStream,
                                               ofbState->keyStream,
                                               block_len);
            if (0 != result)
                return result;

            ofbState->usedKeyStream = 0;
        }

        keyStreamToUse = MIN(data_len, block_len - ofbState->usedKeyStream);
        for (i=0; i<keyStreamToUse; i++)
            *out++ = *in++ ^ ofbState->keyStream[i + ofbState->usedKeyStream];

        data_len -= keyStreamToUse;
        ofbState->usedKeyStream += keyStreamToUse;
    }

    return 0;
}

EXPORT_SYM int OFB_decrypt(OfbModeState *ofbState,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    return OFB_encrypt(ofbState, in, out, data_len);
}

EXPORT_SYM int OFB_stop_operation(OfbModeState *state)
{
    if (NULL == state)
        return ERR_NULL;
    state->cipher->destructor(state->cipher);
    free(state);
    return 0;
}
