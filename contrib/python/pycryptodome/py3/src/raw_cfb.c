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

FAKE_INIT(raw_cfb)

#include "block_base.h"

#define ERR_CFB_IV_LEN           ((2 << 16) | 1)
#define ERR_CFB_INVALID_SEGMENT  ((2 << 16) | 2)

typedef struct {
    BlockBase *cipher;

    size_t segment_len;

    /** How many bytes at the beginning of the key stream
      * have already been used.
      */
    size_t usedKeyStream;

    uint8_t *keyStream;
    uint8_t *next_iv;
} CfbModeState;

EXPORT_SYM int CFB_start_operation(BlockBase *cipher,
                                   const uint8_t iv[],
                                   size_t iv_len,
                                   size_t segment_len, /* In bytes */
                                   CfbModeState **pResult)
{
    CfbModeState *state;

    if ((NULL == cipher) || (NULL == iv) || (NULL == pResult)) {
        return ERR_NULL;
    }

    if (cipher->block_len != iv_len) {
        return ERR_CFB_IV_LEN;
    }

    if ((segment_len == 0) || (segment_len > cipher->block_len)) {
        return ERR_CFB_INVALID_SEGMENT;
    }

    state = *pResult = calloc(1, sizeof(CfbModeState));
    if (NULL == *pResult) {
        return ERR_MEMORY;
    }

    state->next_iv = calloc(1, cipher->block_len);
    if (NULL == state->next_iv) {
        free(state);
        return ERR_MEMORY;
    }

    state->keyStream = calloc(1, cipher->block_len);
    if (NULL == state->keyStream) {
        free(state->next_iv);
        free(state);
        return ERR_MEMORY;
    }

    state->cipher = cipher;
    state->segment_len = segment_len;
    state->usedKeyStream = 0;
    memcpy(state->next_iv, iv + segment_len, iv_len - segment_len);
    return cipher->encrypt(state->cipher, iv, state->keyStream, iv_len);
}

enum Direction { DirEncrypt, DirDecrypt };

static int CFB_transcrypt(CfbModeState *cfbState,
                          const uint8_t *in,
                          uint8_t *out,
                          size_t data_len,
                          enum Direction direction)
{
    uint8_t *next_iv;
    size_t block_len;
    size_t segment_len;

    if ((NULL == cfbState) || (NULL == in) || (NULL == out))
        return ERR_NULL;

    block_len = cfbState->cipher->block_len;
    segment_len = cfbState->segment_len;
    next_iv = cfbState->next_iv;

    assert(cfbState->usedKeyStream <= segment_len);
    assert((direction == DirEncrypt) || (direction == DirDecrypt));

    while (data_len > 0) {
        unsigned i;
        size_t keyStreamToUse;
        uint8_t *segment, *keyStream;

        if (cfbState->usedKeyStream == segment_len) {
            int result;

            result = cfbState->cipher->encrypt(cfbState->cipher,
                                               next_iv,
                                               cfbState->keyStream,
                                               block_len);
            if (0 != result)
                return result;

            /* The next input to the cipher is:
             * - the old input shifted left by the segment length
             * - filled on the right with the enough ciphertext
             *
             * If the segment length equals the block length,
             * the next input is simply the ciphertext block.
             *
             * If the segment length equals 1, only the leftmost
             * byte of the ciphertext block is used.
             */
            memmove(next_iv, next_iv + segment_len, block_len - segment_len);

            /*
             * The rest of the next input is copied when enough ciphertext is
             * available (we need segment_len bytes).
             */
            cfbState->usedKeyStream = 0;
        }

        keyStream = cfbState->keyStream + cfbState->usedKeyStream;
        keyStreamToUse = MIN(segment_len - cfbState->usedKeyStream, data_len);

        segment = next_iv + (block_len - (segment_len - cfbState->usedKeyStream));

        if (direction == DirDecrypt) {
            memcpy(segment, in, keyStreamToUse);
        }

        for (i=0; i<keyStreamToUse; i++) {
            *out++ = *keyStream++ ^ *in++;
        }

        if (direction == DirEncrypt) {
            memcpy(segment, out - keyStreamToUse, keyStreamToUse);
        }

        data_len -= keyStreamToUse;
        cfbState->usedKeyStream += keyStreamToUse;
    }

    return 0;
}

EXPORT_SYM int CFB_encrypt(CfbModeState *cfbState,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    return CFB_transcrypt(cfbState, in, out, data_len, DirEncrypt);
}

EXPORT_SYM int CFB_decrypt(CfbModeState *cfbState,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    return CFB_transcrypt(cfbState, in, out, data_len, DirDecrypt);
}

EXPORT_SYM int CFB_stop_operation(CfbModeState *state)
{
    if (NULL == state)
        return ERR_NULL;

    state->cipher->destructor(state->cipher);
    free(state->next_iv);
    free(state->keyStream);
    free(state);
    return 0;
}
