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

FAKE_INIT(raw_ecb)

#include "block_base.h"

typedef BlockBase EcbModeState;

EXPORT_SYM int ECB_start_operation(BlockBase *cipher,
                                   EcbModeState **pResult)
{
    if ((NULL == cipher) || (NULL == pResult)) {
        return ERR_NULL;
    }

    *pResult = (EcbModeState*)cipher;
    return 0;
}

EXPORT_SYM int ECB_encrypt(EcbModeState *ecbState,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    if ((NULL == ecbState) || (NULL == in) || (NULL == out))
        return ERR_NULL;

    return ecbState->encrypt((BlockBase*)ecbState, in, out, data_len);
}

EXPORT_SYM int ECB_decrypt(EcbModeState *ecbState,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    if ((NULL == ecbState) || (NULL == in) || (NULL == out))
        return ERR_NULL;
    
    return ecbState->decrypt((BlockBase*)ecbState, in, out, data_len);
}


EXPORT_SYM int ECB_stop_operation(EcbModeState *state)
{
    if (NULL == state)
        return ERR_NULL;
    state->destructor((BlockBase*)state);
    return 0;
}
