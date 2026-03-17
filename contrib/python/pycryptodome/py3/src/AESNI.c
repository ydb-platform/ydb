/* ===================================================================
 *
 * Copyright (c) 2018, Helder Eijs <helderijs@gmail.com>
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
#include <stdio.h>
#include <wmmintrin.h>
#include "common.h"
#include "endianess.h"
#include "block_base.h"

FAKE_INIT(raw_aesni)

#define MODULE_NAME AESNI
#define BLOCK_SIZE 16

struct block_state {
    __m128i *erk;   /** Round keys for encryption (11, 13 or 15 elements) **/
    __m128i *drk;   /** Round keys for decryption **/
    unsigned rounds;
};

typedef struct {
    BlockBase  base_state;
    struct block_state algo_state;
} AESNI_State;

/*
 * See https://www.cosic.esat.kuleuven.be/ecrypt/AESday/slides/Use_of_the_AES_Instruction_Set.pdf
 */

enum SubType { OnlySub, SubRotXor };

STATIC uint32_t sub_rot(uint32_t w, unsigned idx /** round/Nk **/, enum SubType subType)
{
    __m128i x, y, z;

    assert((idx>=1) && (idx<=10));

    x = _mm_set1_epi32((int)w); // { w, w, w, w }
    y = _mm_set1_epi32(0);

    switch (idx) {
    case 1:  y = _mm_aeskeygenassist_si128(x, 0x01); break;
    case 2:  y = _mm_aeskeygenassist_si128(x, 0x02); break;
    case 3:  y = _mm_aeskeygenassist_si128(x, 0x04); break;
    case 4:  y = _mm_aeskeygenassist_si128(x, 0x08); break;
    case 5:  y = _mm_aeskeygenassist_si128(x, 0x10); break;
    case 6:  y = _mm_aeskeygenassist_si128(x, 0x20); break;
    case 7:  y = _mm_aeskeygenassist_si128(x, 0x40); break;
    case 8:  y = _mm_aeskeygenassist_si128(x, 0x80); break;
    case 9:  y = _mm_aeskeygenassist_si128(x, 0x1b); break;
    case 10: y = _mm_aeskeygenassist_si128(x, 0x36); break;
    }

    /** Y0 contains SubWord(W) **/
    /** Y1 contains RotWord(SubWord(W)) xor RCON **/
    
    z = y;
    if (subType == SubRotXor) {
        z = _mm_srli_si128(y, 4);
    }
    return (uint32_t)_mm_cvtsi128_si32(z);
}

STATIC int expand_key(__m128i *erk, __m128i *drk, const uint8_t *key, unsigned Nk, unsigned Nr)
{
    uint32_t rk[4*(14+2)];
    unsigned tot_words;
    unsigned i;

    assert(
            ((Nk==4) && (Nr==10)) ||    /** AES-128 **/
            ((Nk==6) && (Nr==12)) ||    /** AES-192 **/
            ((Nk==8) && (Nr==14))       /** AES-256 **/
    );

    tot_words = 4*(Nr+1);

    for (i=0; i<Nk; i++) {
        rk[i] = LOAD_U32_LITTLE(key);
        key += 4;
    }

    for (i=Nk; i<tot_words; i++) {
        uint32_t tmp;

        tmp = rk[i-1];
        if (i % Nk == 0) {
            tmp = sub_rot(tmp, i/Nk, SubRotXor);
        } else {
            if ((i % Nk == 4) && (Nk == 8)) {  /* AES-256 only */
                tmp = sub_rot(tmp, i/Nk, OnlySub);
            }
        }
        rk[i] = rk[i-Nk] ^ tmp;
    }

    for (i=0; i<tot_words; i+=4) {
        *erk++ = _mm_loadu_si128((__m128i*)&rk[i]);
    }

    erk--;  /** Point to the last round **/
    *drk++ = *erk--;
    for (i=0; i<Nr-1; i++) {
        *drk++ = _mm_aesimc_si128(*erk--);
    }
    *drk = *erk;

    return 0;
}

STATIC int internal_AESNI_encrypt(__m128i r[], unsigned rounds, const uint8_t *in, uint8_t *out, size_t data_len)
{
    /** Encrypt 8 blocks (128 bytes) in parallel, when possible **/
    for (; data_len >= 8*16; data_len -= 8*16) {
        __m128i pt[8], data[8];
        unsigned j;

        pt[0] = _mm_loadu_si128((__m128i*)in); in+=16;
        pt[1] = _mm_loadu_si128((__m128i*)in); in+=16;
        pt[2] = _mm_loadu_si128((__m128i*)in); in+=16;
        pt[3] = _mm_loadu_si128((__m128i*)in); in+=16;
        pt[4] = _mm_loadu_si128((__m128i*)in); in+=16;
        pt[5] = _mm_loadu_si128((__m128i*)in); in+=16;
        pt[6] = _mm_loadu_si128((__m128i*)in); in+=16;
        pt[7] = _mm_loadu_si128((__m128i*)in); in+=16;

        data[0] = _mm_xor_si128(pt[0], r[0]);
        data[1] = _mm_xor_si128(pt[1], r[0]);
        data[2] = _mm_xor_si128(pt[2], r[0]);
        data[3] = _mm_xor_si128(pt[3], r[0]);
        data[4] = _mm_xor_si128(pt[4], r[0]);
        data[5] = _mm_xor_si128(pt[5], r[0]);
        data[6] = _mm_xor_si128(pt[6], r[0]);
        data[7] = _mm_xor_si128(pt[7], r[0]);

        for (j=1; j<10; j++) {
            data[0] = _mm_aesenc_si128(data[0], r[j]);
            data[1] = _mm_aesenc_si128(data[1], r[j]);
            data[2] = _mm_aesenc_si128(data[2], r[j]);
            data[3] = _mm_aesenc_si128(data[3], r[j]);
            data[4] = _mm_aesenc_si128(data[4], r[j]);
            data[5] = _mm_aesenc_si128(data[5], r[j]);
            data[6] = _mm_aesenc_si128(data[6], r[j]);
            data[7] = _mm_aesenc_si128(data[7], r[j]);
        }
    
        for (; j<rounds; j++) {
            data[0] = _mm_aesenc_si128(data[0], r[j]);
            data[1] = _mm_aesenc_si128(data[1], r[j]);
            data[2] = _mm_aesenc_si128(data[2], r[j]);
            data[3] = _mm_aesenc_si128(data[3], r[j]);
            data[4] = _mm_aesenc_si128(data[4], r[j]);
            data[5] = _mm_aesenc_si128(data[5], r[j]);
            data[6] = _mm_aesenc_si128(data[6], r[j]);
            data[7] = _mm_aesenc_si128(data[7], r[j]);
        }
        
        data[0] = _mm_aesenclast_si128(data[0], r[rounds]);
        data[1] = _mm_aesenclast_si128(data[1], r[rounds]);
        data[2] = _mm_aesenclast_si128(data[2], r[rounds]);
        data[3] = _mm_aesenclast_si128(data[3], r[rounds]);
        data[4] = _mm_aesenclast_si128(data[4], r[rounds]);
        data[5] = _mm_aesenclast_si128(data[5], r[rounds]);
        data[6] = _mm_aesenclast_si128(data[6], r[rounds]);
        data[7] = _mm_aesenclast_si128(data[7], r[rounds]);

        _mm_storeu_si128((__m128i*)out, data[0]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[1]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[2]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[3]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[4]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[5]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[6]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[7]); out+=16;
    }

    /** There are 7 blocks or fewer left **/
    for (;data_len>=BLOCK_SIZE; data_len-=BLOCK_SIZE, in+=BLOCK_SIZE, out+=BLOCK_SIZE) {
        __m128i pt, data;
        unsigned i;

        pt = _mm_loadu_si128((__m128i*)in);
        data = _mm_xor_si128(pt, r[0]);
        for (i=1; i<10; i++) {
            data = _mm_aesenc_si128(data, r[i]);
        }
        for (i=10; i<rounds; i+=2) {
            data = _mm_aesenc_si128(data, r[i]);
            data = _mm_aesenc_si128(data, r[i+1]);
        }
        data = _mm_aesenclast_si128(data, r[rounds]);
        _mm_storeu_si128((__m128i*)out, data);
    }

    if (data_len) {
        return ERR_NOT_ENOUGH_DATA;
    }

    return 0;
}

int AESNI_encrypt(const BlockBase *bb, const uint8_t *in, uint8_t *out, size_t data_len)
{
    unsigned rounds;
    __m128i r[14+1];
    const struct block_state *state;
    unsigned k;

    if ((bb == NULL) || (in == NULL) || (out == NULL))
        return ERR_NULL;

    state = &((AESNI_State*)bb)->algo_state;
    rounds = state->rounds;

    if (rounds > 14)
        return ERR_NR_ROUNDS;

    for (k=0; k<=rounds; k++) {
        r[k] = state->erk[k];
    }

    return internal_AESNI_encrypt(r, rounds, in, out, data_len);
}

STATIC int internal_AESNI_decrypt(__m128i r[], unsigned rounds, const uint8_t *in, uint8_t *out, size_t data_len)
{
    /** Decrypt 8 blocks (128 bytes) in parallel, when possible **/
    for (; data_len >= 8*16; data_len -= 8*16) {
        __m128i ct[8], data[8];
        unsigned j;

        ct[0] = _mm_loadu_si128((__m128i*)in); in+=16;
        ct[1] = _mm_loadu_si128((__m128i*)in); in+=16;
        ct[2] = _mm_loadu_si128((__m128i*)in); in+=16;
        ct[3] = _mm_loadu_si128((__m128i*)in); in+=16;
        ct[4] = _mm_loadu_si128((__m128i*)in); in+=16;
        ct[5] = _mm_loadu_si128((__m128i*)in); in+=16;
        ct[6] = _mm_loadu_si128((__m128i*)in); in+=16;
        ct[7] = _mm_loadu_si128((__m128i*)in); in+=16;

        data[0] = _mm_xor_si128(ct[0], r[0]);
        data[1] = _mm_xor_si128(ct[1], r[0]);
        data[2] = _mm_xor_si128(ct[2], r[0]);
        data[3] = _mm_xor_si128(ct[3], r[0]);
        data[4] = _mm_xor_si128(ct[4], r[0]);
        data[5] = _mm_xor_si128(ct[5], r[0]);
        data[6] = _mm_xor_si128(ct[6], r[0]);
        data[7] = _mm_xor_si128(ct[7], r[0]);

        for (j=1; j<10; j++) {
            data[0] = _mm_aesdec_si128(data[0], r[j]);
            data[1] = _mm_aesdec_si128(data[1], r[j]);
            data[2] = _mm_aesdec_si128(data[2], r[j]);
            data[3] = _mm_aesdec_si128(data[3], r[j]);
            data[4] = _mm_aesdec_si128(data[4], r[j]);
            data[5] = _mm_aesdec_si128(data[5], r[j]);
            data[6] = _mm_aesdec_si128(data[6], r[j]);
            data[7] = _mm_aesdec_si128(data[7], r[j]);
        }
    
        for (; j<rounds; j++) {
            data[0] = _mm_aesdec_si128(data[0], r[j]);
            data[1] = _mm_aesdec_si128(data[1], r[j]);
            data[2] = _mm_aesdec_si128(data[2], r[j]);
            data[3] = _mm_aesdec_si128(data[3], r[j]);
            data[4] = _mm_aesdec_si128(data[4], r[j]);
            data[5] = _mm_aesdec_si128(data[5], r[j]);
            data[6] = _mm_aesdec_si128(data[6], r[j]);
            data[7] = _mm_aesdec_si128(data[7], r[j]);
        }
        
        data[0] = _mm_aesdeclast_si128(data[0], r[rounds]);
        data[1] = _mm_aesdeclast_si128(data[1], r[rounds]);
        data[2] = _mm_aesdeclast_si128(data[2], r[rounds]);
        data[3] = _mm_aesdeclast_si128(data[3], r[rounds]);
        data[4] = _mm_aesdeclast_si128(data[4], r[rounds]);
        data[5] = _mm_aesdeclast_si128(data[5], r[rounds]);
        data[6] = _mm_aesdeclast_si128(data[6], r[rounds]);
        data[7] = _mm_aesdeclast_si128(data[7], r[rounds]);

        _mm_storeu_si128((__m128i*)out, data[0]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[1]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[2]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[3]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[4]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[5]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[6]); out+=16;
        _mm_storeu_si128((__m128i*)out, data[7]); out+=16;
    }

    /** There are 7 blocks or fewer left **/
    for (;data_len>=BLOCK_SIZE; data_len-=BLOCK_SIZE, in+=BLOCK_SIZE, out+=BLOCK_SIZE) {
        __m128i ct, data;
        unsigned i;

        ct = _mm_loadu_si128((__m128i*)in);
        data = _mm_xor_si128(ct, r[0]);
        for (i=1; i<10; i++) {
            data = _mm_aesdec_si128(data, r[i]);
        }
        for (i=10; i<rounds; i+=2) {
            data = _mm_aesdec_si128(data, r[i]);
            data = _mm_aesdec_si128(data, r[i+1]);
        }
        data = _mm_aesdeclast_si128(data, r[rounds]);
        _mm_storeu_si128((__m128i*)out, data);
    }

    if (data_len) {
        return ERR_NOT_ENOUGH_DATA;
    }

    return 0;
}

int AESNI_decrypt(const BlockBase *bb, const uint8_t *in, uint8_t *out, size_t data_len)
{
    unsigned rounds;
    __m128i r[14+1];
    const struct block_state *state;
    unsigned k;

    if ((bb == NULL) || (in == NULL) || (out == NULL))
        return ERR_NULL;

    state = &((AESNI_State*)bb)->algo_state;
    rounds = state->rounds;

    if (rounds > 14)
        return ERR_NR_ROUNDS;

    for (k=0; k<=rounds; k++) {
        r[k] = state->drk[k];
    }

    return internal_AESNI_decrypt(r, rounds, in, out, data_len);
}

EXPORT_SYM int AESNI_stop_operation(BlockBase *bb)
{
    AESNI_State *state;

    if (NULL == bb)
        return ERR_NULL;

    state = (AESNI_State*)bb;
    align_free(state->algo_state.erk);
    align_free(state->algo_state.drk);
    free(state);
    return 0;
}

EXPORT_SYM int AESNI_start_operation(const uint8_t key[], size_t key_len, AESNI_State **pResult)
{
    unsigned Nr;
    const unsigned Nb = 4;
    int result;
    struct block_state *state;
    BlockBase *block_base;
    
    if ((NULL == key) || (NULL == pResult))
        return ERR_NULL;

    switch (key_len) {
        case 16: Nr = 10; break;
        case 24: Nr = 12; break;
        case 32: Nr = 14; break;
        default: return ERR_KEY_SIZE;
    }
    
    *pResult= calloc(1, sizeof(AESNI_State));
    if (NULL == *pResult)
        return ERR_MEMORY;
    
    block_base = &((*pResult)->base_state);
    block_base->encrypt = &AESNI_encrypt;
    block_base->decrypt = &AESNI_decrypt;
    block_base->destructor = &AESNI_stop_operation;
    block_base->block_len = BLOCK_SIZE;

    state = &((*pResult)->algo_state);
    state->rounds = Nr;
    state->erk = align_alloc(Nb*(Nr+1)*sizeof(uint32_t), 16);
    if (state->erk == NULL) {
        result = ERR_MEMORY;
        goto error;
    }
    
    state->drk = align_alloc(Nb*(Nr+1)*sizeof(uint32_t), 16);
    if (state->drk == NULL) {
        result = ERR_MEMORY;
        goto error;
    }
    
    result = expand_key(state->erk, state->drk, key, (unsigned)key_len/4, Nr);
    if (result) {
        goto error;
    }
    return 0;

error:
    align_free(state->erk);
    align_free(state->drk);
    free(*pResult);
    return result;
}


#ifdef MAIN
#include <stdio.h>
int main(void)
{
    void *c, *d;
    uint8_t key[16] = { 0 };
    struct block_state *s;
    int i;
    int q = 1000000*16;

    AESNI_start_operation(key, 16, &s);
    c = malloc(q);
    d = malloc(q);
  
    for (i=0; i<1000; i++) 
        AESNI_encrypt((void*)s, c, d, q);
    
    printf("Done.\n");
    return 0;
}
#endif
