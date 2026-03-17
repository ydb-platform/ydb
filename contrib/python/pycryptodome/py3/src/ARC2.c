/* ===================================================================
 *
 * Copyright (c) 2015, Legrandin <helderijs@gmail.com>
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

#define NON_STANDARD_START_OPERATION
#include "block_base.h"

FAKE_INIT(raw_arc2)

#define MODULE_NAME pycryptodome_ARC2
#define BLOCK_SIZE 8
#define KEY_SIZE 0

struct block_state {
    unsigned exp_key[64];
};

static int
block_init(struct block_state *self, const uint8_t *key, size_t t /* key_bytes */,
           size_t effective_key_bits)
{
    uint8_t t8, tm;
    int i;
    uint8_t bkey[128];

    static const uint8_t permute[256] = {
	217,120,249,196, 25,221,181,237, 40,233,253,121, 74,160,216,157,
	198,126, 55,131, 43,118, 83,142, 98, 76,100,136, 68,139,251,162,
	23,154, 89,245,135,179, 79, 19, 97, 69,109,141,  9,129,125, 50,
	189,143, 64,235,134,183,123, 11,240,149, 33, 34, 92,107, 78,130,
	84,214,101,147,206, 96,178, 28,115, 86,192, 20,167,140,241,220,
	18,117,202, 31, 59,190,228,209, 66, 61,212, 48,163, 60,182, 38,
	111,191, 14,218, 70,105,  7, 87, 39,242, 29,155,188,148, 67,  3,
	248, 17,199,246,144,239, 62,231,  6,195,213, 47,200,102, 30,215,
	8,232,234,222,128, 82,238,247,132,170,114,172, 53, 77,106, 42,
	150, 26,210,113, 90, 21, 73,116, 75,159,208, 94,  4, 24,164,236,
	194,224, 65,110, 15, 81,203,204, 36,145,175, 80,161,244,112, 57,
	153,124, 58,133, 35,184,180,122,252,  2, 54, 91, 37, 85,151, 49,
	45, 93,250,152,227,138,146,174,  5,223, 41, 16,103,108,186,201,
	211,  0,230,207,225,158,168, 44, 99, 22,  1, 63, 88,226,137,169,
	13, 56, 52, 27,171, 51,255,176,187, 72, 12, 95,185,177,205, 46,
	197,243,219, 71,229,165,156,119, 10,166, 32,104,254,127,193,173
    };

    if (NULL == self)
        return ERR_NULL;

    if ((t < 5) || (t > 128))
        return ERR_KEY_SIZE;

    if ((effective_key_bits < 40) || (effective_key_bits > 1024))
        return ERR_KEY_SIZE;

    memcpy(bkey, key, t);

    t8 = (uint8_t)((effective_key_bits + 7) / 8); /** 5..128 **/
    tm = (uint8_t)((1 << (8 - (t8*8 - (int)effective_key_bits))) - 1);

    for (i=(int)t; i<128; i++)
        bkey[i] = permute[(bkey[i-1] + bkey[i-(int)t]) % 256];
    
    bkey[128-t8] = permute[bkey[128-t8] & tm];

    for (i=127-t8; i>=0; i--)
        bkey[i] = permute[bkey[i+1] ^ bkey[i+t8]];

    for (i=0; i<64; i++)
        self->exp_key[i] = bkey[2*i] + 256U*bkey[2*i+1];

    return 0;
}

#define ROL16(x, p) ((((x) << (p)) | ((uint16_t)(x) >> (16-(p)))))
#define ROR16(x, p) ((((uint16_t)(x) >> (p)) | ((x) << (16-(p)))))

static inline void mix_round(unsigned *r, const unsigned *k, size_t *j)
{
    r[0] += k[(*j)++] + (r[3] & r[2]) + (~r[3] & r[1]);
    r[0] = ROL16(r[0], 1);
    r[1] += k[(*j)++] + (r[0] & r[3]) + (~r[0] & r[2]);
    r[1] = ROL16(r[1], 2);
    r[2] += k[(*j)++] + (r[1] & r[0]) + (~r[1] & r[3]);
    r[2] = ROL16(r[2], 3);
    r[3] += k[(*j)++] + (r[2] & r[1]) + (~r[2] & r[0]);
    r[3] = ROL16(r[3], 5);
}

static inline void inv_mix_round(unsigned *r, const unsigned *k, size_t *j)
{
    r[3] = ROR16(r[3], 5);
    r[3] -= k[(*j)--] + (r[2] & r[1]) + (~r[2] & r[0]);
    r[2] = ROR16(r[2], 3);
    r[2] -= k[(*j)--] + (r[1] & r[0]) + (~r[1] & r[3]);
    r[1] = ROR16(r[1], 2);
    r[1] -= k[(*j)--] + (r[0] & r[3]) + (~r[0] & r[2]);
    r[0] = ROR16(r[0], 1);
    r[0] -= k[(*j)--] + (r[3] & r[2]) + (~r[3] & r[1]);
}

static inline void mash_round(unsigned *r, const unsigned *k)
{
    r[0] += k[r[3] & 63];
    r[1] += k[r[0] & 63];
    r[2] += k[r[1] & 63];
    r[3] += k[r[2] & 63];
}

static inline void inv_mash_round(unsigned *r, const unsigned *k)
{
    r[3] -= k[r[2] & 63];
    r[2] -= k[r[1] & 63];
    r[1] -= k[r[0] & 63];
    r[0] -= k[r[3] & 63];
}

static void block_encrypt(struct block_state *self, const uint8_t *in, uint8_t *out)
{
    unsigned r[4];
    const unsigned *k;
    size_t i, j;

    k = self->exp_key;
    j = 0;

    for (i=0; i<4; i++) {
        r[i] = in[2*i] + 256U*in[2*i+1];
    }

    for (i=0; i<5; i++) mix_round(r, k, &j);
    mash_round(r, k);
    for (i=0; i<6; i++) mix_round(r, k, &j);
    mash_round(r, k);
    for (i=0; i<5; i++) mix_round(r, k, &j);
    
    for (i=0; i<4; i++) {
        out[2*i] = r[i] & 255;
        out[2*i+1] = (uint8_t)(r[i] >> 8);
    }
}

static void block_decrypt(struct block_state *self, const uint8_t *in, uint8_t *out)
{
    unsigned r[4];
    const unsigned *k;
    size_t i, j;

    k = self->exp_key;

    for (i=0; i<4; i++) {
        r[i] = in[2*i] + 256U*in[2*i+1];
    }

    j = 63;
    for (i=0; i<5; i++) inv_mix_round(r, k, &j);
    inv_mash_round(r, k);
    for (i=0; i<6; i++) inv_mix_round(r, k, &j);
    inv_mash_round(r, k);
    for (i=0; i<5; i++) inv_mix_round(r, k, &j);
    
    for (i=0; i<4; i++) {
        out[2*i] = r[i] & 255;
        out[2*i+1] = (uint8_t)(r[i] >> 8);
    }
}

static void
block_finalize(struct block_state* self)
{
}

#include "block_common.c"

EXPORT_SYM int pycryptodome_ARC2_start_operation(const uint8_t key[], size_t key_len, size_t effective_key_len, pycryptodome_ARC2_State **pResult)
{
    BlockBase *block_base;

    if ((key == NULL) || (pResult == NULL))
        return ERR_NULL;

    *pResult = calloc(1, sizeof(pycryptodome_ARC2_State));
    if (NULL == *pResult)
        return ERR_MEMORY;

    block_base = &((*pResult)->base_state);
    block_base->encrypt = &pycryptodome_ARC2_encrypt;
    block_base->decrypt = &pycryptodome_ARC2_decrypt;
    block_base->destructor = &pycryptodome_ARC2_stop_operation;
    block_base->block_len = BLOCK_SIZE;

    return block_init(&(*pResult)->algo_state, (unsigned char*)key,
           key_len, effective_key_len);
}

