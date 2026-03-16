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

#include <stdio.h>
#include "common.h"
#include "endianess.h"

#define WORDS_IN_BLOCK 16
#define WORDS_IN_STATE 8
#define BLOCK_SIZE (WORDS_IN_BLOCK*BLAKE2_WORD_SIZE/8)

typedef struct {
        blake2_word h[WORDS_IN_STATE];
        blake2_word off_counter_low;
        blake2_word off_counter_high;
        unsigned buf_occ;
        uint8_t buf[BLOCK_SIZE];
} hash_state;

typedef enum { NON_FINAL_BLOCK, FINAL_BLOCK } block_type;

EXPORT_SYM int blake2_init (hash_state **state,
                            const uint8_t *key,
                            size_t key_size,
                            size_t digest_size)
{
    hash_state *hs;
    unsigned i;

    if (NULL == state)
        return ERR_NULL;

    if (NULL == key || key_size > MAX_KEY_BYTES)
        return ERR_KEY_SIZE;

    if (0 == digest_size || digest_size > MAX_DIGEST_BYTES)
        return ERR_DIGEST_SIZE;

    *state = hs = (hash_state*) calloc(1, sizeof(hash_state));
    if (NULL == hs)
        return ERR_MEMORY;

    for (i=0; i<WORDS_IN_STATE; i++) {
        hs->h[i] = iv[i];
    }
    hs->h[0] = (blake2_word)(hs->h[0] ^ 0x01010000 ^ (key_size << 8) ^ digest_size);

    /** If the key is present, the first block is the key padded with zeroes **/
    if (key_size>0) {
        memcpy(hs->buf, key, key_size);
        hs->buf_occ = BLOCK_SIZE;
    }

    return 0;
}

EXPORT_SYM int blake2_destroy(hash_state *hs)
{
    free(hs);
    return 0;
}

EXPORT_SYM int blake2_copy(const hash_state *src, hash_state *dst)
{
    if (NULL == src || NULL == dst) {
        return ERR_NULL;
    }

    *dst = *src;
    return 0;
}

#define ROTR(x,n) (((x) >> (n)) ^ ((x) << (BLAKE2_WORD_SIZE - (n))))

#define G(v,a,b,c,d,x,y) \
{ \
    v[a] = v[a] + v[b] + x; \
    v[d] = ROTR(v[d] ^ v[a], G_R1); \
    v[c] = v[c] + v[d]; \
    v[b] = ROTR(v[b] ^ v[c], G_R2); \
    v[a] = v[a] + v[b] + y; \
    v[d] = ROTR(v[d] ^ v[a], G_R3); \
    v[c] = v[c] + v[d]; \
    v[b] = ROTR(v[b] ^ v[c], G_R4); \
}

static void blake2b_compress(blake2_word state[8],
                             const blake2_word m[WORDS_IN_BLOCK],
                             blake2_word off_counter_low,
                             blake2_word off_counter_high,
                             block_type bt
                             )
{
    static const uint8_t sigma[12][16] = {
           { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
           { 14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3 },
           { 11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4 },
           { 7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8 },
           { 9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13 },
           { 2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9 },
           { 12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11 },
           { 13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10 },
           { 6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5 },
           { 10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0 },
           { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
           { 14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3 }
    };
    blake2_word work[WORDS_IN_BLOCK];
    unsigned i;

    for (i=0; i<WORDS_IN_STATE; i++) {
        work[i] = state[i];
        work[i+WORDS_IN_STATE] = iv[i];
    }

    work[12] ^= off_counter_low;
    work[13] ^= off_counter_high;

    if (bt == FINAL_BLOCK)
        work[14] = ~work[14];

    for (i=0; i<F_ROUNDS; i++) {
        const uint8_t *s;

        s = &sigma[i][0];
        G(work, 0, 4,  8, 12, m[s[ 0]], m[s[ 1]]);
        G(work, 1, 5,  9, 13, m[s[ 2]], m[s[ 3]]);
        G(work, 2, 6, 10, 14, m[s[ 4]], m[s[ 5]]);
        G(work, 3, 7, 11, 15, m[s[ 6]], m[s[ 7]]);
        G(work, 0, 5, 10, 15, m[s[ 8]], m[s[ 9]]);
        G(work, 1, 6, 11, 12, m[s[10]], m[s[11]]);
        G(work, 2, 7,  8, 13, m[s[12]], m[s[13]]);
        G(work, 3, 4,  9, 14, m[s[14]], m[s[15]]);
    }

    for (i=0; i<WORDS_IN_STATE; i++)
      state[i] ^= work[i] ^ work[i+WORDS_IN_STATE];
}

static int blake2b_process_buffer(hash_state *hs,
                                  size_t new_data_added,
                                  block_type bt)
{
    blake2_word bufw[WORDS_IN_BLOCK];
    const uint8_t *buf;
    unsigned i;

    buf = hs->buf;
    for (i=0; i<WORDS_IN_BLOCK; i++) {
        bufw[i] = LOAD_WORD_LITTLE(buf);
        buf += sizeof(blake2_word);
    }

    hs->off_counter_low = (blake2_word)(hs->off_counter_low + new_data_added);
    if (hs->off_counter_low < new_data_added) {
        if (0 == ++hs->off_counter_high)
            return ERR_MAX_DATA;
    }

    blake2b_compress(hs->h,
                     bufw,
                     hs->off_counter_low,
                     hs->off_counter_high,
                     bt);

    hs->buf_occ = 0;
    return 0;
}

EXPORT_SYM int blake2_update(hash_state *hs,
                             const uint8_t *in,
                             size_t len)
{
    if (NULL == hs)
        return ERR_NULL;

    if (len > 0 && NULL == in)
        return ERR_NULL;


    while (len > 0) {
        unsigned tc, left;
        
        /** Consume input **/
        left = (unsigned)(BLOCK_SIZE - hs->buf_occ);
        tc = (unsigned)MIN(len, left);
        memcpy(&hs->buf[hs->buf_occ], in, tc);
        len -= tc;
        in += tc;
        hs->buf_occ += tc;
 
       /* Flush buffer if full. However, we must leave at least
        * one byte in the buffer at the end, because we don't
        * know if we are processing the last block.
        */
        if (hs->buf_occ == BLOCK_SIZE && len>0) {
            int result;

            result = blake2b_process_buffer(hs, BLOCK_SIZE, NON_FINAL_BLOCK);
            if (result)
                return result;
        }
    }

    return 0;
}

EXPORT_SYM int blake2_digest(const hash_state *hs,
                             uint8_t digest[MAX_DIGEST_BYTES])
{
    hash_state temp_hs;
    unsigned i;
    int result;

    if (NULL==hs || NULL==digest)
        return ERR_NULL;

    temp_hs = *hs;

    /** Pad buffer with zeroes, if needed. In the special case
     *  of no key and no data, we must process an all zero block.
     */
    for (i=temp_hs.buf_occ; i<BLOCK_SIZE; i++) {
        temp_hs.buf[i] = 0;
    }

    result = blake2b_process_buffer(&temp_hs,
                                    temp_hs.buf_occ,
                                    FINAL_BLOCK);
    if (result)
        return result;

    assert(sizeof temp_hs.h == MAX_DIGEST_BYTES);
    for (i=0; i<WORDS_IN_STATE; i++) {
        STORE_WORD_LITTLE(digest, temp_hs.h[i]);
        digest += sizeof(blake2_word);
    }

    return 0;
}
