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

FAKE_INIT(raw_ctr)

#include "block_base.h"

#define ERR_CTR_COUNTER_BLOCK_LEN   ((6 << 16) | 1)
#define ERR_CTR_REPEATED_KEY_STREAM ((6 << 16) | 2)

#define NR_BLOCKS 8

typedef struct {
    BlockBase *cipher;

    /**
     *  A counter block is always as big as a cipher block.
     *  It is made up by three areas:
     *  1) Prefix  - immutable - can be empty
     *  2) Counter - mutable (+1 per block) - at least 1 byte
     *  3) Postfix - immutable - can be empty
      */
    uint8_t *counter_blocks;    /** block_len * NR_BLOCKS bytes **/
    
    uint8_t *counter;           /** point to counter in 1st block within counter_blocks **/  
    size_t  counter_len;
    unsigned little_endian;

    uint8_t *keystream; /** block_len * NR_BLOCKS bytes **/
    size_t used_ks;     /** Bytes we already used in the key stream **/

    /** Number of bytes we have encrypted so far **/
    uint64_t length_lo, length_hi;

    /** Max number of bytes we may encrypt at most **/
    uint64_t length_max_lo, length_max_hi;
} CtrModeState;

typedef void (*Increment)(uint8_t *pCounter, size_t counter_len, unsigned amount);

static inline void increment_le(uint8_t *pCounter, size_t counter_len, unsigned amount) {
    size_t i;

    for (i=0; i<counter_len && amount>0; i++, pCounter++) {
        *pCounter = (uint8_t)(*pCounter + amount);
        amount = *pCounter < amount;
    }
}

static inline void increment_be(uint8_t *pCounter, size_t counter_len, unsigned amount) {
    size_t i;

    pCounter += counter_len - 1;
    for (i=0; i<counter_len && amount>0; i++, pCounter--) {
        *pCounter = (uint8_t)(*pCounter + amount);
        amount = *pCounter < amount;
    }
}

/*
 * Create the initial sequence of counter blocks
 */
static uint8_t* create_counter_blocks(uint8_t *counter_block0, size_t block_len, size_t prefix_len, unsigned counter_len, Increment increment)
{
    unsigned i;
    uint8_t *counter_blocks, *current;

    counter_blocks = current = align_alloc(block_len * NR_BLOCKS, (unsigned)block_len);
    if (NULL == counter_blocks) {
        return NULL;
    }
 
    memcpy(current, counter_block0, block_len);
    current += block_len;

    for (i=0; i<NR_BLOCKS-1; i++ ) {
        memcpy(current, current - block_len, block_len);
        increment(current + prefix_len, counter_len, 1);
        current += block_len;
    }

    return counter_blocks;
}

static uint8_t* create_keystream(BlockBase *cipher, uint8_t *counter_blocks, size_t block_len)
{
    uint8_t *keystream;

    keystream = align_alloc(block_len * NR_BLOCKS, (unsigned)block_len);
    if (NULL == keystream) {
        return NULL;
    }
    
    cipher->encrypt(cipher,
                    counter_blocks,
                    keystream,
                    cipher->block_len * NR_BLOCKS);

    return keystream;
}
 
EXPORT_SYM int CTR_start_operation(BlockBase *cipher,
                                   uint8_t   counter_block0[],
                                   size_t    counter_block0_len,
                                   size_t    prefix_len,
                                   unsigned  counter_len,
                                   unsigned  little_endian,
                                   CtrModeState **pResult)
{
    CtrModeState *ctr_state;
    size_t block_len;
    Increment increment = little_endian ? increment_le : increment_be;

    if (NULL == cipher || NULL == counter_block0 || NULL == pResult) {
        return ERR_NULL;
    }

    block_len = cipher->block_len;

    if (block_len != counter_block0_len ||
        counter_len == 0 || counter_len > block_len ||
        block_len < (prefix_len + counter_len)) {
        return ERR_CTR_COUNTER_BLOCK_LEN;
    }

    ctr_state = calloc(1, sizeof(CtrModeState));
    if (NULL == ctr_state) {
        return ERR_MEMORY;
    }

    ctr_state->cipher = cipher;
    
    ctr_state->counter_blocks = create_counter_blocks(counter_block0,
                                                      block_len,
                                                      prefix_len,
                                                      counter_len,
                                                      increment);
    if (NULL == ctr_state->counter_blocks) {
        goto error;
    }
    
    ctr_state->counter = ctr_state->counter_blocks + prefix_len;
    ctr_state->counter_len = counter_len;
    ctr_state->little_endian = little_endian;
    
    ctr_state->keystream = create_keystream(cipher, ctr_state->counter_blocks, block_len);
    if (NULL == ctr_state->keystream) {
        goto error;
    }
    ctr_state->used_ks = 0;

    ctr_state->length_lo = ctr_state->length_hi = 0;
    ctr_state->length_max_lo = ctr_state->length_max_hi = 0;

    assert(block_len < 256);
    assert(block_len > 0);
    if (counter_len < 8) 
        ctr_state->length_max_lo = (uint64_t)block_len << (counter_len*8);
    if (counter_len >= 8 && counter_len < 16)
        ctr_state->length_max_hi = (uint64_t)block_len << ((counter_len-8)*8);

    /** length_max_hi and length_max_lo are both zero when counter_len is 16 **/

    *pResult = ctr_state;
    return 0;

error:
    align_free(ctr_state->keystream);
    align_free(ctr_state->counter_blocks);
    free(ctr_state);
    return ERR_MEMORY;
}

static inline void update_keystream(CtrModeState *ctr_state)
{
    unsigned i;
    uint8_t *counter;
    size_t block_len;

    counter = ctr_state->counter;
    block_len = ctr_state->cipher->block_len;
   
   /** Update all consecutive counter blocks **/ 
    if (ctr_state->little_endian) {
        for (i=0; i<NR_BLOCKS; i++) {
            increment_le(counter, ctr_state->counter_len, NR_BLOCKS);
            counter += block_len;
        }
    } else {
        for (i=0; i<NR_BLOCKS; i++) {
            increment_be(counter, ctr_state->counter_len, NR_BLOCKS);
            counter += block_len;
        }
    }

    ctr_state->cipher->encrypt(ctr_state->cipher,
                               ctr_state->counter_blocks,
                               ctr_state->keystream,
                               ctr_state->cipher->block_len * NR_BLOCKS);
    ctr_state->used_ks = 0;
}

EXPORT_SYM int CTR_encrypt(CtrModeState *ctr_state,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    size_t block_len;
    uint64_t max_hi, max_lo;

    if (NULL == ctr_state || NULL == in || NULL == out)
        return ERR_NULL;

    block_len = ctr_state->cipher->block_len;
    max_hi = ctr_state->length_max_hi;
    max_lo = ctr_state->length_max_lo;

    while (data_len > 0) {
        size_t ks_to_use;
        size_t ks_size;
        unsigned j;
        
        ks_size = block_len * NR_BLOCKS;
        if (ctr_state->used_ks == ks_size)
            update_keystream(ctr_state);
        
        ks_to_use = MIN(data_len, ks_size - ctr_state->used_ks);
        
        for (j=0; j<ks_to_use; j++) {
            *out++ = *in++ ^ ctr_state->keystream[j + ctr_state->used_ks];
        }

        data_len -= ks_to_use;
        ctr_state->used_ks += ks_to_use;

        ctr_state->length_lo += ks_to_use;
        if (ctr_state->length_lo < ks_to_use) {
            ctr_state->length_hi++;
            if (ctr_state->length_hi == 0)
                return ERR_CTR_REPEATED_KEY_STREAM;
        }

        /** 128-bit counter **/
        if (0 == max_lo && 0 == max_hi)
            continue;

        if (ctr_state->length_hi > max_hi)
            return ERR_CTR_REPEATED_KEY_STREAM;
        if (ctr_state->length_hi == max_hi &&
            ctr_state->length_lo > max_lo)
                return ERR_CTR_REPEATED_KEY_STREAM;
    }

    return 0;
}

EXPORT_SYM int CTR_decrypt(CtrModeState *ctr_state,
                           const uint8_t *in,
                           uint8_t *out,
                           size_t data_len)
{
    return CTR_encrypt(ctr_state, in, out, data_len);
}

EXPORT_SYM int CTR_stop_operation(CtrModeState *ctr_state)
{
    if (NULL == ctr_state)
        return ERR_NULL;
    ctr_state->cipher->destructor(ctr_state->cipher);
    align_free(ctr_state->keystream);
    align_free(ctr_state->counter_blocks);
    free(ctr_state);
    return 0;
}
