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
#include "endianess.h"

FAKE_INIT(scrypt)

/** Return 1 if the pointer is aligned to the 64-bit boundary **/
static inline int aligned64(const uint8_t *p)
{
    if ((uintptr_t)p % 8 == 0)
        return 1;
    else
        return 0;
}

static inline void strxor_inplace(uint8_t *in_out, const uint8_t *in, size_t data_len)
{
    size_t i;

    if (!aligned64(in_out) || !aligned64(in) || (data_len%8)!=0) {
        for (i=0; i<data_len; i++) {
            *in_out++ ^= *in++;
        }
    } else {
        uint64_t *in_out64 = (uint64_t*)in_out;
        uint64_t *in64 = (uint64_t*)in;
        
        data_len /= 8;
        for (i=0; i<data_len; i++) {
            *in_out64++ ^= *in64++;
        }
    }
}

typedef int (core_t)(const uint8_t [64], const uint8_t [64], uint8_t [64]);

/**
 * scrypt BlockMix function
 *
 * @in:    input, a sequence of blocks, each 64 bytes long
 * @out:   output, a sequence of blocks, each 64 bytes long
 * @two_r: the number of 64 bytes blocks in input and output (even)
 * @core:  the Salsa20 core hashing function (with xor)
 *
 * @in and @out may not overlap.
 */
static void scryptBlockMix(const uint8_t in[][64], uint8_t out[][64], size_t two_r,
                           core_t *core)
{
    unsigned i;
    const uint8_t (*x)[64];
    size_t r = two_r / 2;

    assert((void*)in != (void*)out);

    x = &in[two_r-1];   /* Last 64 byte block */
    for (i=0; i<two_r; i++) {
        uint8_t (*y)[64];

        y = &out[(i/2) + (i & 1)*r];
        (*core)(*x, in[i], *y);
        x = y;
    }
}

/**
 * scrypt ROMix function
 *
 * @data_in:  input
 * @data_out: output
 * @data_len: Length in bytes of the two buffers `data_in` and `data_out`.
 *            It is a multiple of 128.
 * @N:        Memory/CPU cost. It must be a power of 2.
 *
 * @data_in and @data_out may overlap.
 */
EXPORT_SYM int scryptROMix(const uint8_t *data_in, uint8_t *data_out,
                           size_t data_len, unsigned N, core_t *core)
{
    size_t two_r;
    uint8_t (*v)[64], (*x)[64];
    unsigned i;

    if (NULL==data_in || NULL==data_out || NULL==core) {
        return ERR_NULL;
    }

    two_r = data_len / 64;
    if ((two_r*64 != data_len) || (two_r & 1)) {
        return ERR_BLOCK_SIZE;
    }

    /** Allocate a possibly gigantic amount of memory **/
    /* It takes in total (N+1) * (128r) bytes */
    v = calloc(N + 1, data_len);
    if (NULL == v) {
        return ERR_MEMORY;
    }
    memmove(&v[0], data_in, data_len);
   
    /** Create the V array **/ 
    /* The size of each element of V is 64 bytes, so
     * you need to add `two_r` (not 1) to move forward.
     */
    x = v;
    for (i=0; i<N; i++) {
        scryptBlockMix(x, &x[two_r], two_r, core);
        x += two_r;
    }

    /** Mix via a pseudo random sequence **/
    x = &v[N * two_r];
    for (i=0; i<N; i++) {
        uint32_t index;

        index = LOAD_U32_LITTLE(&x[two_r - 1][0]) & (N - 1); /* Extract pseudo
                                                            random index
                                                            from last element
                                                            */
        strxor_inplace(*x, v[index * two_r], data_len);
        scryptBlockMix(x, (uint8_t (*)[64])data_out, two_r, core);
        memmove(x, data_out, data_len);
    }

    /** Outro **/
    /** data_out already contains the right value **/
    free(v);
    return 0;
}
