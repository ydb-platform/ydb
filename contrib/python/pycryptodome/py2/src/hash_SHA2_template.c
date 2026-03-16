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

#include <stdio.h>
#include "common.h"
#include "endianess.h"

FAKE_INIT(MODULE_NAME)

#define FUNC_NAME(pf) _PASTE2(MODULE_NAME, pf)

/**
 * SHA-2 as defined in FIPS 180-4 http://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
 */

#define CH(x,y,z)       (((x) & (y)) ^ (~(x) & (z)))
#define MAJ(x,y,z)      (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z)))

#define ROTR32(n, x)    (((x)>>(n)) | ((x)<<(32-(n))))
#define ROTR64(n, x)    (((x)>>(n)) | ((x)<<(64-(n))))
#define SHR(n,x)        ((x)>>(n))

#if WORD_SIZE==4

/** SHA-224, SHA-256 **/

typedef uint32_t sha2_word_t;

#define SCHEDULE_SIZE 64
#define BLOCK_SIZE 64

#define SIGMA_0_256(x)    (ROTR32(2,x)  ^ ROTR32(13,x) ^ ROTR32(22,x))
#define SIGMA_1_256(x)    (ROTR32(6,x)  ^ ROTR32(11,x) ^ ROTR32(25,x))
#define sigma_0_256(x)    (ROTR32(7,x)  ^ ROTR32(18,x) ^ SHR(3,x))
#define sigma_1_256(x)    (ROTR32(17,x) ^ ROTR32(19,x) ^ SHR(10,x))

static const sha2_word_t K[SCHEDULE_SIZE] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b,
    0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01,
    0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7,
    0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
    0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152,
    0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
    0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
    0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08,
    0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f,
    0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
};

#define SCHEDULE(i) (sigma_1_256(W[i-2]) + W[i-7] + sigma_0_256(W[i-15]) + W[i-16])

#define CYCLE(a,b,c,d,e,f,g,h,t) \
    h += SIGMA_1_256(e) + CH(e,f,g) + K[t]  + W[t]; \
    d += h; \
    h += SIGMA_0_256(a) + MAJ(a,b,c);

#define LOAD_WORD_BIG(p)        LOAD_U32_BIG(p)
#define STORE_WORD_BIG(p, w)    STORE_U32_BIG(p, w)

#elif WORD_SIZE==8

/** SHA-384, SHA-512 **/

typedef uint64_t sha2_word_t;

#define SCHEDULE_SIZE 80
#define BLOCK_SIZE 128

#define SIGMA_0_512(x)    (ROTR64(28,x) ^ ROTR64(34,x) ^ ROTR64(39,x))
#define SIGMA_1_512(x)    (ROTR64(14,x) ^ ROTR64(18,x) ^ ROTR64(41,x))
#define sigma_0_512(x)    (ROTR64(1,x)  ^ ROTR64(8,x)  ^ SHR(7,x))
#define sigma_1_512(x)    (ROTR64(19,x) ^ ROTR64(61,x) ^ SHR(6,x))

static const sha2_word_t K[SCHEDULE_SIZE] = {
    0x428a2f98d728ae22ULL, 0x7137449123ef65cdULL, 0xb5c0fbcfec4d3b2fULL, 0xe9b5dba58189dbbcULL,
    0x3956c25bf348b538ULL, 0x59f111f1b605d019ULL, 0x923f82a4af194f9bULL, 0xab1c5ed5da6d8118ULL,
    0xd807aa98a3030242ULL, 0x12835b0145706fbeULL, 0x243185be4ee4b28cULL, 0x550c7dc3d5ffb4e2ULL,
    0x72be5d74f27b896fULL, 0x80deb1fe3b1696b1ULL, 0x9bdc06a725c71235ULL, 0xc19bf174cf692694ULL,
    0xe49b69c19ef14ad2ULL, 0xefbe4786384f25e3ULL, 0x0fc19dc68b8cd5b5ULL, 0x240ca1cc77ac9c65ULL,
    0x2de92c6f592b0275ULL, 0x4a7484aa6ea6e483ULL, 0x5cb0a9dcbd41fbd4ULL, 0x76f988da831153b5ULL,
    0x983e5152ee66dfabULL, 0xa831c66d2db43210ULL, 0xb00327c898fb213fULL, 0xbf597fc7beef0ee4ULL,
    0xc6e00bf33da88fc2ULL, 0xd5a79147930aa725ULL, 0x06ca6351e003826fULL, 0x142929670a0e6e70ULL,
    0x27b70a8546d22ffcULL, 0x2e1b21385c26c926ULL, 0x4d2c6dfc5ac42aedULL, 0x53380d139d95b3dfULL,
    0x650a73548baf63deULL, 0x766a0abb3c77b2a8ULL, 0x81c2c92e47edaee6ULL, 0x92722c851482353bULL,
    0xa2bfe8a14cf10364ULL, 0xa81a664bbc423001ULL, 0xc24b8b70d0f89791ULL, 0xc76c51a30654be30ULL,
    0xd192e819d6ef5218ULL, 0xd69906245565a910ULL, 0xf40e35855771202aULL, 0x106aa07032bbd1b8ULL,
    0x19a4c116b8d2d0c8ULL, 0x1e376c085141ab53ULL, 0x2748774cdf8eeb99ULL, 0x34b0bcb5e19b48a8ULL,
    0x391c0cb3c5c95a63ULL, 0x4ed8aa4ae3418acbULL, 0x5b9cca4f7763e373ULL, 0x682e6ff3d6b2b8a3ULL,
    0x748f82ee5defb2fcULL, 0x78a5636f43172f60ULL, 0x84c87814a1f0ab72ULL, 0x8cc702081a6439ecULL,
    0x90befffa23631e28ULL, 0xa4506cebde82bde9ULL, 0xbef9a3f7b2c67915ULL, 0xc67178f2e372532bULL,
    0xca273eceea26619cULL, 0xd186b8c721c0c207ULL, 0xeada7dd6cde0eb1eULL, 0xf57d4f7fee6ed178ULL,
    0x06f067aa72176fbaULL, 0x0a637dc5a2c898a6ULL, 0x113f9804bef90daeULL, 0x1b710b35131c471bULL,
    0x28db77f523047d84ULL, 0x32caab7b40c72493ULL, 0x3c9ebe0a15c9bebcULL, 0x431d67c49c100d4cULL,
    0x4cc5d4becb3e42b6ULL, 0x597f299cfc657e2aULL, 0x5fcb6fab3ad6faecULL, 0x6c44198c4a475817ULL
};

#define SCHEDULE(i) (sigma_1_512(W[i-2]) + W[i-7] + sigma_0_512(W[i-15]) + W[i-16])

#define CYCLE(a,b,c,d,e,f,g,h,t) \
    h += SIGMA_1_512(e) + CH(e,f,g) + K[t]  + W[t]; \
    d += h; \
    h += SIGMA_0_512(a) + MAJ(a,b,c);

#define LOAD_WORD_BIG(p)        LOAD_U64_BIG(p)
#define STORE_WORD_BIG(p, w)    STORE_U64_BIG(p, w)

#else
#error Invalid WORD_SIZE
#endif

static inline void put_be(sha2_word_t number, uint8_t *p)
{
    int i;

    for (i=0; i<WORD_SIZE; i++) {
        p[WORD_SIZE-1-i] = (uint8_t)(number >> (i*8));
    }
}

typedef struct t_hash_state {
    sha2_word_t h[8];
    uint8_t buf[BLOCK_SIZE];    /** 16 words **/
    unsigned curlen;            /** Useful message bytes in buf[] (leftmost) **/
    sha2_word_t totbits[2];     /** Total message length in bits **/
    size_t digest_size;         /** Actual digest size in bytes **/
} hash_state;

static int add_bits(hash_state *hs, sha2_word_t bits)
{
    hs->totbits[0] += bits;
    if (hs->totbits[0] >= bits) {
        return 0;
    }

    /** Overflow **/
    hs->totbits[1] += 1;
    if (hs->totbits[1] > 0) {
        return 0;
    }

    return ERR_MAX_DATA;
}

static void sha_compress(hash_state * hs)
{
    sha2_word_t a, b, c, d, e, f, g, h;
    sha2_word_t W[SCHEDULE_SIZE];
    int i;

    /** Words flow in in big-endian mode **/
    for (i=0; i<16; i++) {
        W[i] = LOAD_WORD_BIG(&hs->buf[i*WORD_SIZE]);
    }
    for (;i<SCHEDULE_SIZE; i++) {
        W[i] = SCHEDULE(i);
    }

    a = hs->h[0];
    b = hs->h[1];
    c = hs->h[2];
    d = hs->h[3];
    e = hs->h[4];
    f = hs->h[5];
    g = hs->h[6];
    h = hs->h[7];

    CYCLE(a,b,c,d,e,f,g,h, 0);
    CYCLE(h,a,b,c,d,e,f,g, 1);
    CYCLE(g,h,a,b,c,d,e,f, 2);
    CYCLE(f,g,h,a,b,c,d,e, 3);
    CYCLE(e,f,g,h,a,b,c,d, 4);
    CYCLE(d,e,f,g,h,a,b,c, 5);
    CYCLE(c,d,e,f,g,h,a,b, 6);
    CYCLE(b,c,d,e,f,g,h,a, 7);
    CYCLE(a,b,c,d,e,f,g,h, 8);
    CYCLE(h,a,b,c,d,e,f,g, 9);
    CYCLE(g,h,a,b,c,d,e,f, 10);
    CYCLE(f,g,h,a,b,c,d,e, 11);
    CYCLE(e,f,g,h,a,b,c,d, 12);
    CYCLE(d,e,f,g,h,a,b,c, 13);
    CYCLE(c,d,e,f,g,h,a,b, 14);
    CYCLE(b,c,d,e,f,g,h,a, 15);
    CYCLE(a,b,c,d,e,f,g,h, 16);
    CYCLE(h,a,b,c,d,e,f,g, 17);
    CYCLE(g,h,a,b,c,d,e,f, 18);
    CYCLE(f,g,h,a,b,c,d,e, 19);
    CYCLE(e,f,g,h,a,b,c,d, 20);
    CYCLE(d,e,f,g,h,a,b,c, 21);
    CYCLE(c,d,e,f,g,h,a,b, 22);
    CYCLE(b,c,d,e,f,g,h,a, 23);
    CYCLE(a,b,c,d,e,f,g,h, 24);
    CYCLE(h,a,b,c,d,e,f,g, 25);
    CYCLE(g,h,a,b,c,d,e,f, 26);
    CYCLE(f,g,h,a,b,c,d,e, 27);
    CYCLE(e,f,g,h,a,b,c,d, 28);
    CYCLE(d,e,f,g,h,a,b,c, 29);
    CYCLE(c,d,e,f,g,h,a,b, 30);
    CYCLE(b,c,d,e,f,g,h,a, 31);
    CYCLE(a,b,c,d,e,f,g,h, 32);
    CYCLE(h,a,b,c,d,e,f,g, 33);
    CYCLE(g,h,a,b,c,d,e,f, 34);
    CYCLE(f,g,h,a,b,c,d,e, 35);
    CYCLE(e,f,g,h,a,b,c,d, 36);
    CYCLE(d,e,f,g,h,a,b,c, 37);
    CYCLE(c,d,e,f,g,h,a,b, 38);
    CYCLE(b,c,d,e,f,g,h,a, 39);
    CYCLE(a,b,c,d,e,f,g,h, 40);
    CYCLE(h,a,b,c,d,e,f,g, 41);
    CYCLE(g,h,a,b,c,d,e,f, 42);
    CYCLE(f,g,h,a,b,c,d,e, 43);
    CYCLE(e,f,g,h,a,b,c,d, 44);
    CYCLE(d,e,f,g,h,a,b,c, 45);
    CYCLE(c,d,e,f,g,h,a,b, 46);
    CYCLE(b,c,d,e,f,g,h,a, 47);
    CYCLE(a,b,c,d,e,f,g,h, 48);
    CYCLE(h,a,b,c,d,e,f,g, 49);
    CYCLE(g,h,a,b,c,d,e,f, 50);
    CYCLE(f,g,h,a,b,c,d,e, 51);
    CYCLE(e,f,g,h,a,b,c,d, 52);
    CYCLE(d,e,f,g,h,a,b,c, 53);
    CYCLE(c,d,e,f,g,h,a,b, 54);
    CYCLE(b,c,d,e,f,g,h,a, 55);
    CYCLE(a,b,c,d,e,f,g,h, 56);
    CYCLE(h,a,b,c,d,e,f,g, 57);
    CYCLE(g,h,a,b,c,d,e,f, 58);
    CYCLE(f,g,h,a,b,c,d,e, 59);
    CYCLE(e,f,g,h,a,b,c,d, 60);
    CYCLE(d,e,f,g,h,a,b,c, 61);
    CYCLE(c,d,e,f,g,h,a,b, 62);
    CYCLE(b,c,d,e,f,g,h,a, 63);

#if SCHEDULE_SIZE==80
    CYCLE(a,b,c,d,e,f,g,h, 64);
    CYCLE(h,a,b,c,d,e,f,g, 65);
    CYCLE(g,h,a,b,c,d,e,f, 66);
    CYCLE(f,g,h,a,b,c,d,e, 67);
    CYCLE(e,f,g,h,a,b,c,d, 68);
    CYCLE(d,e,f,g,h,a,b,c, 69);
    CYCLE(c,d,e,f,g,h,a,b, 70);
    CYCLE(b,c,d,e,f,g,h,a, 71);
    CYCLE(a,b,c,d,e,f,g,h, 72);
    CYCLE(h,a,b,c,d,e,f,g, 73);
    CYCLE(g,h,a,b,c,d,e,f, 74);
    CYCLE(f,g,h,a,b,c,d,e, 75);
    CYCLE(e,f,g,h,a,b,c,d, 76);
    CYCLE(d,e,f,g,h,a,b,c, 77);
    CYCLE(c,d,e,f,g,h,a,b, 78);
    CYCLE(b,c,d,e,f,g,h,a, 79);
#endif

    /** compute new intermediate hash **/
    hs->h[0] += a;
    hs->h[1] += b;
    hs->h[2] += c;
    hs->h[3] += d;
    hs->h[4] += e;
    hs->h[5] += f;
    hs->h[6] += g;
    hs->h[7] += h;
}

EXPORT_SYM int FUNC_NAME(_init)(hash_state **shaState
#if DIGEST_SIZE == (512/8)
        , size_t digest_size
#endif
        )
{
    hash_state *hs;
    int i;
#if DIGEST_SIZE == (512/8)
    size_t variant;
#endif

    if (NULL == shaState) {
        return ERR_NULL;
    }

    *shaState = hs = (hash_state*) calloc(1, sizeof(hash_state));
    if (NULL == hs)
        return ERR_MEMORY;

    hs->curlen = 0;
    hs->totbits[0] = hs->totbits[1] = 0;

    /** Digest size and initial intermediate hash value **/
#if DIGEST_SIZE == (512/8)
    hs->digest_size = digest_size;

    switch (digest_size) {
        case 28: variant = 1;   /** SHA-512/224 **/
                 break;
        case 32: variant = 2;   /** SHA-512/256 **/
                 break;
        default: variant = 0;   /** Vanilla SHA-512 **/
    }
    
    for (i=0; i<8; i++) {
        hs->h[i] = H_SHA_512[variant][i];
    }
#else
    hs->digest_size = DIGEST_SIZE;
    for (i=0; i<8; i++) {
        hs->h[i] = H[i];
    }
#endif

    return 0;
}

EXPORT_SYM int FUNC_NAME(_destroy)(hash_state *shaState)
{
    free(shaState);
    return 0;
}

EXPORT_SYM int FUNC_NAME(_update)(hash_state *hs, const uint8_t *buf, size_t len)
{
    if (NULL == hs || NULL == buf) {
        return ERR_NULL;
    }

    assert(hs->curlen < BLOCK_SIZE);

    while (len>0) {
        unsigned btc, left;

        left = BLOCK_SIZE - hs->curlen;
        btc = (unsigned)MIN(left, len);
        memcpy(&hs->buf[hs->curlen], buf, btc);
        buf += btc;
        hs->curlen += btc;
        len -= btc;

        if (hs->curlen == BLOCK_SIZE) {
            sha_compress(hs);
            hs->curlen = 0;
            if (add_bits(hs, BLOCK_SIZE*8)) {
                return ERR_MAX_DATA;
            }
        }
    }

    return 0;
}

static int sha_finalize(hash_state *hs, uint8_t *hash, size_t digest_size)
{
    unsigned left, i;
    uint8_t hash_tmp[WORD_SIZE*8];

    if (digest_size != hs->digest_size) {
        return ERR_DIGEST_SIZE;
    }

    /* remaining length of the message */
    if (add_bits(hs, hs->curlen*8)) {
        return ERR_MAX_DATA;
    }

    /* append the '1' bit */
    /* buf[] is guaranteed to have at least 1 byte free */
    hs->buf[hs->curlen++] = 0x80;

    /** if there are less then 64/128 bits left, just pad with zeroes and compress **/
    left = BLOCK_SIZE - hs->curlen;
    if (left < WORD_SIZE*2) {
        memset(&hs->buf[hs->curlen], 0, left);
        sha_compress(hs);
        hs->curlen = 0;
    }

    /**
     * pad with zeroes and close the block with the bit length
     * encoded as 64-bit integer big endian.
     **/
    left = BLOCK_SIZE - hs->curlen;
    memset(&hs->buf[hs->curlen], 0, left);
    STORE_WORD_BIG(&hs->buf[BLOCK_SIZE-(2*WORD_SIZE)], hs->totbits[1]);
    STORE_WORD_BIG(&hs->buf[BLOCK_SIZE-(  WORD_SIZE)], hs->totbits[0]);

    /** compress one last time **/
    sha_compress(hs);

    /** create final hash **/
    for (i=0; i<8; i++) {
        put_be(hs->h[i], &hash_tmp[i*WORD_SIZE]);
    }
    memcpy(hash, hash_tmp, hs->digest_size);

    return 0;
}

EXPORT_SYM int FUNC_NAME(_digest)(const hash_state *shaState, uint8_t *digest, size_t digest_size)
{
    hash_state temp;

    if (NULL == shaState) {
        return ERR_NULL;
    }

    if (digest_size != shaState->digest_size) {
        return ERR_DIGEST_SIZE;
    }

    temp = *shaState;
    sha_finalize(&temp, digest, digest_size);
    return 0;
}

EXPORT_SYM int FUNC_NAME(_copy)(const hash_state *src, hash_state *dst)
{
    if (NULL == src || NULL == dst) {
        return ERR_NULL;
    }

    *dst = *src;
    return 0;
}

/**
 * This is a specialized function to efficiently perform the inner loop of PBKDF2-HMAC.
 *
 * - inner, the hash after the inner padded secret has been absorbed
 * - outer, the hash after the outer padded secret has been absorbed
 * - first_hmac, the output of the first HMAC iteration (with salt and counter)
 * - result, the XOR of the HMACs from all iterations
 * - iterations, the total number of PBKDF2 iterations (>0)
 *
 * This function does not change the state of either hash.
 */
EXPORT_SYM int FUNC_NAME(_pbkdf2_hmac_assist)(const hash_state *inner, const hash_state *outer,
                                             const uint8_t *first_hmac,
                                             uint8_t *result,
                                             size_t iterations,
                                             size_t digest_size)
{
    hash_state inner_temp, outer_temp;
    size_t i;
    uint8_t last_hmac[DIGEST_SIZE]; /** MAX DIGEST SIZE **/

    if (NULL == inner || NULL == outer || NULL == first_hmac || NULL == result) {
        return ERR_NULL;
    }

    if (iterations == 0) {
        return ERR_NR_ROUNDS;
    }

    if (digest_size != inner->digest_size || digest_size != outer->digest_size) {
        return ERR_DIGEST_SIZE;
    }
    
    memcpy(result, first_hmac, digest_size);
    memcpy(last_hmac, first_hmac, digest_size);

    for (i=1; i<iterations; i++) {
        unsigned j;

        inner_temp = *inner;
        outer_temp = *outer;

        FUNC_NAME(_update)(&inner_temp, last_hmac, digest_size);
        sha_finalize(&inner_temp, last_hmac, digest_size);

        /** last_hmac is now the intermediate digest **/

        FUNC_NAME(_update)(&outer_temp, last_hmac, digest_size);
        sha_finalize(&outer_temp, last_hmac, digest_size);

        for (j=0; j<digest_size; j++) {
            result[j] ^= last_hmac[j];
        }
    }

    return 0;
}

#ifdef MAIN

void initialize(hash_state **hs)
{
#if DIGEST_SIZE == (512/8)
    FUNC_NAME(_init)(hs, 512);
#else
    FUNC_NAME(_init)(hs);
#endif
 
}

int main(void)
{
    hash_state *hs;
    const uint8_t tv[] = "The quick brown fox jumps over the lazy dog";
    uint8_t result[DIGEST_SIZE];
    int i;

    initialize(&hs);
    FUNC_NAME(_update)(hs, tv, sizeof tv - 1);
    FUNC_NAME(_digest)(hs, result);
    FUNC_NAME(_destroy)(hs);

    for (i=0; i < hs->digest_size; i++) {
        printf("%02X", result[i]);
    }
    printf("\n");

    initialize(&hs);
    FUNC_NAME(_digest)(hs, result);
    FUNC_NAME(_destroy)(hs);

    for (i=0; i< hs->digest_size; i++) {
        printf("%02X", result[i]);
    }
    printf("\n");

    initialize(&hs);
    for (i=0; i<10000000; i++) {
        FUNC_NAME(_update)(hs, tv, sizeof tv - 1);
    }
    FUNC_NAME(_destroy)(hs);

    printf("\n");
}
#endif
