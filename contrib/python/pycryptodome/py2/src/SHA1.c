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

FAKE_INIT(SHA1)

/**
 * SHA-1 as defined in FIPS 180-4 http://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
 */

#define CH(x,y,z)       ((x & y) ^ (~x & z))            /** 0  <= t <= 19 **/
#define PARITY(x,y,z)   (x ^ y ^ z)                     /** 20 <= t <= 39  and 60 <= t <= 79 **/
#define MAJ(x,y,z)      ((x & y) ^ (x & z) ^ (y & z))   /** 40 <= t <= 59 **/

#define ROTL1(x)        (((x)<<1)  | ((x)>>(32-1)))
#define ROTL5(x)        (((x)<<5)  | ((x)>>(32-5)))
#define ROTL30(x)       (((x)<<30) | ((x)>>(32-30)))

#define Kx  0x5a827999  /** 0  <= t <= 19 **/
#define Ky  0x6ed9eba1  /** 20 <= t <= 39 **/
#define Kz  0x8f1bbcdc  /** 40 <= t <= 59 **/
#define Kw  0xca62c1d6  /** 60 <= t <= 79 **/

/** Compute and update W[t] for t>=16 **/
#define SCHED(t)        (W[t&15]=ROTL1(W[(t-3)&15] ^ W[(t-8)&15] ^ W[(t-14)&15] ^ W[t&15]))

#define ROUND_0_15(t) {                         \
    uint32_t T;                                 \
    T = ROTL5(a) + CH(b,c,d) + e + Kx + W[t];   \
    e = d;                                      \
    d = c;                                      \
    c = ROTL30(b);                              \
    b = a;                                      \
    a = T; }

#define ROUND_16_19(t) {                                \
    uint32_t T;                                         \
    T = ROTL5(a) + CH(b,c,d) + e + Kx + SCHED(t);       \
    e = d;                                              \
    d = c;                                              \
    c = ROTL30(b);                                      \
    b = a;                                              \
    a = T; }

#define ROUND_20_39(t) {                                \
    uint32_t T;                                         \
    T = ROTL5(a) + PARITY(b,c,d) + e + Ky + SCHED(t); \
    e = d;                                              \
    d = c;                                              \
    c = ROTL30(b);                                      \
    b = a;                                              \
    a = T; }

#define ROUND_40_59(t) {                            \
    uint32_t T;                                     \
    T = ROTL5(a) + MAJ(b,c,d) + e + Kz + SCHED(t);  \
    e = d;                                          \
    d = c;                                          \
    c = ROTL30(b);                                  \
    b = a;                                          \
    a = T; }

#define ROUND_60_79(t) {                                \
    uint32_t T;                                         \
    T = ROTL5(a) + PARITY(b,c,d) + e + Kw + SCHED(t);   \
    e = d;                                              \
    d = c;                                              \
    c = ROTL30(b);                                      \
    b = a;                                              \
    a = T; }

#define BLOCK_SIZE 64

#define DIGEST_SIZE (160/8)

typedef struct t_hash_state {
    uint32_t h[5];
    uint8_t buf[BLOCK_SIZE];    /** 64 bytes == 512 bits == sixteen 32-bit words **/
    unsigned curlen;            /** Useful message bytes in buf[] (leftmost) **/
    uint64_t totbits;           /** Total message length in bits **/
} hash_state;

static int add_bits(hash_state *hs, unsigned bits)
{
    /** Maximum message length for SHA-1 is 2**64 bits **/
    hs->totbits += bits;
    return (hs->totbits < bits) ? ERR_MAX_DATA : 0;
}

static void sha_compress(hash_state * hs)
{
    uint32_t a, b, c, d, e;
    uint32_t W[16];
    int i;

    /** Words flow in in big-endian mode **/
    for (i=0; i<16; i++) {
        W[i] = LOAD_U32_BIG(&hs->buf[i*4]);
    }

    a = hs->h[0];
    b = hs->h[1];
    c = hs->h[2];
    d = hs->h[3];
    e = hs->h[4];

    /** 0 <= t <= 15 **/
    ROUND_0_15(0);
    ROUND_0_15(1);
    ROUND_0_15(2);
    ROUND_0_15(3);
    ROUND_0_15(4);
    ROUND_0_15(5);
    ROUND_0_15(6);
    ROUND_0_15(7);
    ROUND_0_15(8);
    ROUND_0_15(9);
    ROUND_0_15(10);
    ROUND_0_15(11);
    ROUND_0_15(12);
    ROUND_0_15(13);
    ROUND_0_15(14);
    ROUND_0_15(15);
    /** 16 <= t <= 19 **/
    ROUND_16_19(16);
    ROUND_16_19(17);
    ROUND_16_19(18);
    ROUND_16_19(19);
    /** 20 <= t <= 39 **/
    ROUND_20_39(20);
    ROUND_20_39(21);
    ROUND_20_39(22);
    ROUND_20_39(23);
    ROUND_20_39(24);
    ROUND_20_39(25);
    ROUND_20_39(26);
    ROUND_20_39(27);
    ROUND_20_39(28);
    ROUND_20_39(29);
    ROUND_20_39(30);
    ROUND_20_39(31);
    ROUND_20_39(32);
    ROUND_20_39(33);
    ROUND_20_39(34);
    ROUND_20_39(35);
    ROUND_20_39(36);
    ROUND_20_39(37);
    ROUND_20_39(38);
    ROUND_20_39(39);
    /** 40 <= t <= 59 **/
    ROUND_40_59(40);
    ROUND_40_59(41);
    ROUND_40_59(42);
    ROUND_40_59(43);
    ROUND_40_59(44);
    ROUND_40_59(45);
    ROUND_40_59(46);
    ROUND_40_59(47);
    ROUND_40_59(48);
    ROUND_40_59(49);
    ROUND_40_59(50);
    ROUND_40_59(51);
    ROUND_40_59(52);
    ROUND_40_59(53);
    ROUND_40_59(54);
    ROUND_40_59(55);
    ROUND_40_59(56);
    ROUND_40_59(57);
    ROUND_40_59(58);
    ROUND_40_59(59);
    /** 60 <= t <= 79 **/
    ROUND_60_79(60);
    ROUND_60_79(61);
    ROUND_60_79(62);
    ROUND_60_79(63);
    ROUND_60_79(64);
    ROUND_60_79(65);
    ROUND_60_79(66);
    ROUND_60_79(67);
    ROUND_60_79(68);
    ROUND_60_79(69);
    ROUND_60_79(70);
    ROUND_60_79(71);
    ROUND_60_79(72);
    ROUND_60_79(73);
    ROUND_60_79(74);
    ROUND_60_79(75);
    ROUND_60_79(76);
    ROUND_60_79(77);
    ROUND_60_79(78);
    ROUND_60_79(79);

    /** compute new intermediate hash **/
    hs->h[0] += a;
    hs->h[1] += b;
    hs->h[2] += c;
    hs->h[3] += d;
    hs->h[4] += e;
}

EXPORT_SYM int SHA1_init(hash_state **shaState)
{
    hash_state *hs;

    if (NULL == shaState) {
        return ERR_NULL;
    }

    *shaState = hs = (hash_state*) calloc(1, sizeof(hash_state));
    if (NULL == hs)
        return ERR_MEMORY;

    hs->curlen = 0;
    hs->totbits = 0;

    /** Initial intermediate hash value **/
    hs->h[0] = 0x67452301;
    hs->h[1] = 0xefcdab89;
    hs->h[2] = 0x98badcfe;
    hs->h[3] = 0x10325476;
    hs->h[4] = 0xc3d2e1f0;

    return 0;
}

EXPORT_SYM int SHA1_destroy (hash_state *shaState)
{
    free(shaState);
    return 0;
}

EXPORT_SYM int SHA1_update(hash_state *hs, const uint8_t *buf, size_t len)
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

static int sha_finalize(hash_state *hs, uint8_t *hash /** [DIGEST_SIZE] **/)
{
    unsigned left, i;
    uint32_t lo, high;

    assert(hs->curlen < BLOCK_SIZE);

    /* remaining length of the message */
    if (add_bits(hs, hs->curlen*8)) {
        return ERR_MAX_DATA;
    }

    /* append the '1' bit */
    /* buf[] is guaranteed to have at least 1 byte free */
    hs->buf[hs->curlen++] = 0x80;

    /** if there are less then 64 bits lef, just pad with zeroes and compress **/
    left = BLOCK_SIZE - hs->curlen;
    if (left < 8) {
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
    lo = (uint32_t)(hs->totbits >> 32);
    high = (uint32_t)hs->totbits;
    STORE_U32_BIG(&hs->buf[BLOCK_SIZE-8], lo);
    STORE_U32_BIG(&hs->buf[BLOCK_SIZE-4], high);

    /** compress one last time **/
    sha_compress(hs);

    /** create final hash **/
    for (i=0; i<5; i++) {
        STORE_U32_BIG(hash, hs->h[i]);
        hash += 4;
    }

    return 0;
}

EXPORT_SYM int SHA1_digest(const hash_state *shaState, uint8_t digest[DIGEST_SIZE])
{
    hash_state temp;

    if (NULL == shaState) {
        return ERR_NULL;
    }

    temp = *shaState;
    sha_finalize(&temp, digest);
    return 0;
}

EXPORT_SYM int SHA1_copy(const hash_state *src, hash_state *dst)
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
EXPORT_SYM int SHA1_pbkdf2_hmac_assist(const hash_state *inner, const hash_state *outer,
                                             const uint8_t first_hmac[DIGEST_SIZE],
                                             uint8_t result[DIGEST_SIZE],
                                             size_t iterations)
{
    hash_state inner_temp, outer_temp;
    size_t i;
    uint8_t last_hmac[DIGEST_SIZE];

    if (NULL == inner || NULL == outer || NULL == first_hmac || NULL == result) {
        return ERR_NULL;
    }

    if (iterations == 0) {
        return ERR_NR_ROUNDS;
    }

    memcpy(result, first_hmac, DIGEST_SIZE);
    memcpy(last_hmac, first_hmac, DIGEST_SIZE);

    for (i=1; i<iterations; i++) {
        int j;

        inner_temp = *inner;
        outer_temp = *outer;

        SHA1_update(&inner_temp, last_hmac, DIGEST_SIZE);
        sha_finalize(&inner_temp, last_hmac);

        /** last_hmac is now the intermediate digest **/

        SHA1_update(&outer_temp, last_hmac, DIGEST_SIZE);
        sha_finalize(&outer_temp, last_hmac);

        for (j=0; j<DIGEST_SIZE; j++) {
            result[j] ^= last_hmac[j];
        }
    }

    return 0;
}

#ifdef MAIN
int main(void)
{
    hash_state *hs;
    const uint8_t tv[] = "The quick brown fox jumps over the lazy dog";
    uint8_t result[DIGEST_SIZE];
    int i;

    SHA1_init(&hs);
    SHA1_update(hs, tv, sizeof tv - 1);
    SHA1_digest(hs, result);
    SHA1_destroy(hs);

    for (i=0; i<sizeof result; i++) {
        printf("%02X", result[i]);
    }
    printf("\n");

    SHA1_init(&hs);
    SHA1_digest(hs, result);
    SHA1_destroy(hs);

    for (i=0; i<sizeof result; i++) {
        printf("%02X", result[i]);
    }
    printf("\n");

    SHA1_init(&hs);
    for (i=0; i<10000000; i++) {
        SHA1_update(hs, tv, sizeof tv - 1);
    }
    SHA1_destroy(hs);

    printf("\n");
}
#endif
