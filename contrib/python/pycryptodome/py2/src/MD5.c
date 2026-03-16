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

FAKE_INIT(MD5)

/**
 * MD5 as defined in RFC1321
 */

#define F(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define G(x, y, z) (((x) & (z)) | ((y) & (~z)))
#define H(x, y, z) ((x) ^ (y) ^ (z))
#define I(x, y, z) ((y) ^ ((x) | (~z)))

#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32-(n))))

#define FF(a, b, c, d, x, s, ac) { \
 (a) += F ((b), (c), (d)) + (x) + (uint32_t)(ac); \
 (a) = ROTATE_LEFT ((a), (s)); \
 (a) += (b); \
 }

#define GG(a, b, c, d, x, s, ac) { \
 (a) += G ((b), (c), (d)) + (x) + (uint32_t)(ac); \
 (a) = ROTATE_LEFT ((a), (s)); \
 (a) += (b); \
 }

#define HH(a, b, c, d, x, s, ac) { \
 (a) += H ((b), (c), (d)) + (x) + (uint32_t)(ac); \
 (a) = ROTATE_LEFT ((a), (s)); \
 (a) += (b); \
 }

#define II(a, b, c, d, x, s, ac) { \
 (a) += I ((b), (c), (d)) + (x) + (uint32_t)(ac); \
 (a) = ROTATE_LEFT ((a), (s)); \
 (a) += (b); \
 }

#define S11 7
#define S12 12
#define S13 17
#define S14 22
#define S21 5
#define S22 9
#define S23 14
#define S24 20
#define S31 4
#define S32 11
#define S33 16
#define S34 23
#define S41 6
#define S42 10
#define S43 15
#define S44 21

#define BLOCK_SIZE 64

#define DIGEST_SIZE (128/8)

typedef struct t_hash_state {
    uint32_t h[4];
    uint8_t buf[BLOCK_SIZE];    /** 64 bytes == 512 bits == sixteen 32-bit words **/
    unsigned curlen;            /** Useful message bytes in buf[] (leftmost) **/
    uint64_t totbits;           /** Total message length in bits **/
} hash_state;

static int add_bits(hash_state *hs, unsigned bits)
{
    /** Maximum message length for MD5 is 2**64 bits **/
    hs->totbits += bits;
    return (hs->totbits < bits) ? ERR_MAX_DATA : 0;
}

static void md5_compress(hash_state * hs)
{
    uint32_t a, b, c, d;
    uint32_t x[16];
    int i;

    /** Words flow in in little-endian mode **/
    for (i=0; i<16; i++) {
        x[i] = LOAD_U32_LITTLE(&hs->buf[i*4]);
    }

    a = hs->h[0];
    b = hs->h[1];
    c = hs->h[2];
    d = hs->h[3];
  
	/* Round 1 */
  	FF (a, b, c, d, x[ 0], S11, 0xd76aa478); /* 1 */
  	FF (d, a, b, c, x[ 1], S12, 0xe8c7b756); /* 2 */
 	FF (c, d, a, b, x[ 2], S13, 0x242070db); /* 3 */
  	FF (b, c, d, a, x[ 3], S14, 0xc1bdceee); /* 4 */
  	FF (a, b, c, d, x[ 4], S11, 0xf57c0faf); /* 5 */
  	FF (d, a, b, c, x[ 5], S12, 0x4787c62a); /* 6 */
  	FF (c, d, a, b, x[ 6], S13, 0xa8304613); /* 7 */
  	FF (b, c, d, a, x[ 7], S14, 0xfd469501); /* 8 */
  	FF (a, b, c, d, x[ 8], S11, 0x698098d8); /* 9 */
  	FF (d, a, b, c, x[ 9], S12, 0x8b44f7af); /* 10 */
  	FF (c, d, a, b, x[10], S13, 0xffff5bb1); /* 11 */
  	FF (b, c, d, a, x[11], S14, 0x895cd7be); /* 12 */
  	FF (a, b, c, d, x[12], S11, 0x6b901122); /* 13 */
  	FF (d, a, b, c, x[13], S12, 0xfd987193); /* 14 */
  	FF (c, d, a, b, x[14], S13, 0xa679438e); /* 15 */
  	FF (b, c, d, a, x[15], S14, 0x49b40821); /* 16 */

 	/* Round 2 */
  	GG (a, b, c, d, x[ 1], S21, 0xf61e2562); /* 17 */
  	GG (d, a, b, c, x[ 6], S22, 0xc040b340); /* 18 */
  	GG (c, d, a, b, x[11], S23, 0x265e5a51); /* 19 */
  	GG (b, c, d, a, x[ 0], S24, 0xe9b6c7aa); /* 20 */
  	GG (a, b, c, d, x[ 5], S21, 0xd62f105d); /* 21 */
  	GG (d, a, b, c, x[10], S22,  0x2441453); /* 22 */
  	GG (c, d, a, b, x[15], S23, 0xd8a1e681); /* 23 */
  	GG (b, c, d, a, x[ 4], S24, 0xe7d3fbc8); /* 24 */
  	GG (a, b, c, d, x[ 9], S21, 0x21e1cde6); /* 25 */
  	GG (d, a, b, c, x[14], S22, 0xc33707d6); /* 26 */
  	GG (c, d, a, b, x[ 3], S23, 0xf4d50d87); /* 27 */
  	GG (b, c, d, a, x[ 8], S24, 0x455a14ed); /* 28 */
  	GG (a, b, c, d, x[13], S21, 0xa9e3e905); /* 29 */
  	GG (d, a, b, c, x[ 2], S22, 0xfcefa3f8); /* 30 */
  	GG (c, d, a, b, x[ 7], S23, 0x676f02d9); /* 31 */
  	GG (b, c, d, a, x[12], S24, 0x8d2a4c8a); /* 32 */

  	/* Round 3 */
  	HH (a, b, c, d, x[ 5], S31, 0xfffa3942); /* 33 */
  	HH (d, a, b, c, x[ 8], S32, 0x8771f681); /* 34 */
  	HH (c, d, a, b, x[11], S33, 0x6d9d6122); /* 35 */
  	HH (b, c, d, a, x[14], S34, 0xfde5380c); /* 36 */
  	HH (a, b, c, d, x[ 1], S31, 0xa4beea44); /* 37 */
  	HH (d, a, b, c, x[ 4], S32, 0x4bdecfa9); /* 38 */
  	HH (c, d, a, b, x[ 7], S33, 0xf6bb4b60); /* 39 */
  	HH (b, c, d, a, x[10], S34, 0xbebfbc70); /* 40 */
  	HH (a, b, c, d, x[13], S31, 0x289b7ec6); /* 41 */
  	HH (d, a, b, c, x[ 0], S32, 0xeaa127fa); /* 42 */
  	HH (c, d, a, b, x[ 3], S33, 0xd4ef3085); /* 43 */
  	HH (b, c, d, a, x[ 6], S34,  0x4881d05); /* 44 */
  	HH (a, b, c, d, x[ 9], S31, 0xd9d4d039); /* 45 */
  	HH (d, a, b, c, x[12], S32, 0xe6db99e5); /* 46 */
  	HH (c, d, a, b, x[15], S33, 0x1fa27cf8); /* 47 */
  	HH (b, c, d, a, x[ 2], S34, 0xc4ac5665); /* 48 */

  	/* Round 4 */
  	II (a, b, c, d, x[ 0], S41, 0xf4292244); /* 49 */
  	II (d, a, b, c, x[ 7], S42, 0x432aff97); /* 50 */
  	II (c, d, a, b, x[14], S43, 0xab9423a7); /* 51 */
  	II (b, c, d, a, x[ 5], S44, 0xfc93a039); /* 52 */
  	II (a, b, c, d, x[12], S41, 0x655b59c3); /* 53 */
  	II (d, a, b, c, x[ 3], S42, 0x8f0ccc92); /* 54 */
  	II (c, d, a, b, x[10], S43, 0xffeff47d); /* 55 */
  	II (b, c, d, a, x[ 1], S44, 0x85845dd1); /* 56 */
  	II (a, b, c, d, x[ 8], S41, 0x6fa87e4f); /* 57 */
  	II (d, a, b, c, x[15], S42, 0xfe2ce6e0); /* 58 */
  	II (c, d, a, b, x[ 6], S43, 0xa3014314); /* 59 */
  	II (b, c, d, a, x[13], S44, 0x4e0811a1); /* 60 */
  	II (a, b, c, d, x[ 4], S41, 0xf7537e82); /* 61 */
  	II (d, a, b, c, x[11], S42, 0xbd3af235); /* 62 */
  	II (c, d, a, b, x[ 2], S43, 0x2ad7d2bb); /* 63 */
  	II (b, c, d, a, x[ 9], S44, 0xeb86d391); /* 64 */

    /** compute new intermediate hash **/
    hs->h[0] += a;
    hs->h[1] += b;
    hs->h[2] += c;
    hs->h[3] += d;
}

EXPORT_SYM int MD5_init(hash_state **mdState)
{
    hash_state *hs;

    if (NULL == mdState) {
        return ERR_NULL;
    }

    *mdState = hs = (hash_state*) calloc(1, sizeof(hash_state));
    if (NULL == hs)
        return ERR_MEMORY;

    hs->curlen = 0;
    hs->totbits = 0;

    /** Initial intermediate hash value **/
    hs->h[0] = 0x67452301;
    hs->h[1] = 0xefcdab89;
    hs->h[2] = 0x98badcfe;
    hs->h[3] = 0x10325476;

    return 0;
}

EXPORT_SYM int MD5_destroy (hash_state *mdState)
{
    free(mdState);
    return 0;
}

EXPORT_SYM int MD5_update(hash_state *hs, const uint8_t *buf, size_t len)
{
    if (NULL == hs || NULL == buf) {
        return ERR_NULL;
    }

    assert(hs->curlen < BLOCK_SIZE);

    while (len>0) {
        unsigned left, btc;

        left = BLOCK_SIZE - hs->curlen;
        btc = (unsigned)MIN(left, len);
        memcpy(&hs->buf[hs->curlen], buf, btc);
        buf += btc;
        hs->curlen += btc;
        len -= btc;

        if (hs->curlen == BLOCK_SIZE) {
            md5_compress(hs);
            hs->curlen = 0;
            if (add_bits(hs, BLOCK_SIZE*8)) {
                return ERR_MAX_DATA;
            }
        }
    }

    return 0;
}

static int md5_finalize(hash_state *hs, uint8_t *hash /** [DIGEST_SIZE] **/)
{
    unsigned left, i;

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
        md5_compress(hs);
        hs->curlen = 0;
    }

    /**
     * pad with zeroes and close the block with the bit length
     * encoded as 64-bit integer little endian.
     **/
    left = BLOCK_SIZE - hs->curlen;
    memset(&hs->buf[hs->curlen], 0, left);
    STORE_U64_LITTLE(&hs->buf[BLOCK_SIZE-8], hs->totbits);

    /** compress one last time **/
    md5_compress(hs);

    /** create final hash **/
    for (i=0; i<4; i++) {
        STORE_U32_LITTLE(&hash[i*4], hs->h[i]);
    }

    return 0;
}

EXPORT_SYM int MD5_digest(const hash_state *mdState, uint8_t digest[DIGEST_SIZE])
{
    hash_state temp;

    if (NULL == mdState) {
        return ERR_NULL;
    }

    temp = *mdState;
    md5_finalize(&temp, digest);
    return 0;
}

EXPORT_SYM int MD5_copy(const hash_state *src, hash_state *dst)
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
EXPORT_SYM int MD5_pbkdf2_hmac_assist(const hash_state *inner, const hash_state *outer,
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

        MD5_update(&inner_temp, last_hmac, DIGEST_SIZE);
        md5_finalize(&inner_temp, last_hmac);

        /** last_hmac is now the intermediate digest **/

        MD5_update(&outer_temp, last_hmac, DIGEST_SIZE);
        md5_finalize(&outer_temp, last_hmac);

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

    MD5_init(&hs);
    MD5_update(hs, tv, sizeof tv - 1);
    MD5_digest(hs, result);
    MD5_destroy(hs);

    for (i=0; i<sizeof result; i++) {
        printf("%02X", result[i]);
    }
    printf("\n");

    MD5_init(&hs);
    MD5_digest(hs, result);
    MD5_destroy(hs);

    for (i=0; i<sizeof result; i++) {
        printf("%02X", result[i]);
    }
    printf("\n");

    MD5_init(&hs);
    for (i=0; i<10000000; i++) {
        MD5_update(hs, tv, sizeof tv - 1);
    }
    MD5_destroy(hs);

    printf("\n");
}
#endif
