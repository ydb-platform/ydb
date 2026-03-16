/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "md5.h"
#include "grib_api_internal.h"

#include <stdio.h>
#include <string.h>

/* On CRAY, disable all automatic optimisations for this module */
#if _CRAYC
#pragma _CRI noopt
#endif

static const unsigned long r[] = {
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
    5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21
};

static const unsigned long k[] = {
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391
};

#define ROT(x, c) ((x << c) | (x >> (32 - c)))

#define ECC_F(x, y, z) ((x & y) | ((~x) & z))
#define ECC_G(x, y, z) ((x & z) | (y & (~z)))
#define ECC_H(x, y, z) (x ^ y ^ z)
#define ECC_I(x, y, z) (y ^ (x | (~z)))

#define F_(A, B, C, D, g, i)        \
    A += ECC_F(B, C, D) + w[g] + k[i]; \
    A &= 0xffffffff;                \
    A = ROT(A, r[i]);               \
    A += B;
#define G_(A, B, C, D, g, i)        \
    A += ECC_G(B, C, D) + w[g] + k[i]; \
    A &= 0xffffffff;                \
    A = ROT(A, r[i]);               \
    A += B;
#define H_(A, B, C, D, g, i)        \
    A += ECC_H(B, C, D) + w[g] + k[i]; \
    A &= 0xffffffff;                \
    A = ROT(A, r[i]);               \
    A += B;
#define I_(A, B, C, D, g, i)        \
    A += ECC_I(B, C, D) + w[g] + k[i]; \
    A &= 0xffffffff;                \
    A = ROT(A, r[i]);               \
    A += B;

static void grib_md5_flush(grib_md5_state* s)
{
    unsigned long a  = s->h0;
    unsigned long b  = s->h1;
    unsigned long c  = s->h2;
    unsigned long d  = s->h3;
    unsigned long* w = &s->words[0];

    F_(a, b, c, d, 0, 0);
    F_(d, a, b, c, 1, 1);
    F_(c, d, a, b, 2, 2);
    F_(b, c, d, a, 3, 3);

    F_(a, b, c, d, 4, 4);
    F_(d, a, b, c, 5, 5);
    F_(c, d, a, b, 6, 6);
    F_(b, c, d, a, 7, 7);

    F_(a, b, c, d, 8, 8);
    F_(d, a, b, c, 9, 9);
    F_(c, d, a, b, 10, 10);
    F_(b, c, d, a, 11, 11);

    F_(a, b, c, d, 12, 12);
    F_(d, a, b, c, 13, 13);
    F_(c, d, a, b, 14, 14);
    F_(b, c, d, a, 15, 15);

    G_(a, b, c, d, 1, 16);
    G_(d, a, b, c, 6, 17);
    G_(c, d, a, b, 11, 18);
    G_(b, c, d, a, 0, 19);

    G_(a, b, c, d, 5, 20);
    G_(d, a, b, c, 10, 21);
    G_(c, d, a, b, 15, 22);
    G_(b, c, d, a, 4, 23);

    G_(a, b, c, d, 9, 24);
    G_(d, a, b, c, 14, 25);
    G_(c, d, a, b, 3, 26);
    G_(b, c, d, a, 8, 27);

    G_(a, b, c, d, 13, 28);
    G_(d, a, b, c, 2, 29);
    G_(c, d, a, b, 7, 30);
    G_(b, c, d, a, 12, 31);

    H_(a, b, c, d, 5, 32);
    H_(d, a, b, c, 8, 33);
    H_(c, d, a, b, 11, 34);
    H_(b, c, d, a, 14, 35);

    H_(a, b, c, d, 1, 36);
    H_(d, a, b, c, 4, 37);
    H_(c, d, a, b, 7, 38);
    H_(b, c, d, a, 10, 39);

    H_(a, b, c, d, 13, 40);
    H_(d, a, b, c, 0, 41);
    H_(c, d, a, b, 3, 42);
    H_(b, c, d, a, 6, 43);

    H_(a, b, c, d, 9, 44);
    H_(d, a, b, c, 12, 45);
    H_(c, d, a, b, 15, 46);
    H_(b, c, d, a, 2, 47);

    I_(a, b, c, d, 0, 48);
    I_(d, a, b, c, 7, 49);
    I_(c, d, a, b, 14, 50);
    I_(b, c, d, a, 5, 51);

    I_(a, b, c, d, 12, 52);
    I_(d, a, b, c, 3, 53);
    I_(c, d, a, b, 10, 54);
    I_(b, c, d, a, 1, 55);

    I_(a, b, c, d, 8, 56);
    I_(d, a, b, c, 15, 57);
    I_(c, d, a, b, 6, 58);
    I_(b, c, d, a, 13, 59);

    I_(a, b, c, d, 4, 60);
    I_(d, a, b, c, 11, 61);
    I_(c, d, a, b, 2, 62);
    I_(b, c, d, a, 9, 63);

    s->h0 += a;
    s->h1 += b;
    s->h2 += c;
    s->h3 += d;

    s->word_count = 0;
}

void grib_md5_init(grib_md5_state* s)
{
    ECCODES_ASSERT(sizeof(uint64_t) == 8);
    memset(s, 0, sizeof(grib_md5_state));
    s->h0 = 0x67452301;
    s->h1 = 0xefcdab89;
    s->h2 = 0x98badcfe;
    s->h3 = 0x10325476;
}

void grib_md5_add(grib_md5_state* s, const void* data, size_t len)
{
    unsigned char* p = (unsigned char*)data;
    s->size += len;

    while (len-- > 0) {
        s->bytes[s->byte_count++] = *p++;

        if (s->byte_count == 4) {
            s->words[s->word_count++] = (s->bytes[3] << 24) | (s->bytes[2] << 16) | (s->bytes[1] << 8) | (s->bytes[0]);
            s->byte_count             = 0;

            if (s->word_count == 16)
                grib_md5_flush(s);
        }
    }
}

void grib_md5_end(grib_md5_state* s, char* digest)
{
    uint64_t h = 8;
    uint64_t bits, leng = s->size * h;
    unsigned char c = 0x80;
    int i;

    grib_md5_add(s, &c, 1);

    bits = s->size * h;
    c    = 0;
    while ((bits % 512) != 448) {
        grib_md5_add(s, &c, 1);
        bits = s->size * h;
    }

    for (i = 0; i < 8; i++) {
        c = leng & 0xff;
        leng >>= 8;
        grib_md5_add(s, &c, 1);
    }

#define U(x) ((unsigned int)(x))

    snprintf(digest, 1024, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
            U(s->h0 & 0xff), U((s->h0 >> 8) & 0xff), U((s->h0 >> 16) & 0xff), U((s->h0 >> 24) & 0xff),
            U(s->h1 & 0xff), U((s->h1 >> 8) & 0xff), U((s->h1 >> 16) & 0xff), U((s->h1 >> 24) & 0xff),
            U(s->h2 & 0xff), U((s->h2 >> 8) & 0xff), U((s->h2 >> 16) & 0xff), U((s->h2 >> 24) & 0xff),
            U(s->h3 & 0xff), U((s->h3 >> 8) & 0xff), U((s->h3 >> 16) & 0xff), U((s->h3 >> 24) & 0xff));
}

#if defined(TESTING_MD5)

main(int argc, char **argv)
{
    char digest[1024];

    const char* p = "The quick brown fox jumps over the lazy dog";
    grib_md5_state s;
    grib_md5_init(&s);
    if(argc>1) {
        char buffer[10240];
        long len = 0;
        FILE* f = fopen64(argv[1],"r");
        if(!f) {
            perror(argv[1]);
            return 1;
        }
        while((len = fread(buffer, 1, sizeof(buffer), f)) > 0) {
            grib_md5_add(&s,buffer,len);
        }
        fclose(f);
    }
    else
    {
        grib_md5_add(&s,p,strlen(p));
    }
    grib_md5_end(&s, digest);

    printf("%s\n",digest);
}

#endif
