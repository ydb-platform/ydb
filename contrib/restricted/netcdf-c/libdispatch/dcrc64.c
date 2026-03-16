/* crc64.c -- compute CRC-64
 * Copyright (C) 2013 Mark Adler
 * Version 1.4  16 Dec 2013  Mark Adler
 */

/*
  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the author be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.

  Mark Adler
  madler@alumni.caltech.edu
 */

/* Compute CRC-64 in the manner of xz, using the ECMA-182 polynomial,
   bit-reversed, with one's complement pre and post processing.  Provide a
   means to combine separately computed CRC-64's. */

/* Version history:
   1.0  13 Dec 2013  First version
   1.1  13 Dec 2013  Fix comments in test code
   1.2  14 Dec 2013  Determine endianness at run time
   1.3  15 Dec 2013  Add eight-byte processing for big endian as well
                     Make use of the pthread library optional
   1.4  16 Dec 2013  Make once variable volatile for limited thread protection
 */

#include "config.h"
#include <stdio.h>
#include <inttypes.h>
#include <assert.h>

#include "ncexternl.h"

/* The include of pthread.h below can be commented out in order to not use the
   pthread library for table initialization.  In that case, the initialization
   will not be thread-safe.  That's fine, so long as it can be assured that
   there is only one thread using crc64(). */
#if 0
#include <pthread.h>            /* link with -lpthread */
#endif

/* 64-bit CRC polynomial with these coefficients, but reversed:
    64, 62, 57, 55, 54, 53, 52, 47, 46, 45, 40, 39, 38, 37, 35, 33, 32,
    31, 29, 27, 24, 23, 22, 21, 19, 17, 13, 12, 10, 9, 7, 4, 1, 0 */
#define POLY UINT64_C(0xc96c5795d7870f42)

/* Tables for CRC calculation -- filled in by initialization functions that are
   called once.  These could be replaced by constant tables generated in the
   same way.  There are two tables, one for each endianness.  Since these are
   static, i.e. local, one should be compiled out of existence if the compiler
   can evaluate the endianness check in crc64() at compile time. */
static uint64 crc64_little_table[8][256];
static uint64 crc64_big_table[8][256];

/* Fill in the CRC-64 constants table. */
static void crc64_init(uint64 table[][256])
{
    unsigned n, k;
    uint64 crc;

    /* generate CRC-64's for all single byte sequences */
    for (n = 0; n < 256; n++) {
        crc = n;
        for (k = 0; k < 8; k++)
            crc = crc & 1 ? POLY ^ (crc >> 1) : crc >> 1;
        table[0][n] = crc;
    }

    /* generate CRC-64's for those followed by 1 to 7 zeros */
    for (n = 0; n < 256; n++) {
        crc = table[0][n];
        for (k = 1; k < 8; k++) {
            crc = table[0][crc & 0xff] ^ (crc >> 8);
            table[k][n] = crc;
        }
    }
}

/* This function is called once to initialize the CRC-64 table for use on a
   little-endian architecture. */
static void crc64_little_init(void)
{
    crc64_init(crc64_little_table);
}

/* Reverse the bytes in a 64-bit word. */
static inline uint64 rev8(uint64 a)
{
    uint64 m;

    m = UINT64_C(0xff00ff00ff00ff);
    a = ((a >> 8) & m) | (a & m) << 8;
    m = UINT64_C(0xffff0000ffff);
    a = ((a >> 16) & m) | (a & m) << 16;
    return a >> 32 | a << 32;
}

/* This function is called once to initialize the CRC-64 table for use on a
   big-endian architecture. */
static void crc64_big_init(void)
{
    unsigned k, n;

    crc64_init(crc64_big_table);
    for (k = 0; k < 8; k++)
        for (n = 0; n < 256; n++)
            crc64_big_table[k][n] = rev8(crc64_big_table[k][n]);
}

/* Run the init() function exactly once.  If pthread.h is not included, then
   this macro will use a simple static state variable for the purpose, which is
   not thread-safe.  The init function must be of the type void init(void). */
#ifdef PTHREAD_ONCE_INIT
#  define ONCE(init) \
    do { \
        static pthread_once_t once = PTHREAD_ONCE_INIT; \
        pthread_once(&once, init); \
    } while (0)
#else
#  define ONCE(init) \
    do { \
        static volatile int once = 1; \
        if (once) { \
            if (once++ == 1) { \
                init(); \
                once = 0; \
            } \
            else \
                while (once) \
                    ; \
        } \
    } while (0)
#endif

/* Calculate a CRC-64 eight bytes at a time on a little-endian architecture. */
static inline uint64 crc64_little(uint64 crc, void *buf, size_t len)
{
    unsigned char *next = buf;

    ONCE(crc64_little_init);
    crc = ~crc;
    while (len && ((uintptr_t)next & 7) != 0) {
        crc = crc64_little_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }
    while (len >= 8) {
        crc ^= *(uint64 *)next;
        crc = crc64_little_table[7][crc & 0xff] ^
              crc64_little_table[6][(crc >> 8) & 0xff] ^
              crc64_little_table[5][(crc >> 16) & 0xff] ^
              crc64_little_table[4][(crc >> 24) & 0xff] ^
              crc64_little_table[3][(crc >> 32) & 0xff] ^
              crc64_little_table[2][(crc >> 40) & 0xff] ^
              crc64_little_table[1][(crc >> 48) & 0xff] ^
              crc64_little_table[0][crc >> 56];
        next += 8;
        len -= 8;
    }
    while (len) {
        crc = crc64_little_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }
    return ~crc;
}

/* Calculate a CRC-64 eight bytes at a time on a big-endian architecture. */
static inline uint64 crc64_big(uint64 crc, void *buf, size_t len)
{
    unsigned char *next = buf;

    ONCE(crc64_big_init);
    crc = ~rev8(crc);
    while (len && ((uintptr_t)next & 7) != 0) {
        crc = crc64_big_table[0][(crc >> 56) ^ *next++] ^ (crc << 8);
        len--;
    }
    while (len >= 8) {
        crc ^= *(uint64 *)next;
        crc = crc64_big_table[0][crc & 0xff] ^
              crc64_big_table[1][(crc >> 8) & 0xff] ^
              crc64_big_table[2][(crc >> 16) & 0xff] ^
              crc64_big_table[3][(crc >> 24) & 0xff] ^
              crc64_big_table[4][(crc >> 32) & 0xff] ^
              crc64_big_table[5][(crc >> 40) & 0xff] ^
              crc64_big_table[6][(crc >> 48) & 0xff] ^
              crc64_big_table[7][crc >> 56];
        next += 8;
        len -= 8;
    }
    while (len) {
        crc = crc64_big_table[0][(crc >> 56) ^ *next++] ^ (crc << 8);
        len--;
    }
    return ~rev8(crc);
}

/* Return the CRC-64 of buf[0..len-1] with initial crc, processing eight bytes
   at a time.  This selects one of two routines depending on the endianness of
   the architecture.  A good optimizing compiler will determine the endianness
   at compile time if it can, and get rid of the unused code and table.  If the
   endianness can be changed at run time, then this code will handle that as
   well, initializing and using two tables, if called upon to do so. */

static int littleendian = -1;

EXTERNL uint64
NC_crc64(uint64 crc, void *buf, unsigned int len)
{
    /* Is this machine big vs little endian? */
    if(littleendian < 0) {
	unsigned char* p = (void*)&littleendian;
	littleendian = 1;
	if(*p == 0) littleendian = 0; /* big endian */
    }

    return littleendian ? crc64_little(crc, buf, (size_t)len) :
                          crc64_big(crc, buf, (size_t)len);
}

#define GF2_DIM 64      /* dimension of GF(2) vectors (length of CRC) */

static uint64 gf2_matrix_times(uint64 *mat, uint64 vec)
{
    uint64 sum;

    sum = 0;
    while (vec) {
        if (vec & 1)
            sum ^= *mat;
        vec >>= 1;
        mat++;
    }
    return sum;
}

static void gf2_matrix_square(uint64 *square, uint64 *mat)
{
    unsigned n;

    for (n = 0; n < GF2_DIM; n++)
        square[n] = gf2_matrix_times(mat, mat[n]);
}

/* Return the CRC-64 of two sequential blocks, where crc1 is the CRC-64 of the
   first block, crc2 is the CRC-64 of the second block, and len2 is the length
   of the second block. */
uint64 crc64_combine(uint64 crc1, uint64 crc2, uintmax_t len2)
{
    unsigned n;
    uint64 row;
    uint64 even[GF2_DIM];     /* even-power-of-two zeros operator */
    uint64 odd[GF2_DIM];      /* odd-power-of-two zeros operator */

    /* degenerate case */
    if (len2 == 0)
        return crc1;

    /* put operator for one zero bit in odd */
    odd[0] = POLY;              /* CRC-64 polynomial */
    row = 1;
    for (n = 1; n < GF2_DIM; n++) {
        odd[n] = row;
        row <<= 1;
    }

    /* put operator for two zero bits in even */
    gf2_matrix_square(even, odd);

    /* put operator for four zero bits in odd */
    gf2_matrix_square(odd, even);

    /* apply len2 zeros to crc1 (first square will put the operator for one
       zero byte, eight zero bits, in even) */
    do {
        /* apply zeros operator for this bit of len2 */
        gf2_matrix_square(even, odd);
        if (len2 & 1)
            crc1 = gf2_matrix_times(even, crc1);
        len2 >>= 1;

        /* if no more bits set, then done */
        if (len2 == 0)
            break;

        /* another iteration of the loop with odd and even swapped */
        gf2_matrix_square(odd, even);
        if (len2 & 1)
            crc1 = gf2_matrix_times(odd, crc1);
        len2 >>= 1;

        /* if no more bits set, then done */
    } while (len2 != 0);

    /* return combined crc */
    crc1 ^= crc2;
    return crc1;
}

#if 0
/* Test crc64() on vector[0..len-1] which should have CRC-64 crc.  Also test
   crc64_combine() on vector[] split in two. */
static void crc64_test(void *vector, size_t len, uint64 crc)
{
    uint64 crc1, crc2;

    /* test crc64() */
    crc1 = crc64(0, vector, len);
    if (crc1 ^ crc)
        printf("mismatch: %" PRIx64 ", should be %" PRIx64 "n", (ulong)crc1, (ulong)crc);

    /* test crc64_combine() */
    crc1 = crc64(0, vector, (len + 1) >> 1);
    crc2 = crc64(0, vector + ((len + 1) >> 1), len >> 1);
    crc1 = crc64_combine(crc1, crc2, len >> 1);
    if (crc1 ^ crc)
        printf("mismatch: %" PRIx64 ", should be %" PRIx64 "n", (ulong)crc1, (ulong)crc);
}

/* Test vectors. */
#define TEST1 "123456789"
#define TESTLEN1 9
#define TESTCRC1 UINT64_C(0x995dc9bbdf1939fa)
#define TEST2 "This is a test of the emergency broadcast system."
#define TESTLEN2 49
#define TESTCRC2 UINT64_C(0x27db187fc15bbc72)

int main(void)
{
    crc64_test(TEST1, TESTLEN1, TESTCRC1);
    crc64_test(TEST2, TESTLEN2, TESTCRC2);
    return 0;
}
#endif
