/* crc32c.c -- compute CRC-32C using the Intel crc32 instruction
 * Copyright (C) 2013 Mark Adler
 * Version 1.1  1 Aug 2013  Mark Adler
 *
 * Adapted by Rodrigo Tobar for inclusion in the crc32c python package
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

/* Use hardware CRC instruction on Intel SSE 4.2 processors.  This computes a
   CRC-32C, *not* the CRC-32 used by Ethernet and zip, gzip, etc.  A software
   version is provided as a fall-back, as well as for speed comparisons. */

/* Version history:
   1.0  10 Feb 2013  First version
   1.1   1 Aug 2013  Correct comments on why three crc instructions in parallel
 */

/* Altered version
 * This version modified to fit into the benchmarking code retrieved from
 * http://www.evanjones.ca/crc32c.html
 * 1.2  20 Mar 2016  Ferry Toth - Fit into benchmarking
 * 1.3  07 May 2016  Ferry Toth - Applied some speed ups by putting more CRC32 in the short and long loop
 *                              - Moved crc32q into macro's and put alternative code there for 32bit operation
*/

#include "common.h"

#if defined(IS_INTEL)

#if defined(__GNUC__)
# define ATTR_CRC32 __attribute__ ((target("sse4.2")))
#else
# define ATTR_CRC32
#endif

/*
 * MSVC/icc don't have __builtin_ia32_crc32_* functions. Instead they have
 * the _mm_crc32_* intrinsics, which accomplish the same at the end of the day
 */
#if defined(_MSC_VER) || defined(__ICC)
# include <nmmintrin.h>
# define __builtin_ia32_crc32qi _mm_crc32_u8
# define __builtin_ia32_crc32hi _mm_crc32_u16
# define __builtin_ia32_crc32si _mm_crc32_u32
# define __builtin_ia32_crc32di _mm_crc32_u64
#endif /* defined(_MSC_VER) || defined(__ICC) */

/* CRC-32C (iSCSI) polynomial in reversed bit order. */
#define POLY 0x82f63b78

/* Multiply a matrix times a vector over the Galois field of two elements,
   GF(2).  Each element is a bit in an unsigned integer.  mat must have at
   least as many entries as the power of two for most significant one bit in
   vec. */
static CRC32C_INLINE uint32_t gf2_matrix_times ( uint32_t *mat, uint32_t vec )
{
        uint32_t sum;

        sum = 0;
        while ( vec ) {
                if ( vec & 1 )
                        sum ^= *mat;
                vec >>= 1;
                mat++;
        }
        return sum;
}

/* Multiply a matrix by itself over GF(2).  Both mat and square must have 32
   rows. */
static CRC32C_INLINE void gf2_matrix_square ( uint32_t *square, uint32_t *mat )
{
        int n;

        for ( n = 0; n < 32; n++ )
                square[n] = gf2_matrix_times ( mat, mat[n] );
}

/* Construct an operator to apply len zeros to a crc.  len must be a power of
   two.  If len is not a power of two, then the result is the same as for the
   largest power of two less than len.  The result for len == 0 is the same as
   for len == 1.  A version of this routine could be easily written for any
   len, but that is not needed for this application. */
static void crc32c_zeros_op ( uint32_t *even, size_t len )
{
        int n;
        uint32_t row;
        uint32_t odd[32];       /* odd-power-of-two zeros operator */

        /* put operator for one zero bit in odd */
        odd[0] = POLY;              /* CRC-32C polynomial */
        row = 1;
        for ( n = 1; n < 32; n++ ) {
                odd[n] = row;
                row <<= 1;
        }

        /* put operator for two zero bits in even */
        gf2_matrix_square ( even, odd );

        /* put operator for four zero bits in odd */
        gf2_matrix_square ( odd, even );

        /* first square will put the operator for one zero byte (eight zero bits),
           in even -- next square puts operator for two zero bytes in odd, and so
           on, until len has been rotated down to zero */
        do {
                gf2_matrix_square ( even, odd );
                len >>= 1;
                if ( len == 0 )
                        return;
                gf2_matrix_square ( odd, even );
                len >>= 1;
        } while ( len );

        /* answer ended up in odd -- copy to even */
        for ( n = 0; n < 32; n++ )
                even[n] = odd[n];
}

/* Take a length and build four lookup tables for applying the zeros operator
   for that length, byte-by-byte on the operand. */
static void crc32c_zeros ( uint32_t zeros[][256], size_t len )
{
        uint32_t n;
        uint32_t op[32];

        crc32c_zeros_op ( op, len );
        for ( n = 0; n < 256; n++ ) {
                zeros[0][n] = gf2_matrix_times ( op, n );
                zeros[1][n] = gf2_matrix_times ( op, n << 8 );
                zeros[2][n] = gf2_matrix_times ( op, n << 16 );
                zeros[3][n] = gf2_matrix_times ( op, n << 24 );
        }
}


/* Apply the zeros operator table to crc. */
static CRC32C_INLINE uint32_t crc32c_shift ( uint32_t zeros[][256], uint32_t crc )
{
        return zeros[0][crc & 0xff] ^ zeros[1][ ( crc >> 8 ) & 0xff] ^
               zeros[2][ ( crc >> 16 ) & 0xff] ^ zeros[3][crc >> 24];
}

/* Block sizes for three-way parallel crc computation.  LONG and SHORT must
   both be powers of two.  The associated string constants must be set
   accordingly, for use in constructing the assembler instructions. */
#define LONG 8192
#define LONGx1 "8192"
#define LONGx2 "16384"
#define SHORT 256
#define SHORTx1 "256"
#define SHORTx2 "512"

/* Tables for hardware crc that shift a crc by LONG and SHORT zeros. */
static uint32_t crc32c_long[4][256];
static uint32_t crc32c_short[4][256];

/* Initialize tables for shifting crcs. */
void crc32c_init_hw_adler( void )
{
        crc32c_zeros ( crc32c_long, LONG );
        crc32c_zeros ( crc32c_short, SHORT );
}

#ifndef CRC32C_IS_64_BITS
#define CRCtriplet(crc, buf, size, i) \
    crc ## 0 = __builtin_ia32_crc32si(crc ## 0, *(uint32_t*) (buf + i)); \
    crc ## 1 = __builtin_ia32_crc32si(crc ## 1, *(uint32_t*) (buf + i + size)); \
    crc ## 2 = __builtin_ia32_crc32si(crc ## 2, *(uint32_t*) (buf + i + 2 * size)); \
    crc ## 0 = __builtin_ia32_crc32si(crc ## 0, *(uint32_t*) (buf + sizeof(uint32_t) + i)); \
    crc ## 1 = __builtin_ia32_crc32si(crc ## 1, *(uint32_t*) (buf + sizeof(uint32_t) + i + size)); \
    crc ## 2 = __builtin_ia32_crc32si(crc ## 2, *(uint32_t*) (buf + sizeof(uint32_t) + i + 2 * size));
#else
#define CRCtriplet(crc, buf, size, i) \
    crc ## 0 = __builtin_ia32_crc32di(crc ## 0, *(uint64_t*) (buf + i)); \
    crc ## 1 = __builtin_ia32_crc32di(crc ## 1, *(uint64_t*) (buf + i + size)); \
    crc ## 2 = __builtin_ia32_crc32di(crc ## 2, *(uint64_t*) (buf + i + 2 * size));
#endif


#ifndef CRC32C_IS_64_BITS
#define CRCsinglet(crc, buf) \
    crc = __builtin_ia32_crc32si(crc, *(uint32_t*)buf); \
    crc = __builtin_ia32_crc32si(crc, *(uint32_t*)(buf + sizeof(uint32_t))); \
    buf+= 2 *sizeof(uint32_t);
#else
#define CRCsinglet(crc, buf) crc = __builtin_ia32_crc32di(crc, *(uint64_t*)buf); buf+= sizeof(uint64_t);
#endif

/* Compute CRC-32C using the Intel hardware instruction. */
ATTR_CRC32 uint32_t _crc32c_hw_adler(uint32_t crc, const unsigned char *buf, unsigned long len)
{
        const unsigned char *next = buf;
        const unsigned char *end;
        unsigned short count;

#ifndef CRC32C_IS_64_BITS
        uint32_t crc0, crc1, crc2;
#else
        uint64_t crc0, crc1, crc2;      /* need to be 64 bits for crc32q */
#endif
        uint32_t crc32bit;

        crc32bit = crc;
        // in len > 256 compute the crc for up to seven leading bytes to bring the data pointer to an eight-byte boundary
        if ( len > 128 ) {
                unsigned char align = ( 8 - ( uintptr_t ) next ) % 8;            // byte to boundary
                len -= align;
                if ( ( align % 2 ) != 0 ) crc32bit = __builtin_ia32_crc32qi ( crc32bit, *next );
                next += align;
                switch ( align / 2 ) {
                case 3:
                        crc32bit = __builtin_ia32_crc32hi ( crc32bit, * ( uint16_t* ) ( next - 6 ) ); // 6 char, remain 4
                        CRC32C_FALLTHROUGH;
                case 2:
                        crc32bit = __builtin_ia32_crc32si ( crc32bit, * ( uint32_t* ) ( next - 4 ) ); // 4 char, remain 0
                        break;
                case 1:
                        crc32bit = __builtin_ia32_crc32hi ( crc32bit, * ( uint16_t* ) ( next - 2 ) ); // 2 char, remain 0
                        break;
                case 0:
                        break;
                }
        };

        /* compute the crc on sets of LONG*3 bytes, executing three independent crc
           instructions, each on LONG bytes -- this is optimized for the Nehalem,
           Westmere, Sandy Bridge, and Ivy Bridge architectures, which have a
           throughput of one crc per cycle, but a latency of three cycles */

        crc0 = crc32bit;
        while ( len >= LONG*3 ) {
                crc1 = 0;
                crc2 = 0;
                end = next + LONG;
                do {
                        CRCtriplet ( crc, next, LONG, 0 );
                        CRCtriplet ( crc, next, LONG, 8 );
                        CRCtriplet ( crc, next, LONG, 16 );
                        CRCtriplet ( crc, next, LONG, 24 );
                        next += 32;
                } while ( next < end );
                crc0 = crc32c_shift ( crc32c_long, (uint32_t)crc0 ) ^ crc1;
                crc0 = crc32c_shift ( crc32c_long, (uint32_t)crc0 ) ^ crc2;
                next += LONG*2;
                len -= LONG*3;
        }

        /* do the same thing, but now on SHORT*3 blocks for the remaining data less
           than a LONG*3 block */
        while ( len >= SHORT*3 ) {
                crc1 = 0;
                crc2 = 0;
                end = next + SHORT;
                do {
                        CRCtriplet ( crc, next, SHORT, 0 );
                        CRCtriplet ( crc, next, SHORT, 8 );
                        CRCtriplet ( crc, next, SHORT, 16 );
                        CRCtriplet ( crc, next, SHORT, 24 );
                        next += 32;
                } while ( next < end );
                crc0 = crc32c_shift ( crc32c_short, (uint32_t)crc0 ) ^ crc1;
                crc0 = crc32c_shift ( crc32c_short, (uint32_t)crc0 ) ^ crc2;
                next += SHORT*2;
                len -= SHORT*3;
        }

        /* compute the crc on the remaining eight-byte units less than a SHORT*3
           block */

        // use Duff's device, a for() loop inside a switch() statement. This is Legal
        if ( ( count = ( len - ( len & 7 ) ) ) >= 8 ) { // needs to execute crc at least once
                unsigned short n;
                len -= count;
                count /= 8;                        // count number of crc32di
                n = ( count + 15 ) / 16;
                switch ( count % 16 ) {
                case 0:
                        do {
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 15:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 14:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 13:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 12:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 11:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 10:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 9:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 8:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 7:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 6:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 5:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 4:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 3:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 2:
                                CRCsinglet ( crc0, next );
                                CRC32C_FALLTHROUGH;
                        case 1:
                                CRCsinglet ( crc0, next );
                        } while ( --n > 0 );
                }
        };

        /* compute the crc for up to seven trailing bytes */
        crc32bit = (uint32_t)crc0;
        if ( ( len % 2 ) != 0 ) crc32bit = __builtin_ia32_crc32qi ( crc32bit, * ( next ) ); // 1 char, remain even
        next += len;
        switch ( len / 2 ) {
        case 3:
                crc32bit = __builtin_ia32_crc32hi ( crc32bit, * ( uint16_t* ) ( next - 6 ) ); // 2 char, remain 4
                CRC32C_FALLTHROUGH;
        case 2:
                crc32bit = __builtin_ia32_crc32si ( crc32bit, * ( uint32_t* ) ( next - 4 ) ); // 4 char, remain 0
                break;
        case 1:
                crc32bit = __builtin_ia32_crc32hi ( crc32bit, * ( uint16_t* ) ( next - 2 ) ); // 2 char, remain 0
                break;
        case 0:
                break;
        }
        return ( uint32_t ) crc32bit;
}

#endif // defined(IS_INTEL)
