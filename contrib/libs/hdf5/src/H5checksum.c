/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*-------------------------------------------------------------------------
 *
 * Created:		H5checksum.c
 *
 * Purpose:		Internal code for computing fletcher32 checksums
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/
#include "H5module.h" /* This source code file is part of the H5 module */

/***********/
/* Headers */
/***********/
#include "H5private.h" /* Generic Functions			*/

/****************/
/* Local Macros */
/****************/

/* Polynomial quotient */
/* (same as the IEEE 802.3 (Ethernet) quotient) */
#define H5_CRC_QUOTIENT 0x04C11DB7

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Table of CRCs of all 8-bit messages. */
static uint32_t H5_crc_table[256];

/* Flag: has the table been computed? */
static bool H5_crc_table_computed = false;

/*-------------------------------------------------------------------------
 * Function:	H5_checksum_fletcher32
 *
 * Purpose:	This routine provides a generic, fast checksum algorithm for
 *              use in the library.
 *
 * Note:        See the Wikipedia page for Fletcher's checksum:
 *                  http://en.wikipedia.org/wiki/Fletcher%27s_checksum
 *              for more details, etc.
 *
 * Note #2:     Per the information in RFC 3309:
 *                      (http://tools.ietf.org/html/rfc3309)
 *              Fletcher's checksum is not reliable for small buffers.
 *
 * Note #3:     The algorithm below differs from that given in the Wikipedia
 *              page by copying the data into 'sum1' in a more portable way
 *              and also by initializing 'sum1' and 'sum2' to 0 instead of
 *              0xffff (for backward compatibility reasons with earlier
 *              HDF5 fletcher32 I/O filter routine, mostly).
 *
 * Return:	32-bit fletcher checksum of input buffer (can't fail)
 *
 *-------------------------------------------------------------------------
 */
uint32_t
H5_checksum_fletcher32(const void *_data, size_t _len)
{
    const uint8_t *data = (const uint8_t *)_data; /* Pointer to the data to be summed */
    size_t         len  = _len / 2;               /* Length in 16-bit words */
    uint32_t       sum1 = 0, sum2 = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(_data);
    assert(_len > 0);

    /* Compute checksum for pairs of bytes */
    /* (the magic "360" value is the largest number of sums that can be
     *  performed without numeric overflow)
     */
    while (len) {
        size_t tlen = len > 360 ? 360 : len;
        len -= tlen;
        do {
            sum1 += (uint32_t)(((uint16_t)data[0]) << 8) | ((uint16_t)data[1]);
            data += 2;
            sum2 += sum1;
        } while (--tlen);
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    }

    /* Check for odd # of bytes */
    if (_len % 2) {
        sum1 += (uint32_t)(((uint16_t)*data) << 8);
        sum2 += sum1;
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    } /* end if */

    /* Second reduction step to reduce sums to 16 bits */
    sum1 = (sum1 & 0xffff) + (sum1 >> 16);
    sum2 = (sum2 & 0xffff) + (sum2 >> 16);

    FUNC_LEAVE_NOAPI((sum2 << 16) | sum1)
} /* end H5_checksum_fletcher32() */

/*-------------------------------------------------------------------------
 * Function:	H5__checksum_crc_make_table
 *
 * Purpose:	Compute the CRC table for the CRC checksum algorithm
 *
 * Return:	none
 *
 *-------------------------------------------------------------------------
 */
static void
H5__checksum_crc_make_table(void)
{
    uint32_t c;    /* Checksum for each byte value */
    unsigned n, k; /* Local index variables */

    FUNC_ENTER_PACKAGE_NOERR

    /* Compute the checksum for each possible byte value */
    for (n = 0; n < 256; n++) {
        c = (uint32_t)n;
        for (k = 0; k < 8; k++)
            if (c & 1)
                c = H5_CRC_QUOTIENT ^ (c >> 1);
            else
                c = c >> 1;
        H5_crc_table[n] = c;
    }
    H5_crc_table_computed = true;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5__checksum_crc_make_table() */

/*-------------------------------------------------------------------------
 * Function:	H5__checksum_crc_update
 *
 * Purpose:	Update a running CRC with the bytes buf[0..len-1]--the CRC
 *              should be initialized to all 1's, and the transmitted value
 *              is the 1's complement of the final running CRC (see the
 *              H5_checksum_crc() routine below)).
 *
 * Return:	32-bit CRC checksum of input buffer (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static uint32_t
H5__checksum_crc_update(uint32_t crc, const uint8_t *buf, size_t len)
{
    size_t n; /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Initialize the CRC table if necessary */
    if (!H5_crc_table_computed)
        H5__checksum_crc_make_table();

    /* Update the CRC with the results from this buffer */
    for (n = 0; n < len; n++)
        crc = H5_crc_table[(crc ^ buf[n]) & 0xff] ^ (crc >> 8);

    FUNC_LEAVE_NOAPI(crc)
} /* end H5__checksum_crc_update() */

/*-------------------------------------------------------------------------
 * Function:	H5_checksum_crc
 *
 * Purpose:	This routine provides a generic checksum algorithm for
 *              use in the library.
 *
 * Note:        This algorithm was based on the implementation described
 *              in the document describing the PNG image format:
 *                  http://www.w3.org/TR/PNG/#D-CRCAppendix
 *
 * Return:	32-bit CRC checksum of input buffer (can't fail)
 *
 *-------------------------------------------------------------------------
 */
uint32_t
H5_checksum_crc(const void *_data, size_t len)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(_data);
    assert(len > 0);

    FUNC_LEAVE_NOAPI(H5__checksum_crc_update((uint32_t)0xffffffffL, (const uint8_t *)_data, len) ^
                     0xffffffffL)
} /* end H5_checksum_crc() */

/*
-------------------------------------------------------------------------------
H5_lookup3_mix -- mix 3 32-bit values reversibly.

This is reversible, so any information in (a,b,c) before mix() is
still in (a,b,c) after mix().

If four pairs of (a,b,c) inputs are run through mix(), or through
mix() in reverse, there are at least 32 bits of the output that
are sometimes the same for one pair and different for another pair.
This was tested for:
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or
  all zero plus a counter that starts at zero.

Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
satisfy this are
    4  6  8 16 19  4
    9 15  3 18 27 15
   14  9  3  7 17  3
Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing
for "differ" defined as + with a one-bit base and a two-bit delta.  I
used http://burtleburtle.net/bob/hash/avalanche.html to choose
the operations, constants, and arrangements of the variables.

This does not achieve avalanche.  There are input bits of (a,b,c)
that fail to affect some output bits of (a,b,c), especially of a.  The
most thoroughly mixed value is c, but it doesn't really even achieve
avalanche in c.

This allows some parallelism.  Read-after-writes are good at doubling
the number of bits affected, so the goal of mixing pulls in the opposite
direction as the goal of parallelism.  I did what I could.  Rotates
seem to cost as much as shifts on every machine I could lay my hands
on, and rotates are much kinder to the top and bottom bits, so I used
rotates.
-------------------------------------------------------------------------------
*/
#define H5_lookup3_rot(x, k) (((x) << (k)) ^ ((x) >> (32 - (k))))
#define H5_lookup3_mix(a, b, c)                                                                              \
    do {                                                                                                     \
        a -= c;                                                                                              \
        a ^= H5_lookup3_rot(c, 4);                                                                           \
        c += b;                                                                                              \
        b -= a;                                                                                              \
        b ^= H5_lookup3_rot(a, 6);                                                                           \
        a += c;                                                                                              \
        c -= b;                                                                                              \
        c ^= H5_lookup3_rot(b, 8);                                                                           \
        b += a;                                                                                              \
        a -= c;                                                                                              \
        a ^= H5_lookup3_rot(c, 16);                                                                          \
        c += b;                                                                                              \
        b -= a;                                                                                              \
        b ^= H5_lookup3_rot(a, 19);                                                                          \
        a += c;                                                                                              \
        c -= b;                                                                                              \
        c ^= H5_lookup3_rot(b, 4);                                                                           \
        b += a;                                                                                              \
    } while (0)

/*
-------------------------------------------------------------------------------
H5_lookup3_final -- final mixing of 3 32-bit values (a,b,c) into c

Pairs of (a,b,c) values differing in only a few bits will usually
produce values of c that look totally different.  This was tested for
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or
  all zero plus a counter that starts at zero.

These constants passed:
 14 11 25 16 4 14 24
 12 14 25 16 4 14 24
and these came close:
  4  8 15 26 3 22 24
 10  8 15 26 3 22 24
 11  8 15 26 3 22 24
-------------------------------------------------------------------------------
*/
#define H5_lookup3_final(a, b, c)                                                                            \
    do {                                                                                                     \
        c ^= b;                                                                                              \
        c -= H5_lookup3_rot(b, 14);                                                                          \
        a ^= c;                                                                                              \
        a -= H5_lookup3_rot(c, 11);                                                                          \
        b ^= a;                                                                                              \
        b -= H5_lookup3_rot(a, 25);                                                                          \
        c ^= b;                                                                                              \
        c -= H5_lookup3_rot(b, 16);                                                                          \
        a ^= c;                                                                                              \
        a -= H5_lookup3_rot(c, 4);                                                                           \
        b ^= a;                                                                                              \
        b -= H5_lookup3_rot(a, 14);                                                                          \
        c ^= b;                                                                                              \
        c -= H5_lookup3_rot(b, 24);                                                                          \
    } while (0)

/*
-------------------------------------------------------------------------------
H5_checksum_lookup3() -- hash a variable-length key into a 32-bit value
  k       : the key (the unaligned variable-length array of bytes)
  length  : the length of the key, counting by bytes
  initval : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Two keys differing by one or two bits will have
totally different hash values.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (uint8_t **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = H5_checksum_lookup( k[i], len[i], h);

By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
code any way you wish, private, educational, or commercial.  It's free.

Use for hash table lookup, or anything where one collision in 2^^32 is
acceptable.  Do NOT use for cryptographic purposes.
-------------------------------------------------------------------------------
*/

uint32_t
H5_checksum_lookup3(const void *key, size_t length, uint32_t initval)
{
    const uint8_t *k = (const uint8_t *)key;
    uint32_t       a, b, c = 0; /* internal state */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(key);
    assert(length > 0);

    /* Set up the internal state */
    a = b = c = 0xdeadbeef + ((uint32_t)length) + initval;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > 12) {
        a += k[0];
        a += ((uint32_t)k[1]) << 8;
        a += ((uint32_t)k[2]) << 16;
        a += ((uint32_t)k[3]) << 24;
        b += k[4];
        b += ((uint32_t)k[5]) << 8;
        b += ((uint32_t)k[6]) << 16;
        b += ((uint32_t)k[7]) << 24;
        c += k[8];
        c += ((uint32_t)k[9]) << 8;
        c += ((uint32_t)k[10]) << 16;
        c += ((uint32_t)k[11]) << 24;
        H5_lookup3_mix(a, b, c);
        length -= 12;
        k += 12;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch (length) /* all the case statements fall through */
    {
        case 12:
            c += ((uint32_t)k[11]) << 24;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 11:
            c += ((uint32_t)k[10]) << 16;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 10:
            c += ((uint32_t)k[9]) << 8;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 9:
            c += k[8];
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 8:
            b += ((uint32_t)k[7]) << 24;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 7:
            b += ((uint32_t)k[6]) << 16;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 6:
            b += ((uint32_t)k[5]) << 8;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 5:
            b += k[4];
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 4:
            a += ((uint32_t)k[3]) << 24;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 3:
            a += ((uint32_t)k[2]) << 16;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 2:
            a += ((uint32_t)k[1]) << 8;
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case 1:
            a += k[0];
            break;
        case 0:
            goto done;
        default:
            assert(0 && "This Should never be executed!");
    }

    H5_lookup3_final(a, b, c);

done:
    FUNC_LEAVE_NOAPI(c)
} /* end H5_checksum_lookup3() */

/*-------------------------------------------------------------------------
 * Function:	H5_checksum_metadata
 *
 * Purpose:	Provide a more abstract routine for checksumming metadata
 *              in a file, where the policy of which algorithm to choose
 *              is centralized.
 *
 * Return:	checksum of input buffer (can't fail)
 *
 *-------------------------------------------------------------------------
 */
uint32_t
H5_checksum_metadata(const void *data, size_t len, uint32_t initval)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(data);
    assert(len > 0);

    /* Choose the appropriate checksum routine */
    /* (use Bob Jenkin's "lookup3" algorithm for all buffer sizes) */
    FUNC_LEAVE_NOAPI(H5_checksum_lookup3(data, len, initval))
} /* end H5_checksum_metadata() */

/*-------------------------------------------------------------------------
 * Function:	H5_hash_string
 *
 * Purpose:	Provide a simple & fast routine for hashing strings
 *
 * Note:        This algorithm is the 'djb2' algorithm described on this page:
 *              http://www.cse.yorku.ca/~oz/hash.html
 *
 * Return:	hash of input string (can't fail)
 *
 *-------------------------------------------------------------------------
 */
uint32_t
H5_hash_string(const char *str)
{
    uint32_t hash = 5381;
    int      c;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(str);

    while ((c = *str++))
        hash = ((hash << 5) + hash) + (uint32_t)c; /* hash * 33 + c */

    FUNC_LEAVE_NOAPI(hash)
} /* end H5_hash_string() */
