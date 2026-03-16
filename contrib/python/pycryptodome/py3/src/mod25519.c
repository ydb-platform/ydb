#ifndef __MOD25519_C
#define __MOD25519_C

#include "common.h"
#include "endianess.h"
#include "bignum.c"

/*
 * Operations over the field of integers modulo 2²⁵⁵ - 19
 */

#if defined(MASK26) || defined(MASK25) || defined(MASK13) || defined(MASK12)
#error Necessary macros are already defined
#endif

#define MASK26 ((1ULL<<26)-1)
#define MASK25 ((1ULL<<25)-1)
#define MASK13 ((1ULL<<13)-1)
#define MASK12 ((1ULL<<12)-1)

STATIC void reduce_25519_le64(uint64_t x[4]);

/*
 * Convert a 256-bit number in radix 2⁶⁴ and little-endian mode,
 * to mixed radix 2²⁵/2²⁶, also little-endian.
 */
STATIC void convert_le64_to_le25p5(uint32_t out[10], const uint64_t in[4])
{
    /** MSB of in[3] will be ignored */
    out[0] = in[0] & MASK26;                                        /* Fill 26 bits: 38 bits left in in[0] */
    out[1] = (in[0] >> 26) & MASK25;                                /* Fill 25 bits: 13 bits left in in[0] */
    out[2] = (uint32_t)((in[0] >> 51) | (in[1] & MASK13) << 13);    /* Fill 26 bits: 51 bits left in in[1] */
    out[3] = (in[1] >> 13) & MASK25;                                /* Fill 25 bits: 26 bits left in in[1] */
    out[4] = (uint32_t)(in[1] >> 38);                               /* Fill 26 bits: no bits left in in[1] */
    out[5] = in[2] & MASK25;                                        /* Fill 25 bits: 39 bits left in in[2] */
    out[6] = (in[2] >> 25) & MASK26;                                /* Fill 26 bits: 13 bits left in in[2] */
    out[7] = (uint32_t)((in[2] >> 51) | (in[3] & MASK12) << 13);    /* Fill 25 bits: 52 bits left in in[3] */
    out[8] = (in[3] >> 12) & MASK26;                                /* Fill 26 bits: 26 bits left in in[3] */
    out[9] = (uint32_t)(in[3] >> 38);                               /* Fill 26 bits in theory, 25 in practice */
}

/*
 * Convert a 256-bit number in mixed radix 2²⁵/2²⁶ and little-endian mode,
 * to radix 2⁶⁴, also little-endian mode.
 *
 * The input limbs must fulfill the conditions:
 *      in[i] < 2²⁶      for even i (0, 2, 4, etc)
 *      in[j] < 2²⁵      for odd j<9 (1, 3, 5, etc)
 *      in[9] < 2²⁶
 */
STATIC void convert_le25p5_to_le64(uint64_t out[4], const uint32_t in[10])
{
    /** We assume that the 6 or 7 upper bits of in[] words is set to 0 */
    assert(in[0] >> 26 == 0);
    assert(in[1] >> 25 == 0);
    assert(in[2] >> 26 == 0);
    assert(in[3] >> 25 == 0);
    assert(in[4] >> 26 == 0);
    assert(in[5] >> 25 == 0);
    assert(in[6] >> 26 == 0);
    assert(in[7] >> 25 == 0);
    assert(in[8] >> 26 == 0);
    assert(in[9] >> 26 == 0);

    out[0] = in[0] | (uint64_t)in[1] << 26 | ((uint64_t)in[2] & MASK13) << 51;  /* 64 = 26 + 25 + 13: 13 bits left in in[2] */
    out[1] = in[2] >> 13 | (uint64_t)in[3] << 13 | (uint64_t)in[4] << 38;       /* 64 = 13 + 25 + 26: no bits left in[4] */
    out[2] = in[5] | (uint64_t)in[6] << 25 | ((uint64_t)in[7] & MASK13) << 51;  /* 64 = 25 + 26 + 13: 12 bits left in in[7] */
    out[3] = in[7] >> 13 | (uint64_t)in[8] << 12 | (uint64_t)in[9] << 38;       /* 64 = 12 + 26 + 26(!) */
}

/*
 * Deserialize a 255-bit little-endian integer
 * to mixed radix 2²⁶/2²⁵, also little-endian.
 */
STATIC void convert_le8_to_le25p5(uint32_t out[10], const uint8_t in[32])
{
    uint64_t in64[4];
    unsigned i;

    for (i=0; i<4; i++) {
        in64[i] = LOAD_U64_LITTLE(&in[i*8]);
    }
    convert_le64_to_le25p5(out, in64);
}

/*
 * Serialize a 256-bit mixed-radix 2²⁶/2²⁵  little-endian integer
 * into a little-endian byte string.
 *
 * The input limbs must fulfill the conditions:
 *      in[i] < 2²⁶      for even i (0, 2, 4, etc)
 *      in[j] < 2²⁵      for odd j<9 (1, 3, 5, etc)
 *      in[9] < 2²⁶
 */
STATIC void convert_le25p5_to_le8(uint8_t out[32], const uint32_t in[10])
{
    uint64_t out64[4];
    unsigned i;

    convert_le25p5_to_le64(out64, in);
    reduce_25519_le64(out64);
    for (i=0; i<4; i++) {
        STORE_U64_LITTLE(&out[i*8], out64[i]);
    }
}

/*
 * Deserialize a 255-bit big-endian integer
 * to mixed radix 2²⁶/2²⁵ little-endian.
 */
STATIC void convert_be8_to_le25p5(uint32_t out[10], const uint8_t in[32])
{
    uint64_t in64[4];
    unsigned i;

    for (i=0; i<4; i++) {
        in64[3-i] = LOAD_U64_BIG(&in[i*8]);
    }
    convert_le64_to_le25p5(out, in64);
}

/*
 * Serialize a 256-bit mixed-radix 2²⁶/2²⁵  little-endian integer
 * into a big-endian byte string.
 *
 * The input limbs must fulfill the conditions:
 *      in[i] < 2²⁶      for even i (0, 2, 4, etc)
 *      in[j] < 2²⁵      for odd j<9 (1, 3, 5, etc)
 *      in[9] < 2²⁶
 */
STATIC void convert_le25p5_to_be8(uint8_t out[32], const uint32_t in[10])
{
    uint64_t out64[4];
    unsigned i;

    convert_le25p5_to_le64(out64, in);
    reduce_25519_le64(out64);
    for (i=0; i<4; i++) {
        STORE_U64_BIG(&out[i*8], out64[3-i]);
    }
}

/*
 * Return non-zero if a 256-bit mixed-radix 2²⁶/2²⁵  little-endian integer is zero.
 */
STATIC int is_le25p5_zero(const uint32_t in[10])
{
    uint64_t out64[4];

    convert_le25p5_to_le64(out64, in);
    reduce_25519_le64(out64);

    return (out64[0] | out64[1] | out64[2] | out64[3]) == 0;
}

static int hex2bin(char in)
{
    if ((in >= '0') && (in <= '9'))
        return in - '0';
    if ((in >= 'A') && (in <= 'F'))
        return in - 'A' + 10;
    if ((in >= 'a') && (in <= 'f'))
        return in - 'a' + 10;
    return -1;
}

static char bin2hex(uint8_t b)
{
    if (b < 10)
        return (char)('0' + b);
    return (char)('a' + b - 10);
}

/*
 * Convert a big-endian hexadecimal number (up to 256 bits, and as a zero-terminated ASCII string)
 * to mixed radix 2²⁶/2²⁵, also little-endian.
 */
STATIC int convert_behex_to_le25p5(uint32_t out[10], const char *in)
{
    size_t len;
    uint8_t bin[32];
    unsigned i;

    if (in == NULL)
        return ERR_NULL;
    len = strlen(in);
    if (len > 64)
        return ERR_MAX_DATA;
    if (len & 1)
        return ERR_BLOCK_SIZE;

    memset(bin, 0, sizeof bin);

    for (i=0; i<len; i+=2) {
        int c1, c2;

        c1 = hex2bin(in[len-1-i]);
        c2 = hex2bin(in[len-1-i-1]);
        if ((c1 < 0) || (c2 < 0))
            return ERR_VALUE;

        bin[i/2] = (uint8_t)(c2 * 16 + c1);
    }

    convert_le8_to_le25p5(out, bin);
    return 0;
}

/*
 * Convert a number in mixed radix 2²⁶/2²⁵, little-endian form
 * into a new ASCII hex, zero-terminated string. The caller
 * is responsible for deallocating the string.
 */
STATIC int convert_le25p5_to_behex(char **out, uint32_t in[10])
{
    uint8_t bin[32];
    unsigned i;

    if (NULL == out)
        return ERR_NULL;

    convert_le25p5_to_le8(bin, in);
    *out = calloc(64 + 1, 1);

    for (i=0; i<32; i++) {
        *(*out+64-1-i*2) =  bin2hex(bin[i] & 0xF);
        *(*out+64-2-i*2) =  bin2hex(bin[i] >> 4);
    }

    return 0;
}

#if 0
static void print_le25p5(char *str, const uint32_t n[10])
{
    uint8_t bin[32];

    convert_le25p5_to_le8(bin, n);
    printf("%s", str);
    for (unsigned i=0; i<32; i++) {
        printf("%02X", bin[i]);
    }
    printf("\n");
}
#endif

/*
 * Reduce a 256-bit number (2⁶⁴ radix, litte-endian) modulo 2²⁵⁵ - 19.
 */
STATIC void reduce_25519_le64(uint64_t x[4])
{
    unsigned borrow;
    uint64_t tmp1[4], tmp2[4];
    static const uint64_t modulus[4] = { 0xffffffffffffffedULL, 0xffffffffffffffffULL, 0xffffffffffffffffULL, 0x7fffffffffffffffULL };

    borrow = sub(tmp1, x, modulus, 4);
    mod_select(tmp2, x, tmp1, borrow, 4);

    borrow = sub(tmp1, tmp2, modulus, 4);
    mod_select(x, tmp2, tmp1, borrow, 4);
}

/*
 * Multiply f[] and g[] modulo 2²⁵⁵ - 19.
 *
 * The inputs f[] and g[] are encoded in mixed radix 2²⁶/2²⁵ with limbs < 2²⁷.
 *
 * The result out[] is encoded in also mixed radix 2²⁶/2²⁵ such that:
 *      f[i] < 2²⁶      for even i
 *      f[j] < 2²⁵      for odd j<9
 *      f[9] < 2²⁶
 */
STATIC void mul_25519(uint32_t out[10], const uint32_t f[10], const uint32_t g[10])
{
    uint64_t h0, h1, h2, h3, h4, h5, h6, h7, h8, h9;
    uint64_t f0, f1, f2, f3, f4, f5, f6, f7, f8, f9;
    uint64_t f1_38, f2_19, f3_19, f4_19, f5_19, f6_19, f7_19, f8_19, f9_19;
    uint64_t g0, g1, g2, g3, g4, g5, g6, g7, g8, g9;
    uint64_t carry;

    f0 = f[0]; f1 = f[1]; f2 = f[2]; f3 = f[3]; f4 = f[4]; f5 = f[5]; f6 = f[6]; f7 = f[7]; f8 = f[8]; f9 = f[9];
    g0 = g[0]; g1 = g[1]; g2 = g[2]; g3 = g[3]; g4 = g[4]; g5 = g[5]; g6 = g[6]; g7 = g[7]; g8 = g[8]; g9 = g[9];

    f1_38 = (uint64_t)38*f[1];
    f2_19 = (uint64_t)19*f[2];
    f3_19 = (uint64_t)19*f[3];
    f4_19 = (uint64_t)19*f[4];
    f5_19 = (uint64_t)19*f[5];
    f6_19 = (uint64_t)19*f[6];
    f7_19 = (uint64_t)19*f[7];
    f8_19 = (uint64_t)19*f[8];
    f9_19 = (uint64_t)19*f[9];

    /** input terms can the result of at most 4 additions **/

    h0 = f0*g0      + f1_38*g9 + f2_19*g8   + 2*f3_19*g7 + f4_19*g6 +
         2*f5_19*g5 + f6_19*g4 + 2*f7_19*g3 + f8_19*g2   + 2*f9_19*g1;
    h1 = f0*g1      + f1*g0    + f2_19*g9   + f3_19*g8   + f4_19*g7 +
         f5_19*g6   + f6_19*g5 + f7_19*g4   + f8_19*g3   + f9_19*g2;
    h2 = f0*g2      + 2*f1*g1  + f2*g0      + 2*f3_19*g9 + f4_19*g8 +
         2*f5_19*g7 + f6_19*g6 + 2*f7_19*g5 + f8_19*g4   + 2*f9_19*g3;
    h3 = f0*g3      + f1*g2    + f2*g1      + f3*g0      + f4_19*g9 +
         f5_19*g8   + f6_19*g7 + f7_19*g6   + f8_19*g5   + f9_19*g4;
    h4 = f0*g4      + 2*f1*g3  + f2*g2      + 2*f3*g1    + f4*g0 +
         2*f5_19*g9 + f6_19*g8 + 2*f7_19*g7 + f8_19*g6   + 2*f9_19*g5;
    h5 = f0*g5      + f1*g4    + f2*g3      + f3*g2      + f4*g1 +
         f5*g0      + f6_19*g9 + f7_19*g8   + f8_19*g7   + f9_19*g6;
    h6 = f0*g6      + 2*f1*g5  + f2*g4      + 2*f3*g3    + f4*g2 +
         2*f5*g1    + f6*g0    + 2*f7_19*g9 + f8_19*g8   + 2*f9_19*g7;
    h7 = f0*g7      + f1*g6    + f2*g5      + f3*g4      + f4*g3 +
         f5*g2      + f6*g1    + f7*g0      + f8_19*g9   + f9_19*g8;
    h8 = f0*g8      + 2*f1*g7  + f2*g6      + 2*f3*g5    + f4*g4 +
         2*f5*g3    + f6*g2    + 2*f7*g1    + f8*g0      + 2*f9_19*g9;
    h9 = f0*g9      + f1*g8    + f2*g7      + f3*g6      + f4*g5 +
         f5*g4      + f6*g3    + f7*g2      + f8*g1      + f9*g0;

    /* h0..h9 < 2⁶³ */
    carry = h8 >> 26;
    h8 &= MASK26;
    /* carry < 2³⁷ */
    h9 += carry;
    carry = (h9 >> 25)*19;
    h9 &= MASK25;
    /* carry < 2⁴⁴ */
    h0 += carry;
    carry = h0 >> 26;
    h0 &= MASK26;
    /* carry < 2³⁸ */
    h1 += carry;
    carry = h1 >> 25;
    h1 &= MASK25;
    /* carry < 2³⁹ */
    h2 += carry;
    carry = h2 >> 26;
    h2 &= MASK26;
    /* carry < 2³⁸ */
    h3 += carry;
    carry = h3 >> 25;
    h3 &= MASK25;
    /* carry < 2³⁹ */
    h4 += carry;
    carry = h4 >> 26;
    h4 &= MASK26;
    /* carry < 2³⁸ */
    h5 += carry;
    carry = h5 >> 25;
    h5 &= MASK25;
    /* carry < 2³⁹ */
    h6 += carry;
    carry = h6 >> 26;
    h6 &= MASK26;
    /* carry < 2³⁸ */
    h7 += carry;
    carry = h7 >> 25;
    h7 &= MASK25;
    /* carry < 2³⁹ */
    h8 += carry;
    carry = h8 >> 26;
    h8 &= MASK26;
    /* carry < 2¹⁴ */
    h9 += carry;
    /* h9 < 2²⁶ */

    out[0] = (uint32_t)h0;
    out[1] = (uint32_t)h1;
    out[2] = (uint32_t)h2;
    out[3] = (uint32_t)h3;
    out[4] = (uint32_t)h4;
    out[5] = (uint32_t)h5;
    out[6] = (uint32_t)h6;
    out[7] = (uint32_t)h7;
    out[8] = (uint32_t)h8;
    out[9] = (uint32_t)h9;
}

/*
 * Compute addition for mixed-radix 2²⁶/2²⁵.
 * If the biggest input limb fits into x bits (x<32), the biggest output limb will fit into (x+1) bits.
 */
STATIC void add32(uint32_t out[10], const uint32_t a[10], const uint32_t b[10])
{
    unsigned i;

    for (i=0; i<10; i++) {
        out[i] = a[i] + b[i];
    }
}

/*
 * Swap arguments a/c and b/d when condition is NOT ZERO.
 * If the condition IS ZERO, no swapping takes place.
 */
STATIC void cswap(uint32_t a[10], uint32_t b[10], uint32_t c[10], uint32_t d[10], unsigned swap)
{
    uint32_t mask, i, e, f;

    mask = (uint32_t)(0 - (swap!=0));   /* 0 if swap is 0, all 1s if swap is !=0 */
    for (i=0; i<10; i++) {
        e = mask & (a[i] ^ c[i]);
        a[i] ^= e;
        c[i] ^= e;
        f = mask & (b[i] ^ d[i]);
        b[i] ^= f;
        d[i] ^= f;
    }
}

/*
 * Compute x⁻¹ in prime field 2²⁵⁵ - 19
 *
 * The input x[] is encoded in mixed radix 2²⁶/2²⁵ with limbs < 2²⁷.
 *
 * The result out[] is encoded in also mixed radix 2²⁶/2²⁵ such that:
 *      out[i] < 2²⁶      for even i
 *      out[j] < 2²⁵      for odd j<9
 *      out[9] < 2²⁶
 */
STATIC void invert_25519(uint32_t out[10], const uint32_t x[10])
{
    uint32_t a[10], x1[10], x3p0[10], x3p1p0[10], x5m0[10];
    uint32_t x10m0[10], x20m0[10], x50m0[10], x100m0[10];
    unsigned i;

    #define sqr_25519(a,b) mul_25519(a,b,b)

    sqr_25519(x1, x);           /* 2¹ */
    sqr_25519(a, x1);           /* 2² */
    sqr_25519(a, a);            /* 2³ */
    mul_25519(x3p0, a, x);      /* 2³ + 2⁰ */
    mul_25519(x3p1p0, x3p0, x1);/* 2³ + 2¹ + 2⁰ = 11 */
    sqr_25519(a, x3p1p0);       /* 2⁴ + 2² + 2¹  */
    mul_25519(x5m0, a, x3p0);   /* 2⁴ + 2³ + 2² + 2¹ + 2⁰ = 2⁵ - 2⁰ */
    sqr_25519(a, x5m0);         /* 2⁶ - 2¹ */
    sqr_25519(a, a);            /* 2⁷ - 2² */
    sqr_25519(a, a);            /* 2⁸ - 2³ */
    sqr_25519(a, a);            /* 2⁹ - 2⁴ */
    sqr_25519(a, a);            /* 2¹⁰ - 2⁵ */
    mul_25519(a, a, x5m0);      /* 2¹⁰ - 2⁰ */
    memcpy(x10m0, a, sizeof a);
    for (i=0; i<10; i++) {
        sqr_25519(a, a);
    }                           /* 2²⁰ - 2¹⁰ */
    mul_25519(a, a, x10m0);     /* 2²⁰ - 2⁰ */
    memcpy(x20m0, a, sizeof a);
    for (i=0; i<20; i++) {
        sqr_25519(a, a);
    }                           /* 2⁴⁰ - 2²⁰ */
    mul_25519(a, a, x20m0);     /* 2⁴⁰ - 2⁰ */
    for (i=0; i<10; i++) {
        sqr_25519(a, a);
    }                           /* 2⁵⁰ - 2¹⁰ */
    mul_25519(a, a, x10m0);     /* 2⁵⁰ - 2⁰ */
    memcpy(x50m0, a, sizeof a);
    for (i=0; i<50; i++) {
        sqr_25519(a, a);
    }                           /* 2¹⁰⁰ - 2⁵⁰ */
    mul_25519(a, a, x50m0);     /* 2¹⁰⁰ - 2⁰ */
    memcpy(x100m0, a, sizeof a);
    for (i=0; i<100; i++) {
        sqr_25519(a, a);
    }                           /* 2²⁰⁰ - 2¹⁰⁰ */
    mul_25519(a, a, x100m0);    /* 2²⁰⁰ - 2⁰ */
    for (i=0; i<50; i++) {
        sqr_25519(a, a);
    }                           /* 2²⁵⁰ - 2⁵⁰ */
    mul_25519(a, a, x50m0);     /* 2²⁵⁰ - 2⁰ */
    sqr_25519(a, a);            /* 2²⁵¹ - 2¹ */
    sqr_25519(a, a);            /* 2²⁵² - 2² */
    sqr_25519(a, a);            /* 2²⁵³ - 2³ */
    sqr_25519(a, a);            /* 2²⁵⁴ - 2⁴ */
    sqr_25519(a, a);            /* 2²⁵⁵ - 2⁵  = 2²⁵⁵ - 32 */
    mul_25519(out, a, x3p1p0);  /* 2²⁵⁵ - 21 */
}

/*
 * Add f[] and g[] modulo 2²⁵⁵ - 19.
 *
 * f[] and g[] are encoded in radix 2²⁶/2²⁵ and each limb is < 2²⁸.
 *
 * The result out[] is encoded in also radix 2²⁶/2²⁵ such that:
 *      out[i] < 2²⁶      for even i
 *      out[j] < 2²⁵      for odd j<9
 *      out[9] < 2²⁶
 */
STATIC void add_25519(uint32_t out[10], const uint32_t f[10], const uint32_t g[10])
{
    uint32_t h0, h1, h2, h3, h4, h5, h6, h7, h8, h9;
    uint32_t carry;

    h0 = f[0] + g[0];
    h1 = f[1] + g[1];
    h2 = f[2] + g[2];
    h3 = f[3] + g[3];
    h4 = f[4] + g[4];
    h5 = f[5] + g[5];
    h6 = f[6] + g[6];
    h7 = f[7] + g[7];
    h8 = f[8] + g[8];
    h9 = f[9] + g[9];

    /* h0..h9 < 2²⁸ */
    carry = h8 >> 26;
    h8 &= MASK26;
    /* carry < 2² */
    h9 += carry;
    carry = (h9 >> 25)*19;
    h9 &= MASK25;
    /* carry < 2⁹ */
    h0 += carry;
    carry = h0 >> 26;
    h0 &= MASK26;
    /* carry < 2³ */
    h1 += carry;
    carry = h1 >> 25;
    h1 &= MASK25;
    /* carry < 2⁴ */
    h2 += carry;
    carry = h2 >> 26;
    h2 &= MASK26;
    /* carry < 2³ */
    h3 += carry;
    carry = h3 >> 25;
    h3 &= MASK25;
    /* carry < 2⁴ */
    h4 += carry;
    carry = h4 >> 26;
    h4 &= MASK26;
    /* carry < 2³ */
    h5 += carry;
    carry = h5 >> 25;
    h5 &= MASK25;
    /* carry < 2⁴ */
    h6 += carry;
    carry = h6 >> 26;
    h6 &= MASK26;
    /* carry < 2³ */
    h7 += carry;
    carry = h7 >> 25;
    h7 &= MASK25;
    /* carry < 2⁴ */
    h8 += carry;
    carry = h8 >> 26;
    h8 &= MASK26;
    /* carry < 2¹ */
    h9 += carry;
    /* h9 < 2²⁶ */

    out[0] = h0;
    out[1] = h1;
    out[2] = h2;
    out[3] = h3;
    out[4] = h4;
    out[5] = h5;
    out[6] = h6;
    out[7] = h7;
    out[8] = h8;
    out[9] = h9;
}

/*
 * Carry out subtraction a[] - b[] for mixed-radix 2²⁶/2²⁵ modulo 2²⁵⁵-19.
 *
 * The output out[] is such that:
 *      x[i] < 2²⁶      for even i
 *      x[j] < 2²⁵      for odd j<9
 *      x[9] < 2²⁶
 */
STATIC void sub_25519(uint32_t out[10], const uint32_t a[10], const uint32_t b[10])
{
    /*
     * We pre-sum a number which is congruent to zero modulo 2²⁵⁵-19.
     * Limbs with even index are larger than any 26 bit number.
     * Limbs with odd index are larger than any 25 bit number.
     */
    static const uint32_t modulus_32[10] = { 0x7ffffda, 0x3fffffe, 0x7fffffe, 0x3fffffe, 0x7fffffe, 0x3fffffe, 0x7fffffe, 0x3fffffe, 0x7fffffe, 0x3fffffe };
    uint32_t zero[10] = { 0 };
    unsigned i;

    for (i=0; i<10; i++) {
        out[i] = modulus_32[i] + a[i] - b[i];
    }

    /** Reduce the output, because each limb is now < 2²⁸ **/
    add_25519(out, out, zero);
}

/*
 * Reduce a 256-bit number (mixed radix 2²⁶/²⁵, litte-endian) modulo 2²⁵⁵ - 19.
 *
 * Each limb of x[] in input is < 2²⁸
 *
 * The result x[] in output is such that:
 *      x[i] < 2²⁶      for even i
 *      x[j] < 2²⁵      for odd j<9
 *      x[9] < 2²⁶
 */
STATIC void reduce_25519_le25p5(uint32_t x[10])
{
    uint32_t zero[10] = { 0 };
    add_25519(x, x, zero);
    assert((x[0] >> 26) == 0);
}

#endif  /* __MOD25519_C */
