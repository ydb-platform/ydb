/* Copyright (C) 2024 by Helder Eijs <helderijs@gmail.com> -- All rights reserved.
 * This code is licensed under the BSD 2-Clause license. See LICENSE.rst file for info. */

/*
 *  curve25519 is a Montgomery curve with equation:
 *
 *      y² = x³ + Ax² + x
 *
 *  over the prime field 2²⁵⁵ - 19 with A = 486662.
 *  It has cofactor 8 and order 2²⁵² + 0x14def9dea2f79cd65812631a5cf5d3ed.
 *  Also, it is birationally equivalent to the twisted Edwards curve ed25519.
 *
 *  A point is represented by coordinates (X, Z) so that x = X/Z for a
 *  non-zero Z, with two possible y-coordinates.
 *
 *  In this implementation, Z is always 1.
 *
 *  The PAI (or neutral point) is (X, 0).
 */

#include "common.h"
#include "endianess.h"
#include "curve25519.h"

FAKE_INIT(curve25519)

#include "mod25519.c"

/*
 * Perform a step of the Montgomery ladder.
 * It is based on the function f() such that:
 *
 * P_{m+n} = f(P_m, P_n, P_{n-m})
 *
 * limited to the cases:
 *
 * P_{2n}   = f(P_n, P_n,     P_0)
 * P_{2n+1} = f(P_n, P_{n+1}, P_1)
 *
 * so that:
 *
 * P_2 = f(P_1, P_1, P_0)
 * P_3 = f(P_1, P_2, P_1)
 * P_4 = f(P_2, P_2, P_0)
 * P_5 = f(P_2, P_3, P_1)
 * P_6 = f(P_3, P_3, P_0)
 * ...
 *
 * Here we efficiently combine two computations of f():
 *
 * P_{2n}   = f(P_n, P_n, P_0)
 * P_{2n+1} = f(P_n, P_{n+1}, P_1)
 *
 * Ref: https://eprint.iacr.org/2017/293.pdf
 *
 * @param[in]       X1      The x coordinate of the fixed P1 point (Z1=1).
 * @param[in,out]   P2      In input, the point to double.
 *                          In output, the double of the original P2.
 * @param[in,out]   P3      In input, the point to double + P1.
 *                          In output, the double of the original P2 + P1.
 */
STATIC void curve25519_ladder_step(uint32_t x2[10], uint32_t z2[10], uint32_t x3[10], uint32_t z3[10], const uint32_t xp[10])
{
    uint32_t t0[10], t1[10];

    static const uint32_t a24[10] = { 0x1db42 };

    /** https://www.hyperelliptic.org/EFD/g1p/auto-montgom-xz.html#ladder-mladd-1987-m **/
    /* TODO: optimize multiplication by 33 bit constant a24 */

    sub_25519(t0, x3, z3);          /* t0 = D = X3 - Z3         < 2^26 */
    sub_25519(t1, x2, z2);          /* t1 = B = X2 - Z2         < 2^26 */
    add32(x2, x2, z2);              /* x2 = A = X2 + Z2         < 2^27 */
    add32(z2, x3, z3);              /* z2 = C = X3 - Z3         < 2^27 */
    mul_25519(z3, t0, x2);          /* z3 = DA                  < 2^26 */
    mul_25519(z2, z2, t1);          /* z2 = CB                  < 2^26 */
    add32(x3, z3, z2);              /* x3 = DA+CB               < 2^27 */
    sub_25519(z2, z3, z2);          /* z2 = DA-CB               < 2^26 */
    mul_25519(x3, x3, x3);          /* x3 = X5 = (DA+DB)²       < 2^26 */
    mul_25519(z2, z2, z2);          /* z2 = (DA-CB)²            < 2^26 */
    mul_25519(t0, t1, t1);          /* t0 = BB = B²             < 2^26 */
    mul_25519(t1, x2, x2);          /* t1 = AA = A²             < 2^26 */
    sub_25519(x2, t1, t0);          /* x2 = E = AA-BB           < 2^26 */
    mul_25519(z3, xp, z2);          /* z3 = Z5 = X1*(DA-CB)²    < 2^26 */
    mul_25519(z2, a24, x2);         /* z2 = a24*E               < 2^26 */
    add32(z2, t0, z2);              /* z2 = BB+a24*E            < 2^27 */
    mul_25519(z2, x2, z2);          /* z2 = Z4 = E*(BB+a24*E)   < 2^26 */
    mul_25519(x2, t1, t0);          /* x2 = X4 = AA*BB          < 2^26 */
}

/*
 * Scalar multiplication Q = k*B
 *
 * @param[out]  Pout    The output point Q.
 * @param[in]   k       The scalar encoded in big-endian mode.
 * @param[in]   len     Length of the scalar in bytes.
 * @param[in]   Pin     The input point B.
 */
STATIC void curve25519_scalar_internal(Point *Pout,
                                       const uint8_t *k, size_t len,
                                       const Point *Pin)
{
    Point P2, P3;
    unsigned bit_idx, swap;
    size_t scan;

    /* P2 = PAI */
    memset(&P2, 0, sizeof P2);
    P2.X[0] = 1;

    P3 = *Pin;

    /*
     * https://eprint.iacr.org/2020/956.pdf
     * https://www.ams.org/journals/mcom/1987-48-177/S0025-5718-1987-0866113-7/S0025-5718-1987-0866113-7.pdf
     */

    /* Scan all bits from MSB to LSB */
    bit_idx = 7;
    swap = 0;
    scan = 0;
    while (scan<len) {
        unsigned bit;

        bit = (k[scan] >> bit_idx) & 1;
        swap ^= bit;

        cswap(P2.X, P2.Z, P3.X, P3.Z, swap);

        curve25519_ladder_step(P2.X, P2.Z, P3.X, P3.Z, Pin->X);
        swap = bit;

        if (bit_idx-- == 0) {
            bit_idx = 7;
            scan++;
        }
    }
    cswap(P2.X, P2.Z, P3.X, P3.Z, swap);

    /* P2 is the result */
    memset(Pout, 0, sizeof *Pout);
    if (is_le25p5_zero(P2.Z)) {
        Pout->X[0] = 1;
    } else {
        uint32_t invz[10];

        invert_25519(invz, P2.Z);
        mul_25519(Pout->X, P2.X, invz);
        Pout->Z[0] = 1;
    }
}

/* ---- */

EXPORT_SYM int curve25519_new_point(Point **out,
                                    const uint8_t x[32],
                                    size_t modsize,
                                    const void *context)
{
    if (NULL == out)
        return ERR_NULL;

    if (context != NULL)
        return ERR_UNKNOWN;

    if ((modsize != 32) && (modsize != 0))
        return ERR_MODULUS;

    *out = calloc(1, sizeof(Point));
    if (NULL == *out)
        return ERR_MEMORY;

    if ((x != NULL) && (modsize == 32)) {
        convert_be8_to_le25p5((*out)->X, x);
        (*out)->Z[0] = 1;
    } else {
        /** PAI **/
        (*out)->X[0] = 1;
    }

    /* No need to verify if the point is on the Curve25519 curve */

    return 0;
}

EXPORT_SYM int curve25519_clone(Point **P, const Point *Q)
{
    if ((NULL == P) || (NULL == Q))
        return ERR_NULL;

    *P = calloc(1, sizeof(Point));
    if (NULL == *P)
        return ERR_MEMORY;

    **P = *Q;
    return 0;
}

EXPORT_SYM void curve25519_free_point(Point *p)
{
    if (p)
        free(p);
}

EXPORT_SYM int curve25519_get_x(uint8_t *xb, size_t modsize, const Point *p)
{
    if ((NULL == xb) || (NULL == p))
        return ERR_NULL;

    if (modsize != 32)
        return ERR_MODULUS;

    if (is_le25p5_zero(p->Z))
        return ERR_EC_PAI;

    convert_le25p5_to_be8(xb, p->X);

    return 0;
}

EXPORT_SYM int curve25519_scalar(Point *P, const uint8_t *scalar, size_t scalar_len, uint64_t seed)
{
    if ((NULL == P) || (NULL == scalar))
        return ERR_NULL;

    curve25519_scalar_internal(P, scalar, scalar_len, P);
    return 0;
}

EXPORT_SYM int curve25519_cmp(const Point *p1, const Point *p2)
{
    uint32_t tmp[10];
    uint8_t bin1[32], bin2[32];
    unsigned int i;
    int res = 0;

    mul_25519(tmp, p1->X, p2->Z);
    convert_le25p5_to_le8(bin1, tmp);
    mul_25519(tmp, p2->X, p1->Z);
    convert_le25p5_to_le8(bin2, tmp);
    for (i=0; i<sizeof bin1; i++) {
        res |= bin1[i] != bin2[i];
    }

    return res;
}

#ifdef PROFILE
int main(void)
{
    uint8_t pubkey[32];
    uint8_t secret[32];
    unsigned i;
    int res;
    Point *Pin;
    Point Pout;

    secret[0] = pubkey[0] = 0xAA;
    for (i=1; i<32; i++) {
        secret[i] = pubkey[i] = (uint8_t)((secret[i-1] << 1) | (secret[i-1] >> 7));
    }

    res = curve25519_new_point(&Pin, pubkey, 32, NULL);
    if (res) {
        printf("Error: %d\n", res);
        return res;
    }

    for (i=0; i<10000 && res == 0; i++) {
        curve25519_scalar_internal(&Pout, secret, sizeof secret, Pin);
    }

    curve25519_free_point(Pin);
}
#endif
