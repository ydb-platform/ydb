/* ===================================================================
 *
 * Copyright (c) 2022, Helder Eijs <helderijs@gmail.com>
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

/*
 * Twisted Edward curve with equation:
 *
 *      ax² + y² = 1 + dx²y²
 *
 *  where a = -1 and d = - 121665/121666 over the prime field 2²⁵⁵ - 19.
 *
 *  Points (x, y) can be represented as extended homogeneous coordinates
 *  (X, Y, Z, T) with x = X/Z, y = Y/Z, and x*y = T/Z for a non-zero Z.
 *
 *  The PAI (or neutral point) is (0, Z, Z, 0) or equivalently (0, 1).
 *  The point (x, y) can be obtained by normalizing to (x, y, 1, x*y).
 */

#include "common.h"
#include "endianess.h"
#include "ed25519.h"

FAKE_INIT(ed25519)

#include "mod25519.c"

/*
 * P3 can be P1 or P2
 */
STATIC void ed25519_add_internal(Point *P3, const Point *P1, const Point *P2)
{
    uint32_t A[10], B[10], C[10], D[10];
    /* d = 37095705934669439343138083508754565189542113879843219016388785533085940283555 */
    /* k = 2 * d (mod 2²⁵⁵ - 19) */
    static const uint32_t k[10] = { 0x2B2F159, 0x1A6E509, 0x22ADD7A, 0xD4141D, 0x38052,
                                    0xF3D130, 0x3407977, 0x19CE331, 0x1C56DFF, 0x901B67 };

    /* https://www.hyperelliptic.org/EFD/g1p/auto-twisted-extended-1.html#addition-add-2008-hwcd-3 */

    sub_25519(A, P1->Y, P1->X);         /* (Y1-X1)      each limb < 2²⁶ */
    sub_25519(B, P2->Y, P2->X);         /* (Y2-X2)      < 2²⁶ */
    mul_25519(A, A, B);                 /* A            < 2²⁶ */
    add32(B, P1->Y, P1->X);             /* (Y1+X1)      < 2²⁷ */
    add32(C, P2->Y, P2->X);             /* (Y2+X2)      < 2²⁷ */
    mul_25519(B, B, C);                 /* B            < 2²⁶ */
    mul_25519(C, P1->T, P2->T);         /* T1*T2        < 2²⁶ */
    mul_25519(C, C, k);                 /* C            < 2²⁶ */
    mul_25519(D, P1->Z, P2->Z);         /* Z1*Z2        < 2²⁶ */
    add_25519(D, D, D);                 /* D            < 2²⁶ */
    sub_25519(P3->T, B, A);             /* E=B-A        < 2²⁶ */
    sub_25519(P3->Z, D, C);             /* F=D-C        < 2²⁶ */
    add32(D, D, C);                     /* G=D+C        < 2²⁷ */
    add32(B, B, A);                     /* H=B+A        < 2²⁷ */
    mul_25519(P3->X, P3->T, P3->Z);     /* X3=E*F       < 2²⁶ */
    mul_25519(P3->Y, D, B);             /* Y3=G*H       < 2²⁶ */
    mul_25519(P3->T, P3->T, B);         /* T3=E*H       < 2²⁶ */
    mul_25519(P3->Z, P3->Z, D);         /* Z3=F*G       < 2²⁶ */
}

STATIC void ed25519_double_internal(Point *P3, const Point *P1)
{
    uint32_t A[10], B[10], C[10], D[10];

    mul_25519(A, P1->X, P1->X);         /* X1^2             each limb < 2²⁶ */
    mul_25519(B, P1->Y, P1->Y);         /* Y1^2             < 2²⁶ */
    mul_25519(C, P1->Z, P1->Z);         /* Z1^2             < 2²⁶ */
    add_25519(C, C, C);                 /* C=2*Z1^2         < 2²⁶ */
    add32(D, A, B);                     /* H=A+B            < 2²⁷ */
    add32(P3->T, P1->X, P1->Y);         /* X1+Y1            < 2²⁷ */
    mul_25519(P3->T, P3->T, P3->T);     /* (X1+Y1)^2        < 2²⁶ */
    sub_25519(P3->T, D, P3->T);         /* E=H-(X1+Y1)^2    < 2²⁶ */
    sub_25519(P3->Z, A, B);             /* G=A-B            < 2²⁶ */
    add_25519(A, C, P3->Z);             /* F=C+G            < 2²⁶ */
    mul_25519(P3->X, P3->T, A);         /* X3=E*F           < 2²⁶ */
    mul_25519(P3->Y, P3->Z, D);         /* Y3=G*H           < 2²⁶ */
    mul_25519(P3->T, P3->T, D);         /* T3=E*H           < 2²⁶ */
    mul_25519(P3->Z, A, P3->Z);         /* Z3=F*G           < 2²⁶ */
}

#if 0
static void print_point_le8(const Point *p)
{
    uint8_t bin[32];
    unsigned i;
    uint32_t tmp[32];
    uint32_t invz[32];

    invert_25519(invz, p->Z);
    mul_25519(tmp, p->X, invz);
    convert_le25p5_to_le8(bin, tmp);
    printf("X=");
    for (i=0; i<32; i++)
        printf("%02X", bin[i]);
    printf("\n");

    invert_25519(invz, p->Z);
    mul_25519(tmp, p->Y, invz);
    convert_le25p5_to_le8(bin, tmp);
    printf("Y=");
    for (i=0; i<32; i++)
        printf("%02X", bin[i]);
    printf("\n");
}
#endif

/*
 * Scalar multiplication Q = k*B
 *
 * @param[out]  xout    The X-coordinate of the resulting point Q.
 * @param[out]  yout    The Y-coordinate of the resulting point Q.
 * @param[in]   k       The scalar encoded in little-endian mode.
 * @param[in]   len     Length of the scalar in bytes.
 * @param[in]   xin     The X-coordinate of the input point B.
 * @param[in]   yin     The Y-coordinate of the input point B.
 */
STATIC void ed25519_scalar_internal(Point *Pout,
                                    const uint8_t *k, size_t len,
                                    const Point *Pin)
{
    Point R0, R1;
    unsigned bit_idx, swap;
    size_t scan;

    /* Point R0 */
    memset(&R0, 0, sizeof R0);
    R0.Y[0] = R0.Z[0] = 1;

    /* Point R1 */
    R1 = *Pin;

    /* https://eprint.iacr.org/2020/956.pdf */

    /* OPTIMIZE: with pre-computed tables in case of fixed-point multiplication */

    /* Scan all bits from MSB to LSB */
    bit_idx = 7;
    swap = 0;
    scan = 0;
    while (scan<len) {
        unsigned bit;

        bit = (k[scan] >> bit_idx) & 1;
        swap ^= bit;

        cswap(R0.X, R0.Y, R1.X, R1.Y, swap);
        cswap(R0.Z, R0.T, R1.Z, R1.T, swap);

        ed25519_add_internal(&R1, &R0, &R1);     /* R1 <-- R0 + R1 */
        ed25519_double_internal(&R0, &R0);       /* R0 <-- 2R0 */

        swap = bit;
        if (bit_idx-- == 0) {
            bit_idx = 7;
            scan++;
        }
    }
    cswap(R0.X, R0.Y, R1.X, R1.Y, swap);
    cswap(R0.Z, R0.T, R1.Z, R1.T, swap);

    *Pout = R0;
}

/* ---- */

EXPORT_SYM int ed25519_new_point(Point **out,
                      const uint8_t x[32], const uint8_t y[32],
                      size_t modsize, void *context)
{
    uint32_t A[10], B[10], C[10];
    const char d[] = "52036cee2b6ffe738cc740797779e89800700a4d4141d8ab75eb4dca135978a3";

    if ((NULL == out) || (NULL == x) || (NULL == y))
        return ERR_NULL;

    if (modsize != 32)
        return ERR_MODULUS;

    *out = calloc(1, sizeof(Point));
    if (NULL == *out)
        return ERR_MEMORY;

    convert_be8_to_le25p5((*out)->X, x);
    convert_be8_to_le25p5((*out)->Y, y);
    (*out)->Z[0] = 1;
    mul_25519((*out)->T, (*out)->X, (*out)->Y);

    /** Verify that the point is on the Ed25519 curve **/
    mul_25519(A, (*out)->X, (*out)->X);     /* x² */
    mul_25519(B, (*out)->Y, (*out)->Y);     /* y² */

    convert_behex_to_le25p5(C, d);          /* d */
    mul_25519(C, C, B);                     /* dy² */
    mul_25519(C, C, A);                     /* dx²y² */
    add_25519(C, C, A);                     /* dx²y² - ax² */

    memset(A, 0, sizeof A);
    A[0] = 1;
    add_25519(C, C, A);                     /* 1 + dx²y² - ax² */

    reduce_25519_le25p5(B);
    reduce_25519_le25p5(C);
    if (0 != memcmp(B, C, sizeof B)) {
        free(*out);
        *out = NULL;
        return ERR_EC_POINT;
    }

    return 0;
}

EXPORT_SYM int ed25519_clone(Point **P, const Point *Q)
{
    if ((NULL == P) || (NULL == Q))
        return ERR_NULL;

    *P = calloc(1, sizeof(Point));
    if (NULL == *P)
        return ERR_MEMORY;

    **P = *Q;
    return 0;
}

EXPORT_SYM void ed25519_free_point(Point *p)
{
    if (p)
        free(p);
}

EXPORT_SYM int ed25519_cmp(const Point *p1, const Point *p2)
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

    mul_25519(tmp, p1->Y, p2->Z);
    convert_le25p5_to_le8(bin1, tmp);
    mul_25519(tmp, p2->Y, p1->Z);
    convert_le25p5_to_le8(bin2, tmp);
    for (i=0; i<sizeof bin1; i++) {
        res |= bin1[i] != bin2[i];
    }

    return res;
}

EXPORT_SYM int ed25519_neg(Point *p)
{
    const uint32_t zero[10] = { 0 };

    sub_25519(p->X, zero, p->X);
    sub_25519(p->T, zero, p->T);
    return 0;
}

EXPORT_SYM int ed25519_get_xy(uint8_t *xb, uint8_t *yb, size_t modsize, Point *p)
{
    uint32_t invz[10], tmp[10];

    if ((NULL == xb) || (NULL == yb) || (NULL == p))
        return ERR_NULL;
    if (modsize != 32)
        return ERR_MODULUS;

    invert_25519(invz, p->Z);
    mul_25519(tmp, p->X, invz);
    convert_le25p5_to_be8(xb, tmp);
    mul_25519(tmp, p->Y, invz);
    convert_le25p5_to_be8(yb, tmp);

    return 0;
}

EXPORT_SYM int ed25519_double(Point *p)
{
    if (NULL == p)
        return ERR_NULL;
    ed25519_double_internal(p, p);
    return 0;
}

EXPORT_SYM int ed25519_add(Point *P1, const Point *P2)
{
    if ((NULL == P1) || (NULL == P2))
        return ERR_NULL;
    ed25519_add_internal(P1, P1, P2);
    return 0;
}

EXPORT_SYM int ed25519_scalar(Point *P, const uint8_t *scalar, size_t scalar_len, uint64_t seed)
{
    if ((NULL == P) || (NULL == scalar))
        return ERR_NULL;

    ed25519_scalar_internal(P, scalar, scalar_len, P);
    return 0;
}
