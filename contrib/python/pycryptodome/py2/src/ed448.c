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
 *  where a = 1 and d = -39081 over the prime field modulo 2⁴⁴⁸ - 2²²⁴ - 1.
 *
 *  Points (x, y) can be represented with projective coordinates
 *  (X, Y, Z) with x = X/Z and y = Y/Z for any non-zero Z.
 *  The point (x, y) can be obtained by normalizing to (x, y, 1).
 *
 *  The PAI (or neutral point) is (0, 1) or (0, Z, Z) for any non-zero Z.
 */

#include <assert.h>

#include "common.h"
#include "mont.h"
#include "ed448.h"

FAKE_INIT(ed448)

STATIC WorkplaceEd448 *new_workplace(const MontContext *ctx)
{
    WorkplaceEd448 *wp;
    int res;

    wp = calloc(1, sizeof(WorkplaceEd448));
    if (NULL == wp)
        return NULL;

    res = mont_new_number(&wp->a, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&wp->b, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&wp->c, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&wp->d, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&wp->e, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&wp->f, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&wp->scratch, SCRATCHPAD_NR, ctx);
    if (res) goto cleanup;
    return wp;

cleanup:
    free(wp->a);
    free(wp->b);
    free(wp->c);
    free(wp->d);
    free(wp->e);
    free(wp->f);
    free(wp->scratch);
    return NULL;
}

STATIC void free_workplace(WorkplaceEd448 *wp)
{
    if (NULL == wp)
        return;
    free(wp->a);
    free(wp->b);
    free(wp->c);
    free(wp->d);
    free(wp->e);
    free(wp->f);
    free(wp->scratch);
    free(wp);
}

/*
 * Convert projective coordinates of an Ed448 point to affine
 */
STATIC void ed448_projective_to_affine(uint64_t *x3, uint64_t *y3,
                                       const uint64_t *x1, uint64_t *y1, uint64_t *z1,
                                       WorkplaceEd448 *tmp,
                                       const MontContext *ctx)
{
    uint64_t *a = tmp->a;
    uint64_t *s = tmp->scratch;

    mont_inv_prime(a, z1, ctx);
    mont_mult(x3, x1, a, s, ctx);     /* X/Z */
    mont_mult(y3, y1, a, s, ctx);     /* Y/Z */
}

/*
 * Double an EC point on the Ed448 curve
 *
 * Input and output points can match.
 */
STATIC void ed448_double_internal(PointEd448 *Pout, const PointEd448 *Pin,
                                  WorkplaceEd448 *tmp, const MontContext *ctx)
{
    const uint64_t *x1 = Pin->x;
    const uint64_t *y1 = Pin->y;
    const uint64_t *z1 = Pin->z;
    uint64_t *x3 = Pout->x;
    uint64_t *y3 = Pout->y;
    uint64_t *z3 = Pout->z;
    uint64_t *t0 = tmp->a;
    uint64_t *t1 = tmp->b;
    uint64_t *t2 = tmp->c;
    uint64_t *t3 = tmp->d;
    uint64_t *t4 = tmp->e;
    uint64_t *t5 = tmp->f;
    uint64_t *s = tmp->scratch;

    /* https://datatracker.ietf.org/doc/html/rfc8032#section-5.2.4 */

    mont_add(t0,  x1, y1, s, ctx);
    mont_mult(t0, t0, t0, s, ctx);      /* B = (X1+Y1)^2 */
    mont_mult(t1, x1, x1, s, ctx);      /* C = X1^2 */
    mont_mult(t2, y1, y1, s, ctx);      /* D = Y1^2 */
    mont_add(t3,  t1, t2, s, ctx);      /* E = C+D */
    mont_mult(t4, z1, z1, s, ctx);      /* H = Z1^2 */
    mont_sub(t5,  t3, t4, s, ctx);
    mont_sub(t5,  t5, t4, s, ctx);      /* J = E-2*H */
    mont_sub(x3,  t0, t3, s, ctx);
    mont_mult(x3, x3, t5, s, ctx);      /* X3 = (B-E)*J */
    mont_sub(y3,  t1, t2, s, ctx);
    mont_mult(y3, y3, t3, s, ctx);      /* Y3 = E*(C-D) */
    mont_mult(z3, t3, t5, s, ctx);      /* Z3 = E*J */
}

/*
 * Add two EC points on the Ed448 curve
 */
STATIC void ed448_add_internal(PointEd448 *Pout,
                               const PointEd448 *Pin1,
                               const PointEd448 *Pin2,
                               const uint64_t *d,
                               WorkplaceEd448 *tmp,
                               const MontContext *ctx)
{
    const uint64_t *x1 = Pin1->x;
    const uint64_t *y1 = Pin1->y;
    const uint64_t *z1 = Pin1->z;
    const uint64_t *x2 = Pin2->x;
    const uint64_t *y2 = Pin2->y;
    const uint64_t *z2 = Pin2->z;
    uint64_t *x3 = Pout->x;
    uint64_t *y3 = Pout->y;
    uint64_t *z3 = Pout->z;
    uint64_t *t0 = tmp->a;
    uint64_t *t1 = tmp->b;
    uint64_t *t2 = tmp->c;
    uint64_t *t3 = tmp->d;
    uint64_t *t4 = tmp->e;
    uint64_t *t5 = tmp->f;
    uint64_t *s = tmp->scratch;

    /* https://datatracker.ietf.org/doc/html/rfc8032#section-5.2.4 */

    mont_mult(t0, z1, z2, s, ctx);      /* A = Z1*Z2 */
    mont_mult(t1, t0, t0, s, ctx);      /* B = A^2 */
    mont_mult(t2, x1, x2, s, ctx);      /* C = X1*X2 */
    mont_mult(t3, y1, y2, s, ctx);      /* D = Y1*Y2 */
    mont_add(t4, x1, y1,  s, ctx);
    mont_add(t5, x2, y2,  s, ctx);
    mont_mult(t4, t4, t5, s, ctx);      /* H = (X1+Y1)*(X2+Y2) */
    mont_mult(t5, t2, t3, s, ctx);
    mont_mult(t5, t5,  d, s, ctx);      /* E = d*C*D */
    mont_sub(x3, t4, t2,  s, ctx);
    mont_sub(x3, x3, t3,  s, ctx);
    mont_sub(t4, t1, t5,  s, ctx);      /* F = B-E */
    mont_mult(x3, x3, t4, s, ctx);
    mont_mult(x3, x3, t0, s, ctx);      /* X3 = A*F*(H-C-D) */
    mont_add(t5, t1, t5,  s, ctx);      /* G = B+E */
    mont_sub(y3, t3, t2,  s, ctx);
    mont_mult(y3, y3, t5, s, ctx);
    mont_mult(y3, y3, t0, s, ctx);      /* Y3 = A*G*(D-C) */
    mont_mult(z3, t4, t5, s, ctx);      /* Z3 = F*G */
}

STATIC void cswap(PointEd448 *a, PointEd448 *b, unsigned swap)
{
    uint64_t mask, e, f, g;
    unsigned int i;

    mask = (uint64_t)(0 - (swap!=0));   /* 0 if swap is 0, all 1s if swap is !=0 */

    for (i=0; i<7; i++) {
        e = mask & (a->x[i] ^ b->x[i]);
        a->x[i] ^= e;
        b->x[i] ^= e;
        f = mask & (a->y[i] ^ b->y[i]);
        a->y[i] ^= f;
        b->y[i] ^= f;
        g = mask & (a->z[i] ^ b->z[i]);
        a->z[i] ^= g;
        b->z[i] ^= g;
    }
}

/*
 * Scalar multiplication Q = k*B on the Ed448 curve
 */
STATIC int ed448_scalar_internal(PointEd448 *Pout,
                                  const uint8_t *k,
                                  size_t len,
                                  const PointEd448 *Pin)
{
    PointEd448 *R0=NULL;
    PointEd448 *R1=NULL;
    unsigned bit_idx, swap;
    size_t scan;
    int res;

    res = ed448_new_point(&R0, (uint8_t*)"\x00", (uint8_t*)"\x01", 1, Pin->ec_ctx);
    if (res) goto cleanup;

    res = ed448_clone(&R1, Pin);
    if (res) goto cleanup;

    /* https://eprint.iacr.org/2020/956.pdf */

    /* OPTIMIZE: with pre-computed tables in case of fixed-point base multiplication */

    /* Scan all bits from MSB to LSB */
    bit_idx = 7;
    swap = 0;
    scan = 0;
    while (scan<len) {
        unsigned bit;

        bit = (k[scan] >> bit_idx) & 1;
        swap ^= bit;

        cswap(R0, R1, swap);

        /* R1 <-- R0 + R1 */
        ed448_add_internal(R1, R0, R1,
                           Pin->ec_ctx->d,
                           Pin->wp,
                           Pin->ec_ctx->mont_ctx);
        /* R0 <-- 2R0 */
        ed448_double_internal(R0, R0,
                              Pin->wp,
                              Pin->ec_ctx->mont_ctx);

        swap = bit;
        if (bit_idx-- == 0) {
            bit_idx = 7;
            scan++;
        }
    }
    cswap(R0, R1, swap);

    ed448_copy(Pout, R0);
    res = 0;

cleanup:
    ed448_free_point(R0);
    ed448_free_point(R1);
    return res;
}


/* ------------------------------------- */

/*
 * Create an Elliptic Curve context for Ed448
 *
 * @param pec_ctx   The memory area where the pointer to the newly allocated
 *                  EC context will be stored.
 * @return          0 for success, the appropriate error code otherwise
 */
EXPORT_SYM int ed448_new_context(EcContext **pec_ctx)
{
    EcContext *ec_ctx = NULL;
    int res;
    MontContext *ctx;
    const uint8_t mod448_be[56] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    const uint8_t d448_be[56] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                 0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF,
                                 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x67, 0x56};

    if (NULL == pec_ctx)
        return ERR_NULL;

    *pec_ctx = ec_ctx = (EcContext*)calloc(1, sizeof(EcContext));
    if (NULL == ec_ctx)
        return ERR_MEMORY;

    res = mont_context_init(&ec_ctx->mont_ctx, mod448_be, sizeof(mod448_be));
    if (res) goto cleanup;
    ctx = ec_ctx->mont_ctx;

    res = mont_new_from_bytes(&ec_ctx->d, d448_be, sizeof(d448_be), ctx);
    if (res) goto cleanup;

    return 0;

cleanup:
    free(ec_ctx->d);
    mont_context_free(ec_ctx->mont_ctx);
    free(ec_ctx);
    return res;
}

EXPORT_SYM void ed448_free_context(EcContext *ec_ctx)
{
    if (NULL == ec_ctx)
        return;
    free(ec_ctx->d);
    mont_context_free(ec_ctx->mont_ctx);
    free(ec_ctx);
}

/*
 * Create a new EC point on the Ed448 curve.
 *
 *  @param pecp         The memory area where the pointer to the newly allocated EC
 *                      point will be stored.
 *                      Use ed448_free_point() for deallocating it.
 *  @param x            The X-coordinate (affine, big-endian, smaller than modulus)
 *  @param y            The Y-coordinate (affine, big-endian, smaller than modulus)
 *  @param len          The length of x and y in bytes (max 56 bytes)
 *  @param ec_ctx       The EC context
 *  @return             0 for success, the appopriate error code otherwise
 */
EXPORT_SYM int ed448_new_point(PointEd448 **pecp,
                               const uint8_t *x,
                               const uint8_t *y,
                               size_t len,
                               const EcContext *ec_ctx)
{
    int res;
    WorkplaceEd448 *wp = NULL;
    PointEd448 *ecp = NULL;
    MontContext *ctx = NULL;
    uint64_t *scratch = NULL;

    if (NULL == pecp || NULL == x || NULL == y || NULL == ec_ctx)
        return ERR_NULL;
    ctx = ec_ctx->mont_ctx;

    if (len == 0)
        return ERR_NOT_ENOUGH_DATA;

    if (len > ctx->bytes)
        return ERR_VALUE;

    *pecp = ecp = (PointEd448*)calloc(1, sizeof(PointEd448));
    if (NULL == ecp)
        return ERR_MEMORY;

    ecp->ec_ctx = ec_ctx;

    /** No need to treat PAI (x=0, y=1) in a special way **/
    res = mont_new_from_bytes(&ecp->x, x, len, ctx);
    if (res) goto cleanup;
    res = mont_new_from_bytes(&ecp->y, y, len, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&ecp->z, 1, ctx);
    if (res) goto cleanup;
    mont_set(ecp->z, 1, ctx);

    ecp->wp = new_workplace(ctx);
    if (NULL == ecp->wp) goto cleanup;

    wp = ecp->wp;
    scratch = ecp->wp->scratch;

    /* Verify that the point is on the curve **/
    /* x² + y² = 1 + dx²y² */
    mont_mult(wp->a, ecp->y,    ecp->y, scratch, ctx);  /* y² */
    mont_mult(wp->b, ecp->x,    ecp->x, scratch, ctx);  /* x² */
    mont_mult(wp->c, wp->a,     wp->b,  scratch, ctx);  /* x²y² */
    mont_mult(wp->c, ec_ctx->d, wp->c,  scratch, ctx);  /* dx²y² */
    mont_add(wp->c,  ecp->z,    wp->c,  scratch, ctx);  /* 1 + dx²y² */
    mont_add(wp->a,  wp->a,     wp->b,  scratch, ctx);  /* x² + y² */
    res = !mont_is_equal(wp->a, wp->c,  ctx);
    if (res) {
        res = ERR_EC_POINT;
        goto cleanup;
    }
    return 0;

cleanup:
    ed448_free_point(ecp);
    *pecp = NULL;
    return res;
}

EXPORT_SYM void ed448_free_point(PointEd448 *ecp)
{
    if (NULL == ecp)
        return;

    /* The EC context (ecp->ecp_ctx) is allocated once and shared by all
     * points on the same surve, so we will not free it here.
     */

    free_workplace(ecp->wp);
    free(ecp->x);
    free(ecp->y);
    free(ecp->z);
    free(ecp);
}

/*
 * Encode the affine coordinates of an EC point.
 *
 * @param x     The location where the affine X-coordinate will be store in big-endian mode
 * @param y     The location where the affine Y-coordinate will be store in big-endian mode
 * @param len   The memory available for x and y in bytes.
 *              It must be able to contain the prime modulus of the curve field.
 * @param ecp   The EC point to encode.
 */
EXPORT_SYM int ed448_get_xy(uint8_t *x, uint8_t *y, size_t len, const PointEd448 *ecp)
{
    uint64_t *xw=NULL, *yw=NULL;
    MontContext *ctx;
    int res;

    if (NULL == x || NULL == y || NULL == ecp)
        return ERR_NULL;
    ctx = ecp->ec_ctx->mont_ctx;

    if (len < ctx->modulus_len)
        return ERR_NOT_ENOUGH_DATA;

    res = mont_new_number(&xw, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&yw, 1, ctx);
    if (res) goto cleanup;

    ed448_projective_to_affine(xw, yw, ecp->x, ecp->y, ecp->z, ecp->wp, ctx);

    res = mont_to_bytes(x, len, xw, ctx);
    if (res) goto cleanup;
    res = mont_to_bytes(y, len, yw, ctx);
    if (res) goto cleanup;

    res = 0;

cleanup:
    free(xw);
    free(yw);
    return res;
}

/*
 * Double an EC point
 */
EXPORT_SYM int ed448_double(PointEd448 *P)
{
    MontContext *ctx;

    if (NULL == P)
        return ERR_NULL;
    ctx = P->ec_ctx->mont_ctx;

    ed448_double_internal(P, P, P->wp, ctx);
    return 0;
}

/*
 * Add an EC point to another
 */
EXPORT_SYM int ed448_add(PointEd448 *ecpa, const PointEd448 *ecpb)
{
    MontContext *ctx;

    if (NULL == ecpa || NULL == ecpb)
        return ERR_NULL;
    if (ecpa->ec_ctx != ecpb->ec_ctx)
        return ERR_EC_CURVE;
    ctx = ecpa->ec_ctx->mont_ctx;

    ed448_add_internal(ecpa, ecpa, ecpb,
                       ecpb->ec_ctx->d,
                       ecpb->wp, ctx);

    return 0;
}

/*
 * Multiply an EC point by a scalar
 *
 * @param ecp   The EC point to multiply
 * @param k     The scalar, encoded in big endian mode
 * @param len   The length of the scalar, in bytes
 * @param seed  The 64-bit to drive the randomizations against SCAs
 * @return      0 in case of success, the appropriate error code otherwise
 */
EXPORT_SYM int ed448_scalar(PointEd448 *P, const uint8_t *scalar, size_t scalar_len, uint64_t _)
{
    int res;
    if (NULL == P || NULL == scalar)
        return ERR_NULL;

    res = ed448_scalar_internal(P, scalar, scalar_len, P);
    return res;
}

EXPORT_SYM int ed448_clone(PointEd448 **pecp2, const PointEd448 *ecp)
{
    int res = -1;
    PointEd448 *ecp2;
    MontContext *ctx;

    if (NULL == pecp2 || NULL == ecp)
        return ERR_NULL;
    ctx = ecp->ec_ctx->mont_ctx;

    *pecp2 = ecp2 = (PointEd448*)calloc(1, sizeof(PointEd448));
    if (NULL == ecp2)
        return ERR_MEMORY;

    ecp2->ec_ctx = ecp->ec_ctx;

    ecp2->wp = new_workplace(ctx);
    if (NULL == ecp2->wp) goto cleanup;

    res = mont_new_number(&ecp2->x, 1, ctx);
    if (res) goto cleanup;
    mont_copy(ecp2->x, ecp->x, ctx);

    res = mont_new_number(&ecp2->y, 1, ctx);
    if (res) goto cleanup;
    mont_copy(ecp2->y, ecp->y, ctx);

    res = mont_new_number(&ecp2->z, 1, ctx);
    if (res) goto cleanup;
    mont_copy(ecp2->z, ecp->z, ctx);

    return 0;

cleanup:
    free_workplace(ecp2->wp);
    free(ecp2->x);
    free(ecp2->y);
    free(ecp2->z);
    free(ecp2);
    *pecp2 = NULL;
    return res;
}

/*
 * Compare two EC points and return 0 if they match
 */
EXPORT_SYM int ed448_cmp(const PointEd448 *ecp1, const PointEd448 *ecp2)
{
    MontContext *ctx;
    WorkplaceEd448 *wp;
    uint64_t *scratch;
    int result;

    if (NULL == ecp1 || NULL == ecp2)
        return ERR_NULL;

    if (ecp1->ec_ctx != ecp2->ec_ctx)
        return ERR_EC_CURVE;

    /** Normalize to have the same Z coordinate */
    ctx = ecp1->ec_ctx->mont_ctx;
    wp = ecp1->wp;
    scratch = wp->scratch;
    mont_mult(wp->b, ecp1->x, ecp2->z, scratch, ctx);   /* B = X1*Z2 */
    mont_mult(wp->d, ecp2->x, ecp1->z, scratch, ctx);   /* D = X2*Z1 */
    mont_mult(wp->e, ecp1->y, ecp2->z, scratch, ctx);   /* E = Y1*Z2 */
    mont_mult(wp->f, ecp2->y, ecp1->z, scratch, ctx);   /* F = Y2*Z1 */
    result = (mont_is_equal(wp->b, wp->d, ctx) && mont_is_equal(wp->e, wp->f, ctx)) ? 0 : ERR_VALUE;

    return result;
}

EXPORT_SYM int ed448_neg(PointEd448 *P)
{
    MontContext *ctx;

    if (NULL == P)
        return ERR_NULL;

    ctx = P->ec_ctx->mont_ctx;
    mont_sub(P->x, ctx->modulus, P->x, P->wp->scratch, ctx);

    return 0;
}

EXPORT_SYM int ed448_copy(PointEd448 *ecp1, const PointEd448 *ecp2)
{
    const MontContext *ctx;

    if (NULL == ecp1 || NULL == ecp2)
        return ERR_NULL;

    /*
     * The EC context is shared by all EC point
     * on the same curve, so we do not free
     * nor duplicate it here.
     */
    ecp1->ec_ctx = ecp2->ec_ctx;

    ctx = ecp2->ec_ctx->mont_ctx;
    mont_copy(ecp1->x, ecp2->x, ctx);
    mont_copy(ecp1->y, ecp2->y, ctx);
    mont_copy(ecp1->z, ecp2->z, ctx);

    return 0;
}
