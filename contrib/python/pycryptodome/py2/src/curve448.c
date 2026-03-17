/* Copyright (C) 2024 by Helder Eijs <helderijs@gmail.com> -- All rights reserved.
 * This code is licensed under the BSD 2-Clause license. See LICENSE.rst file for info. */

/*
 *  curve448 is a Montgomery curve with equation:
 *
 *      y² = x³ + Ax² + x
 *
 *  over the prime field 2⁴⁴⁸ - 2²²⁴ - 1 with A = 156326.
 *  It has cofactor 4 and order 2⁴⁴⁶ - 0x8335dc163bb124b65129c96fde933d8d723a70aadc873d6d54a7bb0d.
 *  Also, it is birationally equivalent to the untwisted Edwards curve ed448.
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
#include "mont.h"
#include "curve448.h"

FAKE_INIT(curve448)

STATIC void free_workplace(WorkplaceCurve448 *wp)
{
    if (wp) {
        free(wp->a);
        free(wp->b);
        free(wp->scratch);
        free(wp);
    }
}

STATIC WorkplaceCurve448 *new_workplace(const MontContext *ctx)
{
    WorkplaceCurve448 *wp = NULL;
    int res;

    wp = calloc(1, sizeof(WorkplaceCurve448));
    if (NULL == wp)
        return NULL;

    res = mont_new_number(&wp->a, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&wp->b, 1, ctx);
    if (res) goto cleanup;
    res = mont_new_number(&wp->scratch, SCRATCHPAD_NR, ctx);
    if (res) goto cleanup;
    return wp;

cleanup:
    free_workplace(wp);
    return NULL;
}

/*
 * Swap arguments a/c and b/d when condition is NOT ZERO.
 * If the condition IS ZERO, no swapping takes place.
 */
STATIC void cswap(uint64_t a[7], uint64_t b[7], uint64_t c[7], uint64_t d[7], unsigned swap)
{
    uint64_t mask, i, e, f;

    mask = (uint64_t)(0 - (swap!=0));   /* 0 if swap is 0, all 1s if swap is !=0 */
    for (i=0; i<7; i++) {
        e = mask & (a[i] ^ c[i]);
        a[i] ^= e;
        c[i] ^= e;
        f = mask & (b[i] ^ d[i]);
        b[i] ^= f;
        d[i] ^= f;
    }
}

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
 * @param[in,out]   P2      In input, the point to double.
 *                          In output, the double of the original P2.
 * @param[in,out]   P3      In input, the point to double (P2) + P1.
 *                          In output, the double of the original P2 + P1.
 * @param[in]       P1      The fixed P1 point (Z1=1).
 */
STATIC void curve448_ladder_step(Curve448Point *P2, Curve448Point *P3, const Curve448Point *P1)
{
    const MontContext *ctx = P2->ec_ctx->mont_ctx;
    const uint64_t *a24 = P2->ec_ctx->a24;
    uint64_t *t0 = P2->wp->a;
    uint64_t *t1 = P2->wp->b;
    uint64_t *x2 = P2->x;
    uint64_t *z2 = P2->z;
    uint64_t *x3 = P3->x;
    uint64_t *z3 = P3->z;
    const uint64_t *xp = P1->x;
    uint64_t *scratch = P2->wp->scratch;

    /** https://www.hyperelliptic.org/EFD/g1p/auto-montgom-xz.html#ladder-mladd-1987-m **/

    mont_sub(t0, x3, z3, scratch, ctx);      /* t0 = D = X3 - Z3         */
    mont_sub(t1, x2, z2, scratch, ctx);      /* t1 = B = X2 - Z2         */
    mont_add(x2, x2, z2, scratch, ctx);      /* x2 = A = X2 + Z2         */
    mont_add(z2, x3, z3, scratch, ctx);      /* z2 = C = X3 - Z3         */
    mont_mult(z3, t0, x2, scratch, ctx);     /* z3 = DA                  */
    mont_mult(z2, z2, t1, scratch, ctx);     /* z2 = CB                  */
    mont_add(x3, z3, z2, scratch, ctx);      /* x3 = DA+CB               */
    mont_sub(z2, z3, z2, scratch, ctx);      /* z2 = DA-CB               */
    mont_mult(x3, x3, x3, scratch, ctx);     /* x3 = X5 = (DA+CB)²       */
    mont_mult(z2, z2, z2, scratch, ctx);     /* z2 = (DA-CB)²            */
    mont_mult(t0, t1, t1, scratch, ctx);     /* t0 = BB = B²             */
    mont_mult(t1, x2, x2, scratch, ctx);     /* t1 = AA = A²             */
    mont_sub(x2, t1, t0, scratch, ctx);      /* x2 = E = AA-BB           */
    mont_mult(z3, xp, z2, scratch, ctx);     /* z3 = Z5 = X1*(DA-CB)²    */
    mont_mult(z2, a24, x2, scratch, ctx);    /* z2 = a24*E               */
    mont_add(z2, t0, z2, scratch, ctx);      /* z2 = BB+a24*E            */
    mont_mult(z2, x2, z2, scratch, ctx);     /* z2 = Z4 = E*(BB+a24*E)   */
    mont_mult(x2, t1, t0, scratch, ctx);     /* x2 = X4 = AA*BB          */
}

/*
 * Scalar multiplication Q = k*B
 *
 * @param[out]  Pout    The output point Q.
 * @param[in]   k       The scalar encoded in big-endian mode.
 * @param[in]   len     Length of the scalar in bytes.
 * @param[in]   Pin     The input point B.
 */
STATIC int curve448_scalar_internal(Curve448Point *Pout,
                                    const uint8_t *k,
                                    size_t len,
                                    const Curve448Point *Pin)
{
    Curve448Point *P2 = NULL;
    Curve448Point *P3 = NULL;
    const Curve448Context *ec_ctx = Pin->ec_ctx;
    const MontContext *mont_ctx = ec_ctx->mont_ctx;
    unsigned bit_idx, swap;
    size_t scan;
    int res;

    /* P2 = PAI */
    res = curve448_new_point(&P2, NULL, 0, Pin->ec_ctx);
    if (res) goto cleanup;

    /* P3 = Pin */
    res = curve448_clone(&P3, Pin);
    if (res) goto cleanup;

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
        cswap(P2->x, P2->z, P3->x, P3->z, swap);
        curve448_ladder_step(P2, P3, Pin);
        swap = bit;
        if (bit_idx-- == 0) {
            bit_idx = 7;
            scan++;
        }
    }
    cswap(P2->x, P2->z, P3->x, P3->z, swap);

    /* P2 is the result */

    if (mont_is_zero(P2->z, mont_ctx)) {
        mont_set(Pout->x, 1, mont_ctx);
        mont_set(Pout->z, 0, mont_ctx);
    } else {
        uint64_t *invz = Pout->wp->a;
        uint64_t *scratch = P2->wp->scratch;

        res = mont_inv_prime(invz, P2->z, mont_ctx);
        if (res) goto cleanup;
        res = mont_mult(Pout->x, P2->x, invz, scratch, mont_ctx);
        if (res) goto cleanup;
        mont_set(Pout->z, 1, mont_ctx);
    }
    res = 0;

cleanup:
    curve448_free_point(P2);
    curve448_free_point(P3);
    return res;
}

/* ------------------------------------- */

/*
 * Create an Elliptic Curve context for Curve448
 *
 * @param pec_ctx   The memory area where the pointer to the newly allocated
 *                  EC context will be stored.
 * @return          0 for success, the appropriate error code otherwise
 */
EXPORT_SYM int curve448_new_context(Curve448Context **pec_ctx)
{
    Curve448Context *ec_ctx = NULL;
    int res;
    MontContext *ctx;
    const uint8_t mod448_be[56] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

    if (NULL == pec_ctx)
        return ERR_NULL;

    *pec_ctx = ec_ctx = (Curve448Context*)calloc(1, sizeof(Curve448Context));
    if (NULL == ec_ctx)
        return ERR_MEMORY;

    res = mont_context_init(&ec_ctx->mont_ctx, mod448_be, sizeof(mod448_be));
    if (res) goto cleanup;
    ctx = ec_ctx->mont_ctx;

    /* a24 = (a+2)/4 */
    res = mont_new_from_uint64(&ec_ctx->a24, 39082, ctx);
    if (res) goto cleanup;

    return 0;

cleanup:
    free(ec_ctx->a24);
    mont_context_free(ec_ctx->mont_ctx);
    free(ec_ctx);
    return res;
}

EXPORT_SYM void curve448_free_context(Curve448Context *ec_ctx)
{
    if (NULL != ec_ctx) {
        free(ec_ctx->a24);
        mont_context_free(ec_ctx->mont_ctx);
        free(ec_ctx);
    }
}

/*
 * Create a new EC point on the Curve448 curve.
 *
 *  @param out          The memory area where the pointer to the newly allocated EC
 *                      point will be stored.
 *                      Use curve448_free_point() for deallocating it.
 *  @param x            The X-coordinate (affine, big-endian, smaller than modulus)
 *  @param len          The length of x in bytes (max 56 bytes)
 *  @param ec_ctx       The EC context
 *  @return             0 for success, the appropriate error code otherwise
 *
 *  If x is NULL or len is 0, the point will be the point at infinity.
 */
EXPORT_SYM int curve448_new_point(Curve448Point **out,
                                  const uint8_t *x,
                                  size_t len,
                                  const Curve448Context *ec_ctx)
{
    int res;
    Curve448Point *ecp = NULL;
    const MontContext *mont_ctx;

    if (NULL == out || NULL == ec_ctx)
        return ERR_NULL;

    if (len > ec_ctx->mont_ctx->bytes) {
        return ERR_VALUE;
    }

    *out = ecp = (Curve448Point*)calloc(1, sizeof(Curve448Point));
    if (NULL == ecp)
        return ERR_MEMORY;

    ecp->ec_ctx = (Curve448Context*) ec_ctx;
    mont_ctx = ec_ctx->mont_ctx;

    if ((NULL == x) || (0 == len)) {
        res = mont_new_from_uint64(&ecp->x, 1, mont_ctx);
        if (res) goto cleanup;
        res = mont_new_from_uint64(&ecp->z, 0, mont_ctx);
        if (res) goto cleanup;
    } else {
        res = mont_new_from_bytes(&ecp->x, x, len, mont_ctx);
        if (res) goto cleanup;
        res = mont_new_from_uint64(&ecp->z, 1, mont_ctx);
        if (res) goto cleanup;
    }

    ecp->wp = new_workplace(mont_ctx);
    if (NULL == ecp->wp) {
        res = ERR_MEMORY;
        goto cleanup;
    }

    /* No need to verify if the point is on the Curve448 curve */
    return 0;

cleanup:
    free(ecp->x);
    free(ecp->z);
    free(ecp->wp);
    free(ecp);
    *out = NULL;
    return res;
}

EXPORT_SYM void curve448_free_point(Curve448Point *ecp)
{
    /* The EC context (ecp->ecp_ctx) is allocated once and shared by all
     * points on the same surve, so we will not free it here.
     */
    if (ecp) {
        free_workplace(ecp->wp);
        free(ecp->x);
        free(ecp->z);
        free(ecp);
    }
}

EXPORT_SYM int curve448_clone(Curve448Point **pecp2, const Curve448Point *ecp)
{
    int res = -1;
    Curve448Point *ecp2;
    MontContext *ctx;

    if (NULL == pecp2 || NULL == ecp)
        return ERR_NULL;
    ctx = ecp->ec_ctx->mont_ctx;

    *pecp2 = ecp2 = (Curve448Point*)calloc(1, sizeof(Curve448Point));
    if (NULL == ecp2)
        return ERR_MEMORY;

    ecp2->ec_ctx = ecp->ec_ctx;

    ecp2->wp = new_workplace(ctx);
    if (NULL == ecp2->wp) goto cleanup;

    res = mont_new_number(&ecp2->x, 1, ctx);
    if (res) goto cleanup;
    res = mont_copy(ecp2->x, ecp->x, ctx);
    if (res) goto cleanup;

    res = mont_new_number(&ecp2->z, 1, ctx);
    if (res) goto cleanup;
    res = mont_copy(ecp2->z, ecp->z, ctx);
    if (res) goto cleanup;

    return 0;

cleanup:
    free_workplace(ecp2->wp);
    free(ecp2->x);
    free(ecp2->z);
    free(ecp2);
    *pecp2 = NULL;
    return res;
}

EXPORT_SYM int curve448_get_x(uint8_t *xb, size_t modsize, const Curve448Point *p)
{
    MontContext *mont_ctx;

    if ((NULL == xb) || (NULL == p))
        return ERR_NULL;

    mont_ctx = p->ec_ctx->mont_ctx;

    if (modsize != 56)
        return ERR_MODULUS;

    if (mont_is_zero(p->z, mont_ctx))
        return ERR_EC_PAI;

    /** p->Z == 1 **/

    return mont_to_bytes(xb, modsize, p->x, mont_ctx);
}

EXPORT_SYM int curve448_scalar(Curve448Point *P, const uint8_t *scalar, size_t scalar_len, uint64_t seed)
{
    if ((NULL == P) || (NULL == scalar))
        return ERR_NULL;

    curve448_scalar_internal(P, scalar, scalar_len, P);
    return 0;
}

EXPORT_SYM int curve448_cmp(const Curve448Point *p1, const Curve448Point *p2)
{
    MontContext *ctx;
    WorkplaceCurve448 *wp;
    uint64_t *scratch;
    int res;

    if (NULL == p1 || NULL == p2)
        return ERR_NULL;

    if (p1->ec_ctx != p2->ec_ctx)
        return ERR_EC_CURVE;

    ctx = p1->ec_ctx->mont_ctx;
    wp = p1->wp;
    scratch = wp->scratch;

    mont_mult(wp->a, p1->x, p2->z, scratch, ctx);
    mont_mult(wp->b, p1->z, p2->x, scratch, ctx);
    res = mont_is_equal(wp->a, wp->b, ctx);

    return res ? 0 : ERR_VALUE;
}

#ifdef PROFILE
int main(void)
{
    uint8_t pubkey[56];
    uint8_t secret[56];
    unsigned i;
    Curve448Context *ec_ctx;
    Curve448Point *Pin = NULL;
    Curve448Point *Pout = NULL;
    int res;

    secret[0] = pubkey[0] = 0xAA;
    for (i=1; i<56; i++) {
        secret[i] = pubkey[i] = (uint8_t)((secret[i-1] << 1) | (secret[i-1] >> 7));
    }

    res = curve448_new_context(&ec_ctx);
    assert(res == 0);

    res = curve448_new_point(&Pin, pubkey, 56, ec_ctx);
    assert(res == 0);

    res = curve448_new_point(&Pout, NULL, 56, ec_ctx);
    assert(res == 0);

    for (i=0; i<10000; i++) {
        res = curve448_scalar_internal(Pout, secret, sizeof secret, Pin);
    }

    curve448_free_point(Pin);
    curve448_free_point(Pout);
    curve448_free_context(ec_ctx);
}
#endif
