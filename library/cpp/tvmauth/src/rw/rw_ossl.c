#include "rw.h"

#include <openssl/rand.h>

//#define RW_PRINT_DEBUG
//#define AVOID_IF
//#define FAULT_TOLERANCE_CHECK

#ifdef RW_PRINT_DEBUG
    #include <stdio.h>
#endif

static TRwSignature* RwDoSign(const unsigned char* dgst, int dlen, TRwKey* rw);
static int RwDoVerify(const unsigned char* dgst, int dgst_len, TRwSignature* sig, const TRwKey* rw);
static int RwDoApply(BIGNUM* r, BIGNUM* x, BN_CTX* ctx, const TRwKey* rw);

static TRwMethod rw_default_meth = {
    RwDoSign,
    RwDoVerify,
    RwDoApply};

const TRwMethod* RwDefaultMethods(void) {
    return &rw_default_meth;
}

#ifdef RW_PRINT_DEBUG

static void print_bn(char* name, BIGNUM* value) {
    char* str_repr;
    str_repr = BN_bn2dec(value);
    printf("Name: %s\n", name);
    printf("Value: %s\n", str_repr);
    OPENSSL_free(str_repr);
}

    #define DEBUG_PRINT_BN(s, x) \
        do {                     \
            print_bn((s), (x));  \
        } while (0);
    #define DEBUG_PRINT_RW(r)                        \
        do {                                         \
            DEBUG_PRINT_BN("rw->p", (r)->p);         \
            DEBUG_PRINT_BN("rw->q", (r)->q);         \
            DEBUG_PRINT_BN("rw->n", (r)->n);         \
            DEBUG_PRINT_BN("rw->iqmp", (r)->iqmp);   \
            DEBUG_PRINT_BN("rw->twomp", (r)->twomp); \
            DEBUG_PRINT_BN("rw->twomq", (r)->twomq); \
            DEBUG_PRINT_BN("rw->dp", (r)->dp);       \
            DEBUG_PRINT_BN("rw->dq", (r)->dq);       \
        } while (0);
    #define DEBUG_PRINTF(s, v) \
        do {                   \
            printf((s), (v));  \
        } while (0);
#else
    #define DEBUG_PRINT_BN(s, x)
    #define DEBUG_PRINT_RW(r)
    #define DEBUG_PRINTF(s, v)
#endif

/*
 * The algorithms was taken from
 * https://cr.yp.to/sigs/rwsota-20080131.pdf
 * Section 6 -> "Avoiding Jacobi symbols"
 * '^' means power
 * 1. Compute U = h ^ ((q+1) / 8) mod q
 * 2. If U ^ 4 - h mod q == 0, set e = 1 otherwise set e = -1
 * 3. Compute V = (eh) ^ ((p-3)/8) mod p
 * 4. If (V^4 * (eh)^2 - eh) mod p = 0; set f = 1; otherwise set f = 2
 * 5. Precompute 2^((3q-5) / 8) mod q; Compute W = f^((3*q - 5) / 8) * U mod q
 * 6. Precompute 2^((9p-11) / 8) mod p; Compute X = f^((9p-11) / 8) * V^3 * eh mod p
 * 7. Precompute q^(p-2) mod p; Compute Y = W + q(q^(p-2) * (X - W) mod p)
 * 8. Compute s = Y^2 mod pq
 * 9. Fault tolerance: if efs^2 mod pq != h start over
 */
static TRwSignature* RwDoSign(const unsigned char* dgst, int dlen, TRwKey* rw) {
    BIGNUM *m, *U, *V, *tmp, *m_q, *m_p, *tmp2;
    /* additional variables to avoid "if" statements */
    BIGNUM* tmp_mp;
    TRwSignature* ret = NULL;
    BN_CTX* ctx = NULL;
    int ok = 0, e = 0, f = 0;

#ifdef AVOID_IF
    /* additional variables to avoid "if" statements */
    BIGNUM *tmp_U, *tmp_V;
#endif

    if (!rw || !rw->P || !rw->Q || !rw->N || !rw->Iqmp || !rw->Dp || !rw->Dq || !rw->Twomp || !rw->Twomq)
        goto err;

    if ((ctx = BN_CTX_secure_new()) == NULL)
        goto err;
    BN_CTX_start(ctx);

    m = BN_CTX_get(ctx);
    U = BN_CTX_get(ctx);
    V = BN_CTX_get(ctx);
    tmp = BN_CTX_get(ctx);
    tmp2 = BN_CTX_get(ctx);
    m_q = BN_CTX_get(ctx);
    m_p = BN_CTX_get(ctx);
    tmp_mp = BN_CTX_get(ctx);

#ifdef AVOID_IF
    tmp_U = BN_CTX_get(ctx);
    tmp_V = BN_CTX_get(ctx);
#endif

    DEBUG_PRINT_RW(rw)

    /* if (!BN_set_word(four, 4)) goto err; */

    if (!BN_bin2bn(dgst, dlen, m))
        goto err;
    if (BN_ucmp(m, rw->N) >= 0)
        goto err;

    /* check if m % 16 == 12 */
    if (BN_mod_word(m, 16) != 12)
        goto err;
    DEBUG_PRINT_BN("m", m)

    /* TODO: optimization to avoid memory allocation? */
    if ((ret = RwSignatureNew()) == NULL)
        goto err;
    /* memory allocation */
    if ((ret->S = BN_new()) == NULL)
        goto err;

    /* m_q = m mod q */
    if (!BN_nnmod(m_q, m, rw->Q, ctx))
        goto err;
    /* m_p = m mod p */
    if (!BN_nnmod(m_p, m, rw->P, ctx))
        goto err;

    DEBUG_PRINT_BN("m_p", m_p)
    DEBUG_PRINT_BN("m_q", m_q)

    /* U = h ** ((q+1)/8) mod q */
    if (!BN_mod_exp(U, m_q, rw->Dq, rw->Q, ctx))
        goto err;
    DEBUG_PRINT_BN("U", U)

    /* tmp = U^4 - h mod q */
    if (!BN_mod_sqr(tmp, U, rw->Q, ctx))
        goto err;
    if (!BN_mod_sqr(tmp, tmp, rw->Q, ctx))
        goto err;
    DEBUG_PRINT_BN("U**4 mod q", tmp)

    /* e = 1 if tmp == 0 else -1 */
    e = 2 * (BN_ucmp(tmp, m_q) == 0) - 1;
    DEBUG_PRINTF("e == %i\n", e)

    /*
     to avoid "if" branch
     if e == -1: m_p = tmp_mp
     if e ==  1: m_p = m_p
     */
    if (!BN_sub(tmp_mp, rw->P, m_p))
        goto err;
    m_p = (BIGNUM*)((1 - ((1 + e) >> 1)) * (BN_ULONG)tmp_mp + ((1 + e) >> 1) * (BN_ULONG)m_p);
    DEBUG_PRINT_BN("eh mod p", m_p)

    /* V = (eh) ** ((p-3)/8) */
    if (!BN_mod_exp(V, m_p, rw->Dp, rw->P, ctx))
        goto err;
    DEBUG_PRINT_BN("V == ((eh) ** ((p-3)/8))", V)

    /* (eh) ** 2 */
    if (!BN_mod_sqr(tmp2, m_p, rw->P, ctx))
        goto err;
    DEBUG_PRINT_BN("(eh)**2", tmp2)

    /* V ** 4 */
    if (!BN_mod_sqr(tmp, V, rw->P, ctx))
        goto err;
    if (!BN_mod_sqr(tmp, tmp, rw->P, ctx))
        goto err;
    DEBUG_PRINT_BN("V**4", tmp)

    /* V**4 * (eh)**2 */
    if (!BN_mod_mul(tmp, tmp, tmp2, rw->P, ctx))
        goto err;
    DEBUG_PRINT_BN("tmp = (V**4 * (eh)**2) mod p", tmp)

    /* tmp = tmp - eh mod p */
    if (!BN_mod_sub(tmp, tmp, m_p, rw->P, ctx))
        goto err;

    /* f = 1 if zero else 2 */
    f = 2 - BN_is_zero(tmp);
    /* f = 2 - (constant_time_is_zero(BN_ucmp(tmp, m_p)) & 1); */
    DEBUG_PRINTF("f == %i\n", f)

#ifdef AVOID_IF
    if (!BN_mod_mul(tmp_U, U, rw->twomq, rw->q, ctx))
        goto err;

    /*
     to avoid "if" branch we use tiny additional computation
     */
    U = (BIGNUM*)((2 - f) * (BN_ULONG)U + (1 - (2 - f)) * (BN_ULONG)tmp_U);
#else

    if (f == 2) {
        if (!BN_mod_mul(U, U, rw->Twomq, rw->Q, ctx))
            goto err;
    }

#endif

    DEBUG_PRINT_BN("W", U)

    /* V ** 3 */
    if (!BN_mod_sqr(tmp, V, rw->P, ctx))
        goto err;
    if (!BN_mod_mul(V, V, tmp, rw->P, ctx))
        goto err;
    DEBUG_PRINT_BN("V**3", V)

    /* *(eh) */
    if (!BN_mod_mul(V, V, m_p, rw->P, ctx))
        goto err;
    DEBUG_PRINT_BN("V**3 * (eh) mod p", V)

#ifdef AVOID_IF

    /* to avoid "if" statement we use simple computation */
    if (!BN_mod_mul(tmp_V, V, rw->twomp, rw->p, ctx))
        goto err;
    V = (BIGNUM*)((2 - f) * (BN_ULONG)V + (1 - (2 - f)) * (BN_ULONG)tmp_V);

#else

    if (f == 2) {
        if (!BN_mod_mul(V, V, rw->Twomp, rw->P, ctx))
            goto err;
    }

#endif

    DEBUG_PRINT_BN("X", V)

    /* W = U, X = V */
    if (!BN_mod_sub(V, V, U, rw->P, ctx))
        goto err;
    DEBUG_PRINT_BN("X - W mod p", V)

    if (!BN_mod_mul(V, V, rw->Iqmp, rw->P, ctx))
        goto err;
    DEBUG_PRINT_BN("q**(p-2) * (X-W) mod p", V)

    if (!BN_mul(V, V, rw->Q, ctx))
        goto err;
    DEBUG_PRINT_BN("q * prev mod p", V)

    if (!BN_mod_add(V, U, V, rw->N, ctx))
        goto err;
    DEBUG_PRINT_BN("Y", V)

    /* now V = Y */
    if (!BN_mod_sqr(V, V, rw->N, ctx))
        goto err;
    DEBUG_PRINT_BN("s", V)

#ifdef FAULT_TOLERANCE_CHECK

    /* now V = s - principal square root */
    /* fault tolerance check */
    if (!BN_mod_sqr(tmp, V, rw->n, ctx))
        goto err;
    DEBUG_PRINT_BN("s**2", tmp)

    if (!BN_mul_word(tmp, f))
        goto err;
    DEBUG_PRINT_BN("f * s**2", tmp)

    if (!BN_nnmod(tmp, tmp, rw->n, ctx))
        goto err;
    DEBUG_PRINT_BN("s**2 * f mod n", tmp)

    /* to avoid "if" statement */
    if (!BN_sub(tmp2, rw->n, tmp))
        goto err;
    tmp = (BIGNUM*)(((1 + e) >> 1) * (BN_ULONG)tmp + (1 - ((1 + e) >> 1)) * (BN_ULONG)tmp2);
    DEBUG_PRINT_BN("ef(s**2)", tmp)
    DEBUG_PRINT_BN("(tmp == original m)", tmp)

    if (BN_ucmp(tmp, m) != 0)
        goto err;

#endif

    /* making the "principal square root" to be "|principal| square root" */
    if (!BN_sub(tmp, rw->N, V))
        goto err;

    /* if tmp = MIN(V, rw->n - V) */
    tmp = BN_ucmp(tmp, V) >= 0 ? V : tmp;

    if (!BN_copy(ret->S, tmp))
        goto err;

    ok = 1;

err:
    if (ctx != NULL) {
        BN_CTX_end(ctx);
        BN_CTX_free(ctx);
    }
    if (!ok) {
        RwSignatureFree(ret);
        ret = NULL;
    }

    return ret;
}

static int RwDoVerify(const unsigned char* dgst, int dgst_len, TRwSignature* sig, const TRwKey* rw) {
    BIGNUM *m = NULL, *x = NULL, *t1 = NULL, *t2 = NULL, *t1d = NULL, *t2d = NULL;
    BN_CTX* ctx = NULL;
    BN_ULONG rest1 = 0, rest2 = 0;
    int retval = 0;

    if (!rw || !rw->N || !sig || !sig->S)
        goto err;

    if ((ctx = BN_CTX_secure_new()) == NULL)
        goto err;
    BN_CTX_start(ctx);

    m = BN_CTX_get(ctx);
    t1 = BN_CTX_get(ctx);
    t2 = BN_CTX_get(ctx);
    t1d = BN_CTX_get(ctx);
    t2d = BN_CTX_get(ctx);

    if (!BN_bin2bn(dgst, dgst_len, m))
        goto err;
    /* dgst too big */
    if (!BN_copy(t1, rw->N))
        goto err;
    if (!BN_sub_word(t1, 1))
        goto err;
    if (!BN_rshift(t1, t1, 1))
        goto err;

    /* check m and rw->n relation */
    if (BN_ucmp(m, rw->N) >= 0)
        goto err;
    rest1 = BN_mod_word(m, 16);
    if (rest1 != 12)
        goto err;

    if (BN_ucmp(t1, sig->S) < 0)
        goto err;
    if (BN_is_negative(sig->S))
        goto err;

    if (!BN_mod_sqr(t1, sig->S, rw->N, ctx))
        goto err;
    if (!BN_sub(t2, rw->N, t1))
        goto err;
    if (!BN_lshift1(t1d, t1))
        goto err;
    if (!BN_lshift1(t2d, t2))
        goto err;

    rest1 = BN_mod_word(t1, 16);
    rest2 = BN_mod_word(t2, 16);

    /* mod 16 */
    if (rest1 == 12) {
        x = t1;
    }
    /* mod 8 */
    else if ((rest1 & 0x07) == 6) {
        x = t1d;
    }
    /* mod 16 */
    else if (rest2 == 12) {
        x = t2;
    }
    /* mod 8 */
    else if ((rest2 & 0x07) == 6) {
        x = t2d;
    } else
        goto err;

    DEBUG_PRINT_BN("m", m)
    DEBUG_PRINT_BN("x", x)

    /* check signature value */
    retval = BN_ucmp(m, x) == 0;

err:
    if (ctx != NULL) {
        BN_CTX_end(ctx);
        BN_CTX_free(ctx);
    }
    return retval;
}

static int RwDoApply(BIGNUM* r, BIGNUM* x, BN_CTX* ctx, const TRwKey* rw) {
    BIGNUM *t1 = NULL, *t2 = NULL, *t1d = NULL, *t2d = NULL, *rs = NULL;
    BN_ULONG rest1 = 0, rest2 = 0;
    int retval = 0;

    if (!rw || !rw->N || !x || !ctx || !r)
        goto err;

    DEBUG_PRINT_BN("Signature = x = ", x)
    DEBUG_PRINT_BN("n", rw->n)

    BN_CTX_start(ctx);

    t1 = BN_CTX_get(ctx);
    t2 = BN_CTX_get(ctx);
    t1d = BN_CTX_get(ctx);
    t2d = BN_CTX_get(ctx);

    if (!BN_copy(t1, rw->N))
        goto err;
    if (!BN_sub_word(t1, 1))
        goto err;
    if (!BN_rshift(t1, t1, 1))
        goto err;

    /* check m and rw->n relation */
    if (BN_ucmp(x, rw->N) >= 0)
        goto err;

    if (BN_ucmp(t1, x) < 0)
        goto err;
    if (BN_is_negative(x))
        goto err;

    if (!BN_mod_sqr(t1, x, rw->N, ctx))
        goto err;
    DEBUG_PRINT_BN("x**2 mod n", t1)

    if (!BN_sub(t2, rw->N, t1))
        goto err;
    DEBUG_PRINT_BN("n - x**2", t2)

    if (!BN_lshift1(t1d, t1))
        goto err;
    if (!BN_lshift1(t2d, t2))
        goto err;

    rest1 = BN_mod_word(t1, 16);
    rest2 = BN_mod_word(t2, 16);

    /* mod 16 */
    if (rest1 == 12) {
        rs = t1;
    }
    /* mod 8 */
    else if ((rest1 & 0x07) == 6) {
        rs = t1d;
    }
    /* mod 16 */
    else if (rest2 == 12) {
        rs = t2;
    }
    /* mod 8 */
    else if ((rest2 & 0x07) == 6) {
        rs = t2d;
    } else
        goto err;

    DEBUG_PRINT_BN("Squaring and shifting result (rs)", rs)
    retval = BN_copy(r, rs) != NULL;

err:
    BN_CTX_end(ctx);
    return retval;
}
