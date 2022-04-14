#include "rw.h"

#include <contrib/libs/openssl/include/openssl/rand.h>

int RwGenerateKey(TRwKey* rw, int bits) {
    int ok = 0;

    BN_CTX* ctx = NULL;
    BIGNUM *rem3 = NULL, *rem7 = NULL, *mod8 = NULL, *rem5 = NULL;
    BIGNUM *nmod = NULL, *twomqexp = NULL, *twompexp = NULL, *two = NULL;

    int bitsp = (bits + 1) / 2;
    int bitsq = bits - bitsp;

    /* make sure that all components are not null */
    if ((ctx = BN_CTX_secure_new()) == NULL)
        goto err;
    if (!rw)
        goto err;
    if (!rw->N && ((rw->N = BN_new()) == NULL))
        goto err;
    if (!rw->P && ((rw->P = BN_new()) == NULL))
        goto err;
    if (!rw->Q && ((rw->Q = BN_new()) == NULL))
        goto err;
    if (!rw->Iqmp && ((rw->Iqmp = BN_new()) == NULL))
        goto err;
    if (!rw->Twomq && ((rw->Twomq = BN_new()) == NULL))
        goto err;
    if (!rw->Twomp && ((rw->Twomp = BN_new()) == NULL))
        goto err;
    if (!rw->Dq && ((rw->Dq = BN_new()) == NULL))
        goto err;
    if (!rw->Dp && ((rw->Dp = BN_new()) == NULL))
        goto err;

    BN_CTX_start(ctx);

    rem3 = BN_CTX_get(ctx);
    rem7 = BN_CTX_get(ctx);
    rem5 = BN_CTX_get(ctx);
    mod8 = BN_CTX_get(ctx);
    nmod = BN_CTX_get(ctx);
    twomqexp = BN_CTX_get(ctx);
    twompexp = BN_CTX_get(ctx);
    two = BN_CTX_get(ctx);

    if (!BN_set_word(mod8, 8))
        goto err;
    if (!BN_set_word(rem3, 3))
        goto err;
    if (!BN_set_word(rem7, 7))
        goto err;
    if (!BN_set_word(rem5, 5))
        goto err;
    if (!BN_set_word(two, 2))
        goto err;

    /* generate p */
    /* add == 8 */
    /* rem == 3 */
    /* safe == 0 as we don't need (p-1)/2 to be also prime */
    if (!BN_generate_prime_ex(rw->P, bitsp, 0, mod8, rem3, NULL))
        goto err;

    /* generate q */
    /* add == 8 */
    /* rem == 7 */
    /* safe == 0 */
    if (!BN_generate_prime_ex(rw->Q, bitsq, 0, mod8, rem7, NULL))
        goto err;

    /* n == p*q */
    if (!BN_mul(rw->N, rw->P, rw->Q, ctx))
        goto err;

    /* n == 5 mod 8 ? */
    if (!BN_nnmod(nmod, rw->N, mod8, ctx))
        goto err;
    if (BN_ucmp(rem5, nmod) != 0)
        goto err;

    /* q^(-1) mod p */
    if (!BN_mod_inverse(rw->Iqmp, rw->Q, rw->P, ctx))
        goto err;

    /* twomqexp = (3q-5)/8 */
    if (!BN_copy(twomqexp, rw->Q))
        goto err;
    if (!BN_mul_word(twomqexp, 3))
        goto err;
    if (!BN_sub_word(twomqexp, 5))
        goto err;
    if (!BN_rshift(twomqexp, twomqexp, 3))
        goto err;
    if (!BN_mod_exp(rw->Twomq, two, twomqexp, rw->Q, ctx))
        goto err;

    /* twompexp = (9p-11)/8 */
    if (!BN_copy(twompexp, rw->P))
        goto err;
    if (!BN_mul_word(twompexp, 9))
        goto err;
    if (!BN_sub_word(twompexp, 11))
        goto err;
    if (!BN_rshift(twompexp, twompexp, 3))
        goto err;
    if (!BN_mod_exp(rw->Twomp, two, twompexp, rw->P, ctx))
        goto err;

    /* dp = (p-3) / 8 */
    if (!BN_copy(rw->Dp, rw->P))
        goto err;
    if (!BN_sub_word(rw->Dp, 3))
        goto err;
    if (!BN_rshift(rw->Dp, rw->Dp, 3))
        goto err;

    /* dq = (q+1) / 8 */
    if (!BN_copy(rw->Dq, rw->Q))
        goto err;
    if (!BN_add_word(rw->Dq, 1))
        goto err;
    if (!BN_rshift(rw->Dq, rw->Dq, 3))
        goto err;

    ok = 1;

err:
    if (ctx != NULL) {
        BN_CTX_end(ctx);
        BN_CTX_free(ctx);
    }
    return ok;
}
