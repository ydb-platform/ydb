#pragma once

#include <openssl/bn.h>
#include <openssl/crypto.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct {
        BIGNUM* S;
    } TRwSignature;

    /*Rabin–Williams*/
    typedef struct TRwInternal TRwKey;

    typedef struct {
        TRwSignature* (*RwSign)(const unsigned char* dgst, const int dlen, TRwKey* rw);
        int (*RwVerify)(const unsigned char* dgst, int dgst_len, TRwSignature* sig, const TRwKey* rw);
        int (*RwApply)(BIGNUM* r, BIGNUM* x, BN_CTX* ctx, const TRwKey* rw);
    } TRwMethod;

    struct TRwInternal {
        /* first private multiplier */
        BIGNUM* P;
        /* second private multiplier */
        BIGNUM* Q;
        /* n = p*q - RW modulus */
        BIGNUM* N;
        /* precomputed 2^((3q-5)/8) mod q */
        BIGNUM* Twomq;
        /* precomputed 2^((9p-11)/8) mod p*/
        BIGNUM* Twomp;
        /* precomputed q^(p-2) == q^(-1) mod p */
        BIGNUM* Iqmp;
        /* (q+1) / 8 */
        BIGNUM* Dq;
        /* (p-3) / 8 */
        BIGNUM* Dp;
        /* functions for working with RW */
        const TRwMethod* Meth;
    };

    TRwSignature* RwSignatureNew(void);
    void RwSignatureFree(TRwSignature* a);

    /* RW signing functions */
    /* the function can put some tmp values to rw */
    int RwPssrSignHash(const unsigned char* from, unsigned char* to, TRwKey* rw, const EVP_MD* md);
    int RwPssrSignMsg(const int msgLen, const unsigned char* msg, unsigned char* to, TRwKey* rw, const EVP_MD* md);

    /* RW-PSS verification functions */
    int RwPssrVerifyHash(const unsigned char* from, const unsigned char* sig, const int sig_len, const TRwKey* rw, const EVP_MD* md);
    int RwPssrVerifyMsg(const int msgLen, const unsigned char* msg, const unsigned char* sig, const int sig_len, const TRwKey* rw, const EVP_MD* md);

    /* internal functions, use them only if you know what you're doing */
    int RwNoPaddingSign(int flen, const unsigned char* from, unsigned char* to, TRwKey* rw);
    int RwApply(const int flen, const unsigned char* from, unsigned char* to, const TRwKey* rw);

    const TRwMethod* RwDefaultMethods(void);

    TRwKey* RwNew(void);
    void RwFree(TRwKey* r);
    int RwSize(const TRwKey* rw);
    int RwModSize(const TRwKey* rw);

    TRwKey* RwPublicKeyDup(TRwKey* rw);
    TRwKey* RwPrivateKeyDup(TRwKey* rw);

    // NOLINTNEXTLINE(readability-identifier-naming)
    TRwKey* d2i_RWPublicKey(TRwKey** a, const unsigned char** pp, long length);
    // NOLINTNEXTLINE(readability-identifier-naming)
    TRwKey* d2i_RWPrivateKey(TRwKey** a, const unsigned char** pp, long length);

    int RwGenerateKey(TRwKey* a, int bits);
    // NOLINTNEXTLINE(readability-identifier-naming)
    int i2d_RWPublicKey(const TRwKey* a, unsigned char** pp);
    // NOLINTNEXTLINE(readability-identifier-naming)
    int i2d_RWPrivateKey(const TRwKey* a, unsigned char** pp);

    int RwPaddingAddPssr(const TRwKey* rw, unsigned char* EM, const unsigned char* mHash, const EVP_MD* Hash, int sLen);
    int RwVerifyPssr(const TRwKey* rw, const unsigned char* mHash, const EVP_MD* Hash, const unsigned char* EM, int sLen);

#ifdef __cplusplus
}
#endif
