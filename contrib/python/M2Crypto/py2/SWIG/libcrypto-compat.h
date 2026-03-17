#ifndef LIBCRYPTO_COMPAT_H
#define LIBCRYPTO_COMPAT_H

#if OPENSSL_VERSION_NUMBER < 0x10100000L

#include <openssl/rsa.h>
#include <openssl/dsa.h>
#include <openssl/ecdsa.h>
#include <openssl/dh.h>
#include <openssl/evp.h>
#include <openssl/x509.h>

static int RSA_set0_key(RSA *r, BIGNUM *n, BIGNUM *e, BIGNUM *d);
static int RSA_set0_factors(RSA *r, BIGNUM *p, BIGNUM *q);
static int RSA_set0_crt_params(RSA *r, BIGNUM *dmp1, BIGNUM *dmq1, BIGNUM *iqmp);
static void RSA_get0_key(const RSA *r, const BIGNUM **n, const BIGNUM **e, const BIGNUM **d);
static void RSA_get0_factors(const RSA *r, const BIGNUM **p, const BIGNUM **q);
static void RSA_get0_crt_params(const RSA *r, const BIGNUM **dmp1, const BIGNUM **dmq1, const BIGNUM **iqmp);

static void DSA_get0_pqg(const DSA *d, const BIGNUM **p, const BIGNUM **q, const BIGNUM **g);
static int DSA_set0_pqg(DSA *d, BIGNUM *p, BIGNUM *q, BIGNUM *g);
static void DSA_get0_key(const DSA *d, const BIGNUM **pub_key, const BIGNUM **priv_key);
static int DSA_set0_key(DSA *d, BIGNUM *pub_key, BIGNUM *priv_key);

static void DSA_SIG_get0(const DSA_SIG *sig, const BIGNUM **pr, const BIGNUM **ps);
static int DSA_SIG_set0(DSA_SIG *sig, BIGNUM *r, BIGNUM *s);

static void ECDSA_SIG_get0(const ECDSA_SIG *sig, const BIGNUM **pr, const BIGNUM **ps);
static int ECDSA_SIG_set0(ECDSA_SIG *sig, BIGNUM *r, BIGNUM *s);

static void DH_get0_pqg(const DH *dh, const BIGNUM **p, const BIGNUM **q, const BIGNUM **g);
static int DH_set0_pqg(DH *dh, BIGNUM *p, BIGNUM *q, BIGNUM *g);
static void DH_get0_key(const DH *dh, const BIGNUM **pub_key, const BIGNUM **priv_key);
static int DH_set0_key(DH *dh, BIGNUM *pub_key, BIGNUM *priv_key);
static int DH_set_length(DH *dh, long length);

static const unsigned char *EVP_CIPHER_CTX_iv(const EVP_CIPHER_CTX *ctx);
static unsigned char *EVP_CIPHER_CTX_iv_noconst(EVP_CIPHER_CTX *ctx);
static EVP_MD_CTX *EVP_MD_CTX_new(void);
static void EVP_MD_CTX_free(EVP_MD_CTX *ctx);
#define EVP_CIPHER_impl_ctx_size(e) e->ctx_size
#define EVP_CIPHER_CTX_get_cipher_data(ctx) ctx->cipher_data

//int RSA_size(const RSA* rsa);
static RSA_METHOD *RSA_meth_dup(const RSA_METHOD *meth);
static int RSA_meth_set1_name(RSA_METHOD *meth, const char *name);
#define RSA_meth_get_finish(meth) meth->finish
static int RSA_meth_set_priv_enc(RSA_METHOD *meth, int (*priv_enc) (int flen, const unsigned char *from, unsigned char *to, RSA *rsa, int padding));
static int RSA_meth_set_priv_dec(RSA_METHOD *meth, int (*priv_dec) (int flen, const unsigned char *from, unsigned char *to, RSA *rsa, int padding));
static int RSA_meth_set_finish(RSA_METHOD *meth, int (*finish) (RSA *rsa));
static void RSA_meth_free(RSA_METHOD *meth);

static int RSA_bits(const RSA *r);

static RSA *EVP_PKEY_get0_RSA(EVP_PKEY *pkey);

static int X509_NAME_get0_der(X509_NAME *nm, const unsigned char **pder,
                       size_t *pderlen);

#endif /* OPENSSL_VERSION_NUMBER */

#endif /* LIBCRYPTO_COMPAT_H */
