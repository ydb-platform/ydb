/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/*
Copyright (c) 1999 Ng Pheng Siong. All rights reserved.

Portions Copyright (c) 2004-2007 Open Source Applications Foundation.
Author: Heikki Toivonen

Copyright (c) 2009-2010 Heikki Toivonen. All rights reserved.

*/

%include <openssl/opensslconf.h>

%{
#include <assert.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rsa.h>
#include <openssl/ec.h>
#include <openssl/opensslv.h>

#if OPENSSL_VERSION_NUMBER < 0x10100000L

HMAC_CTX *HMAC_CTX_new(void) {
    HMAC_CTX *ret = PyMem_Malloc(sizeof(HMAC_CTX));
    HMAC_CTX_init(ret);
    return ret;
}
#define HMAC_CTX_reset(ctx) HMAC_CTX_init(ctx)
#define HMAC_CTX_free(ctx)          \
    do  {                           \
        HMAC_CTX_cleanup(ctx);      \
        PyMem_Free((void *)ctx);    \
    } while(0)

#define EVP_CIPHER_CTX_reset(ctx) EVP_CIPHER_CTX_init(ctx)
#endif
%}

/*
from openssl/crypto/include/internal/evp_int.h struct evp_md_st
typedef struct evp_md_st EVP_MD;
from openssl/crypto/evp/evp_locl.h evp_md_ctx_st
typedef struct evp_md_ctx_st EVP_MD_CTX;
*/

%apply Pointer NONNULL { EVP_MD_CTX * };
%apply Pointer NONNULL { EVP_MD * };
%apply Pointer NONNULL { EVP_PKEY * };
%apply Pointer NONNULL { HMAC_CTX * };
%apply Pointer NONNULL { EVP_CIPHER_CTX * };
%apply Pointer NONNULL { EVP_CIPHER * };
%apply Pointer NONNULL { RSA * };
%apply Pointer NONNULL { EC_KEY * };

%rename(md5) EVP_md5;
extern const EVP_MD *EVP_md5(void);
%rename(sha1) EVP_sha1;
extern const EVP_MD *EVP_sha1(void);
%rename(ripemd160) EVP_ripemd160;
extern const EVP_MD *EVP_ripemd160(void);

#if OPENSSL_VERSION_NUMBER >= 0x0090800fL
%rename(sha224) EVP_sha224;
extern const EVP_MD *EVP_sha224(void);
%rename(sha256) EVP_sha256;
extern const EVP_MD *EVP_sha256(void);
%rename(sha384) EVP_sha384;
extern const EVP_MD *EVP_sha384(void);
%rename(sha512) EVP_sha512;
extern const EVP_MD *EVP_sha512(void);
#endif

%rename(digest_init) EVP_DigestInit;
extern int EVP_DigestInit(EVP_MD_CTX *, const EVP_MD *);

%rename(des_ecb) EVP_des_ecb;
extern const EVP_CIPHER *EVP_des_ecb(void);
%rename(des_ede_ecb) EVP_des_ede;
extern const EVP_CIPHER *EVP_des_ede(void);
%rename(des_ede3_ecb) EVP_des_ede3;
extern const EVP_CIPHER *EVP_des_ede3(void);
%rename(des_cbc) EVP_des_cbc;
extern const EVP_CIPHER *EVP_des_cbc(void);
%rename(des_ede_cbc) EVP_des_ede_cbc;
extern const EVP_CIPHER *EVP_des_ede_cbc(void);
%rename(des_ede3_cbc) EVP_des_ede3_cbc;
extern const EVP_CIPHER *EVP_des_ede3_cbc(void);
%rename(des_cfb) EVP_des_cfb;
extern const EVP_CIPHER *EVP_des_cfb(void);
%rename(des_ede_cfb) EVP_des_ede_cfb;
extern const EVP_CIPHER *EVP_des_ede_cfb(void);
%rename(des_ede3_cfb) EVP_des_ede3_cfb;
extern const EVP_CIPHER *EVP_des_ede3_cfb(void);
%rename(des_ofb) EVP_des_ofb;
extern const EVP_CIPHER *EVP_des_ofb(void);
%rename(des_ede_ofb) EVP_des_ede_ofb;
extern const EVP_CIPHER *EVP_des_ede_ofb(void);
%rename(des_ede3_ofb) EVP_des_ede3_ofb;
extern const EVP_CIPHER *EVP_des_ede3_ofb(void);
%rename(bf_ecb) EVP_bf_ecb;
extern const EVP_CIPHER *EVP_bf_ecb(void);
%rename(bf_cbc) EVP_bf_cbc;
extern const EVP_CIPHER *EVP_bf_cbc(void);
%rename(bf_cfb) EVP_bf_cfb;
extern const EVP_CIPHER *EVP_bf_cfb(void);
%rename(bf_ofb) EVP_bf_ofb;
extern const EVP_CIPHER *EVP_bf_ofb(void);
/*
%rename(idea_ecb) extern const EVP_CIPHER *EVP_idea_ecb(void);
%rename(idea_cbc) extern const EVP_CIPHER *EVP_idea_cbc(void);
%rename(idea_cfb) extern const EVP_CIPHER *EVP_idea_cfb(void);
%rename(idea_ofb) extern const EVP_CIPHER *EVP_idea_ofb(void);
*/
%rename(cast5_ecb) EVP_cast5_ecb;
extern const EVP_CIPHER *EVP_cast5_ecb(void);
%rename(cast5_cbc) EVP_cast5_cbc;
extern const EVP_CIPHER *EVP_cast5_cbc(void);
%rename(cast5_cfb) EVP_cast5_cfb;
extern const EVP_CIPHER *EVP_cast5_cfb(void);
%rename(cast5_ofb) EVP_cast5_ofb;
extern const EVP_CIPHER *EVP_cast5_ofb(void);
/*
%rename(rc5_ecb) extern const EVP_CIPHER *EVP_rc5_32_12_16_ecb(void);
%rename(rc5_cbc) extern const EVP_CIPHER *EVP_rc5_32_12_16_cbc(void);
%rename(rc5_cfb) extern const EVP_CIPHER *EVP_rc5_32_12_16_cfb(void);
%rename(rc5_ofb) extern const EVP_CIPHER *EVP_rc5_32_12_16_ofb(void);
*/
#if !defined(OPENSSL_NO_RC4)
%rename(rc4) EVP_rc4;
extern const EVP_CIPHER *EVP_rc4(void);
#endif
#if !defined(OPENSSL_NO_RC2)
%rename(rc2_40_cbc) EVP_rc2_40_cbc;
extern const EVP_CIPHER *EVP_rc2_40_cbc(void);
#endif
%rename(aes_128_ecb) EVP_aes_128_ecb;
extern const EVP_CIPHER *EVP_aes_128_ecb(void);
%rename(aes_128_cbc) EVP_aes_128_cbc;
extern const EVP_CIPHER *EVP_aes_128_cbc(void);
%rename(aes_128_cfb) EVP_aes_128_cfb;
extern const EVP_CIPHER *EVP_aes_128_cfb(void);
%rename(aes_128_ofb) EVP_aes_128_ofb;
extern const EVP_CIPHER *EVP_aes_128_ofb(void);
%rename(aes_128_ctr) EVP_aes_128_ctr;
extern const EVP_CIPHER *EVP_aes_128_ctr(void);
%rename(aes_192_ecb) EVP_aes_192_ecb;
extern const EVP_CIPHER *EVP_aes_192_ecb(void);
%rename(aes_192_cbc) EVP_aes_192_cbc;
extern const EVP_CIPHER *EVP_aes_192_cbc(void);
%rename(aes_192_cfb) EVP_aes_192_cfb;
extern const EVP_CIPHER *EVP_aes_192_cfb(void);
%rename(aes_192_ofb) EVP_aes_192_ofb;
extern const EVP_CIPHER *EVP_aes_192_ofb(void);
%rename(aes_192_ctr) EVP_aes_192_ctr;
extern const EVP_CIPHER *EVP_aes_192_ctr(void);
%rename(aes_256_ecb) EVP_aes_256_ecb;
extern const EVP_CIPHER *EVP_aes_256_ecb(void);
%rename(aes_256_cbc) EVP_aes_256_cbc;
extern const EVP_CIPHER *EVP_aes_256_cbc(void);
%rename(aes_256_cfb) EVP_aes_256_cfb;
extern const EVP_CIPHER *EVP_aes_256_cfb(void);
%rename(aes_256_ofb) EVP_aes_256_ofb;
extern const EVP_CIPHER *EVP_aes_256_ofb(void);
%rename(aes_256_ctr) EVP_aes_256_ctr;
extern EVP_CIPHER const *EVP_aes_256_ctr(void);

%rename(cipher_set_padding) EVP_CIPHER_CTX_set_padding;
extern int EVP_CIPHER_CTX_set_padding(EVP_CIPHER_CTX *, int);


%rename(cipher_set_padding) EVP_CIPHER_CTX_set_padding;
extern int EVP_CIPHER_CTX_set_padding(EVP_CIPHER_CTX *x, int padding);
%rename(pkey_free) EVP_PKEY_free;
extern void EVP_PKEY_free(EVP_PKEY *);
%rename(pkey_assign) EVP_PKEY_assign;
extern int EVP_PKEY_assign(EVP_PKEY *, int, char *);
#if OPENSSL_VERSION_NUMBER >= 0x0090800fL && !defined(OPENSSL_NO_EC)
%rename(pkey_assign_ec) EVP_PKEY_assign_EC_KEY;
extern int EVP_PKEY_assign_EC_KEY(EVP_PKEY *, EC_KEY *);
%rename(pkey_set1_ec) EVP_PKEY_set1_EC_KEY;
extern int EVP_PKEY_set1_EC_KEY(EVP_PKEY *, EC_KEY *);
#endif
%rename(pkey_set1_rsa) EVP_PKEY_set1_RSA;
extern int EVP_PKEY_set1_RSA(EVP_PKEY *, RSA *);
%rename(sign_init) EVP_SignInit;
extern int EVP_SignInit(EVP_MD_CTX *, const EVP_MD *);
%rename(verify_init) EVP_VerifyInit;
extern int EVP_VerifyInit(EVP_MD_CTX *, const EVP_MD *);
%rename(digest_sign_init) EVP_DigestSignInit;
extern int EVP_DigestSignInit(EVP_MD_CTX *, EVP_PKEY_CTX **, const EVP_MD *, ENGINE *, EVP_PKEY *);
%rename(digest_verify_init) EVP_DigestVerifyInit;
extern int EVP_DigestVerifyInit(EVP_MD_CTX *, EVP_PKEY_CTX **, const EVP_MD *, ENGINE *, EVP_PKEY *);
%rename(pkey_size) EVP_PKEY_size;
extern int EVP_PKEY_size(EVP_PKEY *);

%warnfilter(454) _evp_err;
%inline %{
#define PKCS5_SALT_LEN  8

static PyObject *_evp_err;

void evp_init(PyObject *evp_err) {
    Py_INCREF(evp_err);
    _evp_err = evp_err;
}
%}

%typemap(out) RSA * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        $result = NULL;
    }
}
%inline %{
RSA *pkey_get1_rsa(EVP_PKEY *pkey) {
    RSA *ret = NULL;

    if ((ret = EVP_PKEY_get1_RSA(pkey)) == NULL) {
        /* _evp_err now inherits from PyExc_ValueError, so we should
         * keep API intact.
         */
        PyErr_Format(_evp_err, "Invalid key in function %s.", __FUNCTION__);
    }

    return ret;
}
%}
%typemap(out) RSA * ;

%typemap(out) EC_KEY * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        $result = NULL;
    }
}
%inline %{
EC_KEY *pkey_get1_ec(EVP_PKEY *pkey) {
    EC_KEY *ret = NULL;

    if ((ret = EVP_PKEY_get1_EC_KEY(pkey)) == NULL) {
        /* _evp_err now inherits from PyExc_ValueError, so we should
         * keep API intact.
         */
        PyErr_Format(_evp_err, "Invalid key in function %s.", __FUNCTION__);
    }

    return ret;
}
%}
%typemap(out) EC_KEY * ;

%inline %{
PyObject *pkcs5_pbkdf2_hmac_sha1(PyObject *pass,
                                 PyObject *salt,
                                 int iter,
                                 int keylen) {
    unsigned char *key;
    const void *saltbuf;
    const void *passbuf;
    PyObject *ret;
    int passlen = 0, saltlen = 0;

    if (m2_PyObject_AsReadBufferInt(pass, &passbuf, &passlen) == -1)
        return NULL;
    if (m2_PyObject_AsReadBufferInt(salt, &saltbuf, &saltlen) == -1)
        return NULL;

    key = PyMem_Malloc(keylen);
    if (key == NULL)
	return PyErr_NoMemory();
    PKCS5_PBKDF2_HMAC_SHA1((const char *)passbuf, passlen, (const unsigned char *)saltbuf, saltlen, iter,
                           keylen, key);
    ret = PyBytes_FromStringAndSize((char*)key, keylen);
    OPENSSL_cleanse(key, keylen);
    PyMem_Free(key);
    return ret;
}

EVP_MD_CTX *md_ctx_new(void) {
    EVP_MD_CTX *ctx;

    if (!(ctx = EVP_MD_CTX_create())) {
        PyErr_SetString(PyExc_MemoryError, "md_ctx_new");
        return NULL;
    }
    return ctx;
}

void md_ctx_free(EVP_MD_CTX *ctx) {
    EVP_MD_CTX_destroy(ctx);
}

int digest_update(EVP_MD_CTX *ctx, PyObject *blob) {
    const void *buf = NULL;
    Py_ssize_t len;

    if (m2_PyObject_AsReadBuffer(blob, &buf, &len) == -1)
        return -1;

    return EVP_DigestUpdate(ctx, buf, (size_t)len);
}

PyObject *digest_final(EVP_MD_CTX *ctx) {
    void *blob;
    int blen;
    PyObject *ret;

    if (!(blob = PyMem_Malloc(EVP_MD_CTX_size(ctx)))) {
        PyErr_SetString(PyExc_MemoryError, "digest_final");
        return NULL;
    }
    if (!EVP_DigestFinal(ctx, blob, (unsigned int *)&blen)) {
        PyMem_Free(blob);
        m2_PyErr_Msg(_evp_err);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize(blob, blen);

    PyMem_Free(blob);
    return ret;
}

HMAC_CTX *hmac_ctx_new(void) {
    HMAC_CTX *ctx;

    if (!(ctx = HMAC_CTX_new())) {
        PyErr_SetString(PyExc_MemoryError, "hmac_ctx_new");
        return NULL;
    }
    return ctx;
}

void hmac_ctx_free(HMAC_CTX *ctx) {
    HMAC_CTX_free(ctx);
}

PyObject *hmac_init(HMAC_CTX *ctx, PyObject *key, const EVP_MD *md) {
    const void *kbuf;
    int klen = 0;

    if (m2_PyObject_AsReadBufferInt(key, &kbuf, &klen) == -1)
        return NULL;

    if (!HMAC_Init_ex(ctx, kbuf, klen, md, NULL)) {
        PyErr_SetString(_evp_err, "HMAC_Init failed");
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject *hmac_update(HMAC_CTX *ctx, PyObject *blob) {
    const void *buf = NULL;
    Py_ssize_t len;

    if (m2_PyObject_AsReadBuffer(blob, &buf, &len) == -1)
        return NULL;

    if (!HMAC_Update(ctx, (const unsigned char *)buf, (size_t)len)) {
        PyErr_SetString(_evp_err, "HMAC_Update failed");
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject *hmac_final(HMAC_CTX *ctx) {
    void *blob;
    int blen;
    PyObject *ret;

    if (!(blob = PyMem_Malloc(HMAC_size(ctx)))) {
        PyErr_SetString(PyExc_MemoryError, "hmac_final");
        return NULL;
    }

    if (!HMAC_Final(ctx, blob, (unsigned int *)&blen)) {
        PyErr_SetString(_evp_err, "HMAC_Final failed");
        return NULL;
    }

    ret = PyBytes_FromStringAndSize(blob, blen);

    PyMem_Free(blob);
    return ret;
}

PyObject *hmac(PyObject *key, PyObject *data, const EVP_MD *md) {
    const void *kbuf, *dbuf;
    void *blob;
    int klen = 0;
    unsigned int blen;
    Py_ssize_t dlen;
    PyObject *ret;

    if ((m2_PyObject_AsReadBufferInt(key, &kbuf, &klen) == -1)
        || (m2_PyObject_AsReadBuffer(data, &dbuf, &dlen) == -1))
        return NULL;

    if (!(blob = PyMem_Malloc(EVP_MAX_MD_SIZE))) {
        PyErr_SetString(PyExc_MemoryError, "hmac");
        return NULL;
    }
    HMAC(md, kbuf, klen, (const unsigned char *)dbuf, (size_t)dlen, (unsigned char *)blob, &blen);
    blob = PyMem_Realloc(blob, blen);

    ret = PyBytes_FromStringAndSize(blob, blen);

    PyMem_Free(blob);
    return ret;
}

EVP_CIPHER_CTX *cipher_ctx_new(void) {
    EVP_CIPHER_CTX *ctx;

    if (!(ctx = EVP_CIPHER_CTX_new())) {
        PyErr_SetString(PyExc_MemoryError, "cipher_ctx_new");
        return NULL;
    }
    EVP_CIPHER_CTX_reset(ctx);
    return ctx;
}

void cipher_ctx_free(EVP_CIPHER_CTX *ctx) {
    EVP_CIPHER_CTX_free(ctx);
}

PyObject *bytes_to_key(const EVP_CIPHER *cipher, EVP_MD *md,
                        PyObject *data, PyObject *salt,
                        PyObject *iv, /* Not used */
                        int iter) {
    unsigned char key[EVP_MAX_KEY_LENGTH];
    const void *dbuf, *sbuf;
    int dlen = 0, klen;
    Py_ssize_t slen;
    PyObject *ret;

    if ((m2_PyObject_AsReadBufferInt(data, &dbuf, &dlen) == -1)
        || (m2_PyObject_AsReadBuffer(salt, &sbuf, &slen) == -1))
        return NULL;

    assert((slen == 8) || (slen == 0));
    klen = EVP_BytesToKey(cipher, md, (unsigned char *)sbuf,
        (unsigned char *)dbuf, dlen, iter,
        key, NULL); /* Since we are not returning IV no need to derive it */

    ret = PyBytes_FromStringAndSize((char*)key, klen);

    return ret;
}

PyObject *cipher_init(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *cipher,
                        PyObject *key, PyObject *iv, int mode) {
    const void *kbuf, *ibuf;
    Py_ssize_t klen, ilen;

    if (key == Py_None)
        kbuf = NULL;
    else if (m2_PyObject_AsReadBuffer(key, &kbuf, &klen) == -1)
        return NULL;

    if (iv == Py_None)
        ibuf = NULL;
    else if (m2_PyObject_AsReadBuffer(iv, &ibuf, &ilen) == -1)
        return NULL;

    if (!EVP_CipherInit(ctx, cipher, (unsigned char *)kbuf,
                        (unsigned char *)ibuf, mode)) {
        m2_PyErr_Msg(_evp_err);
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject *cipher_update(EVP_CIPHER_CTX *ctx, PyObject *blob) {
    const void *buf;
    int len = 0, olen;
    void *obuf;
    PyObject *ret;

    if (m2_PyObject_AsReadBufferInt(blob, &buf, &len) == -1)
        return NULL;

    if (!(obuf = PyMem_Malloc(len + EVP_CIPHER_CTX_block_size(ctx) - 1))) {
        PyErr_SetString(PyExc_MemoryError, "cipher_update");
        return NULL;
    }
    if (!EVP_CipherUpdate(ctx, obuf, &olen, (unsigned char *)buf, len)) {
        PyMem_Free(obuf);
        m2_PyErr_Msg(_evp_err);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize(obuf, olen);

    PyMem_Free(obuf);
    return ret;
}

PyObject *cipher_final(EVP_CIPHER_CTX *ctx) {
    void *obuf;
    int olen;
    PyObject *ret;

    if (!(obuf = PyMem_Malloc(EVP_CIPHER_CTX_block_size(ctx)))) {
        PyErr_SetString(PyExc_MemoryError, "cipher_final");
        return NULL;
    }
    if (!EVP_CipherFinal(ctx, (unsigned char *)obuf, &olen)) {
        PyMem_Free(obuf);
        m2_PyErr_Msg(_evp_err);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize(obuf, olen);

    PyMem_Free(obuf);
    return ret;
}

PyObject *sign_update(EVP_MD_CTX *ctx, PyObject *blob) {
    const void *buf;
    int len;

    if (m2_PyObject_AsReadBufferInt(blob, &buf, &len) == -1)
        return NULL;

    if (!EVP_SignUpdate(ctx, buf, (Py_ssize_t)len)) {
        m2_PyErr_Msg(_evp_err);
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject *sign_final(EVP_MD_CTX *ctx, EVP_PKEY *pkey) {
    PyObject *ret;
    unsigned char *sigbuf;
    unsigned int siglen = EVP_PKEY_size(pkey);

    sigbuf = (unsigned char*)OPENSSL_malloc(siglen);
    if (!sigbuf) {
        PyErr_SetString(PyExc_MemoryError, "sign_final");
        return NULL;
    }

    if (!EVP_SignFinal(ctx, sigbuf, &siglen, pkey)) {
        m2_PyErr_Msg(_evp_err);
        OPENSSL_cleanse(sigbuf, siglen);
        OPENSSL_free(sigbuf);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize((char*)sigbuf, siglen);

    OPENSSL_cleanse(sigbuf, siglen);
    OPENSSL_free(sigbuf);
    return ret;
}

int verify_update(EVP_MD_CTX *ctx, PyObject *blob) {
    const void *buf;
    int len;

    if (m2_PyObject_AsReadBufferInt(blob, &buf, &len) == -1)
        return -1;

    return EVP_VerifyUpdate(ctx, buf, (Py_ssize_t)len);
}

int verify_final(EVP_MD_CTX *ctx, PyObject *blob, EVP_PKEY *pkey) {
    const void *kbuf = NULL;
    int len = 0;

    if (m2_PyObject_AsReadBufferInt(blob, &kbuf, &len) == -1)
        return -1;

    return EVP_VerifyFinal(ctx, (const unsigned char *)kbuf, len, pkey);
}

int digest_sign_init(EVP_MD_CTX *ctx, EVP_PKEY *pkey) {
    return EVP_DigestSignInit(ctx, NULL, NULL, NULL, pkey);
}

PyObject *digest_sign_update(EVP_MD_CTX *ctx, PyObject *blob) {
    const void *buf;
    int len = 0;

    if (m2_PyObject_AsReadBufferInt(blob, (const void **)&buf, &len) == -1)
        return NULL;

    if (!EVP_DigestSignUpdate(ctx, buf, len)) {
        m2_PyErr_Msg(_evp_err);
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject *digest_sign_final(EVP_MD_CTX *ctx) {
    PyObject *ret;
    unsigned char *sigbuf;
    size_t siglen;

    if (!EVP_DigestSignFinal(ctx, NULL, &siglen)) {
        m2_PyErr_Msg(_evp_err);
        return NULL;
    }

    sigbuf = (unsigned char*)OPENSSL_malloc(siglen);
    if (!sigbuf) {
        PyErr_SetString(PyExc_MemoryError, "digest_sign_final");
        return NULL;
    }

    if (!EVP_DigestSignFinal(ctx, sigbuf, &siglen)) {
        m2_PyErr_Msg(_evp_err);
        OPENSSL_cleanse(sigbuf, siglen);
        OPENSSL_free(sigbuf);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize((char*)sigbuf, siglen);

    OPENSSL_cleanse(sigbuf, siglen);
    OPENSSL_free(sigbuf);
    return ret;
}

#if OPENSSL_VERSION_NUMBER >= 0x10101000L
PyObject *digest_sign(EVP_MD_CTX *ctx, PyObject *msg) {
    PyObject *ret;
    const void *msgbuf;
    unsigned char *sigbuf;
    int msglen = 0;
    size_t siglen = 0;

    if (m2_PyObject_AsReadBufferInt(msg, (const void **)&msgbuf, &msglen) == -1)
        return NULL;

    if (!EVP_DigestSign(ctx, NULL, &siglen, msgbuf, msglen)) {
        m2_PyErr_Msg(_evp_err);
        return NULL;
    }

    sigbuf = (unsigned char*)OPENSSL_malloc(siglen);
    if (!sigbuf) {
        PyErr_SetString(PyExc_MemoryError, "digest_sign");
        return NULL;
    }

    if (!EVP_DigestSign(ctx, sigbuf, &siglen, msgbuf, msglen)) {
        m2_PyErr_Msg(_evp_err);
        OPENSSL_cleanse(sigbuf, siglen);
        OPENSSL_free(sigbuf);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize((char*)sigbuf, siglen);

    OPENSSL_cleanse(sigbuf, siglen);
    OPENSSL_free(sigbuf);
    return ret;

}
#endif

int digest_verify_init(EVP_MD_CTX *ctx, EVP_PKEY *pkey) {
    return EVP_DigestVerifyInit(ctx, NULL, NULL, NULL, pkey);
}

int digest_verify_update(EVP_MD_CTX *ctx, PyObject *blob) {
    const void *buf = NULL;
    int len = 0;

    if (m2_PyObject_AsReadBufferInt(blob, (const void **)&buf, &len) == -1)
        return -1;

    return EVP_DigestVerifyUpdate(ctx, buf, len);
}

int digest_verify_final(EVP_MD_CTX *ctx, PyObject *blob) {
    unsigned char *sigbuf = NULL;
    int len = 0;

    if (m2_PyObject_AsReadBufferInt(blob, (const void **)&sigbuf, &len) == -1)
        return -1;

    return EVP_DigestVerifyFinal(ctx, sigbuf, len);
}

#if OPENSSL_VERSION_NUMBER >= 0x10101000L
int digest_verify(EVP_MD_CTX *ctx, PyObject *sig, PyObject *msg) {
    unsigned char *sigbuf;
    unsigned char *msgbuf;
    int siglen = 0;
    int msglen = 0;

    if (m2_PyObject_AsReadBufferInt(sig, (const void **)&sigbuf, &siglen) == -1)
        return -1;

    if (m2_PyObject_AsReadBufferInt(msg, (const void **)&msgbuf, &msglen) == -1)
        return -1;

    return EVP_DigestVerify(ctx, sigbuf, siglen, msgbuf, msglen);
}
#endif
%}

%typemap(out) EVP_MD * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        $result = NULL;
    }
}
%inline %{
const EVP_MD *get_digestbyname(const char* name) {
    const EVP_MD *ret = NULL;

    if ((ret = EVP_get_digestbyname(name)) == NULL) {
        m2_PyErr_Msg(_evp_err);
    }

    return ret;
}
%}
%typemap(out) EVP_MD *;

%inline %{
int pkey_write_pem_no_cipher(EVP_PKEY *pkey, BIO *f, PyObject *pyfunc) {
    int ret;

    Py_INCREF(pyfunc);
    Py_BEGIN_ALLOW_THREADS
    ret = PEM_write_bio_PKCS8PrivateKey(f, pkey, NULL, NULL, 0,
            passphrase_callback, (void *)pyfunc);
    Py_END_ALLOW_THREADS
    Py_DECREF(pyfunc);
    return ret;
}
%}

%inline %{
int pkey_write_pem(EVP_PKEY *pkey, BIO *f, EVP_CIPHER *cipher, PyObject *pyfunc) {
    int ret;

    Py_INCREF(pyfunc);
    Py_BEGIN_ALLOW_THREADS
    ret = PEM_write_bio_PKCS8PrivateKey(f, pkey, cipher, NULL, 0,
            passphrase_callback, (void *)pyfunc);
    Py_END_ALLOW_THREADS
    Py_DECREF(pyfunc);
    return ret;
}
%}

%typemap(out) EVP_PKEY * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        $result = NULL;
    }
}
%inline %{
EVP_PKEY *pkey_new(void) {
    EVP_PKEY *ret;

    if ((ret = EVP_PKEY_new()) == NULL) {
        PyErr_Format(PyExc_MemoryError,
                     "Insufficient memory for new key in function %s.", __FUNCTION__);
    }

    return ret;
}

EVP_PKEY *pkey_read_pem(BIO *f, PyObject *pyfunc) {
    EVP_PKEY *pk;

    Py_INCREF(pyfunc);
    Py_BEGIN_ALLOW_THREADS
    pk = PEM_read_bio_PrivateKey(f, NULL, passphrase_callback, (void *)pyfunc);
    Py_END_ALLOW_THREADS
    Py_DECREF(pyfunc);

    if (pk == NULL) {
        PyErr_Format(_evp_err,
                     "Unable to read private key in function %s.", __FUNCTION__);
    }

    return pk;
}

EVP_PKEY *pkey_read_pem_pubkey(BIO *f, PyObject *pyfunc) {
    EVP_PKEY *pk;

    Py_INCREF(pyfunc);
    Py_BEGIN_ALLOW_THREADS
    pk = PEM_read_bio_PUBKEY(f, NULL, passphrase_callback, (void *)pyfunc);
    Py_END_ALLOW_THREADS
    Py_DECREF(pyfunc);

    if (pk == NULL) {
        PyErr_Format(_evp_err,
                     "Unable to read public key in function %s.", __FUNCTION__);
    }

    return pk;
}
%}
%typemap(out) EVP_PKEY * ;

%inline %{
int pkey_assign_rsa(EVP_PKEY *pkey, RSA *rsa) {
    return EVP_PKEY_assign_RSA(pkey, rsa);
}

PyObject *pkey_as_der(EVP_PKEY *pkey) {
    unsigned char * pp = NULL;
    int len;
    PyObject * der;
    len = i2d_PUBKEY(pkey, &pp);
    if (len < 0){
        PyErr_SetString(_evp_err, "EVP_PKEY as DER failed");
        return NULL;
    }

    der = PyBytes_FromStringAndSize((char*)pp, len);

    OPENSSL_free(pp);
    return der;
}

PyObject *pkey_get_modulus(EVP_PKEY *pkey)
{
    RSA *rsa;
    DSA *dsa;
    BIO *bio;
    BUF_MEM *bptr;
    PyObject *ret;
    const BIGNUM* bn;

    switch (EVP_PKEY_base_id(pkey)) {
        case EVP_PKEY_RSA:
            rsa = EVP_PKEY_get1_RSA(pkey);

            bio = BIO_new(BIO_s_mem());
            if (!bio) {
                RSA_free(rsa);
                PyErr_SetString(PyExc_MemoryError, "pkey_get_modulus");
                return NULL;
            }

            RSA_get0_key(rsa, &bn, NULL, NULL);
            if (!BN_print(bio, bn)) {
                m2_PyErr_Msg(PyExc_RuntimeError);
                BIO_free(bio);
                RSA_free(rsa);
                return NULL;
            }
            BIO_get_mem_ptr(bio, &bptr);

            ret = PyBytes_FromStringAndSize(bptr->data, bptr->length);

            (void)BIO_set_close(bio, BIO_CLOSE);
            BIO_free(bio);
            RSA_free(rsa);

            return ret;
            break;

        case EVP_PKEY_DSA:
            dsa = EVP_PKEY_get1_DSA(pkey);

            bio = BIO_new(BIO_s_mem());
            if (!bio) {
                DSA_free(dsa);
                PyErr_SetString(PyExc_MemoryError, "pkey_get_modulus");
                return NULL;
            }

            DSA_get0_key(dsa, &bn, NULL);
            if (!BN_print(bio, bn)) {
                m2_PyErr_Msg(PyExc_RuntimeError);
                BIO_free(bio);
                DSA_free(dsa);
                return NULL;
            }
            BIO_get_mem_ptr(bio, &bptr);

            ret = PyBytes_FromStringAndSize(bptr->data, bptr->length);

            (void)BIO_set_close(bio, BIO_CLOSE);
            BIO_free(bio);
            DSA_free(dsa);

            return ret;
            break;

        default:
            PyErr_SetString(_evp_err, "unsupported key type");
            return NULL;
    }
}

%}
