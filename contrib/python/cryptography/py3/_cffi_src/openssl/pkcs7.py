# This file is dual licensed under the terms of the Apache License, Version
# 2.0, and the BSD License. See the LICENSE file in the root of this repository
# for complete details.

from __future__ import absolute_import, division, print_function

INCLUDES = """
#include <openssl/pkcs7.h>
"""

TYPES = """
typedef struct {
    Cryptography_STACK_OF_X509 *cert;
    Cryptography_STACK_OF_X509_CRL *crl;
    ...;
} PKCS7_SIGNED;

typedef struct {
    Cryptography_STACK_OF_X509 *cert;
    Cryptography_STACK_OF_X509_CRL *crl;
    ...;
} PKCS7_SIGN_ENVELOPE;

typedef ... PKCS7_DIGEST;
typedef ... PKCS7_ENCRYPT;
typedef ... PKCS7_ENVELOPE;
typedef ... PKCS7_SIGNER_INFO;

typedef struct {
    ASN1_OBJECT *type;
    union {
        char *ptr;
        ASN1_OCTET_STRING *data;
        PKCS7_SIGNED *sign;
        PKCS7_ENVELOPE *enveloped;
        PKCS7_SIGN_ENVELOPE *signed_and_enveloped;
        PKCS7_DIGEST *digest;
        PKCS7_ENCRYPT *encrypted;
        ASN1_TYPE *other;
     } d;
    ...;
} PKCS7;

static const int PKCS7_BINARY;
static const int PKCS7_DETACHED;
static const int PKCS7_NOATTR;
static const int PKCS7_NOCERTS;
static const int PKCS7_NOCHAIN;
static const int PKCS7_NOINTERN;
static const int PKCS7_NOSIGS;
static const int PKCS7_NOSMIMECAP;
static const int PKCS7_NOVERIFY;
static const int PKCS7_STREAM;
static const int PKCS7_TEXT;
static const int PKCS7_PARTIAL;
"""

FUNCTIONS = """
void PKCS7_free(PKCS7 *);
PKCS7 *PKCS7_sign(X509 *, EVP_PKEY *, Cryptography_STACK_OF_X509 *,
                   BIO *, int);
int SMIME_write_PKCS7(BIO *, PKCS7 *, BIO *, int);
int PEM_write_bio_PKCS7_stream(BIO *, PKCS7 *, BIO *, int);
PKCS7_SIGNER_INFO *PKCS7_sign_add_signer(PKCS7 *, X509 *, EVP_PKEY *,
                                         const EVP_MD *, int);
int PKCS7_final(PKCS7 *, BIO *, int);
/* Included verify due to external consumer, see
   https://github.com/pyca/cryptography/issues/5433 */
int PKCS7_verify(PKCS7 *, Cryptography_STACK_OF_X509 *, X509_STORE *, BIO *,
                 BIO *, int);
PKCS7 *SMIME_read_PKCS7(BIO *, BIO **);

int PKCS7_type_is_signed(PKCS7 *);
int PKCS7_type_is_enveloped(PKCS7 *);
int PKCS7_type_is_signedAndEnveloped(PKCS7 *);
int PKCS7_type_is_data(PKCS7 *);
"""

CUSTOMIZATIONS = ""
