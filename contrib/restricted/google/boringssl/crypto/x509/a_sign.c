/* Copyright (C) 1995-1998 Eric Young (eay@cryptsoft.com)
 * All rights reserved.
 *
 * This package is an SSL implementation written
 * by Eric Young (eay@cryptsoft.com).
 * The implementation was written so as to conform with Netscapes SSL.
 *
 * This library is free for commercial and non-commercial use as long as
 * the following conditions are aheared to.  The following conditions
 * apply to all code found in this distribution, be it the RC4, RSA,
 * lhash, DES, etc., code; not just the SSL code.  The SSL documentation
 * included with this distribution is covered by the same copyright terms
 * except that the holder is Tim Hudson (tjh@cryptsoft.com).
 *
 * Copyright remains Eric Young's, and as such any Copyright notices in
 * the code are not to be removed.
 * If this package is used in a product, Eric Young should be given attribution
 * as the author of the parts of the library used.
 * This can be in the form of a textual message at program startup or
 * in documentation (online or textual) provided with the package.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    "This product includes cryptographic software written by
 *     Eric Young (eay@cryptsoft.com)"
 *    The word 'cryptographic' can be left out if the rouines from the library
 *    being used are not cryptographic related :-).
 * 4. If you include any Windows specific code (or a derivative thereof) from
 *    the apps directory (application code) you must include an acknowledgement:
 *    "This product includes software written by Tim Hudson (tjh@cryptsoft.com)"
 *
 * THIS SOFTWARE IS PROVIDED BY ERIC YOUNG ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * The licence and distribution terms for any publically available version or
 * derivative of this code cannot be changed.  i.e. this code cannot simply be
 * copied and put under another distribution licence
 * [including the GNU Public Licence.] */

#include <contrib/restricted/google/boringssl/include/openssl/asn1.h>
#include <contrib/restricted/google/boringssl/include/openssl/digest.h>
#include <contrib/restricted/google/boringssl/include/openssl/err.h>
#include <contrib/restricted/google/boringssl/include/openssl/evp.h>
#include <contrib/restricted/google/boringssl/include/openssl/mem.h>
#include <contrib/restricted/google/boringssl/include/openssl/obj.h>
#include <contrib/restricted/google/boringssl/include/openssl/x509.h>

#include <limits.h>

#include "internal.h"

int ASN1_item_sign(const ASN1_ITEM *it, X509_ALGOR *algor1, X509_ALGOR *algor2,
                   ASN1_BIT_STRING *signature, void *asn, EVP_PKEY *pkey,
                   const EVP_MD *type) {
  if (signature->type != V_ASN1_BIT_STRING) {
    OPENSSL_PUT_ERROR(ASN1, ASN1_R_WRONG_TYPE);
    return 0;
  }
  EVP_MD_CTX ctx;
  EVP_MD_CTX_init(&ctx);
  if (!EVP_DigestSignInit(&ctx, NULL, type, NULL, pkey)) {
    EVP_MD_CTX_cleanup(&ctx);
    return 0;
  }
  return ASN1_item_sign_ctx(it, algor1, algor2, signature, asn, &ctx);
}

int ASN1_item_sign_ctx(const ASN1_ITEM *it, X509_ALGOR *algor1,
                       X509_ALGOR *algor2, ASN1_BIT_STRING *signature,
                       void *asn, EVP_MD_CTX *ctx) {
  int ret = 0;
  uint8_t *in = NULL, *out = NULL;
  if (signature->type != V_ASN1_BIT_STRING) {
    OPENSSL_PUT_ERROR(ASN1, ASN1_R_WRONG_TYPE);
    goto err;
  }

  // Write out the requested copies of the AlgorithmIdentifier.
  if (algor1 && !x509_digest_sign_algorithm(ctx, algor1)) {
    goto err;
  }
  if (algor2 && !x509_digest_sign_algorithm(ctx, algor2)) {
    goto err;
  }

  int in_len = ASN1_item_i2d(asn, &in, it);
  if (in_len < 0) {
    goto err;
  }

  EVP_PKEY *pkey = EVP_PKEY_CTX_get0_pkey(ctx->pctx);
  size_t out_len = EVP_PKEY_size(pkey);
  if (out_len > INT_MAX) {
    OPENSSL_PUT_ERROR(X509, ERR_R_OVERFLOW);
    goto err;
  }

  out = OPENSSL_malloc(out_len);
  if (out == NULL) {
    goto err;
  }

  if (!EVP_DigestSign(ctx, out, &out_len, in, in_len)) {
    OPENSSL_PUT_ERROR(X509, ERR_R_EVP_LIB);
    goto err;
  }

  ASN1_STRING_set0(signature, out, (int)out_len);
  out = NULL;
  signature->flags &= ~(ASN1_STRING_FLAG_BITS_LEFT | 0x07);
  signature->flags |= ASN1_STRING_FLAG_BITS_LEFT;
  ret = (int)out_len;

err:
  EVP_MD_CTX_cleanup(ctx);
  OPENSSL_free(in);
  OPENSSL_free(out);
  return ret;
}
