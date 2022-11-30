/*
 * This code was taken from the OpenSSL's RSA implementation
 * and added to the RW project with some changes
 *
 * Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 2005.
 *
 */
/* ====================================================================
 * Copyright (c) 2005 The OpenSSL Project.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. All advertising materials mentioning features or use of this
 *    software must display the following acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit. (http://www.OpenSSL.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    licensing@OpenSSL.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.OpenSSL.org/)"
 *
 * THIS SOFTWARE IS PROVIDED BY THE OpenSSL PROJECT ``AS IS'' AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE OpenSSL PROJECT OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * ====================================================================
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */

#include "rw.h"

#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <stdio.h>
#include <string.h>

static const unsigned char zeroes[] = { 0, 0, 0, 0, 0, 0, 0, 0 };

static int PkcS1MgF1(unsigned char *mask, const int len, const unsigned char *seed, const int seedlen, const EVP_MD *dgst) {
    int i, outlen = 0;
    unsigned char cnt[4];
    EVP_MD_CTX* c = EVP_MD_CTX_create();
    unsigned char md[EVP_MAX_MD_SIZE];
    int mdlen;
    int rv = -1;

    if (!c) {
        return rv;
    }

    mdlen = EVP_MD_size(dgst);

    if (mdlen < 0 || seedlen < 0)
        goto err;

    for (i = 0; outlen < len; i++) {
        cnt[0] = (unsigned char)((i >> 24) & 255);
        cnt[1] = (unsigned char)((i >> 16) & 255);
        cnt[2] = (unsigned char)((i >> 8)) & 255;
        cnt[3] = (unsigned char)(i & 255);

        if (!EVP_DigestInit_ex(c,dgst, NULL) || !EVP_DigestUpdate(c, seed, seedlen) || !EVP_DigestUpdate(c, cnt, 4))
            goto err;

        if (outlen + mdlen <= len) {
            if (!EVP_DigestFinal_ex(c, mask + outlen, NULL))
                goto err;
            outlen += mdlen;
        } else {
            if (!EVP_DigestFinal_ex(c, md, NULL))
                goto err;
            memcpy(mask + outlen, md, len - outlen);
            outlen = len;
        }
    }
    rv = 0;

err:
    EVP_MD_CTX_destroy(c);
    return rv;
}

int RwVerifyPssr(const TRwKey *rw, const unsigned char *mHash, const EVP_MD *Hash, const unsigned char *EM, int sLen) {
    int i = 0, ret = 0, hLen = 0, maskedDBLen = 0, MSBits = 0, emLen = 0;
    const unsigned char *H = NULL;
    unsigned char *DB = NULL;
    EVP_MD_CTX* ctx = NULL;
    unsigned char H_[EVP_MAX_MD_SIZE];
    const EVP_MD *mgf1Hash = Hash;

    ctx = EVP_MD_CTX_create();
    if (!ctx) {
        return ret;
    }
    hLen = EVP_MD_size(Hash);

    if (hLen < 0)
        goto err;
    /*
     * Negative sLen has special meanings:
     *    -1    sLen == hLen
     *    -2    salt length is autorecovered from signature
     *    -N    reserved
     */
    if (sLen == -1)
        sLen = hLen;
    else if (sLen < -2)
        goto err;

    {
        int bits = BN_num_bits(rw->N);
        if (bits <= 0)
            goto err;

        MSBits = (bits - 1) & 0x7;
    }
    emLen = RwModSize(rw);

    if (EM[0] & (0xFF << MSBits)) {
        goto err;
    }

    if (MSBits == 0) {
        EM++;
        emLen--;
    }

    if (emLen < (hLen + sLen + 2)) /* sLen can be small negative */
        goto err;

    if (emLen < 1)
        goto err;

    if (EM[emLen - 1] != 0xbc)
        goto err;

    maskedDBLen = emLen - hLen - 1;
    if (maskedDBLen <= 0)
        goto err;

    H = EM + maskedDBLen;
    DB = malloc(maskedDBLen);

    if (!DB)
        goto err;

    if (PkcS1MgF1(DB, maskedDBLen, H, hLen, mgf1Hash) < 0)
        goto err;

    for (i = 0; i < maskedDBLen; i++)
        DB[i] ^= EM[i];

    if (MSBits)
        DB[0] &= 0xFF >> (8 - MSBits);

    for (i = 0; DB[i] == 0 && i < (maskedDBLen-1); i++) ;

    if (DB[i++] != 0x1)
        goto err;

    if (sLen >= 0 && (maskedDBLen - i) != sLen)
        goto err;

    if (!EVP_DigestInit_ex(ctx, Hash, NULL) || !EVP_DigestUpdate(ctx, zeroes, sizeof zeroes) || !EVP_DigestUpdate(ctx, mHash, hLen))
        goto err;

    if (maskedDBLen - i) {
        if (!EVP_DigestUpdate(ctx, DB + i, maskedDBLen - i))
            goto err;
    }

    if (!EVP_DigestFinal_ex(ctx, H_, NULL))
        goto err;

    ret = memcmp(H, H_, hLen) ? 0 : 1;

err:
    if (DB)
        free(DB);

    EVP_MD_CTX_destroy(ctx);

    return ret;
}

/*
 rw - public key
 EM - buffer to write padding value
 mHash - hash value
 Hash - EVP_MD() that will be used to pad
 sLen - random salt len (usually == hashLen)
 */
int RwPaddingAddPssr(const TRwKey *rw, unsigned char *EM, const unsigned char *mHash, const EVP_MD *Hash, int sLen) {
    int i = 0, ret = 0, hLen = 0, maskedDBLen = 0, MSBits = 0, emLen = 0;
    unsigned char *H = NULL, *salt = NULL, *p = NULL;
    const EVP_MD *mgf1Hash = Hash;
    EVP_MD_CTX* ctx = EVP_MD_CTX_create();
    if (!ctx) {
        return ret;
    }

    hLen = EVP_MD_size(Hash);
    if (hLen < 0)
        goto err;
    /*
     * Negative sLen has special meanings:
     *    -1    sLen == hLen
     *    -2    salt length is maximized
     *    -N    reserved
     */
    if (sLen == -1)
        sLen = hLen;
    else if (sLen < -2)
        goto err;

    {
        int bits = BN_num_bits(rw->N);
        if (bits <= 0)
            goto err;
        MSBits = (bits - 1) & 0x7;
    }
    emLen = RwModSize(rw);
    if (emLen <= 0)
        goto err;

    if (MSBits == 0) {
        *EM++ = 0;
        emLen--;
        fprintf(stderr, "MSBits == 0\n");
    }

    if (sLen == -2) {
        sLen = emLen - hLen - 2;
    }
    else if (emLen < (hLen + sLen + 2))
        goto err;

    if (sLen > 0) {
        salt = malloc(sLen);
        if (!salt) goto err;
        if (RAND_bytes(salt, sLen) <= 0)
            goto err;
    }

    maskedDBLen = emLen - hLen - 1;
    if (maskedDBLen < 0)
        goto err;
    H = EM + maskedDBLen;

    if (!EVP_DigestInit_ex(ctx, Hash, NULL) || !EVP_DigestUpdate(ctx, zeroes, sizeof zeroes) || !EVP_DigestUpdate(ctx, mHash, hLen))
        goto err;

    if (sLen && !EVP_DigestUpdate(ctx, salt, sLen))
        goto err;

    if (!EVP_DigestFinal_ex(ctx, H, NULL))
        goto err;

    /* Generate dbMask in place then perform XOR on it */
    if (PkcS1MgF1(EM, maskedDBLen, H, hLen, mgf1Hash))
        goto err;

    p = EM;

    /* Initial PS XORs with all zeroes which is a NOP so just update
     * pointer. Note from a test above this value is guaranteed to
     * be non-negative.
     */
    p += emLen - sLen - hLen - 2;
    *p++ ^= 0x1;

    if (sLen > 0) {
        for (i = 0; i < sLen; i++)
            *p++ ^= salt[i];
    }

    if (MSBits)
        EM[0] &= 0xFF >> (8 - MSBits);

    /* H is already in place so just set final 0xbc */
    EM[emLen - 1] = 0xbc;

    ret = 1;

err:
    EVP_MD_CTX_destroy(ctx);

    if (salt)
        free(salt);

    return ret;
}
