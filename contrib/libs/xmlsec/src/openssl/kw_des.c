/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
/**
 * SECTION:kw_des
 * @Short_description: DES Key Transport transforms implementation for OpenSSL.
 * @Stability: Private
 *
 */

#ifndef XMLSEC_NO_DES
#include "globals.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <openssl/des.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>
#include <xmlsec/errors.h>
#include <xmlsec/private.h>

#include <xmlsec/openssl/crypto.h>

#include "../kw_aes_des.h"
#include "../cast_helpers.h"
#include "openssl_compat.h"

#ifdef XMLSEC_OPENSSL_API_300
#error #include <openssl/core_names.h>
#endif /* XMLSEC_OPENSSL_API_300 */


/*********************************************************************
 *
 * DES KW implementation
 *
 *********************************************************************/
static int       xmlSecOpenSSLKWDes3GenerateRandom               (xmlSecTransformPtr transform,
                                                                 xmlSecByte * out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize * outWritten);
static int       xmlSecOpenSSLKWDes3Sha1                         (xmlSecTransformPtr transform,
                                                                 const xmlSecByte * in,
                                                                 xmlSecSize inSize,
                                                                 xmlSecByte * out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize * outWritten);
static int      xmlSecOpenSSLKWDes3BlockEncrypt                  (xmlSecTransformPtr transform,
                                                                 const xmlSecByte * iv,
                                                                 xmlSecSize ivSize,
                                                                 const xmlSecByte * in,
                                                                 xmlSecSize inSize,
                                                                 xmlSecByte * out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize * outWritten);
static int      xmlSecOpenSSLKWDes3BlockDecrypt                  (xmlSecTransformPtr transform,
                                                                 const xmlSecByte * iv,
                                                                 xmlSecSize ivSize,
                                                                 const xmlSecByte * in,
                                                                 xmlSecSize inSize,
                                                                 xmlSecByte * out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize * outWritten);

static xmlSecKWDes3Klass xmlSecOpenSSLKWDes3ImplKlass = {
    /* callbacks */
    xmlSecOpenSSLKWDes3GenerateRandom,       /* xmlSecKWDes3GenerateRandomMethod     generateRandom; */
    xmlSecOpenSSLKWDes3Sha1,                 /* xmlSecKWDes3Sha1Method               sha1; */
    xmlSecOpenSSLKWDes3BlockEncrypt,         /* xmlSecKWDes3BlockEncryptMethod       encrypt; */
    xmlSecOpenSSLKWDes3BlockDecrypt,         /* xmlSecKWDes3BlockDecryptMethod       decrypt; */

    /* for the future */
    NULL,                                   /* void*                               reserved0; */
    NULL,                                   /* void*                               reserved1; */
};

static int      xmlSecOpenSSLKWDes3Encrypt                      (const xmlSecByte *key,
                                                                 xmlSecSize keySize,
                                                                 const xmlSecByte *iv,
                                                                 xmlSecSize ivSize,
                                                                 const xmlSecByte *in,
                                                                 xmlSecSize inSize,
                                                                 xmlSecByte *out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize * outWritten,
                                                                 int enc);


/*********************************************************************
 *
 * Triple DES Key Wrap transform context
 *
 ********************************************************************/
typedef xmlSecTransformKWDes3Ctx  xmlSecOpenSSLKWDes3Ctx,
                                 *xmlSecOpenSSLKWDes3CtxPtr;

/*********************************************************************
 *
 * Triple DES Key Wrap transform
 *
 ********************************************************************/
XMLSEC_TRANSFORM_DECLARE(OpenSSLKWDes3, xmlSecOpenSSLKWDes3Ctx)
#define xmlSecOpenSSLKWDes3Size XMLSEC_TRANSFORM_SIZE(OpenSSLKWDes3)

static int      xmlSecOpenSSLKWDes3Initialize                   (xmlSecTransformPtr transform);
static void     xmlSecOpenSSLKWDes3Finalize                     (xmlSecTransformPtr transform);
static int      xmlSecOpenSSLKWDes3SetKeyReq                    (xmlSecTransformPtr transform,
                                                                 xmlSecKeyReqPtr keyReq);
static int      xmlSecOpenSSLKWDes3SetKey                       (xmlSecTransformPtr transform,
                                                                 xmlSecKeyPtr key);
static int      xmlSecOpenSSLKWDes3Execute                      (xmlSecTransformPtr transform,
                                                                 int last,
                                                                 xmlSecTransformCtxPtr transformCtx);
static xmlSecTransformKlass xmlSecOpenSSLKWDes3Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLKWDes3Size,                    /* xmlSecSize objSize */

    xmlSecNameKWDes3,                           /* const xmlChar* name; */
    xmlSecHrefKWDes3,                           /* const xmlChar* href; */
    xmlSecTransformUsageEncryptionMethod,       /* xmlSecAlgorithmUsage usage; */

    xmlSecOpenSSLKWDes3Initialize,              /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLKWDes3Finalize,                /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLKWDes3SetKeyReq,               /* xmlSecTransformSetKeyMethod setKeyReq; */
    xmlSecOpenSSLKWDes3SetKey,                  /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLKWDes3Execute,                 /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformKWDes3GetKlass:
 *
 * The Triple DES key wrapper transform klass.
 *
 * Returns: Triple DES key wrapper transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformKWDes3GetKlass(void) {
    return(&xmlSecOpenSSLKWDes3Klass);
}

static int
xmlSecOpenSSLKWDes3Initialize(xmlSecTransformPtr transform) {
    xmlSecOpenSSLKWDes3CtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWDes3Id), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWDes3Size), -1);

    ctx = xmlSecOpenSSLKWDes3GetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);
    memset(ctx, 0, sizeof(xmlSecOpenSSLKWDes3Ctx));

    ret = xmlSecTransformKWDes3Initialize(transform, ctx, &xmlSecOpenSSLKWDes3ImplKlass,
        xmlSecOpenSSLKeyDataDesId);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformKWDes3Initialize", xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static void
xmlSecOpenSSLKWDes3Finalize(xmlSecTransformPtr transform) {
    xmlSecOpenSSLKWDes3CtxPtr ctx;

    xmlSecAssert(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWDes3Id));
    xmlSecAssert(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWDes3Size));

    ctx = xmlSecOpenSSLKWDes3GetCtx(transform);
    xmlSecAssert(ctx != NULL);

    xmlSecTransformKWDes3Finalize(transform, ctx);
    memset(ctx, 0, sizeof(xmlSecOpenSSLKWDes3Ctx));
}

static int
xmlSecOpenSSLKWDes3SetKeyReq(xmlSecTransformPtr transform,  xmlSecKeyReqPtr keyReq) {
    xmlSecOpenSSLKWDes3CtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWDes3Id), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWDes3Size), -1);

    ctx = xmlSecOpenSSLKWDes3GetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecTransformKWDes3SetKeyReq(transform, ctx, keyReq);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformKWDes3SetKeyReq", xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static int
xmlSecOpenSSLKWDes3SetKey(xmlSecTransformPtr transform, xmlSecKeyPtr key) {
    xmlSecOpenSSLKWDes3CtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWDes3Id), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWDes3Size), -1);

    ctx = xmlSecOpenSSLKWDes3GetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecTransformKWDes3SetKey(transform, ctx, key);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformKWDes3SetKey", xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static int
xmlSecOpenSSLKWDes3Execute(xmlSecTransformPtr transform, int last,
                           xmlSecTransformCtxPtr transformCtx ATTRIBUTE_UNUSED) {
    xmlSecOpenSSLKWDes3CtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWDes3Id), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWDes3Size), -1);
    UNREFERENCED_PARAMETER(transformCtx);

    ctx = xmlSecOpenSSLKWDes3GetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecTransformKWDes3Execute(transform, ctx, last);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformKWDes3Execute", xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

/*********************************************************************
 *
 * DES KW implementation
 *
 *********************************************************************/
#ifndef XMLSEC_OPENSSL_API_300

static int
xmlSecOpenSSLKWDes3Sha1(xmlSecTransformPtr transform ATTRIBUTE_UNUSED,
                       const xmlSecByte * in, xmlSecSize inSize,
                       xmlSecByte * out, xmlSecSize outSize,
                       xmlSecSize * outWritten) {
    UNREFERENCED_PARAMETER(transform);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize > 0, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= SHA_DIGEST_LENGTH, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    if(SHA1(in, inSize, out) == NULL) {
        xmlSecOpenSSLError("SHA1", NULL);
        return(-1);
    }

    /* success */
    (*outWritten) = SHA_DIGEST_LENGTH;
    return(0);
}

#else /* XMLSEC_OPENSSL_API_300 */

static int
xmlSecOpenSSLKWDes3Sha1(xmlSecTransformPtr transform ATTRIBUTE_UNUSED,
                       const xmlSecByte * in, xmlSecSize inSize,
                       xmlSecByte * out, xmlSecSize outSize,
                       xmlSecSize * outWritten) {
    size_t outSizeT;
    int ret;

    UNREFERENCED_PARAMETER(transform);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize > 0, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= SHA_DIGEST_LENGTH, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    outSizeT = outSize;
    ret = EVP_Q_digest(xmlSecOpenSSLGetLibCtx(), OSSL_DIGEST_NAME_SHA1, NULL,
                       in, inSize, out, &outSizeT);
    if(ret != 1) {
        xmlSecOpenSSLError("EVP_Q_digest(SHA1)", NULL);
        return(-1);
    }

    /* success */
    XMLSEC_SAFE_CAST_SIZE_T_TO_SIZE(outSizeT, (*outWritten), return(-1), NULL);
    return(0);
}

#endif /* XMLSEC_OPENSSL_API_300 */


static int
xmlSecOpenSSLKWDes3GenerateRandom(xmlSecTransformPtr transform ATTRIBUTE_UNUSED,
                                 xmlSecByte * out, xmlSecSize outSize,
                                 xmlSecSize * outWritten) {
    int ret;

    UNREFERENCED_PARAMETER(transform);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize > 0, -1);

    ret = RAND_priv_bytes_ex(xmlSecOpenSSLGetLibCtx(), out, outSize, XMLSEEC_OPENSSL_RAND_BYTES_STRENGTH);
    if(ret != 1) {
        xmlSecOpenSSLError2("RAND_priv_bytes_ex", NULL, "size=" XMLSEC_SIZE_FMT, outSize);
        return(-1);
    }
    (*outWritten) = outSize;

    return(0);
}

static int
xmlSecOpenSSLKWDes3BlockEncrypt(xmlSecTransformPtr transform,
                               const xmlSecByte * iv, xmlSecSize ivSize,
                               const xmlSecByte * in, xmlSecSize inSize,
                               xmlSecByte * out, xmlSecSize outSize,
                               xmlSecSize * outWritten) {
    xmlSecOpenSSLKWDes3CtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWDes3Id), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWDes3Size), -1);
    xmlSecAssert2(iv != NULL, -1);
    xmlSecAssert2(ivSize >= XMLSEC_KW_DES3_IV_LENGTH, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize > 0, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= inSize, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    ctx = xmlSecOpenSSLKWDes3GetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(xmlSecBufferGetData(&(ctx->keyBuffer)) != NULL, -1);
    xmlSecAssert2(xmlSecBufferGetSize(&(ctx->keyBuffer)) >= XMLSEC_KW_DES3_KEY_LENGTH, -1);

    ret = xmlSecOpenSSLKWDes3Encrypt(
            xmlSecBufferGetData(&(ctx->keyBuffer)),XMLSEC_KW_DES3_KEY_LENGTH,
            iv, XMLSEC_KW_DES3_IV_LENGTH,
            in, inSize,
            out, outSize, outWritten,
            1); /* encrypt */
    if(ret < 0) {
        xmlSecInternalError("xmlSecOpenSSLKWDes3Encrypt", NULL);
        return(-1);
    }

    return(0);
}

static int
xmlSecOpenSSLKWDes3BlockDecrypt(xmlSecTransformPtr transform,
                               const xmlSecByte * iv, xmlSecSize ivSize,
                               const xmlSecByte * in, xmlSecSize inSize,
                               xmlSecByte * out, xmlSecSize outSize,
                               xmlSecSize * outWritten) {
    xmlSecOpenSSLKWDes3CtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWDes3Id), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWDes3Size), -1);
    xmlSecAssert2(iv != NULL, -1);
    xmlSecAssert2(ivSize >= XMLSEC_KW_DES3_IV_LENGTH, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize > 0, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= inSize, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    ctx = xmlSecOpenSSLKWDes3GetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(xmlSecBufferGetData(&(ctx->keyBuffer)) != NULL, -1);
    xmlSecAssert2(xmlSecBufferGetSize(&(ctx->keyBuffer)) >= XMLSEC_KW_DES3_KEY_LENGTH, -1);

    ret = xmlSecOpenSSLKWDes3Encrypt(
        xmlSecBufferGetData(&(ctx->keyBuffer)), XMLSEC_KW_DES3_KEY_LENGTH,
        iv, XMLSEC_KW_DES3_IV_LENGTH,
        in, inSize,
        out, outSize, outWritten,
        0); /* decrypt */
    if(ret < 0) {
        xmlSecInternalError("xmlSecOpenSSLKWDes3Encrypt", NULL);
        return(-1);
    }

    return(0);
}



static int
xmlSecOpenSSLKWDes3Encrypt(const xmlSecByte* key, xmlSecSize keySize,
                           const xmlSecByte* iv, xmlSecSize ivSize,
                           const xmlSecByte* in, xmlSecSize inSize,
                           xmlSecByte* out, xmlSecSize outSize,
                           xmlSecSize* outWritten,
                           int enc) {
#ifndef XMLSEC_OPENSSL_API_300
    const EVP_CIPHER*   cipher = NULL;
#else /* XMLSEC_OPENSSL_API_300 */
    EVP_CIPHER*         cipher = NULL;
#endif /* XMLSEC_OPENSSL_API_300 */
    EVP_CIPHER_CTX* cipherCtx = NULL;
    xmlSecSize size;
    int inLen, outLen, updateLen, finalLen;
    int ret;
    int res = -1;

    xmlSecAssert2(key != NULL, -1);
    xmlSecAssert2(iv != NULL, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize > 0, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= inSize, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    ret = EVP_CIPHER_key_length(EVP_des_ede3_cbc());
    if(ret <= 0) {
        xmlSecOpenSSLError("EVP_CIPHER_key_length(EVP_des_ede3_cbc)", NULL);
        goto done;
    }
    XMLSEC_SAFE_CAST_INT_TO_SIZE(ret, size, goto done, NULL);
    xmlSecAssert2(keySize == size, -1);

    ret = EVP_CIPHER_iv_length(EVP_des_ede3_cbc());
    if (ret <= 0) {
        xmlSecOpenSSLError("EVP_CIPHER_iv_length(EVP_des_ede3_cbc)", NULL);
        goto done;
    }
    XMLSEC_SAFE_CAST_INT_TO_SIZE(ret, size, goto done, NULL);
    xmlSecAssert2(ivSize == size, -1);

#ifndef XMLSEC_OPENSSL_API_300
    cipher = EVP_des_ede3_cbc();
#else /* XMLSEC_OPENSSL_API_300 */
    cipher = EVP_CIPHER_fetch(xmlSecOpenSSLGetLibCtx(), XMLSEEC_OPENSSL_CIPHER_NAME_DES3_EDE, NULL);
    if(cipher == NULL) {
        xmlSecOpenSSLError("EVP_CIPHER_fetch(DES3_EDE)", NULL);
        goto done;
    }

#endif /* XMLSEC_OPENSSL_API_300 */

    cipherCtx = EVP_CIPHER_CTX_new();
    if(cipherCtx == NULL) {
        xmlSecOpenSSLError("EVP_CIPHER_CTX_new", NULL);
        goto done;
    }

    ret = EVP_CipherInit(cipherCtx, cipher, key, iv, enc);
    if(ret != 1) {
        xmlSecOpenSSLError("EVP_CipherInit", NULL);
        goto done;
    }

    EVP_CIPHER_CTX_set_padding(cipherCtx, 0);

    XMLSEC_SAFE_CAST_SIZE_TO_INT(inSize, inLen, goto done, NULL);
    ret = EVP_CipherUpdate(cipherCtx, out, &updateLen, in, inLen);
    if(ret != 1) {
        xmlSecOpenSSLError("EVP_CipherUpdate", NULL);
        goto done;
    }

    ret = EVP_CipherFinal(cipherCtx, out + updateLen, &finalLen);
    if(ret != 1) {
        xmlSecOpenSSLError("EVP_CipherFinal", NULL);
        goto done;
    }

    /* success */
    outLen = updateLen + finalLen;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(outLen, (*outWritten), goto done, NULL);
    res = 0;

done:
    /* cleanup */
    if(cipherCtx != NULL) {
        EVP_CIPHER_CTX_free(cipherCtx);
    }
#ifdef XMLSEC_OPENSSL_API_300
    if(cipher != NULL) {
        EVP_CIPHER_free(cipher);
    }
#endif /* XMLSEC_OPENSSL_API_300 */

    /* done */
    return(res);
}


#endif /* XMLSEC_NO_DES */

