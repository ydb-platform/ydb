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
 * SECTION:kw_aes
 * @Short_description: AES Key Transport transforms implementation for OpenSSL.
 * @Stability: Private
 *
 */

#ifndef XMLSEC_NO_AES
#include "globals.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <openssl/aes.h>
#include <openssl/rand.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>
#include <xmlsec/errors.h>
#include <xmlsec/private.h>

#include <xmlsec/openssl/crypto.h>

#include "../kw_aes_des.h"
#include "../cast_helpers.h"
#include "openssl_compat.h"

/*********************************************************************
 *
 * AES KW implementation
 *
 *********************************************************************/
static int        xmlSecOpenSSLKWAesBlockEncrypt                (xmlSecTransformPtr transform,
                                                                 const xmlSecByte * in,
                                                                 xmlSecSize inSize,
                                                                 xmlSecByte * out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize * outWritten);
static int        xmlSecOpenSSLKWAesBlockDecrypt                (xmlSecTransformPtr transform,
                                                                 const xmlSecByte * in,
                                                                 xmlSecSize inSize,
                                                                 xmlSecByte * out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize * outWritten);
static xmlSecKWAesKlass xmlSecOpenSSLKWAesKlass = {
    /* callbacks */
    xmlSecOpenSSLKWAesBlockEncrypt,         /* xmlSecKWAesBlockEncryptMethod       encrypt; */
    xmlSecOpenSSLKWAesBlockDecrypt,         /* xmlSecKWAesBlockDecryptMethod       decrypt; */

    /* for the future */
    NULL,                                   /* void*                               reserved0; */
    NULL                                    /* void*                               reserved1; */
};

/*********************************************************************
 *
 * AES KW transforms context
 *
 ********************************************************************/
typedef struct _xmlSecOpenSSLKWAesCtx   xmlSecOpenSSLKWAesCtx,
                                        *xmlSecOpenSSLKWAesCtxPtr;
struct _xmlSecOpenSSLKWAesCtx {
    xmlSecTransformKWAesCtx parentCtx;

#ifdef XMLSEC_OPENSSL_API_300
    const char*  cipherName;
    EVP_CIPHER*  cipher;
#endif /* XMLSEC_OPENSSL_API_300 */
};

/*********************************************************************
 *
 * AES KW transforms
 *
 ********************************************************************/
XMLSEC_TRANSFORM_DECLARE(OpenSSLKWAes, xmlSecOpenSSLKWAesCtx)
#define xmlSecOpenSSLKWAesSize XMLSEC_TRANSFORM_SIZE(OpenSSLKWAes)

#define xmlSecOpenSSLKWAesCheckId(transform) \
    (xmlSecTransformCheckId((transform), xmlSecOpenSSLTransformKWAes128Id) || \
     xmlSecTransformCheckId((transform), xmlSecOpenSSLTransformKWAes192Id) || \
     xmlSecTransformCheckId((transform), xmlSecOpenSSLTransformKWAes256Id))

static int      xmlSecOpenSSLKWAesInitialize                    (xmlSecTransformPtr transform);
static void     xmlSecOpenSSLKWAesFinalize                      (xmlSecTransformPtr transform);
static int      xmlSecOpenSSLKWAesSetKeyReq                     (xmlSecTransformPtr transform,
                                                                 xmlSecKeyReqPtr keyReq);
static int      xmlSecOpenSSLKWAesSetKey                        (xmlSecTransformPtr transform,
                                                                 xmlSecKeyPtr key);
static int      xmlSecOpenSSLKWAesExecute                       (xmlSecTransformPtr transform,
                                                                 int last,
                                                                 xmlSecTransformCtxPtr transformCtx);


/* small helper macro to reduce clutter in the code */
#ifndef XMLSEC_OPENSSL_API_300
#define XMLSEC_OPENSSL_KW_AES_SET_CIPHER(ctx, cipherNameVal)

#else /* XMLSEC_OPENSSL_API_300 */
#define XMLSEC_OPENSSL_KW_AES_SET_CIPHER(ctx, cipherNameVal) \
    (ctx)->cipherName = (cipherNameVal)
#endif /* XMLSEC_OPENSSL_API_300 */

static int
xmlSecOpenSSLKWAesInitialize(xmlSecTransformPtr transform) {
    xmlSecOpenSSLKWAesCtxPtr ctx;
    xmlSecSize keyExpectedSize;
    int ret;

    xmlSecAssert2(xmlSecOpenSSLKWAesCheckId(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWAesSize), -1);

    ctx = xmlSecOpenSSLKWAesGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);
    memset(ctx, 0, sizeof(xmlSecOpenSSLKWAesCtx));

    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWAes128Id)) {
        XMLSEC_OPENSSL_KW_AES_SET_CIPHER(ctx, XMLSEEC_OPENSSL_CIPHER_NAME_AES128_CBC);
        keyExpectedSize = XMLSEC_KW_AES128_KEY_SIZE;
    } else if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWAes192Id)) {
        XMLSEC_OPENSSL_KW_AES_SET_CIPHER(ctx, XMLSEEC_OPENSSL_CIPHER_NAME_AES192_CBC);
        keyExpectedSize = XMLSEC_KW_AES192_KEY_SIZE;
    } else if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformKWAes256Id)) {
        XMLSEC_OPENSSL_KW_AES_SET_CIPHER(ctx, XMLSEEC_OPENSSL_CIPHER_NAME_AES256_CBC);
        keyExpectedSize = XMLSEC_KW_AES256_KEY_SIZE;
    } else {
        xmlSecInvalidTransfromError(transform)
        return(-1);
    }

    ret = xmlSecTransformKWAesInitialize(transform, &(ctx->parentCtx),
        &xmlSecOpenSSLKWAesKlass, xmlSecOpenSSLKeyDataAesId,
        keyExpectedSize);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformKWAesInitialize", xmlSecTransformGetName(transform));
        xmlSecOpenSSLKWAesFinalize(transform);
        return(-1);
    }

#ifdef XMLSEC_OPENSSL_API_300
    /* fetch cipher */
    xmlSecAssert2(ctx->cipherName != NULL, -1);
    ctx->cipher = EVP_CIPHER_fetch(xmlSecOpenSSLGetLibCtx(), ctx->cipherName, NULL);
    if(ctx->cipher == NULL) {
        xmlSecOpenSSLError2("EVP_CIPHER_fetch", xmlSecTransformGetName(transform),
            "cipherName=%s", xmlSecErrorsSafeString(ctx->cipherName));
        xmlSecOpenSSLKWAesFinalize(transform);
        return(-1);
    }
#endif /* XMLSEC_OPENSSL_API_300 */

    return(0);
}

static void
xmlSecOpenSSLKWAesFinalize(xmlSecTransformPtr transform) {
    xmlSecOpenSSLKWAesCtxPtr ctx;

    xmlSecAssert(xmlSecOpenSSLKWAesCheckId(transform));
    xmlSecAssert(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWAesSize));

    ctx = xmlSecOpenSSLKWAesGetCtx(transform);
    xmlSecAssert(ctx != NULL);

#ifdef XMLSEC_OPENSSL_API_300
    if(ctx->cipher != NULL) {
        EVP_CIPHER_free(ctx->cipher);
    }
#endif /* XMLSEC_OPENSSL_API_300 */

    xmlSecTransformKWAesFinalize(transform, &(ctx->parentCtx));
    memset(ctx, 0, sizeof(xmlSecOpenSSLKWAesCtx));
}

static int
xmlSecOpenSSLKWAesSetKeyReq(xmlSecTransformPtr transform,  xmlSecKeyReqPtr keyReq) {
    xmlSecOpenSSLKWAesCtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecOpenSSLKWAesCheckId(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWAesSize), -1);

    ctx = xmlSecOpenSSLKWAesGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecTransformKWAesSetKeyReq(transform, &(ctx->parentCtx),keyReq);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformKWAesSetKeyReq", xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static int
xmlSecOpenSSLKWAesSetKey(xmlSecTransformPtr transform, xmlSecKeyPtr key) {
    xmlSecOpenSSLKWAesCtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecOpenSSLKWAesCheckId(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWAesSize), -1);

    ctx = xmlSecOpenSSLKWAesGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecTransformKWAesSetKey(transform, &(ctx->parentCtx), key);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformKWAesSetKey", xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static int
xmlSecOpenSSLKWAesExecute(xmlSecTransformPtr transform, int last,
                          xmlSecTransformCtxPtr transformCtx ATTRIBUTE_UNUSED) {
    xmlSecOpenSSLKWAesCtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecOpenSSLKWAesCheckId(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWAesSize), -1);
    UNREFERENCED_PARAMETER(transformCtx);

    ctx = xmlSecOpenSSLKWAesGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecTransformKWAesExecute(transform, &(ctx->parentCtx), last);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformKWAesExecute", xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static xmlSecTransformKlass xmlSecOpenSSLKWAes128Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLKWAesSize,                     /* xmlSecSize objSize */

    xmlSecNameKWAes128,                         /* const xmlChar* name; */
    xmlSecHrefKWAes128,                         /* const xmlChar* href; */
    xmlSecTransformUsageEncryptionMethod,       /* xmlSecAlgorithmUsage usage; */

    xmlSecOpenSSLKWAesInitialize,               /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLKWAesFinalize,                 /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLKWAesSetKeyReq,                /* xmlSecTransformSetKeyMethod setKeyReq; */
    xmlSecOpenSSLKWAesSetKey,                   /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLKWAesExecute,                  /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformKWAes128GetKlass:
 *
 * The AES-128 kew wrapper transform klass.
 *
 * Returns: AES-128 kew wrapper transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformKWAes128GetKlass(void) {
    return(&xmlSecOpenSSLKWAes128Klass);
}

static xmlSecTransformKlass xmlSecOpenSSLKWAes192Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLKWAesSize,                     /* xmlSecSize objSize */

    xmlSecNameKWAes192,                         /* const xmlChar* name; */
    xmlSecHrefKWAes192,                         /* const xmlChar* href; */
    xmlSecTransformUsageEncryptionMethod,       /* xmlSecAlgorithmUsage usage; */

    xmlSecOpenSSLKWAesInitialize,               /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLKWAesFinalize,                 /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLKWAesSetKeyReq,                /* xmlSecTransformSetKeyMethod setKeyReq; */
    xmlSecOpenSSLKWAesSetKey,                   /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLKWAesExecute,                  /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};


/**
 * xmlSecOpenSSLTransformKWAes192GetKlass:
 *
 * The AES-192 kew wrapper transform klass.
 *
 * Returns: AES-192 kew wrapper transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformKWAes192GetKlass(void) {
    return(&xmlSecOpenSSLKWAes192Klass);
}

static xmlSecTransformKlass xmlSecOpenSSLKWAes256Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLKWAesSize,                     /* xmlSecSize objSize */

    xmlSecNameKWAes256,                         /* const xmlChar* name; */
    xmlSecHrefKWAes256,                         /* const xmlChar* href; */
    xmlSecTransformUsageEncryptionMethod,       /* xmlSecAlgorithmUsage usage; */

    xmlSecOpenSSLKWAesInitialize,               /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLKWAesFinalize,                 /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLKWAesSetKeyReq,                /* xmlSecTransformSetKeyMethod setKeyReq; */
    xmlSecOpenSSLKWAesSetKey,                   /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLKWAesExecute,                  /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformKWAes256GetKlass:
 *
 * The AES-256 kew wrapper transform klass.
 *
 * Returns: AES-256 kew wrapper transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformKWAes256GetKlass(void) {
    return(&xmlSecOpenSSLKWAes256Klass);
}

/*********************************************************************
 *
 * AES KW implementation
 *
 *********************************************************************/
#ifndef XMLSEC_OPENSSL_API_300
static int
xmlSecOpenSSLKWAesEncryptDecrypt(xmlSecOpenSSLKWAesCtxPtr ctx, const xmlSecByte * in, xmlSecSize inSize,
                                xmlSecByte * out, xmlSecSize outSize, xmlSecSize * outWritten,
                                int encrypt) {
    xmlSecByte* keyData;
    xmlSecSize keySize;
    AES_KEY aesKey;
    int keyLen;
    int ret;

    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize >= AES_BLOCK_SIZE, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= AES_BLOCK_SIZE, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    keyData = xmlSecBufferGetData(&(ctx->parentCtx.keyBuffer));
    keySize = xmlSecBufferGetSize(&(ctx->parentCtx.keyBuffer));
    xmlSecAssert2(keyData != NULL, -1);
    xmlSecAssert2(keySize > 0, -1);
    xmlSecAssert2(keySize == ctx->parentCtx.keyExpectedSize, -1);

    /* prepare key and encrypt/decrypt */
    XMLSEC_SAFE_CAST_SIZE_TO_INT(keySize, keyLen, return(-1), NULL);
    if(encrypt != 0) {
        ret = AES_set_encrypt_key(keyData, 8 * keyLen, &aesKey);
        if(ret != 0) {
            xmlSecOpenSSLError("AES_set_encrypt_key", NULL);
            return(-1);
        }
        AES_encrypt(in, out, &aesKey);
    } else {
        ret = AES_set_decrypt_key(keyData, 8 * keyLen, &aesKey);
        if(ret != 0) {
            xmlSecOpenSSLError("AES_set_decrypt_key", NULL);
            return(-1);
        }
        AES_decrypt(in, out, &aesKey);
    }

    /* success */
    (*outWritten) = AES_BLOCK_SIZE;
    return(0);
}

#else /* XMLSEC_OPENSSL_API_300 */

static int
xmlSecOpenSSLKWAesEncryptDecrypt(xmlSecOpenSSLKWAesCtxPtr ctx, const xmlSecByte * in, xmlSecSize inSize,
                                xmlSecByte * out, xmlSecSize outSize, xmlSecSize * outWritten,
                                int encrypt) {
    xmlSecByte* keyData;
    xmlSecSize keySize;
    EVP_CIPHER_CTX* cctx = NULL;
    int nOut, inLen, outLen, totalLen;
    int ret;
    int res = -1;

    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->cipher != NULL, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize >= AES_BLOCK_SIZE, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= AES_BLOCK_SIZE, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    keyData = xmlSecBufferGetData(&(ctx->parentCtx.keyBuffer));
    keySize = xmlSecBufferGetSize(&(ctx->parentCtx.keyBuffer));
    xmlSecAssert2(keyData != NULL, -1);
    xmlSecAssert2(keySize > 0, -1);
    xmlSecAssert2(keySize == ctx->parentCtx.keyExpectedSize, -1);

    cctx = EVP_CIPHER_CTX_new();
    if (cctx == NULL) {
        xmlSecOpenSSLError("EVP_CIPHER_CTX_new", NULL);
        goto done;
    }

    ret = EVP_CipherInit_ex2(cctx, ctx->cipher, keyData,
        NULL, ((encrypt != 0) ? 1 : 0), NULL);
    if (ret != 1) {
        xmlSecOpenSSLError("EVP_CIPHER_init_ex2(encrypt)", NULL);
        goto done;
    }

    ret = EVP_CIPHER_CTX_set_padding(cctx, 0);
    if (ret != 1) {
        xmlSecOpenSSLError("EVP_CIPHER_CTX_set_padding)", NULL);
        goto done;
    }

    XMLSEC_SAFE_CAST_SIZE_TO_INT(inSize, inLen, goto done, NULL);
    ret = EVP_CipherUpdate(cctx, out, &nOut, in, inLen);
    if (ret != 1) {
        xmlSecOpenSSLError("EVP_CipherUpdate(encrypt)", NULL);
        goto done;
    }

    outLen = nOut;
    ret = EVP_CipherFinal_ex(cctx, out + outLen, &nOut);
    if (ret != 1) {
        xmlSecOpenSSLError("EVP_CipherFinal_ex(encrypt)", NULL);
        goto done;
    }

    /* success */
    totalLen = outLen + nOut;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(totalLen, (*outWritten), goto done, NULL);
    res = 0;

done:
    if(cctx != NULL) {
        EVP_CIPHER_CTX_free(cctx);
    }
    return(res);
}
#endif /* XMLSEC_OPENSSL_API_300 */

static int
xmlSecOpenSSLKWAesBlockEncrypt(xmlSecTransformPtr transform, const xmlSecByte * in, xmlSecSize inSize,
                               xmlSecByte * out, xmlSecSize outSize,
                               xmlSecSize * outWritten) {
    xmlSecOpenSSLKWAesCtxPtr ctx;

    int ret;

    xmlSecAssert2(xmlSecOpenSSLKWAesCheckId(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWAesSize), -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize >= AES_BLOCK_SIZE, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= AES_BLOCK_SIZE, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    ctx = xmlSecOpenSSLKWAesGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecOpenSSLKWAesEncryptDecrypt(ctx, in, inSize, out, outSize, outWritten, 1); /* encrypt */
    if(ret < 0) {
        xmlSecInternalError("xmlSecOpenSSLKWAesEncryptDecrypt",
            xmlSecTransformGetName(transform));
        return(-1);
    }

    /* success */
    return(0);
}

static int
xmlSecOpenSSLKWAesBlockDecrypt(xmlSecTransformPtr transform, const xmlSecByte * in, xmlSecSize inSize,
                               xmlSecByte * out, xmlSecSize outSize,
                               xmlSecSize * outWritten) {
    xmlSecOpenSSLKWAesCtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecOpenSSLKWAesCheckId(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLKWAesSize), -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize >= AES_BLOCK_SIZE, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= AES_BLOCK_SIZE, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    ctx = xmlSecOpenSSLKWAesGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecOpenSSLKWAesEncryptDecrypt(ctx, in, inSize, out, outSize, outWritten, 0); /* decrypt */
    if(ret < 0) {
        xmlSecInternalError("xmlSecOpenSSLKWAesEncryptDecrypt",
            xmlSecTransformGetName(transform));
        return(-1);
    }

    /* success */
    return(0);
}

#endif /* XMLSEC_NO_AES */
