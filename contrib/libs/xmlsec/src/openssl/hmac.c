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
 * SECTION:hmac
 * @Short_description: HMAC transforms implementation for OpenSSL.
 * @Stability: Private
 *
 * [HMAC Algorithm support](http://www.w3.org/TR/xmldsig-core/#sec-HMAC):
 * The HMAC algorithm (RFC2104 [HMAC]) takes the truncation length in bits
 * as a parameter; if the parameter is not specified then all the bits of the
 * hash are output. An example of an HMAC SignatureMethod element:
 *
 * |[<!-- language="XML" -->
 * <SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#hmac-sha1">
 *   <HMACOutputLength>128</HMACOutputLength>
 * </SignatureMethod>
 * |]
 */

#ifndef XMLSEC_NO_HMAC
#include "globals.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include <openssl/hmac.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/base64.h>
#include <xmlsec/keys.h>
#include <xmlsec/errors.h>
#include <xmlsec/private.h>

#include <xmlsec/openssl/crypto.h>
#include "openssl_compat.h"

#ifdef XMLSEC_OPENSSL_API_300
#error #include <openssl/core_names.h>
#error #include <openssl/param_build.h>
#endif /* XMLSEC_OPENSSL_API_300 */

#include "../cast_helpers.h"
#include "../keysdata_helpers.h"
#include "../transform_helpers.h"


/**************************************************************************
 *
 * Configuration
 *
 *****************************************************************************/

/**
 * xmlSecOpenSSLHmacGetMinOutputLength:
 *
 * DEPRECATED (use @xmlSecTransformHmacGetMinOutputBitsSize instead).
 * Gets the value of min HMAC length.
 *
 * Returns: the min HMAC output length
 */
int xmlSecOpenSSLHmacGetMinOutputLength(void)
{
    xmlSecSize val = xmlSecTransformHmacGetMinOutputBitsSize();
    int res;

    XMLSEC_SAFE_CAST_SIZE_TO_INT(val, res, return(-1), NULL);
    return res;
}

/**
 * xmlSecOpenSSLHmacSetMinOutputLength:
 * @min_length: the new min length
 *
 * DEPRECATED (use @xmlSecTransformHmacSetMinOutputBitsSize instead).
 * Sets the min HMAC output length
 */
void xmlSecOpenSSLHmacSetMinOutputLength(int min_length)
{
    xmlSecSize val;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(min_length, val, return, NULL);
    xmlSecTransformHmacSetMinOutputBitsSize(val);
}

/**************************************************************************
 *
 * Internal OpenSSL HMAC CTX
 *
 *****************************************************************************/
typedef struct _xmlSecOpenSSLHmacCtx            xmlSecOpenSSLHmacCtx, *xmlSecOpenSSLHmacCtxPtr;
struct _xmlSecOpenSSLHmacCtx {
#ifndef XMLSEC_OPENSSL_API_300
    const EVP_MD*       hmacDgst;
    HMAC_CTX*           hmacCtx;
#else /* XMLSEC_OPENSSL_API_300 */
    const char*         evpHmacDgstName;
    EVP_MAC*            evpHmac;
    EVP_MAC_CTX*        evpHmacCtx;
#endif /* XMLSEC_OPENSSL_API_300 */
    int                 ctxInitialized;
    xmlSecByte          dgst[EVP_MAX_MD_SIZE];
    xmlSecSize          dgstSize;       /* dgst size in bits */
};

/**************************************************************************
 *
 * HMAC transforms
 *
 *****************************************************************************/
XMLSEC_TRANSFORM_DECLARE(OpenSSLHmac, xmlSecOpenSSLHmacCtx)
#define xmlSecOpenSSLHmacSize XMLSEC_TRANSFORM_SIZE(OpenSSLHmac)

static int      xmlSecOpenSSLHmacCheckId                        (xmlSecTransformPtr transform);
static int      xmlSecOpenSSLHmacInitialize                     (xmlSecTransformPtr transform);
static void     xmlSecOpenSSLHmacFinalize                       (xmlSecTransformPtr transform);
static int      xmlSecOpenSSLHmacNodeRead                       (xmlSecTransformPtr transform,
                                                                 xmlNodePtr node,
                                                                 xmlSecTransformCtxPtr transformCtx);
static int      xmlSecOpenSSLHmacSetKeyReq                      (xmlSecTransformPtr transform,
                                                                 xmlSecKeyReqPtr keyReq);
static int      xmlSecOpenSSLHmacSetKey                         (xmlSecTransformPtr transform,
                                                                 xmlSecKeyPtr key);
static int      xmlSecOpenSSLHmacVerify                         (xmlSecTransformPtr transform,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize,
                                                                 xmlSecTransformCtxPtr transformCtx);
static int      xmlSecOpenSSLHmacExecute                        (xmlSecTransformPtr transform,
                                                                 int last,
                                                                 xmlSecTransformCtxPtr transformCtx);


static int
xmlSecOpenSSLHmacCheckId(xmlSecTransformPtr transform) {

#ifndef XMLSEC_NO_SHA1
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha1Id)) {
        return(1);
    } else
#endif /* XMLSEC_NO_SHA1 */

#ifndef XMLSEC_NO_SHA224
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha224Id)) {
        return(1);
    } else
#endif /* XMLSEC_NO_SHA224 */

#ifndef XMLSEC_NO_SHA256
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha256Id)) {
        return(1);
    } else
#endif /* XMLSEC_NO_SHA256 */

#ifndef XMLSEC_NO_SHA384
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha384Id)) {
        return(1);
    } else
#endif /* XMLSEC_NO_SHA384 */

#ifndef XMLSEC_NO_SHA512
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha512Id)) {
        return(1);
    } else
#endif /* XMLSEC_NO_SHA512 */

#ifndef XMLSEC_NO_RIPEMD160
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacRipemd160Id)) {
        return(1);
    } else
#endif /* XMLSEC_NO_RIPEMD160 */

#ifndef XMLSEC_NO_MD5
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacMd5Id)) {
        return(1);
    } else
#endif /* XMLSEC_NO_MD5 */

    /* not found */
    {
        return(0);
    }
}

/* small helper macro to reduce clutter in the code */
#ifndef XMLSEC_OPENSSL_API_300
#define XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, digestVal, digestNameVal) \
    (ctx)->hmacDgst = (digestVal)
#else /* XMLSEC_OPENSSL_API_300 */
#define XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, digestVal, digestNameVal) \
    (ctx)->evpHmacDgstName = (digestNameVal)
#endif /* XMLSEC_OPENSSL_API_300 */

static int
xmlSecOpenSSLHmacInitialize(xmlSecTransformPtr transform) {
    xmlSecOpenSSLHmacCtxPtr ctx;

    xmlSecAssert2(xmlSecOpenSSLHmacCheckId(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLHmacSize), -1);

    ctx = xmlSecOpenSSLHmacGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    /* initialize context */
    memset(ctx, 0, sizeof(xmlSecOpenSSLHmacCtx));

#ifndef XMLSEC_NO_SHA1
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha1Id)) {
        XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, EVP_sha1(), OSSL_DIGEST_NAME_SHA1);
    } else
#endif /* XMLSEC_NO_SHA1 */

#ifndef XMLSEC_NO_SHA224
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha224Id)) {
        XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, EVP_sha224(), OSSL_DIGEST_NAME_SHA2_224);
    } else
#endif /* XMLSEC_NO_SHA224 */

#ifndef XMLSEC_NO_SHA256
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha256Id)) {
        XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, EVP_sha256(), OSSL_DIGEST_NAME_SHA2_256);
    } else
#endif /* XMLSEC_NO_SHA256 */

#ifndef XMLSEC_NO_SHA384
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha384Id)) {
        XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, EVP_sha384(), OSSL_DIGEST_NAME_SHA2_384);
    } else
#endif /* XMLSEC_NO_SHA384 */

#ifndef XMLSEC_NO_SHA512
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacSha512Id)) {
        XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, EVP_sha512(), OSSL_DIGEST_NAME_SHA2_512);
    } else
#endif /* XMLSEC_NO_SHA512 */

#ifndef XMLSEC_NO_RIPEMD160
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacRipemd160Id)) {
        XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, EVP_ripemd160(), OSSL_DIGEST_NAME_RIPEMD160);
    } else
#endif /* XMLSEC_NO_RIPEMD160 */

#ifndef XMLSEC_NO_MD5
    if(xmlSecTransformCheckId(transform, xmlSecOpenSSLTransformHmacMd5Id)) {
        XMLSEC_OPENSSL_HMAC_SET_DIGEST(ctx, EVP_md5(), OSSL_DIGEST_NAME_MD5);
    } else
#endif /* XMLSEC_NO_MD5 */

    {
        xmlSecInvalidTransfromError(transform)
        return(-1);
    }

#ifndef XMLSEC_OPENSSL_API_300
    /* create hmac CTX */
    ctx->hmacCtx = HMAC_CTX_new();
    if(ctx->hmacCtx == NULL) {
        xmlSecOpenSSLError("HMAC_CTX_new", xmlSecTransformGetName(transform));
        xmlSecOpenSSLHmacFinalize(transform);
        return(-1);
    }
#else /* XMLSEC_OPENSSL_API_300 */
    ctx->evpHmac = EVP_MAC_fetch(xmlSecOpenSSLGetLibCtx(), OSSL_MAC_NAME_HMAC, NULL);
    if (ctx->evpHmac == NULL) {
        xmlSecOpenSSLError("EVP_MAC_fetch", xmlSecTransformGetName(transform));
        xmlSecOpenSSLHmacFinalize(transform);
        return(-1);
    }
    ctx->evpHmacCtx = EVP_MAC_CTX_new(ctx->evpHmac);
    if (ctx->evpHmacCtx == NULL) {
        xmlSecOpenSSLError("EVP_MAC_CTX_new", xmlSecTransformGetName(transform));
        xmlSecOpenSSLHmacFinalize(transform);
        return(-1);
    }
#endif /* XMLSEC_OPENSSL_API_300 */

    /* done */
    return(0);
}

static void
xmlSecOpenSSLHmacFinalize(xmlSecTransformPtr transform) {
    xmlSecOpenSSLHmacCtxPtr ctx;

    xmlSecAssert(xmlSecOpenSSLHmacCheckId(transform));
    xmlSecAssert(xmlSecTransformCheckSize(transform, xmlSecOpenSSLHmacSize));

    ctx = xmlSecOpenSSLHmacGetCtx(transform);
    xmlSecAssert(ctx != NULL);

#ifndef XMLSEC_OPENSSL_API_300
    if(ctx->hmacCtx != NULL) {
        HMAC_CTX_free(ctx->hmacCtx);
    }
#else /* XMLSEC_OPENSSL_API_300 */
    if(ctx->evpHmacCtx != NULL) {
        EVP_MAC_CTX_free(ctx->evpHmacCtx);
    }
    if (ctx->evpHmac != NULL) {
        EVP_MAC_free(ctx->evpHmac);
    }
#endif /* XMLSEC_OPENSSL_API_300 */

    memset(ctx, 0, sizeof(xmlSecOpenSSLHmacCtx));
}

static int
xmlSecOpenSSLHmacNodeRead(xmlSecTransformPtr transform, xmlNodePtr node,
                          xmlSecTransformCtxPtr transformCtx ATTRIBUTE_UNUSED) {
    xmlSecOpenSSLHmacCtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecOpenSSLHmacCheckId(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLHmacSize), -1);
    xmlSecAssert2(node!= NULL, -1);
    UNREFERENCED_PARAMETER(transformCtx);

    ctx = xmlSecOpenSSLHmacGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    ret = xmlSecTransformHmacReadOutputBitsSize(node, ctx->dgstSize, &ctx->dgstSize);
    if (ret < 0) {
        xmlSecInternalError("xmlSecTransformHmacReadOutputBitsSize()",
            xmlSecTransformGetName(transform));
        return(-1);
    }

    return(0);
}

static int
xmlSecOpenSSLHmacSetKeyReq(xmlSecTransformPtr transform,  xmlSecKeyReqPtr keyReq) {
    xmlSecAssert2(xmlSecOpenSSLHmacCheckId(transform), -1);
    xmlSecAssert2((transform->operation == xmlSecTransformOperationSign)
               || (transform->operation == xmlSecTransformOperationVerify), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLHmacSize), -1);
    xmlSecAssert2(keyReq != NULL, -1);

    keyReq->keyId   = xmlSecOpenSSLKeyDataHmacId;
    keyReq->keyType = xmlSecKeyDataTypeSymmetric;
    if(transform->operation == xmlSecTransformOperationSign) {
        keyReq->keyUsage = xmlSecKeyUsageSign;
    } else {
        keyReq->keyUsage = xmlSecKeyUsageVerify;
    }

    return(0);
}

#ifndef XMLSEC_OPENSSL_API_300
static int
xmlSecOpenSSLHmacSetKeyImpl(xmlSecOpenSSLHmacCtxPtr ctx, const xmlSecByte* key, xmlSecSize keySize) {
    int keyLen;
    int ret;

    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->hmacCtx != NULL, -1);
    xmlSecAssert2(ctx->hmacDgst != NULL, -1);
    xmlSecAssert2(key != NULL, -1);
    xmlSecAssert2(keySize > 0, -1);

    XMLSEC_SAFE_CAST_SIZE_TO_INT(keySize, keyLen, return(-1), NULL);
    ret = HMAC_Init_ex(ctx->hmacCtx, key, keyLen, ctx->hmacDgst, NULL);
    if(ret != 1) {
        xmlSecOpenSSLError("HMAC_Init_ex", NULL);
        return(-1);
    }

    /* success */
    return(0);
}

#else /* XMLSEC_OPENSSL_API_300 */

static int
xmlSecOpenSSLHmacSetKeyImpl(xmlSecOpenSSLHmacCtxPtr ctx, const xmlSecByte* key, xmlSecSize keySize) {
    OSSL_PARAM_BLD* param_bld = NULL;
    OSSL_PARAM* params = NULL;
    int ret;
    int res = -1;

    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->evpHmacCtx != NULL, -1);
    xmlSecAssert2(ctx->evpHmacDgstName != NULL, -1);
    xmlSecAssert2(key != NULL, -1);
    xmlSecAssert2(keySize > 0, -1);

    param_bld = OSSL_PARAM_BLD_new();
    if (param_bld == NULL) {
        xmlSecOpenSSLError("OSSL_PARAM_BLD_new", NULL);
        goto done;
    }

    ret = OSSL_PARAM_BLD_push_utf8_string(param_bld, OSSL_MAC_PARAM_DIGEST,
        ctx->evpHmacDgstName, strlen(ctx->evpHmacDgstName));
    if(ret != 1) {
        xmlSecOpenSSLError("OSSL_PARAM_BLD_push_utf8_string", NULL);
        goto done;
    }

    params = OSSL_PARAM_BLD_to_param(param_bld);
    if (params == NULL) {
        xmlSecOpenSSLError("OSSL_PARAM_BLD_to_param", NULL);
        goto done;
    }

    ret = EVP_MAC_init(ctx->evpHmacCtx, key, keySize, params);
    if (ret != 1) {
        xmlSecOpenSSLError("EVP_MAC_init", NULL);
        goto done;
    }

    /* success */
    res = 0;

done:
    if(params != NULL) {
        OSSL_PARAM_free(params);
    }
    if(param_bld != NULL) {
        OSSL_PARAM_BLD_free(param_bld);
    }
    return(res);

}

#endif /* XMLSEC_OPENSSL_API_300 */

static int
xmlSecOpenSSLHmacSetKey(xmlSecTransformPtr transform, xmlSecKeyPtr key) {
    xmlSecOpenSSLHmacCtxPtr ctx;
    xmlSecKeyDataPtr value;
    xmlSecBufferPtr buffer;
    int ret;

    xmlSecAssert2(xmlSecOpenSSLHmacCheckId(transform), -1);
    xmlSecAssert2((transform->operation == xmlSecTransformOperationSign) || (transform->operation == xmlSecTransformOperationVerify), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLHmacSize), -1);
    xmlSecAssert2(key != NULL, -1);

    ctx = xmlSecOpenSSLHmacGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->ctxInitialized == 0, -1);

    value = xmlSecKeyGetValue(key);
    xmlSecAssert2(xmlSecKeyDataCheckId(value, xmlSecOpenSSLKeyDataHmacId), -1);

    buffer = xmlSecKeyDataBinaryValueGetBuffer(value);
    xmlSecAssert2(buffer != NULL, -1);

    if(xmlSecBufferGetSize(buffer) == 0) {
        xmlSecInvalidZeroKeyDataSizeError(xmlSecTransformGetName(transform));
       return(-1);
    }
    xmlSecAssert2(xmlSecBufferGetData(buffer) != NULL, -1);

    ret = xmlSecOpenSSLHmacSetKeyImpl(ctx, xmlSecBufferGetData(buffer), xmlSecBufferGetSize(buffer));
    if(ret < 0) {
        xmlSecInternalError("xmlSecOpenSSLHmacSetKeyImpl", xmlSecTransformGetName(transform));
       return(-1);
    }

    /* success */
    ctx->ctxInitialized = 1;
    return(0);
}

static int
xmlSecOpenSSLHmacVerify(xmlSecTransformPtr transform,
                        const xmlSecByte* data, xmlSecSize dataSize,
                        xmlSecTransformCtxPtr transformCtx ATTRIBUTE_UNUSED) {
    static xmlSecByte last_byte_masks[] =
                { 0xFF, 0x80, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC, 0xFE };

    xmlSecOpenSSLHmacCtxPtr ctx;
    xmlSecByte mask;

    xmlSecAssert2(xmlSecTransformIsValid(transform), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLHmacSize), -1);
    xmlSecAssert2(transform->operation == xmlSecTransformOperationVerify, -1);
    xmlSecAssert2(transform->status == xmlSecTransformStatusFinished, -1);
    xmlSecAssert2(data != NULL, -1);
    UNREFERENCED_PARAMETER(transformCtx);

    ctx = xmlSecOpenSSLHmacGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->dgstSize > 0, -1);

    /* compare the digest size in bytes */
    if(dataSize != ((ctx->dgstSize + 7) / 8)){
        xmlSecInvalidSizeError("HMAC digest",
                               dataSize, ((ctx->dgstSize + 7) / 8),
                               xmlSecTransformGetName(transform));
        transform->status = xmlSecTransformStatusFail;
        return(0);
    }

    /* we check the last byte separately */
    xmlSecAssert2(dataSize > 0, -1);
    mask = last_byte_masks[ctx->dgstSize % 8];
    if((ctx->dgst[dataSize - 1] & mask) != (data[dataSize - 1]  & mask)) {
        xmlSecOtherError(XMLSEC_ERRORS_R_DATA_NOT_MATCH,
                         xmlSecTransformGetName(transform),
                         "data and digest do not match (last byte)");
        transform->status = xmlSecTransformStatusFail;
        return(0);
    }

    /* now check the rest of the digest */
    if((dataSize > 1) && (memcmp(ctx->dgst, data, dataSize - 1) != 0)) {
        xmlSecOtherError(XMLSEC_ERRORS_R_DATA_NOT_MATCH,
                         xmlSecTransformGetName(transform),
                         "data and digest do not match");
        transform->status = xmlSecTransformStatusFail;
        return(0);
    }

    transform->status = xmlSecTransformStatusOk;
    return(0);
}

static int
xmlSecOpenSSLHmacExecute(xmlSecTransformPtr transform, int last, xmlSecTransformCtxPtr transformCtx) {
    xmlSecOpenSSLHmacCtxPtr ctx;
    xmlSecBufferPtr in, out;
    int ret;

    xmlSecAssert2(xmlSecTransformIsValid(transform), -1);
    xmlSecAssert2((transform->operation == xmlSecTransformOperationSign) || (transform->operation == xmlSecTransformOperationVerify), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecOpenSSLHmacSize), -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    in = &(transform->inBuf);
    out = &(transform->outBuf);

    ctx = xmlSecOpenSSLHmacGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->ctxInitialized != 0, -1);

    if(transform->status == xmlSecTransformStatusNone) {
        /* we should be already initialized when we set key */
        transform->status = xmlSecTransformStatusWorking;
    }

    if(transform->status == xmlSecTransformStatusWorking) {
        xmlSecSize inSize;

        inSize = xmlSecBufferGetSize(in);
        if(inSize > 0) {
#ifndef XMLSEC_OPENSSL_API_300
            xmlSecAssert2(ctx->hmacCtx != NULL, -1);

            ret = HMAC_Update(ctx->hmacCtx, xmlSecBufferGetData(in), inSize);
            if(ret != 1) {
                xmlSecOpenSSLError("HMAC_Update",
                                   xmlSecTransformGetName(transform));
                return(-1);
            }
#else /* XMLSEC_OPENSSL_API_300 */
            xmlSecAssert2(ctx->evpHmacCtx != NULL, -1);

            ret = EVP_MAC_update(ctx->evpHmacCtx, xmlSecBufferGetData(in), inSize);
            if(ret != 1) {
                xmlSecOpenSSLError("EVP_MAC_update",
                                   xmlSecTransformGetName(transform));
                return(-1);
            }
#endif /* XMLSEC_OPENSSL_API_300 */

            ret = xmlSecBufferRemoveHead(in, inSize);
            if(ret < 0) {
                xmlSecInternalError2("xmlSecBufferRemoveHead",
                                     xmlSecTransformGetName(transform),
                                     "size=" XMLSEC_SIZE_FMT, inSize);
                return(-1);
            }
        }

        if(last) {
            xmlSecSize dgstSize;

#ifndef XMLSEC_OPENSSL_API_300
            unsigned int dgstLen = 0;

            xmlSecAssert2(ctx->hmacCtx != NULL, -1);
            ret = HMAC_Final(ctx->hmacCtx, ctx->dgst, &dgstLen);
            if(ret != 1) {
                xmlSecOpenSSLError("HMAC_Final", xmlSecTransformGetName(transform));
                return(-1);
            }
            XMLSEC_SAFE_CAST_UINT_TO_SIZE(dgstLen, dgstSize, return(-1), xmlSecTransformGetName(transform));
#else /* XMLSEC_OPENSSL_API_300 */
            size_t dgstSizeT = 0;

            xmlSecAssert2(ctx->evpHmacCtx != NULL, -1);
            ret = EVP_MAC_final(ctx->evpHmacCtx, ctx->dgst, &dgstSizeT, sizeof(ctx->dgst));
            if(ret != 1) {
                xmlSecOpenSSLError("EVP_MAC_final", xmlSecTransformGetName(transform));
                return(-1);
            }
            XMLSEC_SAFE_CAST_SIZE_T_TO_SIZE(dgstSizeT, dgstSize, return(-1), xmlSecTransformGetName(transform));
#endif /* XMLSEC_OPENSSL_API_300 */
            xmlSecAssert2(dgstSize > 0, -1);

            /* check/set the result digest size */
            if(ctx->dgstSize == 0) {
                ctx->dgstSize = dgstSize * 8; /* no dgst size specified, use all we have */
            } else if(ctx->dgstSize <= 8 * dgstSize) {
                xmlSecSize adjustedDigestSize = ((ctx->dgstSize + 7) / 8); /* we need to truncate result digest */
                XMLSEC_SAFE_CAST_SIZE_TO_UINT(adjustedDigestSize, dgstSize, return(-1), xmlSecTransformGetName(transform));
            } else {
                xmlSecInvalidSizeLessThanError("HMAC digest (bits)",
                    8 * dgstSize, ctx->dgstSize,
                    xmlSecTransformGetName(transform));
                return(-1);
            }

            /* finally write result to output */
            if(transform->operation == xmlSecTransformOperationSign) {
                ret = xmlSecBufferAppend(out, ctx->dgst, dgstSize);
                if(ret < 0) {
                    xmlSecInternalError2("xmlSecBufferAppend",
                        xmlSecTransformGetName(transform),
                        "size=" XMLSEC_SIZE_FMT, dgstSize);
                    return(-1);
                }
            }
            transform->status = xmlSecTransformStatusFinished;
        }
    } else if(transform->status == xmlSecTransformStatusFinished) {
        /* the only way we can get here is if there is no input */
        xmlSecAssert2(xmlSecBufferGetSize(in) == 0, -1);
    } else {
        xmlSecInvalidTransfromStatusError(transform);
        return(-1);
    }

    return(0);
}

#ifndef XMLSEC_NO_MD5

/********************************************************************
 *
 * HMAC MD5
 *
 ********************************************************************/
static xmlSecTransformKlass xmlSecOpenSSLHmacMd5Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLHmacSize,                      /* xmlSecSize objSize */

    xmlSecNameHmacMd5,                          /* const xmlChar* name; */
    xmlSecHrefHmacMd5,                          /* const xmlChar* href; */
    xmlSecTransformUsageSignatureMethod,        /* xmlSecTransformUsage usage; */

    xmlSecOpenSSLHmacInitialize,                /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLHmacFinalize,                  /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecOpenSSLHmacNodeRead,                  /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLHmacSetKeyReq,                 /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    xmlSecOpenSSLHmacSetKey,                    /* xmlSecTransformSetKeyMethod setKey; */
    xmlSecOpenSSLHmacVerify,                    /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLHmacExecute,                   /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformHmacMd5GetKlass:
 *
 * The HMAC-MD5 transform klass.
 *
 * Returns: the HMAC-MD5 transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformHmacMd5GetKlass(void) {
    return(&xmlSecOpenSSLHmacMd5Klass);
}

#endif /* XMLSEC_NO_MD5 */


#ifndef XMLSEC_NO_RIPEMD160
/********************************************************************
 *
 * HMAC RIPEMD160
 *
 ********************************************************************/
static xmlSecTransformKlass xmlSecOpenSSLHmacRipemd160Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLHmacSize,                      /* xmlSecSize objSize */

    xmlSecNameHmacRipemd160,                    /* const xmlChar* name; */
    xmlSecHrefHmacRipemd160,                    /* const xmlChar* href; */
    xmlSecTransformUsageSignatureMethod,        /* xmlSecTransformUsage usage; */

    xmlSecOpenSSLHmacInitialize,                /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLHmacFinalize,                  /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecOpenSSLHmacNodeRead,                  /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLHmacSetKeyReq,                 /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    xmlSecOpenSSLHmacSetKey,                    /* xmlSecTransformSetKeyMethod setKey; */
    xmlSecOpenSSLHmacVerify,                    /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLHmacExecute,                   /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformHmacRipemd160GetKlass:
 *
 * The HMAC-RIPEMD160 transform klass.
 *
 * Returns: the HMAC-RIPEMD160 transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformHmacRipemd160GetKlass(void) {
    return(&xmlSecOpenSSLHmacRipemd160Klass);
}
#endif /* XMLSEC_NO_RIPEMD160 */

#ifndef XMLSEC_NO_SHA1
/********************************************************************
 *
 * HMAC SHA1
 *
 ********************************************************************/
static xmlSecTransformKlass xmlSecOpenSSLHmacSha1Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLHmacSize,                      /* xmlSecSize objSize */

    xmlSecNameHmacSha1,                         /* const xmlChar* name; */
    xmlSecHrefHmacSha1,                         /* const xmlChar* href; */
    xmlSecTransformUsageSignatureMethod,        /* xmlSecTransformUsage usage; */

    xmlSecOpenSSLHmacInitialize,                /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLHmacFinalize,                  /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecOpenSSLHmacNodeRead,                  /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLHmacSetKeyReq,                 /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    xmlSecOpenSSLHmacSetKey,                    /* xmlSecTransformSetKeyMethod setKey; */
    xmlSecOpenSSLHmacVerify,                    /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLHmacExecute,                   /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformHmacSha1GetKlass:
 *
 * The HMAC-SHA1 transform klass.
 *
 * Returns: the HMAC-SHA1 transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformHmacSha1GetKlass(void) {
    return(&xmlSecOpenSSLHmacSha1Klass);
}

#endif /* XMLSEC_NO_SHA1 */

#ifndef XMLSEC_NO_SHA224
/********************************************************************
 *
 * HMAC SHA224
 *
 ********************************************************************/
static xmlSecTransformKlass xmlSecOpenSSLHmacSha224Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLHmacSize,                      /* xmlSecSize objSize */

    xmlSecNameHmacSha224,                       /* const xmlChar* name; */
    xmlSecHrefHmacSha224,                       /* const xmlChar* href; */
    xmlSecTransformUsageSignatureMethod,        /* xmlSecTransformUsage usage; */

    xmlSecOpenSSLHmacInitialize,                /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLHmacFinalize,                  /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecOpenSSLHmacNodeRead,                  /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLHmacSetKeyReq,                 /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    xmlSecOpenSSLHmacSetKey,                    /* xmlSecTransformSetKeyMethod setKey; */
    xmlSecOpenSSLHmacVerify,                    /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLHmacExecute,                   /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformHmacSha224GetKlass:
 *
 * The HMAC-SHA224 transform klass.
 *
 * Returns: the HMAC-SHA224 transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformHmacSha224GetKlass(void) {
    return(&xmlSecOpenSSLHmacSha224Klass);
}

#endif /* XMLSEC_NO_SHA224 */

#ifndef XMLSEC_NO_SHA256
/********************************************************************
 *
 * HMAC SHA256
 *
 ********************************************************************/
static xmlSecTransformKlass xmlSecOpenSSLHmacSha256Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLHmacSize,                      /* xmlSecSize objSize */

    xmlSecNameHmacSha256,                       /* const xmlChar* name; */
    xmlSecHrefHmacSha256,                       /* const xmlChar* href; */
    xmlSecTransformUsageSignatureMethod,        /* xmlSecTransformUsage usage; */

    xmlSecOpenSSLHmacInitialize,                /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLHmacFinalize,                  /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecOpenSSLHmacNodeRead,                  /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLHmacSetKeyReq,                 /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    xmlSecOpenSSLHmacSetKey,                    /* xmlSecTransformSetKeyMethod setKey; */
    xmlSecOpenSSLHmacVerify,                    /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLHmacExecute,                   /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformHmacSha256GetKlass:
 *
 * The HMAC-SHA256 transform klass.
 *
 * Returns: the HMAC-SHA256 transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformHmacSha256GetKlass(void) {
    return(&xmlSecOpenSSLHmacSha256Klass);
}

#endif /* XMLSEC_NO_SHA256 */

#ifndef XMLSEC_NO_SHA384
/********************************************************************
 *
 * HMAC SHA384
 *
 ********************************************************************/
static xmlSecTransformKlass xmlSecOpenSSLHmacSha384Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLHmacSize,                      /* xmlSecSize objSize */

    xmlSecNameHmacSha384,                       /* const xmlChar* name; */
    xmlSecHrefHmacSha384,                       /* const xmlChar* href; */
    xmlSecTransformUsageSignatureMethod,        /* xmlSecTransformUsage usage; */

    xmlSecOpenSSLHmacInitialize,                /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLHmacFinalize,                  /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecOpenSSLHmacNodeRead,                  /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLHmacSetKeyReq,                 /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    xmlSecOpenSSLHmacSetKey,                    /* xmlSecTransformSetKeyMethod setKey; */
    xmlSecOpenSSLHmacVerify,                    /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLHmacExecute,                   /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformHmacSha384GetKlass:
 *
 * The HMAC-SHA384 transform klass.
 *
 * Returns: the HMAC-SHA384 transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformHmacSha384GetKlass(void) {
    return(&xmlSecOpenSSLHmacSha384Klass);
}

#endif /* XMLSEC_NO_SHA384 */

#ifndef XMLSEC_NO_SHA512
/********************************************************************
 *
 * HMAC SHA512
 *
 ********************************************************************/
static xmlSecTransformKlass xmlSecOpenSSLHmacSha512Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecOpenSSLHmacSize,                      /* xmlSecSize objSize */

    xmlSecNameHmacSha512,                       /* const xmlChar* name; */
    xmlSecHrefHmacSha512,                       /* const xmlChar* href; */
    xmlSecTransformUsageSignatureMethod,        /* xmlSecTransformUsage usage; */

    xmlSecOpenSSLHmacInitialize,                /* xmlSecTransformInitializeMethod initialize; */
    xmlSecOpenSSLHmacFinalize,                  /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecOpenSSLHmacNodeRead,                  /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    xmlSecOpenSSLHmacSetKeyReq,                 /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    xmlSecOpenSSLHmacSetKey,                    /* xmlSecTransformSetKeyMethod setKey; */
    xmlSecOpenSSLHmacVerify,                    /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecOpenSSLHmacExecute,                   /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLTransformHmacSha512GetKlass:
 *
 * The HMAC-SHA512 transform klass.
 *
 * Returns: the HMAC-SHA512 transform klass.
 */
xmlSecTransformId
xmlSecOpenSSLTransformHmacSha512GetKlass(void) {
    return(&xmlSecOpenSSLHmacSha512Klass);
}

#endif /* XMLSEC_NO_SHA512 */


#endif /* XMLSEC_NO_HMAC */

