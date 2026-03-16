/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
/**
 * SECTION:kw_aes_des
 * @Short_description: AES/DES Key Transport implementation.
 * @Stability: Private
 *
 */

#include "globals.h"

#include <stdlib.h>
#include <string.h>

#include <libxml/tree.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/buffer.h>
#include <xmlsec/errors.h>

#include "kw_aes_des.h"
#include "cast_helpers.h"
#include "keysdata_helpers.h"

#ifndef XMLSEC_NO_DES


/*********************************************************************
*
* Triple DES helper functions
*
********************************************************************/
static int      xmlSecKWDes3Encode                             (xmlSecKWDes3Id kwDes3Id,
                                                                xmlSecTransformPtr transform,
                                                                const xmlSecByte* in,
                                                                xmlSecSize inSize,
                                                                xmlSecByte* out,
                                                                xmlSecSize outSize,
                                                                xmlSecSize* outWritten);
static int      xmlSecKWDes3Decode                              (xmlSecKWDes3Id kwDes3Id,
                                                                 xmlSecTransformPtr transform,
                                                                 const xmlSecByte* in,
                                                                 xmlSecSize inSize,
                                                                 xmlSecByte* out,
                                                                 xmlSecSize outSize,
                                                                xmlSecSize* outWritten);
static int      xmlSecKWDes3BufferReverse                       (xmlSecByte *buf,
                                                                 xmlSecSize size);

/*********************************************************************
 *
 * Triple DES Key Wrap transform
 *
 ********************************************************************/
int
xmlSecTransformKWDes3Initialize(xmlSecTransformPtr transform, xmlSecTransformKWDes3CtxPtr ctx,
                    xmlSecKWDes3Id kwDes3Id, xmlSecKeyDataId keyId) {
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(kwDes3Id != NULL, -1);
    xmlSecAssert2(keyId != NULL, -1);

    ret = xmlSecBufferInitialize(&(ctx->keyBuffer), 0);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBufferInitialize", xmlSecTransformGetName(transform));
        return(-1);
    }
    ctx->kwDes3Id = kwDes3Id;
    ctx->keyId    = keyId;

    return(0);
}

void
xmlSecTransformKWDes3Finalize(xmlSecTransformPtr transform, xmlSecTransformKWDes3CtxPtr ctx) {
    xmlSecAssert(transform != NULL);
    xmlSecAssert(ctx != NULL);

    xmlSecBufferFinalize(&(ctx->keyBuffer));
}

int
xmlSecTransformKWDes3SetKeyReq(xmlSecTransformPtr transform, xmlSecTransformKWDes3CtxPtr ctx,
                        xmlSecKeyReqPtr keyReq) {
    keyReq->keyId   = ctx->keyId;
    keyReq->keyType = xmlSecKeyDataTypeSymmetric;
    if(transform->operation == xmlSecTransformOperationEncrypt) {
        keyReq->keyUsage = xmlSecKeyUsageEncrypt;
    } else {
        keyReq->keyUsage = xmlSecKeyUsageDecrypt;
    }
    keyReq->keyBitsSize = 8 * XMLSEC_KW_DES3_KEY_LENGTH;
    return(0);
}

int
xmlSecTransformKWDes3SetKey(xmlSecTransformPtr transform, xmlSecTransformKWDes3CtxPtr ctx,
                        xmlSecKeyPtr key) {
    xmlSecBufferPtr buffer;
    xmlSecSize keySize;
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->keyId != NULL, -1);
    xmlSecAssert2(key != NULL, -1);
    xmlSecAssert2(xmlSecKeyDataCheckId(xmlSecKeyGetValue(key), ctx->keyId), -1);

    buffer = xmlSecKeyDataBinaryValueGetBuffer(xmlSecKeyGetValue(key));
    xmlSecAssert2(buffer != NULL, -1);

    keySize = xmlSecBufferGetSize(buffer);
    if(keySize < XMLSEC_KW_DES3_KEY_LENGTH) {
        xmlSecInvalidKeyDataSizeError(keySize, XMLSEC_KW_DES3_KEY_LENGTH,
                xmlSecTransformGetName(transform));
        return(-1);
    }

    ret = xmlSecBufferSetData(&(ctx->keyBuffer), xmlSecBufferGetData(buffer), XMLSEC_KW_DES3_KEY_LENGTH);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBufferSetData(XMLSEC_KW_DES3_KEY_LENGTH)",
            xmlSecTransformGetName(transform));
        return(-1);
    }

    return(0);
}

int
xmlSecTransformKWDes3Execute(xmlSecTransformPtr transform, xmlSecTransformKWDes3CtxPtr ctx, int last) {
    xmlSecBufferPtr in, out;
    xmlSecSize inSize, outSize, keySize;
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2((transform->operation == xmlSecTransformOperationEncrypt) || (transform->operation == xmlSecTransformOperationDecrypt), -1);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->kwDes3Id != NULL, -1);

    keySize = xmlSecBufferGetSize(&(ctx->keyBuffer));
    xmlSecAssert2(keySize == XMLSEC_KW_DES3_KEY_LENGTH, -1);

    in = &(transform->inBuf);
    out = &(transform->outBuf);
    inSize = xmlSecBufferGetSize(in);
    outSize = xmlSecBufferGetSize(out);
    xmlSecAssert2(outSize == 0, -1);

    if(transform->status == xmlSecTransformStatusNone) {
        transform->status = xmlSecTransformStatusWorking;
    }

    if((transform->status == xmlSecTransformStatusWorking) && (last == 0)) {
        /* just do nothing */
    } else  if((transform->status == xmlSecTransformStatusWorking) && (last != 0)) {
        if((inSize % XMLSEC_KW_DES3_BLOCK_LENGTH) != 0) {
            xmlSecInvalidSizeNotMultipleOfError("Input data",
                                inSize, XMLSEC_KW_DES3_BLOCK_LENGTH,
                                xmlSecTransformGetName(transform));
            return(-1);
        }

        if(transform->operation == xmlSecTransformOperationEncrypt) {
            /* the encoded key might be 16 bytes longer plus one block just in case */
            outSize = inSize + XMLSEC_KW_DES3_IV_LENGTH +
                               XMLSEC_KW_DES3_BLOCK_LENGTH +
                               XMLSEC_KW_DES3_BLOCK_LENGTH;
        } else {
            /* just in case, add a block */
            outSize = inSize + XMLSEC_KW_DES3_BLOCK_LENGTH;
        }

        ret = xmlSecBufferSetMaxSize(out, outSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferSetMaxSize",
                                 xmlSecTransformGetName(transform),
                                 "size=" XMLSEC_SIZE_FMT, outSize);
            return(-1);
        }

        if(transform->operation == xmlSecTransformOperationEncrypt) {
            ret = xmlSecKWDes3Encode(ctx->kwDes3Id, transform, xmlSecBufferGetData(in), inSize,
                xmlSecBufferGetData(out), outSize, &outSize);
            if(ret < 0) {
                xmlSecInternalError4("xmlSecKWDes3Encode", xmlSecTransformGetName(transform),
                    "keySize=" XMLSEC_SIZE_FMT "; inSize=" XMLSEC_SIZE_FMT "; outSize=" XMLSEC_SIZE_FMT,
                    keySize, inSize, outSize);

                return(-1);
            }
        } else {
            ret = xmlSecKWDes3Decode(ctx->kwDes3Id, transform, xmlSecBufferGetData(in), inSize,
                xmlSecBufferGetData(out), outSize, &outSize);
            if(ret < 0) {
                xmlSecInternalError4("xmlSecKWDes3Decode", xmlSecTransformGetName(transform),
                    "keySize=" XMLSEC_SIZE_FMT "; inSize=" XMLSEC_SIZE_FMT "; outSize=" XMLSEC_SIZE_FMT,
                    keySize, inSize, outSize);

                return(-1);
            }
        }

        ret = xmlSecBufferSetSize(out, outSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferSetSize", xmlSecTransformGetName(transform),
                "size=" XMLSEC_SIZE_FMT, outSize);
            return(-1);
        }

        ret = xmlSecBufferRemoveHead(in, inSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferRemoveHead",
                                 xmlSecTransformGetName(transform),
                                 "size=" XMLSEC_SIZE_FMT, inSize);
            return(-1);
        }

        transform->status = xmlSecTransformStatusFinished;
    } else if(transform->status == xmlSecTransformStatusFinished) {
        /* the only way we can get here is if there is no input */
        xmlSecAssert2(xmlSecBufferGetSize(&(transform->inBuf)) == 0, -1);
    } else {
        xmlSecInvalidTransfromStatusError(transform);
        return(-1);
    }

    return(0);
}




/********************************************************************
 *
 * CMS Triple DES Key Wrap
 *
 * http://www.w3.org/TR/xmlenc-core/#sec-Alg-SymmetricKeyWrap
 *
 * The following algorithm wraps (encrypts) a key (the wrapped key, WK)
 * under a TRIPLEDES key-encryption-key (KEK) as specified in [CMS-Algorithms]:
 *
 * 1. Represent the key being wrapped as an octet sequence. If it is a
 *    TRIPLEDES key, this is 24 octets (192 bits) with odd parity bit as
 *    the bottom bit of each octet.
 * 2. Compute the CMS key checksum (section 5.6.1) call this CKS.
 * 3. Let WKCKS = WK || CKS, where || is concatenation.
 * 4. Generate 8 random octets [RANDOM] and call this IV.
 * 5. Encrypt WKCKS in CBC mode using KEK as the key and IV as the
 *    initialization vector. Call the results TEMP1.
 * 6. Left TEMP2 = IV || TEMP1.
 * 7. Reverse the order of the octets in TEMP2 and call the result TEMP3.
 * 8. Encrypt TEMP3 in CBC mode using the KEK and an initialization vector
 *    of 0x4adda22c79e82105. The resulting cipher text is the desired result.
 *    It is 40 octets long if a 168 bit key is being wrapped.
 *
 * The following algorithm unwraps (decrypts) a key as specified in
 * [CMS-Algorithms]:
 *
 * 1. Check if the length of the cipher text is reasonable given the key type.
 *    It must be 40 bytes for a 168 bit key and either 32, 40, or 48 bytes for
 *    a 128, 192, or 256 bit key. If the length is not supported or inconsistent
 *    with the algorithm for which the key is intended, return error.
 * 2. Decrypt the cipher text with TRIPLEDES in CBC mode using the KEK and
 *    an initialization vector (IV) of 0x4adda22c79e82105. Call the output TEMP3.
 * 3. Reverse the order of the octets in TEMP3 and call the result TEMP2.
 * 4. Decompose TEMP2 into IV, the first 8 octets, and TEMP1, the remaining
 *    octets.
 * 5. Decrypt TEMP1 using TRIPLEDES in CBC mode using the KEK and the IV found
 *    in the previous step. Call the result WKCKS.
 * 6. Decompose WKCKS. CKS is the last 8 octets and WK, the wrapped key, are
 *    those octets before the CKS.
 * 7. Calculate a CMS key checksum (section 5.6.1) over the WK and compare
 *    with the CKS extracted in the above step. If they are not equal, return
 *    error.
 * 8. WK is the wrapped key, now extracted for use in data decryption.
 *
 ********************************************************************/
static xmlSecByte xmlSecKWDes3Iv[XMLSEC_KW_DES3_IV_LENGTH] = {
    0x4a, 0xdd, 0xa2, 0x2c, 0x79, 0xe8, 0x21, 0x05
};

int
xmlSecKWDes3Encode(xmlSecKWDes3Id kwDes3Id, xmlSecTransformPtr transform,
                  const xmlSecByte *in, xmlSecSize inSize,
                  xmlSecByte *out, xmlSecSize outSize,
                  xmlSecSize* outWritten) {
    xmlSecByte sha1[XMLSEC_KW_DES3_SHA_DIGEST_LENGTH];
    xmlSecByte iv[XMLSEC_KW_DES3_IV_LENGTH];
    xmlSecSize tmpSize, outWritten2;
    int ret;

    xmlSecAssert2(xmlSecKWDes3CheckId(kwDes3Id), -1);
    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize > 0, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= inSize + XMLSEC_KW_DES3_BLOCK_LENGTH + XMLSEC_KW_DES3_IV_LENGTH, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    /* step 2: calculate sha1 and CMS */
    outWritten2 = 0;
    ret = kwDes3Id->sha1(transform, in, inSize, sha1, sizeof(sha1), &outWritten2);
    if((ret < 0) || (outWritten2 != sizeof(sha1))) {
        xmlSecInternalError2("kwDes3Id->sha1", NULL,
            "outWritten2=" XMLSEC_SIZE_FMT, outWritten2);
        return(-1);
    }

    /* step 3: construct WKCKS as WK || CKS */
    memcpy(out, in, inSize);
    memcpy(out + inSize, sha1, XMLSEC_KW_DES3_BLOCK_LENGTH);

    /* step 4: generate random iv */
    outWritten2 = 0;
    ret = kwDes3Id->generateRandom(transform, iv, sizeof(iv), &outWritten2);
    if((ret < 0) || (outWritten2 != sizeof(iv))) {
        xmlSecInternalError2("kwDes3Id->generateRandom", NULL,
            "outWritten2=" XMLSEC_SIZE_FMT, outWritten2);
        return(-1);
    }

    /* step 5: first encryption, result is TEMP1 */
    outWritten2 = 0;
    ret = kwDes3Id->encrypt(transform, iv, sizeof(iv),
        out, inSize + XMLSEC_KW_DES3_BLOCK_LENGTH,
        out, outSize, &outWritten2);
    if(ret < 0) {
        xmlSecInternalError("kwDes3Id->encrypt", NULL);
        return(-1);
    }
    if((inSize + XMLSEC_KW_DES3_BLOCK_LENGTH) != outWritten2) {
        xmlSecInvalidSizeError("kwDes3Id->encrypt",
            outWritten2, (inSize + XMLSEC_KW_DES3_BLOCK_LENGTH), NULL);
        return(-1);
    }

    /* step 6: construct TEMP2=IV || TEMP1 */
    memmove(out + XMLSEC_KW_DES3_IV_LENGTH, out, outWritten2);
    memcpy(out, iv, XMLSEC_KW_DES3_IV_LENGTH);
    tmpSize = XMLSEC_KW_DES3_IV_LENGTH + outWritten2;

    /* step 7: reverse octets order, result is TEMP3 */
    ret = xmlSecKWDes3BufferReverse(out, tmpSize);
    if(ret < 0) {
        xmlSecInternalError("xmlSecKWDes3BufferReverse", NULL);
        return(-1);
    }

    /* step 8: second encryption with static IV */
    outWritten2 = 0;
    ret = kwDes3Id->encrypt(transform, xmlSecKWDes3Iv, sizeof(xmlSecKWDes3Iv),
        out, tmpSize, out, outSize, &outWritten2);
    if(ret < 0) {
        xmlSecInternalError("kwDes3Id->encrypt", NULL);
        return(-1);
    }
    if(tmpSize != outWritten2) {
        xmlSecInvalidSizeError("kwDes3Id->encrypt", tmpSize, outWritten2, NULL);
        return(-1);
    }
    (*outWritten) = outWritten2;

    /* done */
    return(0);
}

int
xmlSecKWDes3Decode(xmlSecKWDes3Id kwDes3Id, xmlSecTransformPtr transform,
                  const xmlSecByte *in, xmlSecSize inSize,
                  xmlSecByte *out, xmlSecSize outSize,
                  xmlSecSize* outWritten)
{
    xmlSecByte sha1[XMLSEC_KW_DES3_SHA_DIGEST_LENGTH];
    xmlSecBufferPtr tmp = NULL;
    xmlSecByte* tmpBuf;
    xmlSecSize tmpSize, outSz, outWritten2;
    int ret;
    int res = -1;

    xmlSecAssert2(xmlSecKWDes3CheckId(kwDes3Id), -1);
    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize > 0, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= inSize, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    /* step 2: first decryption with static IV, result is TEMP3 */
    tmp = xmlSecBufferCreate(inSize);
    if(tmp == NULL) {
        xmlSecInternalError2("xmlSecBufferCreate", NULL,
            "inSize=" XMLSEC_SIZE_FMT, inSize);
        goto done;
    }
    tmpBuf = xmlSecBufferGetData(tmp);
    tmpSize = xmlSecBufferGetMaxSize(tmp);

    outWritten2 = 0;
    ret = kwDes3Id->decrypt(transform, xmlSecKWDes3Iv, sizeof(xmlSecKWDes3Iv),
        in, inSize, tmpBuf, tmpSize, &outWritten2);
    if(ret < 0) {
        xmlSecInternalError("kwDes3Id->decrypt", NULL);
        goto done;
    }
    if (outWritten2 < XMLSEC_KW_DES3_IV_LENGTH) {
        xmlSecInvalidSizeLessThanError("kwDes3Id->decrypt(iv)",
            outWritten2, XMLSEC_KW_DES3_IV_LENGTH, NULL);
        goto done;
    }
    tmpSize = outWritten2;

    /* step 3: reverse octets order in TEMP3, result is TEMP2 */
    ret = xmlSecKWDes3BufferReverse(xmlSecBufferGetData(tmp), tmpSize);
    if(ret < 0) {
        xmlSecInternalError("xmlSecKWDes3BufferReverse", NULL);
        goto done;
    }

    /* steps 4 and 5: get IV and decrypt second time, result is WKCKS */
    outWritten2 = 0;
    ret = kwDes3Id->decrypt(transform,
        tmpBuf, XMLSEC_KW_DES3_IV_LENGTH,
        tmpBuf + XMLSEC_KW_DES3_IV_LENGTH,
        tmpSize - XMLSEC_KW_DES3_IV_LENGTH,
        out, outSize, &outWritten2);
    if(ret < 0) {
        xmlSecInternalError("kwDes3Id->decrypt", NULL);
        goto done;
    }
    if (outWritten2 < XMLSEC_KW_DES3_BLOCK_LENGTH) {
        xmlSecInvalidSizeLessThanError("kwDes3Id->decrypt(block)",
            outWritten2, XMLSEC_KW_DES3_BLOCK_LENGTH, NULL);
        goto done;
    }
    outSz = outWritten2 - XMLSEC_KW_DES3_BLOCK_LENGTH;

    /* steps 6 and 7: calculate SHA1 and validate it */
    outWritten2 = 0;
    ret = kwDes3Id->sha1(transform, out, outSz, sha1, sizeof(sha1), &outWritten2);
    if((ret < 0) || (outWritten2 != sizeof(sha1))) {
        xmlSecInternalError2("kwDes3Id->sha1", NULL, "outWritten2=" XMLSEC_SIZE_FMT, outWritten2);
        goto done;
    }

    /* check sha1 */
    xmlSecAssert2(XMLSEC_KW_DES3_BLOCK_LENGTH <= sizeof(sha1), -1);
    if(memcmp(sha1, out + outSz, XMLSEC_KW_DES3_BLOCK_LENGTH) != 0) {
        xmlSecInvalidDataError("SHA1 does not match", NULL);
        goto done;
    }

    /* success */
    (*outWritten) = outSz;
    res = 0;

done:
    if(tmp != NULL) {
        xmlSecBufferDestroy(tmp);
    }
    return(res);
}

static int
xmlSecKWDes3BufferReverse(xmlSecByte *buf, xmlSecSize size)
{
    xmlSecByte * p;
    xmlSecByte ch;

    xmlSecAssert2(buf != NULL, -1);
    xmlSecAssert2(size > 0, -1);

    for(p = buf + size - 1; p >= buf; ++buf, --p) {
        ch = (*p);
        (*p) = (*buf);
        (*buf) = ch;
    }
    return (0);
}
#endif /* XMLSEC_NO_DES */


#ifndef XMLSEC_NO_AES
static int      xmlSecKWAesEncode                   (xmlSecKWAesId kwAesId,
                                                    xmlSecTransformPtr transform,
                                                    const xmlSecByte* in,
                                                    xmlSecSize inSize,
                                                    xmlSecByte* out,
                                                    xmlSecSize outSize,
                                                    xmlSecSize* outWritten);

static int      xmlSecKWAesDecode                   (xmlSecKWAesId kwAesId,
                                                    xmlSecTransformPtr transform,
                                                    const xmlSecByte* in,
                                                    xmlSecSize inSize,
                                                    xmlSecByte* out,
                                                    xmlSecSize outSize,
                                                    xmlSecSize* outWritten);

int
xmlSecTransformKWAesInitialize(xmlSecTransformPtr transform, xmlSecTransformKWAesCtxPtr ctx,
                               xmlSecKWAesId kwAesId, xmlSecKeyDataId keyId,
                               xmlSecSize keyExpectedSize) {
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(kwAesId != NULL, -1);
    xmlSecAssert2(keyId != NULL, -1);
    xmlSecAssert2(keyExpectedSize > 0, -1);

    ret = xmlSecBufferInitialize(&(ctx->keyBuffer), 0);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBufferInitialize", xmlSecTransformGetName(transform));
        return(-1);
    }
    ctx->kwAesId         = kwAesId;
    ctx->keyId           = keyId;
    ctx->keyExpectedSize = keyExpectedSize;

    return(0);
}

void
xmlSecTransformKWAesFinalize(xmlSecTransformPtr transform, xmlSecTransformKWAesCtxPtr ctx) {
    xmlSecAssert(transform != NULL);
    xmlSecAssert(ctx != NULL);

    xmlSecBufferFinalize(&(ctx->keyBuffer));
}

int
xmlSecTransformKWAesSetKeyReq(xmlSecTransformPtr transform, xmlSecTransformKWAesCtxPtr ctx,
                              xmlSecKeyReqPtr keyReq) {
    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->keyId != NULL, -1);
    xmlSecAssert2(keyReq != NULL, -1);

    keyReq->keyId   = ctx->keyId;;
    keyReq->keyType = xmlSecKeyDataTypeSymmetric;
    if(transform->operation == xmlSecTransformOperationEncrypt) {
        keyReq->keyUsage = xmlSecKeyUsageEncrypt;
    } else {
        keyReq->keyUsage = xmlSecKeyUsageDecrypt;
    }
    keyReq->keyBitsSize = 8 * ctx->keyExpectedSize;
    return(0);
}

int
xmlSecTransformKWAesSetKey(xmlSecTransformPtr transform, xmlSecTransformKWAesCtxPtr ctx,
                           xmlSecKeyPtr key) {
    xmlSecBufferPtr buffer;
    xmlSecSize keySize;
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->keyId != NULL, -1);
    xmlSecAssert2(key != NULL, -1);
    xmlSecAssert2(xmlSecKeyDataCheckId(xmlSecKeyGetValue(key), ctx->keyId), -1);

    buffer = xmlSecKeyDataBinaryValueGetBuffer(xmlSecKeyGetValue(key));
    xmlSecAssert2(buffer != NULL, -1);

    keySize = xmlSecBufferGetSize(buffer);
    if(keySize < ctx->keyExpectedSize) {
        xmlSecInvalidKeyDataSizeError(keySize, ctx->keyExpectedSize,
                xmlSecTransformGetName(transform));
        return(-1);
    }

    ret = xmlSecBufferSetData(&(ctx->keyBuffer), xmlSecBufferGetData(buffer),
        ctx->keyExpectedSize);
    if(ret < 0) {
        xmlSecInternalError2("xmlSecBufferSetData", xmlSecTransformGetName(transform),
            "expected-size=" XMLSEC_SIZE_FMT, ctx->keyExpectedSize);
        return(-1);
    }

    return(0);
}

int
xmlSecTransformKWAesExecute(xmlSecTransformPtr transform, xmlSecTransformKWAesCtxPtr ctx, int last) {
    xmlSecBufferPtr in, out;
    xmlSecSize inSize, outSize, keySize;
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2((transform->operation == xmlSecTransformOperationEncrypt) || (transform->operation == xmlSecTransformOperationDecrypt), -1);
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(ctx->kwAesId != NULL, -1);

    keySize = xmlSecBufferGetSize(&(ctx->keyBuffer));
    xmlSecAssert2(keySize == ctx->keyExpectedSize, -1);

    in = &(transform->inBuf);
    out = &(transform->outBuf);
    inSize = xmlSecBufferGetSize(in);
    outSize = xmlSecBufferGetSize(out);
    xmlSecAssert2(outSize == 0, -1);

    if(transform->status == xmlSecTransformStatusNone) {
        transform->status = xmlSecTransformStatusWorking;
    }

    if((transform->status == xmlSecTransformStatusWorking) && (last == 0)) {
        /* just do nothing */
    } else  if((transform->status == xmlSecTransformStatusWorking) && (last != 0)) {
        if((inSize % XMLSEC_KW_AES_IN_SIZE_MULTIPLY) != 0) {
            xmlSecInvalidSizeNotMultipleOfError("Input data",
                inSize, XMLSEC_KW_AES_IN_SIZE_MULTIPLY,
                xmlSecTransformGetName(transform));
            return(-1);
        }

        if(transform->operation == xmlSecTransformOperationEncrypt) {
            /* the encoded key might be 8 bytes longer plus 8 bytes just in case */
            outSize = inSize + XMLSEC_KW_AES_MAGIC_BLOCK_SIZE +
                               XMLSEC_KW_AES_BLOCK_SIZE;
        } else {
            outSize = inSize + XMLSEC_KW_AES_BLOCK_SIZE;
        }

        ret = xmlSecBufferSetMaxSize(out, outSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferSetMaxSize",
                                 xmlSecTransformGetName(transform),
                                 "size=" XMLSEC_SIZE_FMT, outSize);
            return(-1);
        }

        if(transform->operation == xmlSecTransformOperationEncrypt) {
            ret = xmlSecKWAesEncode(ctx->kwAesId, transform,
                xmlSecBufferGetData(in), inSize,
                xmlSecBufferGetData(out), outSize,
                &outSize);
            if(ret < 0) {
                xmlSecInternalError("xmlSecKWAesEncode", xmlSecTransformGetName(transform));
                return(-1);
            }
        } else {
            ret = xmlSecKWAesDecode(ctx->kwAesId, transform,
                xmlSecBufferGetData(in), inSize,
                xmlSecBufferGetData(out), outSize,
                &outSize);
            if(ret < 0) {
                xmlSecInternalError("xmlSecKWAesDecode", xmlSecTransformGetName(transform));
                return(-1);
            }
        }

        ret = xmlSecBufferSetSize(out, outSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferSetSize",
                                 xmlSecTransformGetName(transform),
                                 "size=" XMLSEC_SIZE_FMT, outSize);
            return(-1);
        }

        ret = xmlSecBufferRemoveHead(in, inSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferRemoveHead",
                                 xmlSecTransformGetName(transform),
                                  "size=" XMLSEC_SIZE_FMT, inSize);
            return(-1);
        }

        transform->status = xmlSecTransformStatusFinished;
    } else if(transform->status == xmlSecTransformStatusFinished) {
        /* the only way we can get here is if there is no input */
        xmlSecAssert2(xmlSecBufferGetSize(&(transform->inBuf)) == 0, -1);
    } else {
        xmlSecInvalidTransfromStatusError(transform);
        return(-1);
    }
    return(0);
}


/********************************************************************
 *
 * KT AES
 *
 * http://www.w3.org/TR/xmlenc-core/#sec-Alg-SymmetricKeyWrap:
 *
 * Assume that the data to be wrapped consists of N 64-bit data blocks
 * denoted P(1), P(2), P(3) ... P(N). The result of wrapping will be N+1
 * 64-bit blocks denoted C(0), C(1), C(2), ... C(N). The key encrypting
 * key is represented by K. Assume integers i, j, and t and intermediate
 * 64-bit register A, 128-bit register B, and array of 64-bit quantities
 * R(1) through R(N).
 *
 * "|" represents concatenation so x|y, where x and y and 64-bit quantities,
 * is the 128-bit quantity with x in the most significant bits and y in the
 * least significant bits. AES(K)enc(x) is the operation of AES encrypting
 * the 128-bit quantity x under the key K. AES(K)dec(x) is the corresponding
 * decryption operation. XOR(x,y) is the bitwise exclusive or of x and y.
 * MSB(x) and LSB(y) are the most significant 64 bits and least significant
 * 64 bits of x and y respectively.
 *
 * If N is 1, a single AES operation is performed for wrap or unwrap.
 * If N>1, then 6*N AES operations are performed for wrap or unwrap.
 *
 * The key wrap algorithm is as follows:
 *
 *   1. If N is 1:
 *          * B=AES(K)enc(0xA6A6A6A6A6A6A6A6|P(1))
 *          * C(0)=MSB(B)
 *          * C(1)=LSB(B)
 *      If N>1, perform the following steps:
 *   2. Initialize variables:
 *          * Set A to 0xA6A6A6A6A6A6A6A6
 *          * Fori=1 to N,
 *            R(i)=P(i)
 *   3. Calculate intermediate values:
 *          * Forj=0 to 5,
 *                o For i=1 to N,
 *                  t= i + j*N
 *                  B=AES(K)enc(A|R(i))
 *                  A=XOR(t,MSB(B))
 *                  R(i)=LSB(B)
 *   4. Output the results:
 *          * Set C(0)=A
 *          * For i=1 to N,
 *            C(i)=R(i)
 *
 * The key unwrap algorithm is as follows:
 *
 *   1. If N is 1:
 *          * B=AES(K)dec(C(0)|C(1))
 *          * P(1)=LSB(B)
 *          * If MSB(B) is 0xA6A6A6A6A6A6A6A6, return success. Otherwise,
 *            return an integrity check failure error.
 *      If N>1, perform the following steps:
 *   2. Initialize the variables:
 *          * A=C(0)
 *          * For i=1 to N,
 *            R(i)=C(i)
 *   3. Calculate intermediate values:
 *          * For j=5 to 0,
 *                o For i=N to 1,
 *                  t= i + j*N
 *                  B=AES(K)dec(XOR(t,A)|R(i))
 *                  A=MSB(B)
 *                  R(i)=LSB(B)
 *   4. Output the results:
 *          * For i=1 to N,
 *            P(i)=R(i)
 *          * If A is 0xA6A6A6A6A6A6A6A6, return success. Otherwise, return
 *            an integrity check failure error.
 ********************************************************************/
static const xmlSecByte xmlSecKWAesMagicBlock[XMLSEC_KW_AES_MAGIC_BLOCK_SIZE] = {
    0xA6,  0xA6,  0xA6,  0xA6,  0xA6,  0xA6,  0xA6,  0xA6
};

int
xmlSecKWAesEncode(xmlSecKWAesId kwAesId, xmlSecTransformPtr transform,
                  const xmlSecByte *in, xmlSecSize inSize,
                  xmlSecByte *out, xmlSecSize outSize,
                  xmlSecSize* outWritten) {
    xmlSecByte block[XMLSEC_KW_AES_BLOCK_SIZE];
    xmlSecByte *p;
    xmlSecSize NN, ii, jj, tt, outWritten2;
    int ret;

    xmlSecAssert2(kwAesId != NULL, -1);
    xmlSecAssert2(kwAesId->encrypt != NULL, -1);
    xmlSecAssert2(kwAesId->decrypt != NULL, -1);
    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize > 0, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= inSize + XMLSEC_KW_AES_MAGIC_BLOCK_SIZE, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    /* prepend magic block */
    if(in != out) {
        memcpy(out + XMLSEC_KW_AES_MAGIC_BLOCK_SIZE, in, inSize);
    } else {
        memmove(out + XMLSEC_KW_AES_MAGIC_BLOCK_SIZE, out, inSize);
    }
    memcpy(out, xmlSecKWAesMagicBlock, XMLSEC_KW_AES_MAGIC_BLOCK_SIZE);

    NN = (inSize / 8);
    if(NN == 1) {
        outWritten2 = 0;
        ret = kwAesId->encrypt(transform, out, inSize + XMLSEC_KW_AES_MAGIC_BLOCK_SIZE,
            out, outSize, &outWritten2);
        if((ret < 0) || (outWritten2 != XMLSEC_KW_AES_BLOCK_SIZE)) {
            xmlSecInternalError2("kwAesId->encrypt", NULL,
                "outWritten2=" XMLSEC_SIZE_FMT, outWritten2);
            return(-1);
        }
    } else {
        for(jj = 0; jj <= 5; ++jj) {
            for(ii = 1; ii <= NN; ++ii) {
                tt = ii + (jj * NN);
                p = out + ii * 8;

                memcpy(block, out, 8);
                memcpy(block + 8, p, 8);

                outWritten2 = 0;
                ret = kwAesId->encrypt(transform, block, sizeof(block),
                    block, sizeof(block), &outWritten2);
                if((ret < 0) || (outWritten2 != XMLSEC_KW_AES_BLOCK_SIZE)) {
                    xmlSecInternalError2("kwAesId->encrypt", NULL,
                        "outWritten2=" XMLSEC_SIZE_FMT, outWritten2);
                    return(-1);
                }
                block[7] ^=  (xmlSecByte)tt;
                memcpy(out, block, 8);
                memcpy(p, block + 8, 8);
            }
        }
    }
    /* don't forget the magic block */
    (*outWritten) = inSize + XMLSEC_KW_AES_MAGIC_BLOCK_SIZE;
    return(0);
}

int
xmlSecKWAesDecode(xmlSecKWAesId kwAesId, xmlSecTransformPtr transform,
                  const xmlSecByte *in, xmlSecSize inSize,
                  xmlSecByte *out, xmlSecSize outSize,
                  xmlSecSize* outWritten) {
    xmlSecByte block[XMLSEC_KW_AES_BLOCK_SIZE];
    xmlSecByte *p;
    xmlSecSize NN, ii, jj, tt, outWritten2;
    int ret;

    xmlSecAssert2(kwAesId != NULL, -1);
    xmlSecAssert2(kwAesId->encrypt != NULL, -1);
    xmlSecAssert2(kwAesId->decrypt != NULL, -1);
    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(inSize >= XMLSEC_KW_AES_MAGIC_BLOCK_SIZE, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize >= inSize, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    /* copy input */
    if(in != out) {
        memcpy(out, in, inSize);
    }

    NN = (inSize / 8) - 1;
    if(NN == 1) {
        outWritten2 = 0;
        ret = kwAesId->decrypt(transform, out, inSize,
            out, outSize, &outWritten2);
        if((ret < 0) || (outWritten2 != XMLSEC_KW_AES_BLOCK_SIZE)) {
            xmlSecInternalError2("kwAesId->decrypt", NULL,
                "outWritten2=" XMLSEC_SIZE_FMT, outWritten2);
            return(-1);
        }
    } else {
        for(jj = 6; jj > 0; --jj) {
            for(ii = NN; ii > 0; --ii) {
                tt = ii + ((jj - 1) * NN);
                p = out + ii * 8;

                memcpy(block, out, 8);
                memcpy(block + 8, p, 8);
                block[7] ^= (xmlSecByte)tt;

                outWritten2 = 0;
                ret = kwAesId->decrypt(transform, block, sizeof(block),
                    block, sizeof(block), &outWritten2);
                if((ret < 0) || (outWritten2 != XMLSEC_KW_AES_BLOCK_SIZE)) {
                    xmlSecInternalError2("kwAesId->decrypt", NULL,
                        "outWritten2=" XMLSEC_SIZE_FMT, outWritten2);
                    return(-1);
                }
                memcpy(out, block, 8);
                memcpy(p, block + 8, 8);
            }
        }
    }
    /* do not left data in memory */
    memset(block, 0, sizeof(block));

    /* check the output */
    if(memcmp(xmlSecKWAesMagicBlock, out, XMLSEC_KW_AES_MAGIC_BLOCK_SIZE) != 0) {
        xmlSecInvalidDataError("bad magic block", NULL);
        return(-1);
    }

    /* get rid of magic block */
    memmove(out, out + XMLSEC_KW_AES_MAGIC_BLOCK_SIZE, inSize - XMLSEC_KW_AES_MAGIC_BLOCK_SIZE);
    (*outWritten) = inSize - XMLSEC_KW_AES_MAGIC_BLOCK_SIZE;
    return(0);
}

#endif /* XMLSEC_NO_AES */

