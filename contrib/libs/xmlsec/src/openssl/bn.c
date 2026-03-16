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
 * SECTION:bn
 * @Short_description: Big numbers (BIGNUM) support functions implementation for OpenSSL.
 * @Stability: Stable
 *
 */

#include "globals.h"

#include <stdlib.h>
#include <string.h>

#include <openssl/bn.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/buffer.h>
#include <xmlsec/base64.h>
#include <xmlsec/errors.h>

#include <xmlsec/openssl/crypto.h>
#include <xmlsec/openssl/bn.h>

#include "../cast_helpers.h"

/**
 * xmlSecOpenSSLNodeGetBNValue:
 * @cur: the pointer to an XML node.
 * @a: the BIGNUM buffer.
 *
 * DEPRECATED. Converts the node content from CryptoBinary format
 * (http://www.w3.org/TR/xmldsig-core/#sec-CryptoBinary)
 * to a BIGNUM. If no BIGNUM buffer provided then a new
 * BIGNUM is created (caller is responsible for freeing it).
 *
 * Returns: a pointer to BIGNUM produced from CryptoBinary string
 * or NULL if an error occurs.
 */
BIGNUM*
xmlSecOpenSSLNodeGetBNValue(const xmlNodePtr cur, BIGNUM **a) {
    xmlSecBuffer buf;
    int bufInitialized = 0;
    xmlSecByte* bufPtr;
    xmlSecSize bufSize;
    int bufLen;
    int ret;
    BIGNUM* res = NULL;

    xmlSecAssert2(cur != NULL, NULL);

    ret = xmlSecBufferInitialize(&buf, 128);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBufferInitialize", NULL);
        goto done;
    }
    bufInitialized = 1;

    ret = xmlSecBufferBase64NodeContentRead(&buf, cur);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBufferBase64NodeContentRead", NULL);
        goto done;
    }

    bufPtr = xmlSecBufferGetData(&buf);
    bufSize = xmlSecBufferGetSize(&buf);
    XMLSEC_SAFE_CAST_SIZE_TO_INT(bufSize, bufLen, goto done, NULL);

    (*a) = BN_bin2bn(bufPtr, bufLen, (*a));
    if( (*a) == NULL) {
        xmlSecOpenSSLError2("BN_bin2bn", NULL, "size=%d", bufLen);
        goto done;
    }
    res = (*a);

done:
    if(bufInitialized != 0) {
        xmlSecBufferFinalize(&buf);
    }
    return(res);
}

/**
 * xmlSecOpenSSLNodeSetBNValue:
 * @cur: the pointer to an XML node.
 * @a: the BIGNUM.
 * @addLineBreaks: if the flag is equal to 1 then
 *              linebreaks will be added before and after
 *              new buffer content.
 *
 * DEPRECATED. Converts BIGNUM to CryptoBinary string
 * (http://www.w3.org/TR/xmldsig-core/#sec-CryptoBinary)
 * and sets it as the content of the given node. If the
 * addLineBreaks is set then line breaks are added
 * before and after the CryptoBinary string.
 *
 * Returns: 0 on success or -1 otherwise.
 */
int
xmlSecOpenSSLNodeSetBNValue(xmlNodePtr cur, const BIGNUM *a, int addLineBreaks) {
    xmlSecBuffer buf;
    int bufInitialized = 0;
    xmlSecSize size;
    int ret;
    int res = -1;

    xmlSecAssert2(a != NULL, -1);
    xmlSecAssert2(cur != NULL, -1);

    ret = BN_num_bytes(a);
    if(ret < 0) {
        xmlSecOpenSSLError("BN_num_bytes", NULL);
        goto done;
    }
    XMLSEC_SAFE_CAST_INT_TO_SIZE(ret, size, goto done, NULL);

    ret = xmlSecBufferInitialize(&buf, size + 1);
    if(ret < 0) {
        xmlSecInternalError2("xmlSecBufferInitialize", NULL,
            "size=" XMLSEC_SIZE_FMT, (size + 1));
        goto done;
    }
    bufInitialized = 1;

    ret = BN_bn2bin(a, xmlSecBufferGetData(&buf));
    if(ret < 0) {
        xmlSecOpenSSLError("BN_bn2bin", NULL);
        goto done;
    }
    XMLSEC_SAFE_CAST_INT_TO_SIZE(ret, size, goto done, NULL);

    ret = xmlSecBufferSetSize(&buf, size);
    if(ret < 0) {
        xmlSecInternalError2("xmlSecBufferSetSize", NULL,
                             "size=" XMLSEC_SIZE_FMT, size);
        goto done;
    }

    if(addLineBreaks) {
        xmlNodeSetContent(cur, xmlSecGetDefaultLineFeed());
    } else {
        xmlNodeSetContent(cur, xmlSecStringEmpty);
    }

    ret = xmlSecBufferBase64NodeContentWrite(&buf, cur, xmlSecBase64GetDefaultLineSize());
    if(ret < 0) {
        xmlSecInternalError("xmlSecBufferBase64NodeContentWrite", NULL);
        goto done;
    }

    if(addLineBreaks) {
        xmlNodeAddContent(cur, xmlSecGetDefaultLineFeed());
    }

    /* success */
    res = 0;

done:
    if(bufInitialized != 0) {
        xmlSecBufferFinalize(&buf);
    }
    return(res);
}

