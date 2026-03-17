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
 * SECTION:membuf
 * @Short_description:  Memory buffer transform functions.
 * @Stability: Stable
 *
 */

#include "globals.h"

#include <stdlib.h>
#include <string.h>

#include <libxml/tree.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/buffer.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>
#include <xmlsec/keys.h>
#include <xmlsec/base64.h>
#include <xmlsec/membuf.h>
#include <xmlsec/errors.h>

#include "cast_helpers.h"

/*****************************************************************************
 *
 * Memory Buffer Transform
 *
 * xmlSecTransform + xmlSecBuffer
 *
 ****************************************************************************/
XMLSEC_TRANSFORM_DECLARE(MemBuf, xmlSecBuffer)
#define xmlSecMemBufSize XMLSEC_TRANSFORM_SIZE(MemBuf)

static int              xmlSecTransformMemBufInitialize         (xmlSecTransformPtr transform);
static void             xmlSecTransformMemBufFinalize           (xmlSecTransformPtr transform);
static int              xmlSecTransformMemBufExecute            (xmlSecTransformPtr transform,
                                                                 int last,
                                                                 xmlSecTransformCtxPtr transformCtx);
static xmlSecTransformKlass xmlSecTransformMemBufKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecMemBufSize,                           /* xmlSecSize objSize */

    xmlSecNameMemBuf,                           /* const xmlChar* name; */
    NULL,                                       /* const xmlChar* href; */
    0,                                          /* xmlSecAlgorithmUsage usage; */

    xmlSecTransformMemBufInitialize,            /* xmlSecTransformInitializeMethod initialize; */
    xmlSecTransformMemBufFinalize,              /* xmlSecTransformFianlizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecTransformDefaultPushBin,              /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformDefaultPopBin,               /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecTransformMemBufExecute,               /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformMemBufGetKlass:
 *
 * The memory buffer transform (used to store the data that go through it).
 *
 * Returns: memory buffer transform klass.
 */
xmlSecTransformId
xmlSecTransformMemBufGetKlass(void) {
    return(&xmlSecTransformMemBufKlass);
}

/**
 * xmlSecTransformMemBufGetBuffer:
 * @transform:          the pointer to memory buffer transform.
 *
 * Gets the pointer to memory buffer transform buffer.
 *
 * Returns: pointer to the transform's #xmlSecBuffer.
 */
xmlSecBufferPtr
xmlSecTransformMemBufGetBuffer(xmlSecTransformPtr transform) {
    xmlSecBufferPtr buffer;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformMemBufId), NULL);

    buffer = xmlSecMemBufGetCtx(transform);
    xmlSecAssert2(buffer != NULL, NULL);

    return(buffer);
}

static int
xmlSecTransformMemBufInitialize(xmlSecTransformPtr transform) {
    xmlSecBufferPtr buffer;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformMemBufId), -1);

    buffer = xmlSecMemBufGetCtx(transform);
    xmlSecAssert2(buffer != NULL, -1);

    ret = xmlSecBufferInitialize(buffer, 0);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBufferInitialize",
                            xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static void
xmlSecTransformMemBufFinalize(xmlSecTransformPtr transform) {
    xmlSecBufferPtr buffer;

    xmlSecAssert(xmlSecTransformCheckId(transform, xmlSecTransformMemBufId));

    buffer = xmlSecMemBufGetCtx(transform);
    xmlSecAssert(buffer != NULL);

    xmlSecBufferFinalize(xmlSecMemBufGetCtx(transform));
}

static int
xmlSecTransformMemBufExecute(xmlSecTransformPtr transform, int last, xmlSecTransformCtxPtr transformCtx) {
    xmlSecBufferPtr buffer;
    xmlSecBufferPtr in, out;
    xmlSecSize inSize;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformMemBufId), -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    buffer = xmlSecMemBufGetCtx(transform);
    xmlSecAssert2(buffer != NULL, -1);

    in = &(transform->inBuf);
    out = &(transform->outBuf);
    inSize = xmlSecBufferGetSize(in);

    if(transform->status == xmlSecTransformStatusNone) {
        transform->status = xmlSecTransformStatusWorking;
    }

    if(transform->status == xmlSecTransformStatusWorking) {
        /* just copy everything from in to our buffer and out */
        ret = xmlSecBufferAppend(buffer, xmlSecBufferGetData(in), inSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferAppend",
                                 xmlSecTransformGetName(transform),
                                 "size=" XMLSEC_SIZE_FMT, inSize);
            return(-1);
        }

        ret = xmlSecBufferAppend(out, xmlSecBufferGetData(in), inSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferAppend",
                                 xmlSecTransformGetName(transform),
                                 "size=" XMLSEC_SIZE_FMT, inSize);
            return(-1);
        }

        ret = xmlSecBufferRemoveHead(in, inSize);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBufferRemoveHead",
                                 xmlSecTransformGetName(transform),
                                "size=" XMLSEC_SIZE_FMT, inSize);
            return(-1);
        }

        if(last != 0) {
            transform->status = xmlSecTransformStatusFinished;
        }
    } else if(transform->status == xmlSecTransformStatusFinished) {
        /* the only way we can get here is if there is no input */
        xmlSecAssert2(inSize == 0, -1);
    } else {
        xmlSecInvalidTransfromStatusError(transform);
        return(-1);
    }
    return(0);
}

