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
 * SECTION:c14n
 * @Short_description: C14N transform implementation.
 * @Stability: Private
 *
 */
#include "globals.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <libxml/tree.h>
#include <libxml/c14n.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/keys.h>
#include <xmlsec/list.h>
#include <xmlsec/transforms.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/errors.h>

#include "cast_helpers.h"

/******************************************************************************
 *
 * C14N transforms
 *
 * xmlSecTransform + xmlSecStringList (inclusive namespaces list for ExclC14N).
 *
 *****************************************************************************/
XMLSEC_TRANSFORM_DECLARE(C14N, xmlSecPtrList)
#define xmlSecC14NSize XMLSEC_TRANSFORM_SIZE(C14N)

#define xmlSecTransformC14NCheckId(transform) \
    (xmlSecTransformInclC14NCheckId((transform)) || \
     xmlSecTransformInclC14N11CheckId((transform)) || \
     xmlSecTransformExclC14NCheckId((transform)) || \
     xmlSecTransformCheckId((transform), xmlSecTransformRemoveXmlTagsC14NId))

#define xmlSecTransformInclC14NCheckId(transform) \
    (xmlSecTransformCheckId((transform), xmlSecTransformInclC14NId) || \
     xmlSecTransformCheckId((transform), xmlSecTransformInclC14NWithCommentsId))

#define xmlSecTransformInclC14N11CheckId(transform) \
    (xmlSecTransformCheckId((transform), xmlSecTransformInclC14N11Id) || \
     xmlSecTransformCheckId((transform), xmlSecTransformInclC14N11WithCommentsId))

#define xmlSecTransformExclC14NCheckId(transform) \
    (xmlSecTransformCheckId((transform), xmlSecTransformExclC14NId) || \
     xmlSecTransformCheckId((transform), xmlSecTransformExclC14NWithCommentsId) )


static int              xmlSecTransformC14NInitialize   (xmlSecTransformPtr transform);
static void             xmlSecTransformC14NFinalize     (xmlSecTransformPtr transform);
static int              xmlSecTransformC14NNodeRead     (xmlSecTransformPtr transform,
                                                         xmlNodePtr node,
                                                         xmlSecTransformCtxPtr transformCtx);
static int              xmlSecTransformC14NPushXml      (xmlSecTransformPtr transform,
                                                         xmlSecNodeSetPtr nodes,
                                                         xmlSecTransformCtxPtr transformCtx);
static int              xmlSecTransformC14NPopBin       (xmlSecTransformPtr transform,
                                                         xmlSecByte* data,
                                                         xmlSecSize maxDataSize,
                                                         xmlSecSize* dataSize,
                                                         xmlSecTransformCtxPtr transformCtx);
static int              xmlSecTransformC14NExecute      (xmlSecTransformId id,
                                                         xmlSecNodeSetPtr nodes,
                                                         xmlSecPtrListPtr nsList,
                                                         xmlOutputBufferPtr buf);
static int
xmlSecTransformC14NInitialize(xmlSecTransformPtr transform) {
    xmlSecPtrListPtr nsList;
    int ret;

    xmlSecAssert2(xmlSecTransformC14NCheckId(transform), -1);

    nsList = xmlSecC14NGetCtx(transform);
    xmlSecAssert2(nsList != NULL, -1);

    ret = xmlSecPtrListInitialize(nsList, xmlSecStringListId);
    if(ret < 0) {
        xmlSecInternalError("xmlSecPtrListInitialize",
                            xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static void
xmlSecTransformC14NFinalize(xmlSecTransformPtr transform) {
    xmlSecPtrListPtr nsList;

    xmlSecAssert(xmlSecTransformC14NCheckId(transform));

    nsList = xmlSecC14NGetCtx(transform);
    xmlSecAssert(xmlSecPtrListCheckId(nsList, xmlSecStringListId));

    xmlSecPtrListFinalize(nsList);
}

static int
xmlSecTransformC14NNodeRead(xmlSecTransformPtr transform, xmlNodePtr node, xmlSecTransformCtxPtr transformCtx) {
    xmlSecPtrListPtr nsList;
    xmlNodePtr cur;
    xmlChar *list;
    xmlChar *p, *n, *tmp;
    int ret;

    /* we have something to read only for exclusive c14n transforms */
    xmlSecAssert2(xmlSecTransformExclC14NCheckId(transform), -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    nsList = xmlSecC14NGetCtx(transform);
    xmlSecAssert2(xmlSecPtrListCheckId(nsList, xmlSecStringListId), -1);
    xmlSecAssert2(xmlSecPtrListGetSize(nsList) == 0, -1);

    /* there is only one optional node */
    cur = xmlSecGetNextElementNode(node->children);
    if(cur != NULL) {
        if(!xmlSecCheckNodeName(cur, xmlSecNodeInclusiveNamespaces, xmlSecNsExcC14N)) {
            xmlSecInvalidNodeError(cur, xmlSecNodeInclusiveNamespaces,
                                   xmlSecTransformGetName(transform));
            return(-1);
        }

        list = xmlGetProp(cur, xmlSecAttrPrefixList);
        if(list == NULL) {
            xmlSecInvalidNodeAttributeError(cur, xmlSecAttrPrefixList,
                                            xmlSecTransformGetName(transform),
                                            "empty");
            return(-1);
        }

        /* the list of namespaces is space separated */
        for(p = n = list; ((p != NULL) && ((*p) != '\0')); p = n) {
            n = (xmlChar*)xmlStrchr(p, ' ');
            if(n != NULL) {
                *(n++) = '\0';
            }

            tmp = xmlStrdup(p);
            if(tmp == NULL) {
                xmlSecStrdupError(p, xmlSecTransformGetName(transform));
                xmlFree(list);
                return(-1);
            }

            ret = xmlSecPtrListAdd(nsList, tmp);
            if(ret < 0) {
                xmlSecInternalError("xmlSecPtrListAdd",
                                    xmlSecTransformGetName(transform));
                xmlFree(tmp);
                xmlFree(list);
                return(-1);
            }
        }
        xmlFree(list);

        /* add NULL at the end */
        ret = xmlSecPtrListAdd(nsList, NULL);
        if(ret < 0) {
            xmlSecInternalError("xmlSecPtrListAdd",
                                xmlSecTransformGetName(transform));
            return(-1);
        }

        cur = xmlSecGetNextElementNode(cur->next);
    }

    /* check that we have nothing else */
    if(cur != NULL) {
        xmlSecUnexpectedNodeError(cur, NULL);
        return(-1);
    }

    return(0);
}

static int
xmlSecTransformC14NPushXml(xmlSecTransformPtr transform, xmlSecNodeSetPtr nodes,
                            xmlSecTransformCtxPtr transformCtx) {
    xmlOutputBufferPtr buf;
    int ret;

    xmlSecAssert2(xmlSecTransformC14NCheckId(transform), -1);
    xmlSecAssert2(nodes != NULL, -1);
    xmlSecAssert2(nodes->doc != NULL, -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    /* check/update current transform status */
    switch(transform->status) {
    case xmlSecTransformStatusNone:
        transform->status = xmlSecTransformStatusWorking;
        break;
    case xmlSecTransformStatusWorking:
    case xmlSecTransformStatusFinished:
        return(0);
    default:
        xmlSecInvalidTransfromStatusError(transform);
        return(-1);
    }
    xmlSecAssert2(transform->status == xmlSecTransformStatusWorking, -1);

    /* prepare output buffer: next transform or ourselves */
    if(transform->next != NULL) {
        buf = xmlSecTransformCreateOutputBuffer(transform->next, transformCtx);
        if(buf == NULL) {
            xmlSecInternalError("xmlSecTransformCreateOutputBuffer",
                                xmlSecTransformGetName(transform));
            return(-1);
        }
    } else {
        buf = xmlSecBufferCreateOutputBuffer(&(transform->outBuf));
        if(buf == NULL) {
            xmlSecInternalError("xmlSecBufferCreateOutputBuffer",
                                xmlSecTransformGetName(transform));
            return(-1);
        }
    }

    ret = xmlSecTransformC14NExecute(transform->id, nodes,
            xmlSecC14NGetCtx(transform), buf);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformC14NExecute",
                            xmlSecTransformGetName(transform));
        (void)xmlOutputBufferClose(buf);
        return(-1);
    }

    ret = xmlOutputBufferClose(buf);
    if(ret < 0) {
        xmlSecXmlError("xmlOutputBufferClose", xmlSecTransformGetName(transform));
        return(-1);
    }
    transform->status = xmlSecTransformStatusFinished;
    return(0);
}

static int
xmlSecTransformC14NPopBin(xmlSecTransformPtr transform, xmlSecByte* data,
                            xmlSecSize maxDataSize, xmlSecSize* dataSize,
                            xmlSecTransformCtxPtr transformCtx) {
    xmlSecBufferPtr out;
    int ret;

    xmlSecAssert2(xmlSecTransformC14NCheckId(transform), -1);
    xmlSecAssert2(data != NULL, -1);
    xmlSecAssert2(dataSize != NULL, -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    out = &(transform->outBuf);
    if(transform->status == xmlSecTransformStatusNone) {
        xmlOutputBufferPtr buf;

        xmlSecAssert2(transform->inNodes == NULL, -1);

        /* todo: isn't it an error? */
        if(transform->prev == NULL) {
            (*dataSize) = 0;
            transform->status = xmlSecTransformStatusFinished;
            return(0);
        }

        /* get xml data from previous transform */
        ret = xmlSecTransformPopXml(transform->prev, &(transform->inNodes), transformCtx);
        if(ret < 0) {
            xmlSecInternalError("xmlSecTransformPopXml",
                                xmlSecTransformGetName(transform));
            return(-1);
        }

        /* dump everything to internal buffer */
        buf = xmlSecBufferCreateOutputBuffer(out);
        if(buf == NULL) {
            xmlSecInternalError("xmlSecBufferCreateOutputBuffer",
                                xmlSecTransformGetName(transform));
            return(-1);
        }

        /* we are using a semi-hack here: we know that xmlSecPtrList keeps
         * all pointers in the big array */
        ret = xmlSecTransformC14NExecute(transform->id, transform->inNodes,
                xmlSecC14NGetCtx(transform), buf);
        if(ret < 0) {
            xmlSecInternalError("xmlSecTransformC14NExecute",
                                xmlSecTransformGetName(transform));
            (void)xmlOutputBufferClose(buf);
            return(-1);
        }
        ret = xmlOutputBufferClose(buf);
        if(ret < 0) {
            xmlSecXmlError("xmlOutputBufferClose", xmlSecTransformGetName(transform));
            return(-1);
        }
        transform->status = xmlSecTransformStatusWorking;
    }

    if(transform->status == xmlSecTransformStatusWorking) {
        xmlSecSize outSize;

        /* return chunk after chunk */
        outSize = xmlSecBufferGetSize(out);
        if(outSize > maxDataSize) {
            outSize = maxDataSize;
        }
        if(outSize > XMLSEC_TRANSFORM_BINARY_CHUNK) {
            outSize = XMLSEC_TRANSFORM_BINARY_CHUNK;
        }
        if(outSize > 0) {
            xmlSecAssert2(xmlSecBufferGetData(&(transform->outBuf)), -1);

            memcpy(data, xmlSecBufferGetData(&(transform->outBuf)), outSize);
            ret = xmlSecBufferRemoveHead(&(transform->outBuf), outSize);
            if(ret < 0) {
                xmlSecInternalError2("xmlSecBufferRemoveHead", xmlSecTransformGetName(transform),
                    "size=" XMLSEC_SIZE_FMT, outSize);
                return(-1);
            }
        } else if(xmlSecBufferGetSize(out) == 0) {
            transform->status = xmlSecTransformStatusFinished;
        }
        (*dataSize) = outSize;
    } else if(transform->status == xmlSecTransformStatusFinished) {
        /* the only way we can get here is if there is no output */
        xmlSecAssert2(xmlSecBufferGetSize(out) == 0, -1);
        (*dataSize) = 0;
    } else {
        xmlSecInvalidTransfromStatusError(transform);
        return(-1);
    }

    return(0);
}

static int
xmlSecTransformC14NExecute(xmlSecTransformId id, xmlSecNodeSetPtr nodes, xmlSecPtrListPtr nsList,
                           xmlOutputBufferPtr buf) {
    int ret;

    xmlSecAssert2(id != xmlSecTransformIdUnknown, -1);
    xmlSecAssert2(nodes != NULL, -1);
    xmlSecAssert2(nodes->doc != NULL, -1);
    xmlSecAssert2(nsList != NULL, -1);
    xmlSecAssert2(xmlSecPtrListCheckId(nsList, xmlSecStringListId), -1);
    xmlSecAssert2(buf != NULL, -1);

    /* execute c14n transform */
    if(id == xmlSecTransformInclC14NId) {
        ret = xmlC14NExecute(nodes->doc,
                        (xmlC14NIsVisibleCallback)xmlSecNodeSetContains,
                        nodes, XML_C14N_1_0, NULL, 0, buf);
    } else if(id == xmlSecTransformInclC14NWithCommentsId) {
         ret = xmlC14NExecute(nodes->doc,
                        (xmlC14NIsVisibleCallback)xmlSecNodeSetContains,
                        nodes, XML_C14N_1_0, NULL, 1, buf);
    } else if(id == xmlSecTransformInclC14N11Id) {
        ret = xmlC14NExecute(nodes->doc,
                        (xmlC14NIsVisibleCallback)xmlSecNodeSetContains,
                        nodes, XML_C14N_1_1, NULL, 0, buf);
    } else if(id == xmlSecTransformInclC14N11WithCommentsId) {
         ret = xmlC14NExecute(nodes->doc,
                        (xmlC14NIsVisibleCallback)xmlSecNodeSetContains,
                        nodes, XML_C14N_1_1, NULL, 1, buf);
    } else if(id == xmlSecTransformExclC14NId) {
        /* we are using a semi-hack here: we know that xmlSecPtrList keeps
         * all pointers in the big array */
        ret = xmlC14NExecute(nodes->doc,
                        (xmlC14NIsVisibleCallback)xmlSecNodeSetContains,
                        nodes, XML_C14N_EXCLUSIVE_1_0, (xmlChar**)(nsList->data), 0, buf);
    } else if(id == xmlSecTransformExclC14NWithCommentsId) {
        /* we are using a semi-hack here: we know that xmlSecPtrList keeps
         * all pointers in the big array */
        ret = xmlC14NExecute(nodes->doc,
                        (xmlC14NIsVisibleCallback)xmlSecNodeSetContains,
                        nodes, XML_C14N_EXCLUSIVE_1_0, (xmlChar**)(nsList->data), 1, buf);
    } else if(id == xmlSecTransformRemoveXmlTagsC14NId) {
        ret = xmlSecNodeSetDumpTextNodes(nodes, buf);
    } else {
        /* shoudn't be possible to come here, actually */
        xmlSecOtherError(XMLSEC_ERRORS_R_INVALID_TRANSFORM,
                         xmlSecTransformKlassGetName(id), NULL);
        return(-1);
    }

    if(ret < 0) {
        xmlSecXmlError("xmlC14NExecute", xmlSecTransformKlassGetName(id));
        return(-1);
    }

    return(0);
}

/***************************************************************************
 *
 * C14N
 *
 ***************************************************************************/
static xmlSecTransformKlass xmlSecTransformInclC14NKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecC14NSize,                             /* xmlSecSize objSize */

    xmlSecNameC14N,                             /* const xmlChar* name; */
    xmlSecHrefC14N,                             /* const xmlChar* href; */
    xmlSecTransformUsageC14NMethod | xmlSecTransformUsageDSigTransform,
                                                /* xmlSecAlgorithmUsage usage; */

    xmlSecTransformC14NInitialize,              /* xmlSecTransformInitializeMethod initialize; */
    xmlSecTransformC14NFinalize,                /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformC14NPopBin,                  /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformC14NPushXml,                 /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformInclC14NGetKlass:
 *
 * Inclusive (regular) canonicalization that omits comments transform klass
 * (http://www.w3.org/TR/xmldsig-core/#sec-c14nAlg and
 * http://www.w3.org/TR/2001/REC-xml-c14n-20010315).
 *
 * Returns: c14n transform id.
 */
xmlSecTransformId
xmlSecTransformInclC14NGetKlass(void) {
    return(&xmlSecTransformInclC14NKlass);
}

/***************************************************************************
 *
 * C14N With Comments
 *
 ***************************************************************************/
static xmlSecTransformKlass xmlSecTransformInclC14NWithCommentsKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecC14NSize,                             /* xmlSecSize objSize */

    /* same as xmlSecTransformId */
    xmlSecNameC14NWithComments,                 /* const xmlChar* name; */
    xmlSecHrefC14NWithComments,                 /* const xmlChar* href; */
    xmlSecTransformUsageC14NMethod | xmlSecTransformUsageDSigTransform,
                                                /* xmlSecAlgorithmUsage usage; */

    xmlSecTransformC14NInitialize,              /* xmlSecTransformInitializeMethod initialize; */
    xmlSecTransformC14NFinalize,                /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod read; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformC14NPopBin,                  /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformC14NPushXml,                 /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformInclC14NWithCommentsGetKlass:
 *
 * Inclusive (regular) canonicalization that includes comments transform klass
 * (http://www.w3.org/TR/xmldsig-core/#sec-c14nAlg and
 * http://www.w3.org/TR/2001/REC-xml-c14n-20010315).
 *
 * Returns: c14n with comments transform id.
 */
xmlSecTransformId
xmlSecTransformInclC14NWithCommentsGetKlass(void) {
    return(&xmlSecTransformInclC14NWithCommentsKlass);
}

/***************************************************************************
 *
 * C14N v1.1
 *
 ***************************************************************************/
static xmlSecTransformKlass xmlSecTransformInclC14N11Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecC14NSize,                             /* xmlSecSize objSize */

    xmlSecNameC14N11,                           /* const xmlChar* name; */
    xmlSecHrefC14N11,                           /* const xmlChar* href; */
    xmlSecTransformUsageC14NMethod | xmlSecTransformUsageDSigTransform,
                                                /* xmlSecAlgorithmUsage usage; */

    xmlSecTransformC14NInitialize,              /* xmlSecTransformInitializeMethod initialize; */
    xmlSecTransformC14NFinalize,                /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformC14NPopBin,                  /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformC14NPushXml,                 /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformInclC14N11GetKlass:
 *
 * C14N version 1.1 (http://www.w3.org/TR/xml-c14n11)
 *
 * Returns: c14n v1.1 transform id.
 */
xmlSecTransformId
xmlSecTransformInclC14N11GetKlass(void) {
    return(&xmlSecTransformInclC14N11Klass);
}

/***************************************************************************
 *
 * C14N v1.1 With Comments
 *
 ***************************************************************************/
static xmlSecTransformKlass xmlSecTransformInclC14N11WithCommentsKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecC14NSize,                             /* xmlSecSize objSize */

    /* same as xmlSecTransformId */
    xmlSecNameC14N11WithComments,               /* const xmlChar* name; */
    xmlSecHrefC14N11WithComments,               /* const xmlChar* href; */
    xmlSecTransformUsageC14NMethod | xmlSecTransformUsageDSigTransform,
                                                /* xmlSecAlgorithmUsage usage; */

    xmlSecTransformC14NInitialize,              /* xmlSecTransformInitializeMethod initialize; */
    xmlSecTransformC14NFinalize,                /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod read; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformC14NPopBin,                  /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformC14NPushXml,                 /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformInclC14N11WithCommentsGetKlass:
 *
 * C14N version 1.1 (http://www.w3.org/TR/xml-c14n11) with comments
 *
 * Returns: c14n v1.1 with comments transform id.
 */
xmlSecTransformId
xmlSecTransformInclC14N11WithCommentsGetKlass(void) {
    return(&xmlSecTransformInclC14N11WithCommentsKlass);
}


/***************************************************************************
 *
 * Excl C14N
 *
 ***************************************************************************/
static xmlSecTransformKlass xmlSecTransformExclC14NKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecC14NSize,                             /* xmlSecSize objSize */

    xmlSecNameExcC14N,                          /* const xmlChar* name; */
    xmlSecHrefExcC14N,                          /* const xmlChar* href; */
    xmlSecTransformUsageC14NMethod | xmlSecTransformUsageDSigTransform,
                                                /* xmlSecAlgorithmUsage usage; */

    xmlSecTransformC14NInitialize,              /* xmlSecTransformInitializeMethod initialize; */
    xmlSecTransformC14NFinalize,                /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecTransformC14NNodeRead,                /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformC14NPopBin,                  /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformC14NPushXml,                 /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformExclC14NGetKlass:
 *
 * Exclusive canoncicalization that omits comments transform klass
 * (http://www.w3.org/TR/xml-exc-c14n/).
 *
 * Returns: exclusive c14n transform id.
 */
xmlSecTransformId
xmlSecTransformExclC14NGetKlass(void) {
    return(&xmlSecTransformExclC14NKlass);
}

/***************************************************************************
 *
 * Excl C14N With Comments
 *
 ***************************************************************************/
static xmlSecTransformKlass xmlSecTransformExclC14NWithCommentsKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecC14NSize,                             /* xmlSecSize objSize */

    xmlSecNameExcC14NWithComments,              /* const xmlChar* name; */
    xmlSecHrefExcC14NWithComments,              /* const xmlChar* href; */
    xmlSecTransformUsageC14NMethod | xmlSecTransformUsageDSigTransform,
                                                /* xmlSecAlgorithmUsage usage; */

    xmlSecTransformC14NInitialize,              /* xmlSecTransformInitializeMethod initialize; */
    xmlSecTransformC14NFinalize,                /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecTransformC14NNodeRead,                /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformC14NPopBin,                  /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformC14NPushXml,                 /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformExclC14NWithCommentsGetKlass:
 *
 * Exclusive canoncicalization that includes comments transform klass
 * (http://www.w3.org/TR/xml-exc-c14n/).
 *
 * Returns: exclusive c14n with comments transform id.
 */
xmlSecTransformId
xmlSecTransformExclC14NWithCommentsGetKlass(void) {
    return(&xmlSecTransformExclC14NWithCommentsKlass);
}

/***************************************************************************
 *
 * Remove XML tags C14N
 *
 ***************************************************************************/
static xmlSecTransformKlass xmlSecTransformRemoveXmlTagsC14NKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecC14NSize,                             /* xmlSecSize objSize */

    BAD_CAST "remove-xml-tags-transform",       /* const xmlChar* name; */
    NULL,                                       /* const xmlChar* href; */
    xmlSecTransformUsageC14NMethod | xmlSecTransformUsageDSigTransform,
                                                /* xmlSecAlgorithmUsage usage; */

    xmlSecTransformC14NInitialize,              /* xmlSecTransformInitializeMethod initialize; */
    xmlSecTransformC14NFinalize,                /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformC14NPopBin,                  /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformC14NPushXml,                 /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformRemoveXmlTagsC14NGetKlass:
 *
 * The "remove xml tags" transform klass (http://www.w3.org/TR/xmldsig-core/#sec-Base-64):
 * Base64 transform requires an octet stream for input. If an XPath node-set
 * (or sufficiently functional alternative) is given as input, then it is
 * converted to an octet stream by performing operations logically equivalent
 * to 1) applying an XPath transform with expression self::text(), then 2)
 * taking the string-value of the node-set. Thus, if an XML element is
 * identified by a barename XPointer in the Reference URI, and its content
 * consists solely of base64 encoded character data, then this transform
 * automatically strips away the start and end tags of the identified element
 * and any of its descendant elements as well as any descendant comments and
 * processing instructions. The output of this transform is an octet stream.
 *
 * Returns: "remove xml tags" transform id.
 */
xmlSecTransformId
xmlSecTransformRemoveXmlTagsC14NGetKlass(void) {
    return(&xmlSecTransformRemoveXmlTagsC14NKlass);
}

