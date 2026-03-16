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
 * SECTION:enveloped
 * @Short_description: Enveloped transform implementation.
 * @Stability: Private
 *
 */
#include "globals.h"

#include <stdlib.h>
#include <string.h>

#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>
#include <xmlsec/errors.h>

/**************************************************************************
 *
 *  Enveloped transform
 *
 *************************************************************************/
static int      xmlSecTransformEnvelopedExecute         (xmlSecTransformPtr transform,
                                                         int last,
                                                         xmlSecTransformCtxPtr transformCtx);


static xmlSecTransformKlass xmlSecTransformEnvelopedKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    sizeof(xmlSecTransform),                    /* xmlSecSize objSize */

    xmlSecNameEnveloped,                        /* const xmlChar* name; */
    xmlSecHrefEnveloped,                        /* const xmlChar* href; */
    xmlSecTransformUsageDSigTransform,          /* xmlSecTransformUsage usage; */

    NULL,                                       /* xmlSecTransformInitializeMethod initialize; */
    NULL,                                       /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    NULL,                                       /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformDefaultPushXml,              /* xmlSecTransformPushXmlMethod pushXml; */
    xmlSecTransformDefaultPopXml,               /* xmlSecTransformPopXmlMethod popXml; */
    xmlSecTransformEnvelopedExecute,            /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformEnvelopedGetKlass:
 *
 * The enveloped transform klass (http://www.w3.org/TR/xmldsig-core/#sec-EnvelopedSignature):
 *
 * An enveloped signature transform T removes the whole Signature element
 * containing T from the digest calculation of the Reference element
 * containing T. The entire string of characters used by an XML processor
 * to match the Signature with the XML production element is removed.
 * The output of the transform is equivalent to the output that would
 * result from replacing T with an XPath transform containing the following
 * XPath parameter element:
 *
 *   <XPath xmlns:dsig="...">
 *     count(ancestor-or-self::dsig:Signature |
 *     here()/ancestor::dsig:Signature[1]) >
 *     count(ancestor-or-self::dsig:Signature)
 *   </XPath>
 *
 * The input and output requirements of this transform are identical to
 * those of the XPath transform, but may only be applied to a node-set from
 * its parent XML document. Note that it is not necessary to use an XPath
 * expression evaluator to create this transform. However, this transform
 * MUST produce output in exactly the same manner as the XPath transform
 * parameterized by the XPath expression above.
 *
 * Returns: enveloped transform id.
 */
xmlSecTransformId
xmlSecTransformEnvelopedGetKlass(void) {
    return(&xmlSecTransformEnvelopedKlass);
}

static int
xmlSecTransformEnvelopedExecute(xmlSecTransformPtr transform, int last,
                                 xmlSecTransformCtxPtr transformCtx) {
    xmlNodePtr node;
    xmlSecNodeSetPtr children;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformEnvelopedId), -1);
    xmlSecAssert2(transform->hereNode != NULL, -1);
    xmlSecAssert2(transform->outNodes == NULL, -1);
    xmlSecAssert2(last != 0, -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    if((transform->inNodes != NULL) && (transform->inNodes->doc != transform->hereNode->doc)) {
        xmlSecOtherError(XMLSEC_ERRORS_R_TRANSFORM_SAME_DOCUMENT_REQUIRED,
                         xmlSecTransformGetName(transform),
                         NULL);
        return(-1);
    }

    /* find signature node and get all its children in the nodes set */
    node = xmlSecFindParent(transform->hereNode, xmlSecNodeSignature, xmlSecDSigNs);
    if(node == NULL) {
        xmlSecNodeNotFoundError("xmlSecFindParent", transform->hereNode,
                                xmlSecNodeSignature,
                                xmlSecTransformGetName(transform));
        return(-1);
    }

    children = xmlSecNodeSetGetChildren(node->doc, node, 1, 1);
    if(children == NULL) {
        xmlSecInternalError2("xmlSecNodeSetGetChildren",
                             xmlSecTransformGetName(transform),
                             "node=%s",
                             xmlSecErrorsSafeString(xmlSecNodeGetName(node)));
        return(-1);
    }

    /* intersect <dsig:Signature/> node children with input nodes (if exist) */
    transform->outNodes = xmlSecNodeSetAdd(transform->inNodes, children, xmlSecNodeSetIntersection);
    if(transform->outNodes == NULL) {
        xmlSecInternalError("xmlSecNodeSetAdd",
                            xmlSecTransformGetName(transform));
        xmlSecNodeSetDestroy(children);
        return(-1);
    }

    return(0);
}

