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
 * SECTION:relationship
 * @Short_description: Relationship transform implementation
 * @Stability: Private
 *
 * [Relationship transform](http://standards.iso.org/ittf/PubliclyAvailableStandards/c061796_ISO_IEC_29500-2_2012.zip)
 *
 * The relationships transform takes the XML document from the Relationships part and converts
 * it to another XML document.
 *
 * The package implementer might create relationships XML that contains content from several namespaces,
 * along with versioning instructions as defined in Part 3, “Markup Compatibility and Extensibility”. [O6.11]
 *
 * The relationships transform algorithm is as follows:
 *
 * Step 1: Process versioning instructions
 *   1. The package implementer shall process the versioning instructions, considering that the only
 *   known namespace is the Relationships namespace.
 *   2. The package implementer shall remove all ignorable content, ignoring preservation attributes.
 *   3. The package implementer shall remove all versioning instructions.
 *
 * Step 2: Sort and filter relationships
 *   1. The package implementer shall remove all namespace declarations except the Relationships
 *   namespace declaration.
 *   2. The package implementer shall remove the Relationships namespace prefix, if it is present.
 *   3. The package implementer shall sort relationship elements by Id value in lexicographical
 *   order, considering Id values as case-sensitive Unicode strings.
 *   4. The package implementer shall remove all Relationship elements that do not have either an Id
 *   value that matches any SourceId value or a Type value that matches any SourceType value, among
 *   the SourceId and SourceType values specified in the transform definition. Producers and consumers
 *   shall compare values as case-sensitive Unicode strings. [M6.27] The resulting XML document holds
 *   all Relationship elements that either have an Id value that matches a SourceId value or a Type value
 *   that matches a SourceType value specified in the transform definition.
 *
 * Step 3: Prepare for canonicalization
 *   1. The package implementer shall remove all characters between the Relationships start tag and
 *   the first Relationship start tag.
 *   2. The package implementer shall remove any contents of the Relationship element.
 *   3. The package implementer shall remove all characters between the last Relationship end tag and
 *   the Relationships end tag.
 *   4. If there are no Relationship elements, the package implementer shall remove all characters
 *   between the Relationships start tag and the Relationships end tag.
 *   5. The package implementer shall remove comments from the Relationships XML content.
 *   6. The package implementer shall add a TargetMode attribute with its default value, if this
 *   optional attribute is missing from the Relationship element.
 *   7. The package implementer can generate Relationship elements as start-tag/end-tag pairs with
 *   empty content, or as empty elements. A canonicalization transform, applied immediately after the
 *   Relationships Transform, converts all XML elements into start-tag/end-tag pairs.
 *
 *
 *   IMPLEMENTATION NOTES (https://github.com/lsh123/xmlsec/pull/24):
 *
 *   * We don't simply manipulate the XML tree, but do an XML tree -> output bytes transformation,
 *     because we never write characters inside XML elements, we implicitly remove all character
 *     contents, as required by step 3, point 1. It also simplifies the task of the situation that
 *     realistically the input of the transformation is always a document that conforms to the OOXML
 *     relationships XML schema, so in practice it'll never happen that the input document has e.g.
 *     characters, as the schema requires that the document has only XML elements and attributes,
 *     but no characters.
 *
 *   * Step 2, point 4 talks about a SourceType value, but given that neither Microsoft Office, nor LibreOffice
 *     writes that theoretical attribute, the implementation doesn't handle it. If there is a real-world situation
 *     when there will be such an input, then it'll be easy to add support for that. But I didn't want to clutter
 *     the current implementation with details that doesn't seem to be used in practice
 *
 */
#include "globals.h"

#include <stdlib.h>
#include <string.h>

#include <libxml/tree.h>
#include <libxml/xpointer.h>
#include <libxml/c14n.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/keys.h>
#include <xmlsec/list.h>
#include <xmlsec/transforms.h>
#include <xmlsec/errors.h>

#include "cast_helpers.h"


/**************************************************************************
 *
 * XML Relationshi transform
 *
 * xmlSecTransform + xmlSecRelationshipCtx
 *
 ***************************************************************************/
typedef struct _xmlSecRelationshipCtx           xmlSecRelationshipCtx,
                                                *xmlSecRelationshipCtxPtr;
struct _xmlSecRelationshipCtx {
    xmlSecPtrListPtr                    sourceIdList;
};

XMLSEC_TRANSFORM_DECLARE(Relationship, xmlSecRelationshipCtx)
#define xmlSecRelationshipSize XMLSEC_TRANSFORM_SIZE(Relationship)

static int              xmlSecRelationshipInitialize      (xmlSecTransformPtr transform);
static void             xmlSecRelationshipFinalize        (xmlSecTransformPtr transform);
static int              xmlSecTransformRelationshipPopBin (xmlSecTransformPtr transform,
                                                           xmlSecByte* data,
                                                           xmlSecSize maxDataSize,
                                                           xmlSecSize* dataSize,
                                                           xmlSecTransformCtxPtr transformCtx);
static int              xmlSecTransformRelationshipPushXml(xmlSecTransformPtr transform,
                                                           xmlSecNodeSetPtr nodes,
                                                           xmlSecTransformCtxPtr transformCtx);
static int              xmlSecRelationshipReadNode        (xmlSecTransformPtr transform,
                                                           xmlNodePtr node,
                                                           xmlSecTransformCtxPtr transformCtx);

static int              xmlSecTransformRelationshipProcessElementNode(xmlSecTransformPtr transform,
                                                            xmlOutputBufferPtr buf,
                                                            xmlNodePtr cur);


static xmlSecTransformKlass xmlSecRelationshipKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecRelationshipSize,                     /* xmlSecSize objSize */

    xmlSecNameRelationship,                     /* const xmlChar* name; */
    xmlSecHrefRelationship,                     /* const xmlChar* href; */
    xmlSecTransformUsageDSigTransform,          /* xmlSecTransformUsage usage; */

    xmlSecRelationshipInitialize,               /* xmlSecTransformInitializeMethod initialize; */
    xmlSecRelationshipFinalize,                 /* xmlSecTransformFinalizeMethod finalize; */
    xmlSecRelationshipReadNode,                 /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    NULL,                                       /* xmlSecTransformPushBinMethod pushBin; */
    xmlSecTransformRelationshipPopBin,          /* xmlSecTransformPopBinMethod popBin; */
    xmlSecTransformRelationshipPushXml,         /* xmlSecTransformPushXmlMethod pushXml; */
    NULL,                                       /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

xmlSecTransformId
xmlSecTransformRelationshipGetKlass(void) {
    return(&xmlSecRelationshipKlass);
}

static int
xmlSecRelationshipInitialize(xmlSecTransformPtr transform) {
    xmlSecRelationshipCtxPtr ctx;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformRelationshipId), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecRelationshipSize), -1);

    ctx = xmlSecRelationshipGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    /* initialize context */
    memset(ctx, 0, sizeof(xmlSecRelationshipCtx));

    ctx->sourceIdList = xmlSecPtrListCreate(xmlSecStringListId);
    if(ctx->sourceIdList == NULL) {
        xmlSecInternalError("xmlSecPtrListCreate",
                            xmlSecTransformGetName(transform));
        return(-1);
    }
    return(0);
}

static void
xmlSecRelationshipFinalize(xmlSecTransformPtr transform) {
    xmlSecRelationshipCtxPtr ctx;

    xmlSecAssert(xmlSecTransformCheckId(transform, xmlSecTransformRelationshipId));
    xmlSecAssert(xmlSecTransformCheckSize(transform, xmlSecRelationshipSize));

    ctx = xmlSecRelationshipGetCtx(transform);
    xmlSecAssert(ctx != NULL);

    if(ctx->sourceIdList != NULL) {
       xmlSecPtrListDestroy(ctx->sourceIdList);
    }

    memset(ctx, 0, sizeof(xmlSecRelationshipCtx));
}

static int
xmlSecRelationshipReadNode(xmlSecTransformPtr transform, xmlNodePtr node, xmlSecTransformCtxPtr transformCtx) {
    xmlSecRelationshipCtxPtr ctx;
    xmlNodePtr cur;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformRelationshipId), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecRelationshipSize), -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(transformCtx != NULL, -1);
    ctx = xmlSecRelationshipGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    cur = node->children;
    while(cur != NULL) {
        if(xmlSecCheckNodeName(cur, xmlSecNodeRelationshipReference, xmlSecRelationshipReferenceNs)) {
            xmlChar* sourceId;

            sourceId = xmlGetProp(cur, xmlSecRelationshipAttrSourceId);
            if(sourceId == NULL) {
                xmlSecInvalidNodeAttributeError(cur, xmlSecRelationshipAttrSourceId,
                                                NULL, "empty");
                return(-1);
            }

            ret = xmlSecPtrListAdd(ctx->sourceIdList, sourceId);
            if(ret < 0) {
                xmlSecInternalError("xmlSecPtrListAdd",
                                    xmlSecTransformGetName(transform));
                xmlFree(sourceId);
                return(-1);
            }
        }

        cur = cur->next;
    }

    return(0);
}

/* Sorts Relationship elements by Id value in lexicographical order. */
static int
xmlSecTransformRelationshipCompare(xmlNodePtr node1, xmlNodePtr node2) {
    xmlChar* id1 = NULL;
    xmlChar* id2 = NULL;
    int ret;

    if(node1 == node2) {
        return(0);
    }
    if(node1 == NULL) {
        return(-1);
    }
    if(node2 == NULL) {
        return(1);
    }

    id1 = xmlGetProp(node1, xmlSecRelationshipAttrId);
    id2 = xmlGetProp(node2, xmlSecRelationshipAttrId);
    if(id1 == NULL) {
        ret = -1;
        goto done;
    }
    if(id2 == NULL) {
        ret = 1;
        goto done;
    }

    ret = xmlStrcmp(id1, id2);

done:
    if (id1 != NULL) {
        xmlFree(id1);
    }
    if (id2 != NULL) {
        xmlFree(id2);
    }

    return ret;
}

/*
 * This is step 2, point 4: if the input sourceId list doesn't contain the Id attribute of the current node,
 * then exclude it from the output, instead of processing it.
 */
static int
xmlSecTransformRelationshipProcessNode(xmlSecTransformPtr transform, xmlOutputBufferPtr buf, xmlNodePtr cur) {
    int found = -1;
    xmlSecRelationshipCtxPtr ctx;
    xmlSecSize ii;
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);
    xmlSecAssert2(cur != NULL, -1);

    if(xmlSecCheckNodeName(cur, xmlSecNodeRelationship, xmlSecRelationshipsNs)) {
        xmlChar* id = xmlGetProp(cur, xmlSecRelationshipAttrId);
        if(id == NULL) {
            xmlSecXmlError2("xmlGetProp(xmlSecRelationshipAttrId)",
                            xmlSecTransformGetName(transform),
                            "name=%s", xmlSecRelationshipAttrId);
            return(-1);
        }

        ctx = xmlSecRelationshipGetCtx(transform);
        for(ii = 0; ii < xmlSecPtrListGetSize(ctx->sourceIdList); ++ii) {
            if(xmlStrcmp((xmlChar *)xmlSecPtrListGetItem(ctx->sourceIdList, ii), id) == 0) {
                found = 1;
                break;
            }
        }

        xmlFree(id);

        if(found < 0) {
            return(0);
        }
    }

    ret = xmlSecTransformRelationshipProcessElementNode(transform, buf, cur);
    if(ret < 0) {
        xmlSecInternalError("xmlSecTransformRelationshipProcessElementNode",
                            xmlSecTransformGetName(transform));
        return(-1);
    }

    return(0);
}

/*
 * This is step 2, point 3: sort elements by Id: we process other elements as-is, but for elements we collect them in a list,
 * then sort, and finally process them (process the head of the list, then pop the head, till the list becomes empty).
 */
static int
xmlSecTransformRelationshipProcessNodeList(xmlSecTransformPtr transform, xmlOutputBufferPtr buf, xmlNodePtr cur) {
    xmlListPtr list;
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);
    xmlSecAssert2(cur != NULL, -1);

    list = xmlListCreate(NULL, (xmlListDataCompare)xmlSecTransformRelationshipCompare);
    if(list == NULL) {
        xmlSecXmlError("xmlListCreate", xmlSecTransformGetName(transform));
        return(-1);
    }

    for(; cur; cur = cur->next) {
        if(xmlStrcmp(cur->name, xmlSecNodeRelationship) == 0) {
            if(xmlListInsert(list, cur) != 0) {
                xmlSecXmlError("xmlListInsert", xmlSecTransformGetName(transform));
                return(-1);
            }
        } else {
            ret = xmlSecTransformRelationshipProcessNode(transform, buf, cur);
            if(ret < 0) {
                xmlSecInternalError("xmlSecTransformRelationshipProcessNode",
                                    xmlSecTransformGetName(transform));
                xmlListDelete(list);
                return(-1);
            }
        }
    }

    xmlListSort(list);

    while(!xmlListEmpty(list)) {
        xmlLinkPtr link = xmlListFront(list);
        xmlNodePtr node = (xmlNodePtr)xmlLinkGetData(link);

        ret = xmlSecTransformRelationshipProcessNode(transform, buf, node);
        if(ret < 0) {
            xmlSecInternalError("xmlSecTransformRelationshipProcessNode",
                                xmlSecTransformGetName(transform));
            xmlListDelete(list);
            return(-1);
        }

        xmlListPopFront(list);
    }

    /* done */
    xmlListDelete(list);
    return(0);
}

static int
xmlSecTransformRelationshipWriteProp(xmlOutputBufferPtr buf, const xmlChar * name, const xmlChar * value) {
    int ret;

    xmlSecAssert2(buf != NULL, -1);
    xmlSecAssert2(name != NULL, -1);

    ret = xmlOutputBufferWriteString(buf, " ");
    if(ret < 0) {
        xmlSecXmlError("xmlOutputBufferWriteString", NULL);
        return(-1);
    }

    ret = xmlOutputBufferWriteString(buf, (const char*) name);
    if(ret < 0) {
        xmlSecXmlError("xmlOutputBufferWriteString", NULL);
        return(-1);
    }

    if(value != NULL) {
        ret = xmlOutputBufferWriteString(buf, "=\"");
        if(ret < 0) {
            xmlSecXmlError("xmlOutputBufferWriteString", NULL);
            return(-1);
        }
        ret = xmlOutputBufferWriteString(buf, (const char*) value);
        if(ret < 0) {
            xmlSecXmlError("xmlOutputBufferWriteString", NULL);
            return(-1);
        }
        ret = xmlOutputBufferWriteString(buf, "\"");
        if(ret < 0) {
            xmlSecXmlError("xmlOutputBufferWriteString", NULL);
            return(-1);
        }
    }

    return (0);
}

static int
xmlSecTransformRelationshipWriteNs(xmlOutputBufferPtr buf, const xmlChar * href) {
    xmlSecAssert2(buf != NULL, -1);

    return(xmlSecTransformRelationshipWriteProp(buf, BAD_CAST "xmlns", (href != NULL) ? href : BAD_CAST ""));
}


static int
xmlSecTransformRelationshipProcessElementNode(xmlSecTransformPtr transform, xmlOutputBufferPtr buf, xmlNodePtr cur) {
    xmlAttrPtr attr;
    int foundTargetMode = 0;
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);
    xmlSecAssert2(cur != NULL, -1);
    xmlSecAssert2(cur->name != NULL, -1);

    /* write open node */
    ret = xmlOutputBufferWriteString(buf, "<");
    if(ret < 0) {
        xmlSecXmlError("xmlOutputBufferWriteString",
                            xmlSecTransformGetName(transform));
        return(-1);
    }
    ret = xmlOutputBufferWriteString(buf, (const char *)cur->name);
    if(ret < 0) {
        xmlSecXmlError("xmlOutputBufferWriteString",
                            xmlSecTransformGetName(transform));
        return(-1);
    }

    /* write namespaces */
    if(cur->nsDef != NULL) {
        ret = xmlSecTransformRelationshipWriteNs(buf, cur->nsDef->href);
        if(ret < 0) {
            xmlSecInternalError("xmlSecTransformRelationshipWriteNs",
                                xmlSecTransformGetName(transform));
            return(-1);
        }
    }

    /*
     *  write attributes:
     *
     *  This is step 3, point 6: add default value of TargetMode if there is no such attribute.
     */
    for(attr = cur->properties; attr != NULL; attr = attr->next) {
        xmlChar * value = xmlGetProp(cur, attr->name);

        if(xmlStrcmp(attr->name, xmlSecRelationshipAttrTargetMode) == 0) {
            foundTargetMode = 1;
        }

        ret = xmlSecTransformRelationshipWriteProp(buf, attr->name, value);
        if(ret < 0) {
            xmlSecInternalError("xmlSecTransformRelationshipWriteProp",
                                xmlSecTransformGetName(transform));
            xmlFree(value);
            return(-1);
        }

        xmlFree(value);
    }

    /* write TargetMode */
    if(xmlStrcmp(cur->name, xmlSecNodeRelationship) == 0 && !foundTargetMode) {
        ret = xmlSecTransformRelationshipWriteProp(buf, xmlSecRelationshipAttrTargetMode, BAD_CAST "Internal");
        if(ret < 0) {
            xmlSecInternalError("xmlSecTransformRelationshipWriteProp(TargetMode=Internal)",
                                xmlSecTransformGetName(transform));
            return(-1);
        }
    }

    /* finish writing open node */
    ret = xmlOutputBufferWriteString(buf, ">");
    if(ret < 0) {
        xmlSecXmlError("xmlOutputBufferWriteString",
                            xmlSecTransformGetName(transform));
        return(-1);
    }

    /* write children */
    if(cur->children != NULL) {
        ret = xmlSecTransformRelationshipProcessNodeList(transform, buf, cur->children);
        if(ret < 0) {
            xmlSecInternalError("xmlSecTransformRelationshipProcessNodeList",
                                xmlSecTransformGetName(transform));
            return(-1);
        }
    }

    /* write closing node */
    ret = xmlOutputBufferWriteString(buf, "</");
    if(ret < 0) {
        xmlSecXmlError("xmlOutputBufferWriteString",
                            xmlSecTransformGetName(transform));
        return(-1);
    }
    ret = xmlOutputBufferWriteString(buf, (const char *)cur->name);
    if(ret < 0) {
        xmlSecXmlError("xmlOutputBufferWriteString",
                            xmlSecTransformGetName(transform));
        return(-1);
    }
    if(xmlOutputBufferWriteString(buf, ">") < 0) {
        xmlSecXmlError("xmlOutputBufferWriteString",
                            xmlSecTransformGetName(transform));
        return(-1);
    }

    /* done */
    return(0);
}

static int
xmlSecTransformRelationshipExecute(xmlSecTransformPtr transform, xmlOutputBufferPtr buf, xmlDocPtr doc) {
    int ret;

    xmlSecAssert2(transform != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);
    xmlSecAssert2(doc != NULL, -1);

    if(doc->children != NULL) {
        ret = xmlSecTransformRelationshipProcessNodeList(transform, buf, doc->children);
        if(ret < 0) {
            xmlSecInternalError("xmlSecTransformRelationshipProcessNodeList",
                                xmlSecTransformGetName(transform));
            return(-1);
        }
    }

    return(0);
}

static int
xmlSecTransformRelationshipPushXml(xmlSecTransformPtr transform, xmlSecNodeSetPtr nodes, xmlSecTransformCtxPtr transformCtx)
{
    xmlOutputBufferPtr buf;
    xmlSecRelationshipCtxPtr ctx;
    int ret;

    xmlSecAssert2(nodes != NULL, -1);
    xmlSecAssert2(nodes->doc != NULL, -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    ctx = xmlSecRelationshipGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

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

    ret = xmlSecTransformRelationshipExecute(transform, buf, nodes->doc);
    if(ret < 0) {
       xmlSecInternalError("xmlSecTransformRelationshipExecute",
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
xmlSecTransformRelationshipPopBin(xmlSecTransformPtr transform, xmlSecByte* data, xmlSecSize maxDataSize, xmlSecSize* dataSize, xmlSecTransformCtxPtr transformCtx) {
    xmlSecBufferPtr out;
    int ret;

    xmlSecAssert2(data != NULL, -1);
    xmlSecAssert2(dataSize != NULL, -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    out = &(transform->outBuf);
    if(transform->status == xmlSecTransformStatusNone) {
       xmlOutputBufferPtr buf;

       xmlSecAssert2(transform->inNodes == NULL, -1);

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

       ret = xmlC14NExecute(transform->inNodes->doc, (xmlC14NIsVisibleCallback)xmlSecNodeSetContains, transform->inNodes, XML_C14N_1_0, NULL, 0, buf);
       if(ret < 0) {
            xmlSecInternalError("xmlC14NExecute",
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
           xmlSecAssert2(xmlSecBufferGetData(out), -1);

           memcpy(data, xmlSecBufferGetData(out), outSize);
           ret = xmlSecBufferRemoveHead(out, outSize);
           if(ret < 0) {
               xmlSecInternalError2("xmlSecBufferRemoveHead",
                                    xmlSecTransformGetName(transform),
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
