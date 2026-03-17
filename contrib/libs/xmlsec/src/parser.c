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
 * SECTION:parser
 * @Short_description: XML parser functions and the XML parser transform implementation.
 * @Stability: Stable
 *
 */

#include "globals.h"

#include <stdlib.h>
#include <string.h>

#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/parserInternals.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>
#include <xmlsec/parser.h>
#include <xmlsec/errors.h>

#include "cast_helpers.h"

/**************************************************************************
 *
 * Internal parser
 *
 *****************************************************************************/
typedef struct _xmlSecParserCtx                                 xmlSecParserCtx,
                                                                *xmlSecParserCtxPtr;
struct _xmlSecParserCtx {
    xmlParserCtxtPtr    parserCtx;
};

/**************************************************************************
 *
 * XML Parser transform
 *
 * xmlSecTransform + xmlSecParserCtx
 *
 ***************************************************************************/
XMLSEC_TRANSFORM_DECLARE(Parser, xmlSecParserCtx)
#define xmlSecParserSize XMLSEC_TRANSFORM_SIZE(Parser)

static int              xmlSecParserInitialize                  (xmlSecTransformPtr transform);
static void             xmlSecParserFinalize                    (xmlSecTransformPtr transform);
static int              xmlSecParserPushBin                     (xmlSecTransformPtr transform,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize,
                                                                 int final,
                                                                 xmlSecTransformCtxPtr transformCtx);
static int              xmlSecParserPopXml                      (xmlSecTransformPtr transform,
                                                                 xmlSecNodeSetPtr* nodes,
                                                                 xmlSecTransformCtxPtr transformCtx);

static xmlSecTransformKlass xmlSecParserKlass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecParserSize,                           /* xmlSecSize objSize */

    BAD_CAST "xml-parser",                      /* const xmlChar* name; */
    NULL,                                       /* const xmlChar* href; */
    xmlSecTransformUsageDSigTransform,          /* xmlSecTransformUsage usage; */

    xmlSecParserInitialize,                     /* xmlSecTransformInitializeMethod initialize; */
    xmlSecParserFinalize,                       /* xmlSecTransformFinalizeMethod finalize; */
    NULL,                                       /* xmlSecTransformNodeReadMethod readNode; */
    NULL,                                       /* xmlSecTransformNodeWriteMethod writeNode; */
    NULL,                                       /* xmlSecTransformSetKeyReqMethod setKeyReq; */
    NULL,                                       /* xmlSecTransformSetKeyMethod setKey; */
    NULL,                                       /* xmlSecTransformValidateMethod validate; */
    xmlSecTransformDefaultGetDataType,          /* xmlSecTransformGetDataTypeMethod getDataType; */
    xmlSecParserPushBin,                /* xmlSecTransformPushBinMethod pushBin; */
    NULL,                                       /* xmlSecTransformPopBinMethod popBin; */
    NULL,                                       /* xmlSecTransformPushXmlMethod pushXml; */
    xmlSecParserPopXml,         /* xmlSecTransformPopXmlMethod popXml; */
    NULL,                                       /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};


/**
 * xmlSecTransformXmlParserGetKlass:
 *
 * The XML parser transform.
 *
 * Returns: XML parser transform klass.
 */
xmlSecTransformId
xmlSecTransformXmlParserGetKlass(void) {
    return(&xmlSecParserKlass);
}

static int
xmlSecParserInitialize(xmlSecTransformPtr transform) {
    xmlSecParserCtxPtr ctx;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformXmlParserId), -1);
    xmlSecAssert2(xmlSecTransformCheckSize(transform, xmlSecParserSize), -1);

    ctx = xmlSecParserGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    /* initialize context */
    memset(ctx, 0, sizeof(xmlSecParserCtx));
    return(0);
}

static void
xmlSecParserFinalize(xmlSecTransformPtr transform) {
    xmlSecParserCtxPtr ctx;

    xmlSecAssert(xmlSecTransformCheckId(transform, xmlSecTransformXmlParserId));
    xmlSecAssert(xmlSecTransformCheckSize(transform, xmlSecParserSize));

    ctx = xmlSecParserGetCtx(transform);
    xmlSecAssert(ctx != NULL);

    if(ctx->parserCtx != NULL) {
        if(ctx->parserCtx->myDoc != NULL) {
            xmlFreeDoc(ctx->parserCtx->myDoc);
        ctx->parserCtx->myDoc = NULL;
        }
        xmlFreeParserCtxt(ctx->parserCtx);
    }
    memset(ctx, 0, sizeof(xmlSecParserCtx));
}

static int
xmlSecParserPushBin(xmlSecTransformPtr transform, const xmlSecByte* data,
                    xmlSecSize dataSize, int final, xmlSecTransformCtxPtr transformCtx) {
    xmlSecParserCtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformXmlParserId), -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    ctx = xmlSecParserGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    /* check/update current transform status */
    if(transform->status == xmlSecTransformStatusNone) {
        xmlSecAssert2(ctx->parserCtx == NULL, -1);

        ctx->parserCtx = xmlCreatePushParserCtxt(NULL, NULL, NULL, 0, NULL);
        if(ctx->parserCtx == NULL) {
            xmlSecXmlError("xmlCreatePushParserCtxt", xmlSecTransformGetName(transform));
            return(-1);
        }
        xmlSecParsePrepareCtxt(ctx->parserCtx);

        transform->status = xmlSecTransformStatusWorking;
    } else if(transform->status == xmlSecTransformStatusFinished) {
        return(0);
    } else if(transform->status != xmlSecTransformStatusWorking) {
        xmlSecInvalidTransfromStatusError(transform);
        return(-1);
    }
    xmlSecAssert2(transform->status == xmlSecTransformStatusWorking, -1);
    xmlSecAssert2(ctx->parserCtx != NULL, -1);

    /* push data to the input buffer */
    if((data != NULL) && (dataSize > 0)) {
        int dataLen;
        XMLSEC_SAFE_CAST_SIZE_TO_INT(dataSize, dataLen, return(-1), xmlSecTransformGetName(transform));
        ret = xmlParseChunk(ctx->parserCtx, (const char*)data, dataLen, 0);
        if(ret != 0) {
            xmlSecXmlParserError2("xmlParseChunk", ctx->parserCtx,
                xmlSecTransformGetName(transform),
                "size=%d", dataLen);
            return(-1);
        }
    }

    /* finish parsing and push to next in the chain */
    if(final != 0) {
        ret = xmlParseChunk(ctx->parserCtx, NULL, 0, 1);
        if((ret != 0) || (ctx->parserCtx->myDoc == NULL)) {
            xmlSecXmlParserError("xmlParseChunk", ctx->parserCtx,
                xmlSecTransformGetName(transform));
            return(-1);
        }

        /* todo: check that document is well formed? */
        transform->outNodes = xmlSecNodeSetCreate(ctx->parserCtx->myDoc,
                                                  NULL, xmlSecNodeSetTree);
        if(transform->outNodes == NULL) {
            xmlSecInternalError("xmlSecNodeSetCreate", xmlSecTransformGetName(transform));
            xmlFreeDoc(ctx->parserCtx->myDoc);
            ctx->parserCtx->myDoc = NULL;
            return(-1);
        }
        xmlSecNodeSetDocDestroy(transform->outNodes); /* this node set "owns" the doc pointer */
        ctx->parserCtx->myDoc = NULL;

        /* push result to the next transform (if exist) */
        if(transform->next != NULL) {
            ret = xmlSecTransformPushXml(transform->next, transform->outNodes, transformCtx);
            if(ret < 0) {
                xmlSecInternalError("xmlSecTransformPushXml", xmlSecTransformGetName(transform));
                return(-1);
            }
        }

        transform->status = xmlSecTransformStatusFinished;
    }

    return(0);
}

static int
xmlSecParserPopXml(xmlSecTransformPtr transform, xmlSecNodeSetPtr* nodes,
                               xmlSecTransformCtxPtr transformCtx) {
    xmlSecParserCtxPtr ctx;
    xmlParserInputBufferPtr buf;
    xmlParserInputPtr input;
    xmlParserCtxtPtr ctxt;
    xmlDocPtr doc;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformXmlParserId), -1);
    xmlSecAssert2(nodes != NULL, -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    ctx = xmlSecParserGetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    /* check/update current transform status */
    switch(transform->status) {
    case xmlSecTransformStatusNone:
        transform->status = xmlSecTransformStatusWorking;
        break;
    case xmlSecTransformStatusWorking:
        /* just do nothing */
        break;
    case xmlSecTransformStatusFinished:
        (*nodes) = NULL;
        return(0);
    default:
        xmlSecInvalidTransfromStatusError(transform);
        return(-1);
    }
    xmlSecAssert2(transform->status == xmlSecTransformStatusWorking, -1);

    /* prepare parser context */
    if(transform->prev == NULL) {
        xmlSecInvalidTransfromError2(transform,
                                     "prev transform=\"%s\"",
                                     xmlSecErrorsSafeString(transform->prev));
        return(-1);
    }

    buf = xmlSecTransformCreateInputBuffer(transform->prev, transformCtx);
    if(buf == NULL) {
        xmlSecInternalError("xmlSecTransformCreateInputBuffer",
                            xmlSecTransformGetName(transform));
        return(-1);
    }

    ctxt = xmlNewParserCtxt();
    if (ctxt == NULL) {
        xmlSecXmlError("xmlNewParserCtxt",
                       xmlSecTransformGetName(transform));
        xmlFreeParserInputBuffer(buf);
        return(-1);
    }
    xmlSecParsePrepareCtxt(ctxt);

    input = xmlNewIOInputStream(ctxt, buf, XML_CHAR_ENCODING_NONE);
    if(input == NULL) {
        xmlSecXmlParserError("xmlNewParserCtxt", ctxt,
                             xmlSecTransformGetName(transform));
        xmlFreeParserCtxt(ctxt);
        xmlFreeParserInputBuffer(buf);
        return(-1);
    }

    ret = inputPush(ctxt, input);
    if(ret < 0) {
        xmlSecXmlParserError("inputPush", ctxt,
                             xmlSecTransformGetName(transform));
        xmlFreeInputStream(input);
        if(ctxt->myDoc != NULL) {
            xmlFreeDoc(ctxt->myDoc);
            ctxt->myDoc = NULL;
        }
        xmlFreeParserCtxt(ctxt);
        return(-1);
    }

    /* finaly do the parsing */
    ret = xmlParseDocument(ctxt);
    if(ret < 0) {
        xmlSecXmlParserError("xmlParseDocument", ctxt,
                             xmlSecTransformGetName(transform));
        if(ctxt->myDoc != NULL) {
            xmlFreeDoc(ctxt->myDoc);
            ctxt->myDoc = NULL;
        }
        xmlFreeParserCtxt(ctxt);
        return(-1);
    }

    /* remember the result and free parsing context */
    doc = ctxt->myDoc;
    ctxt->myDoc = NULL;
    xmlFreeParserCtxt(ctxt);

    /* return result to the caller */
    (*nodes) = xmlSecNodeSetCreate(doc, NULL, xmlSecNodeSetTree);
    if((*nodes) == NULL) {
        xmlSecInternalError("xmlSecNodeSetCreate",
                            xmlSecTransformGetName(transform));
        xmlFreeDoc(doc);
        return(-1);
    }
    xmlSecNodeSetDocDestroy((*nodes)); /* this node set "owns" the doc pointer */
    transform->status = xmlSecTransformStatusFinished;
    return(0);
}

/**************************************************************************
 *
 * XML Parser functions
 *
 *************************************************************************/
typedef struct _xmlSecExtMemoryParserCtx {
    const xmlSecByte    *prefix;
    xmlSecSize                  prefixSize;
    const xmlSecByte    *buffer;
    xmlSecSize                  bufferSize;
    const xmlSecByte    *postfix;
    xmlSecSize                  postfixSize;
} xmlSecExtMemoryParserCtx, *xmlSecExtMemoryParserCtxPtr;

/**
 * xmlSecParseFile:
 * @filename:           the filename.
 *
 * Loads XML Doc from file @filename. We need a special version because of
 * c14n issue. The code is copied from xmlSAXParseFileWithData() function.
 *
 * Returns: pointer to the loaded XML document or NULL if an error occurs.
 */
xmlDocPtr
xmlSecParseFile(const char *filename) {
    xmlParserCtxtPtr ctxt;
    int ret;
    xmlDocPtr res = NULL;

    xmlSecAssert2(filename != NULL, NULL);

    xmlInitParser();
    ctxt = xmlCreateFileParserCtxt(filename);
    if (ctxt == NULL) {
        xmlSecXmlError2("xmlCreateFileParserCtxt", NULL,
                        "filename=%s", xmlSecErrorsSafeString(filename));
        goto done;
    }
    xmlSecParsePrepareCtxt(ctxt);

    /* todo: set directories from current doc? */
    if (ctxt->directory == NULL) {
        ctxt->directory = xmlParserGetDirectory(filename);
        if(ctxt->directory == NULL) {
            xmlSecXmlError2("xmlParserGetDirectory", NULL,
                "filename=%s", xmlSecErrorsSafeString(filename));
            goto done;
        }
    }

    ret = xmlParseDocument(ctxt);
    if(ret < 0) {
        xmlSecXmlParserError2("xmlParseDocument", ctxt, NULL,
            "filename=%s",
            xmlSecErrorsSafeString(filename));
        goto done;
    }

    if(!ctxt->wellFormed) {
       xmlSecInternalError("document is not well formed", NULL);
       goto done;
    }

    /* success */
    res = ctxt->myDoc;
    ctxt->myDoc = NULL;

done:
    /* cleanup */
    if(ctxt != NULL) {
        if(ctxt->myDoc != NULL) {
            xmlFreeDoc(ctxt->myDoc);
            ctxt->myDoc = NULL;
        }
        xmlFreeParserCtxt(ctxt);
    }
    return(res);

}

/**
 * xmlSecParseMemoryExt:
 * @prefix:             the first part of the input.
 * @prefixSize:         the size of the first part of the input.
 * @buffer:             the second part of the input.
 * @bufferSize:         the size of the second part of the input.
 * @postfix:            the third part of the input.
 * @postfixSize:        the size of the third part of the input.
 *
 * Loads XML Doc from 3 chunks of memory: @prefix, @buffer and @postfix.
 *
 * Returns: pointer to the loaded XML document or NULL if an error occurs.
 */
xmlDocPtr
xmlSecParseMemoryExt(const xmlSecByte *prefix, xmlSecSize prefixSize,
                     const xmlSecByte *buffer, xmlSecSize bufferSize,
                     const xmlSecByte *postfix, xmlSecSize postfixSize) {
    xmlParserCtxtPtr ctxt = NULL;
    xmlDocPtr doc = NULL;
    int ret;

    /* create context */
    ctxt = xmlCreatePushParserCtxt(NULL, NULL, NULL, 0, NULL);
    if(ctxt == NULL) {
        xmlSecXmlError("xmlCreatePushParserCtxt", NULL);
        goto done;
    }
    xmlSecParsePrepareCtxt(ctxt);

    /* prefix */
    if((prefix != NULL) && (prefixSize > 0)) {
        int prefixLen;

        XMLSEC_SAFE_CAST_SIZE_TO_INT(prefixSize, prefixLen, goto done, NULL);
        ret = xmlParseChunk(ctxt, (const char*)prefix, prefixLen, 0);
        if(ret != 0) {
            xmlSecXmlParserError2("xmlParseChunk", ctxt, NULL,
                "chunkSize=%d", prefixLen);
            goto done;
        }
    }

    /* buffer */
    if((buffer != NULL) && (bufferSize > 0)) {
        int bufferLen;

        XMLSEC_SAFE_CAST_SIZE_TO_INT(bufferSize, bufferLen, goto done, NULL);
        ret = xmlParseChunk(ctxt, (const char*)buffer, bufferLen, 0);
        if(ret != 0) {
            xmlSecXmlParserError2("xmlParseChunk", ctxt, NULL,
                "chunkSize=%d", bufferLen);
            goto done;
        }
    }

    /* postfix */
    if((postfix != NULL) && (postfixSize > 0)) {
        int postfixLen;

        XMLSEC_SAFE_CAST_SIZE_TO_INT(postfixSize, postfixLen, goto done, NULL);
        ret = xmlParseChunk(ctxt, (const char*)postfix, postfixLen, 0);
        if(ret != 0) {
            xmlSecXmlParserError2("xmlParseChunk", ctxt, NULL,
                "chunkSize=%d", postfixLen);
            goto done;
        }
    }

    /* finishing */
    ret = xmlParseChunk(ctxt, NULL, 0, 1);
    if((ret != 0) || (ctxt->myDoc == NULL)) {
        xmlSecXmlParserError("xmlParseChunk", ctxt, NULL);
        goto done;
    }
    doc = ctxt->myDoc;
    ctxt->myDoc = NULL;

done:
    if(ctxt != NULL) {
        if(ctxt->myDoc != NULL) {
            xmlFreeDoc(ctxt->myDoc);
            ctxt->myDoc = NULL;
        }
        xmlFreeParserCtxt(ctxt);
    }
    return(doc);
}


/**
 * xmlSecParseMemory:
 * @buffer:             the input buffer.
 * @size:               the input buffer size.
 * @recovery:           the flag.
 *
 * Loads XML Doc from memory. We need a special version because of
 * c14n issue. The code is copied from xmlSAXParseMemory() function.
 *
 * Returns: pointer to the loaded XML document or NULL if an error occurs.
 */
xmlDocPtr
xmlSecParseMemory(const xmlSecByte *buffer, xmlSecSize size, int recovery) {
    xmlParserCtxtPtr ctxt;
    xmlDocPtr res = NULL;
    int len;
    int ret;

    xmlSecAssert2(buffer != NULL, NULL);

    XMLSEC_SAFE_CAST_SIZE_TO_INT(size, len, return(NULL), NULL);
    ctxt = xmlCreateMemoryParserCtxt((char*)buffer, len);
    if (ctxt == NULL) {
        xmlSecXmlError("xmlCreateMemoryParserCtxt", NULL);
        return(NULL);
    }
    xmlSecParsePrepareCtxt(ctxt);

    ret = xmlParseDocument(ctxt);
    if(ret < 0) {
        xmlSecXmlParserError("xmlParseDocument", ctxt, NULL);
        if(ctxt->myDoc != NULL) {
            xmlFreeDoc(ctxt->myDoc);
            ctxt->myDoc = NULL;
        }
        xmlFreeParserCtxt(ctxt);
        return(NULL);
    }

    if(!(ctxt->wellFormed) && !recovery) {
        xmlSecInternalError("document is not well formed", NULL);
        if(ctxt->myDoc != NULL) {
            xmlFreeDoc(ctxt->myDoc);
            ctxt->myDoc = NULL;
        }
        xmlFreeParserCtxt(ctxt);
        return(NULL);
    }

    /* done */
    res = ctxt->myDoc;
    ctxt->myDoc = NULL;
    xmlFreeParserCtxt(ctxt);
    return(res);
}

/**
 * xmlSecParsePrepareCtxt:
 * @ctxt:               the parser context
 *
 * Prepares parser context for parsing XML for XMLSec.
 */
void
xmlSecParsePrepareCtxt(xmlParserCtxtPtr ctxt) {
    xmlSecAssert(ctxt != NULL);

    /* required for c14n! */
    ctxt->loadsubset = XML_DETECT_IDS | XML_COMPLETE_ATTRS;
    ctxt->replaceEntities = 1;

    xmlCtxtUseOptions(ctxt, xmlSecParserGetDefaultOptions());
}

/*
 * XML_PARSE_NONET  to support c14n
 * XML_PARSE_NODICT to avoid problems with moving nodes around
 * XML_PARSE_HUGE   to enable parsing of XML documents with large text nodes
 */
static int g_xmlsec_parser_default_options = XML_PARSE_NONET | XML_PARSE_NODICT | XML_PARSE_HUGE;

/**
 * xmlSecParserGetDefaultOptions:
 *
 * Gets default LibXML2 parser options.
 *
 * Returns: the current default LibXML2 parser options.
 */
int
xmlSecParserGetDefaultOptions(void) {
    return (g_xmlsec_parser_default_options);
}

/**
 * xmlSecParserSetDefaultOptions:
 * @options:            the new parser options.
 *
 * Sets default LibXML2 parser options.
 */
void xmlSecParserSetDefaultOptions(int options) {
    g_xmlsec_parser_default_options = options;
}
