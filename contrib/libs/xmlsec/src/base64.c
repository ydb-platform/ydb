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
 * SECTION:base64
 * @Short_description: Base64 encoding/decoding functions and base64 transform implementation.
 * @Stability: Stable
 *
 */

#include "globals.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <libxml/tree.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>
#include <xmlsec/base64.h>
#include <xmlsec/errors.h>

#include "cast_helpers.h"

/*
 * the table to map numbers to base64
 */
static const xmlSecByte base64[] =
{
/*   0    1    2    3    4    5    6    7   */
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', /* 0 */
    'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', /* 1 */
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', /* 2 */
    'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', /* 3 */
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', /* 4 */
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v', /* 5 */
    'w', 'x', 'y', 'z', '0', '1', '2', '3', /* 6 */
    '4', '5', '6', '7', '8', '9', '+', '/'  /* 7 */
};


/* few macros to simplify the code */
#define xmlSecBase64Encode1(a)          (((a) >> 2) & 0x3F)
#define xmlSecBase64Encode2(a, b)       ((((a) << 4) & 0x30) + (((b) >> 4) & 0x0F))
#define xmlSecBase64Encode3(b, c)       ((((b) << 2) & 0x3c) + (((c) >> 6) & 0x03))
#define xmlSecBase64Encode4(c)          ((c) & 0x3F)

#define xmlSecBase64Decode1(a, b)       ((xmlSecByte)(((a) << 2) | (((b) & 0x3F) >> 4)))
#define xmlSecBase64Decode2(b, c)       ((xmlSecByte)(((b) << 4) | (((c) & 0x3F) >> 2)))
#define xmlSecBase64Decode3(c, d)       ((xmlSecByte)(((c) << 6) | ((d) & 0x3F)))

#define xmlSecIsBase64Char(ch)          ((((ch) >= 'A') && ((ch) <= 'Z')) || \
                                         (((ch) >= 'a') && ((ch) <= 'z')) || \
                                         (((ch) >= '0') && ((ch) <= '9')) || \
                                         ((ch) == '+') || ((ch) == '/'))
#define xmlSecIsBase64Space(ch)         (((ch) == ' ') || ((ch) == '\t') || \
                                         ((ch) == '\x0d') || ((ch) == '\x0a'))



/***********************************************************************
 *
 * Base64 Context
 *
 ***********************************************************************/
typedef enum {
    xmlSecBase64StatusConsumeAndNext  = 0,
    xmlSecBase64StatusConsumeAndRepeat,
    xmlSecBase64StatusNext,
    xmlSecBase64StatusDone,
    xmlSecBase64StatusFailed
} xmlSecBase64Status;

struct _xmlSecBase64Ctx {
    int                 encode;
    xmlSecSize          columns;
    int                 inByte;
    xmlSecSize          inPos;
    xmlSecSize          linePos;
    int                 finished;
};

static xmlSecBase64Status       xmlSecBase64CtxEncodeByte       (xmlSecBase64CtxPtr ctx,
                                                                 xmlSecByte  inByte,
                                                                 xmlSecByte* outByte);
static xmlSecBase64Status       xmlSecBase64CtxEncodeByteFinal  (xmlSecBase64CtxPtr ctx,
                                                                 xmlSecByte* outByte);
static xmlSecBase64Status       xmlSecBase64CtxDecodeByte       (xmlSecBase64CtxPtr ctx,
                                                                 xmlSecByte inByte,
                                                                 xmlSecByte* outByte);
static int                      xmlSecBase64CtxEncode           (xmlSecBase64CtxPtr ctx,
                                                                 const xmlSecByte* inBuf,
                                                                 xmlSecSize inBufSize,
                                                                 xmlSecSize* inBufResSize,
                                                                 xmlSecByte* outBuf,
                                                                 xmlSecSize outBufSize,
                                                                 xmlSecSize* outBufResSize);
static int                      xmlSecBase64CtxEncodeFinal      (xmlSecBase64CtxPtr ctx,
                                                                 xmlSecByte* outBuf,
                                                                 xmlSecSize outBufSize,
                                                                 xmlSecSize* outBufResSize);
static int                      xmlSecBase64CtxDecode           (xmlSecBase64CtxPtr ctx,
                                                                 const xmlSecByte* inBuf,
                                                                 xmlSecSize inBufSize,
                                                                 xmlSecSize* inBufResSize,
                                                                 xmlSecByte* outBuf,
                                                                 xmlSecSize outBufSize,
                                                                 xmlSecSize* outBufResSize);
static int                      xmlSecBase64CtxDecodeIsFinished (xmlSecBase64CtxPtr ctx);


static int g_xmlsec_base64_default_line_size = XMLSEC_BASE64_LINESIZE;

/**
 * xmlSecBase64GetDefaultLineSize:
 *
 * Gets the current default line size.
 *
 * Returns: the current default line size.
 */
int
xmlSecBase64GetDefaultLineSize(void)
{
    return g_xmlsec_base64_default_line_size;
}

/**
 * xmlSecBase64SetDefaultLineSize:
 * @columns: number of columns
 *
 * Sets the current default line size.
 */
void
xmlSecBase64SetDefaultLineSize(int columns)
{
    g_xmlsec_base64_default_line_size = columns;
}

/**
 * xmlSecBase64CtxCreate:
 * @encode:             the encode/decode flag (1 - encode, 0 - decode)
 * @columns:            the max line length.
 *
 * Allocates and initializes new base64 context.
 *
 * Returns: a pointer to newly created #xmlSecBase64Ctx structure
 * or NULL if an error occurs.
 */
xmlSecBase64CtxPtr
xmlSecBase64CtxCreate(int encode, int columns) {
    xmlSecBase64CtxPtr ctx;
    int ret;

    /*
     * Allocate a new xmlSecBase64CtxPtr and fill the fields.
     */
    ctx = (xmlSecBase64CtxPtr) xmlMalloc(sizeof(xmlSecBase64Ctx));
    if (ctx == NULL) {
        xmlSecMallocError(sizeof(xmlSecBase64Ctx), NULL);
        return(NULL);
    }

    ret = xmlSecBase64CtxInitialize(ctx, encode, columns);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBase64CtxInitialize", NULL);
        xmlSecBase64CtxDestroy(ctx);
        return(NULL);
    }
    return(ctx);
}

/**
 * xmlSecBase64CtxDestroy:
 * @ctx:                the pointer to #xmlSecBase64Ctx structure.
 *
 * Destroys base64 context.
 */
void
xmlSecBase64CtxDestroy(xmlSecBase64CtxPtr ctx) {
    xmlSecAssert(ctx != NULL);

    xmlSecBase64CtxFinalize(ctx);
    xmlFree(ctx);
}

/**
 * xmlSecBase64CtxInitialize:
 * @ctx:                the pointer to #xmlSecBase64Ctx structure,
 * @encode:             the encode/decode flag (1 - encode, 0 - decode)
 * @columns:            the max line length.
 *
 * Initializes new base64 context.
 *
 * Returns: 0 on success and a negative value otherwise.
 */
int
xmlSecBase64CtxInitialize(xmlSecBase64CtxPtr ctx, int encode, int columns) {
    xmlSecAssert2(ctx != NULL, -1);

    memset(ctx, 0, sizeof(xmlSecBase64Ctx));

    ctx->encode  = encode;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(columns, ctx->columns, return(-1), NULL);
    return(0);
}

/**
 * xmlSecBase64CtxFinalize:
 * @ctx:                the pointer to #xmlSecBase64Ctx structure,
 *
 * Frees all the resources allocated by @ctx.
 */
void
xmlSecBase64CtxFinalize(xmlSecBase64CtxPtr ctx) {
    xmlSecAssert(ctx != NULL);

    memset(ctx, 0, sizeof(xmlSecBase64Ctx));
}

/**
 * xmlSecBase64CtxUpdate_ex:
 * @ctx:                the pointer to #xmlSecBase64Ctx structure
 * @in:                 the input buffer
 * @inSize:             the input buffer size
 * @out:                the output buffer
 * @outSize:            the output buffer size
 * @outWritten:         the pointer to store the number of bytes written into the output
 *
 * Encodes or decodes the next piece of data from input buffer.
 *
 * Returns: 0 on success and a negative value otherwise.
 */
int
xmlSecBase64CtxUpdate_ex(xmlSecBase64CtxPtr ctx, const xmlSecByte *in, xmlSecSize inSize,
    xmlSecByte *out, xmlSecSize outSize, xmlSecSize* outWritten) {
    xmlSecSize inRead = 0;
    int ret;

    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(in != NULL, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    if(ctx->encode != 0) {
        ret = xmlSecBase64CtxEncode(ctx, in, inSize, &inRead, out, outSize, outWritten);
        if((ret < 0) || (inRead != inSize)) {
            xmlSecInternalError("xmlSecBase64CtxEncode", NULL);
            return(-1);
        }
    } else {
        ret = xmlSecBase64CtxDecode(ctx, in, inSize, &inRead, out, outSize, outWritten);
        if((ret < 0) || (inRead != inSize)) {
            xmlSecInternalError("xmlSecBase64CtxDecode", NULL);
            return(-1);
        }
    }

    return(0);
}

/**
 * xmlSecBase64CtxFinal_ex:
 * @ctx:                the pointer to #xmlSecBase64Ctx structure
 * @out:                the output buffer
 * @outSize:            the output buffer size
 * @outWritten:         the pointer to store the number of bytes written into the output
 *
 * Encodes or decodes the last piece of data stored in the context
 * and finalizes the result.
 *
 * Returns: 0 on success and a negative value otherwise.
 */
int
xmlSecBase64CtxFinal_ex(xmlSecBase64CtxPtr ctx, xmlSecByte *out, xmlSecSize outSize, xmlSecSize* outWritten) {
    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outSize > 0, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    if(ctx->encode != 0) {
        int ret;

        ret = xmlSecBase64CtxEncodeFinal(ctx, out, outSize, outWritten);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecBase64CtxEncodeFinal", NULL,
                "outSize=" XMLSEC_SIZE_FMT, outSize);
            return(-1);
        }
    } else {
        if(!xmlSecBase64CtxDecodeIsFinished(ctx)) {
            xmlSecInternalError("xmlSecBase64CtxDecodeIsFinished", NULL);
            return(-1);
        }
        (*outWritten) = 0;
    }

    /* add \0 just in case (if we can) */
    if(((*outWritten) + 1) < outSize) {
        out[(*outWritten)] = '\0';
    }
    return(0);
}

static xmlSecBase64Status
xmlSecBase64CtxEncodeByte(xmlSecBase64CtxPtr ctx, xmlSecByte inByte, xmlSecByte* outByte) {
    xmlSecAssert2(ctx != NULL, xmlSecBase64StatusFailed);
    xmlSecAssert2(outByte != NULL, xmlSecBase64StatusFailed);

    if((ctx->columns > 0) && (ctx->linePos >= ctx->columns)) {
        (*outByte) = '\n';
        ctx->linePos = 0;
        return(xmlSecBase64StatusConsumeAndRepeat);
    } else if(ctx->inPos == 0) {
        /* we just started new block */
        (*outByte) = base64[xmlSecBase64Encode1(inByte)];
        ctx->inByte = inByte;
        ++ctx->linePos;
        ++ctx->inPos;
        return(xmlSecBase64StatusConsumeAndNext);
    } else if(ctx->inPos == 1) {
        (*outByte) = base64[xmlSecBase64Encode2(ctx->inByte, inByte)];
        ctx->inByte = inByte;
        ++ctx->linePos;
        ++ctx->inPos;
        return(xmlSecBase64StatusConsumeAndNext);
    } else if(ctx->inPos == 2) {
        (*outByte) = base64[xmlSecBase64Encode3(ctx->inByte, inByte)];
        ctx->inByte = inByte;
        ++ctx->linePos;
        ++ctx->inPos;
        return(xmlSecBase64StatusConsumeAndRepeat);
    } else if(ctx->inPos == 3) {
        (*outByte) = base64[xmlSecBase64Encode4(ctx->inByte)];
        ++ctx->linePos;
        ctx->inByte = 0;
        ctx->inPos  = 0;
        return(xmlSecBase64StatusConsumeAndNext);
    }

    xmlSecInvalidSizeDataError("ctx->inPos", ctx->inPos, "0,1,2,3", NULL);
    return(xmlSecBase64StatusFailed);
}

static xmlSecBase64Status
xmlSecBase64CtxEncodeByteFinal(xmlSecBase64CtxPtr ctx, xmlSecByte* outByte) {
    xmlSecAssert2(ctx != NULL, xmlSecBase64StatusFailed);
    xmlSecAssert2(outByte != NULL, xmlSecBase64StatusFailed);

    if(ctx->inPos == 0) {
        return(xmlSecBase64StatusDone);
    } else if((ctx->columns > 0) && (ctx->linePos >= ctx->columns)) {
        (*outByte) = '\n';
        ctx->linePos = 0;
        return(xmlSecBase64StatusConsumeAndRepeat);
    } else if(ctx->finished == 0) {
        ctx->finished = 1;
        return(xmlSecBase64CtxEncodeByte(ctx, 0, outByte));
    } else if(ctx->inPos < 3) {
        (*outByte) = '=';
        ++ctx->inPos;
        ++ctx->linePos;
        return(xmlSecBase64StatusConsumeAndRepeat);
    } else if(ctx->inPos == 3) {
        (*outByte) = '=';
        ++ctx->linePos;
        ctx->inPos = 0;
        return(xmlSecBase64StatusConsumeAndRepeat);
    }

    xmlSecInvalidSizeDataError("ctx->inPos", ctx->inPos, "0,1,2,3", NULL);
    return(xmlSecBase64StatusFailed);
}

static xmlSecBase64Status
xmlSecBase64CtxDecodeByte(xmlSecBase64CtxPtr ctx, xmlSecByte inByte, xmlSecByte* outByte) {
    xmlSecAssert2(ctx != NULL, xmlSecBase64StatusFailed);
    xmlSecAssert2(outByte != NULL, xmlSecBase64StatusFailed);

    if((ctx->finished != 0) && (ctx->inPos == 0)) {
        return(xmlSecBase64StatusDone);
    } if(inByte == '=') {
        ctx->finished = 1;
        if(ctx->inPos == 2) {
            ++ctx->inPos;
            return(xmlSecBase64StatusNext);
        } else if(ctx->inPos == 3) {
            ctx->inPos = 0;
            return(xmlSecBase64StatusNext);
        } else {
            xmlSecInvalidSizeDataError("ctx->inPos", ctx->inPos, "2,3", NULL);
            return(xmlSecBase64StatusFailed);
        }
    } else if(xmlSecIsBase64Space(inByte)) {
        return(xmlSecBase64StatusNext);
    } else if(!xmlSecIsBase64Char(inByte) || (ctx->finished != 0)) {
        xmlSecInvalidIntegerDataError("inByte", inByte, "base64 character", NULL);
        return(xmlSecBase64StatusFailed);
    }

    /* convert from character to position in base64 array */
    if((inByte >= 'A') && (inByte <= 'Z')) {
        inByte = (xmlSecByte)(inByte - 'A');
    } else if((inByte >= 'a') && (inByte <= 'z')) {
        inByte = (xmlSecByte)(26 + (inByte - 'a'));
    } else if((inByte >= '0') && (inByte <= '9')) {
        inByte = (xmlSecByte)(52 + (inByte - '0'));
    } else if(inByte == '+') {
        inByte = (xmlSecByte)62;
    } else if(inByte == '/') {
        inByte = (xmlSecByte)63;
    }

    if(ctx->inPos == 0) {
        ctx->inByte = inByte;
        ++ctx->inPos;
        return(xmlSecBase64StatusNext);
    } else if(ctx->inPos == 1) {
        (*outByte) = xmlSecBase64Decode1(ctx->inByte, inByte);
        ctx->inByte = inByte;
        ++ctx->inPos;
        return(xmlSecBase64StatusConsumeAndNext);
    } else if(ctx->inPos == 2) {
        (*outByte) = xmlSecBase64Decode2(ctx->inByte, inByte);
        ctx->inByte = inByte;
        ++ctx->inPos;
        return(xmlSecBase64StatusConsumeAndNext);
    } else if(ctx->inPos == 3) {
        (*outByte) = xmlSecBase64Decode3(ctx->inByte, inByte);
        ctx->inByte = 0;
        ctx->inPos = 0;
        return(xmlSecBase64StatusConsumeAndNext);
    }

    xmlSecInvalidSizeDataError("ctx->inPos", ctx->inPos, "0,1,2,3", NULL);
    return(xmlSecBase64StatusFailed);
}


static int
xmlSecBase64CtxEncode(xmlSecBase64CtxPtr ctx,
                     const xmlSecByte* inBuf, xmlSecSize inBufSize, xmlSecSize* inBufResSize,
                     xmlSecByte* outBuf, xmlSecSize outBufSize, xmlSecSize* outBufResSize) {
    xmlSecBase64Status status = xmlSecBase64StatusNext;
    xmlSecSize inPos, outPos;

    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(inBuf != NULL, -1);
    xmlSecAssert2(inBufResSize != NULL, -1);
    xmlSecAssert2(outBuf != NULL, -1);
    xmlSecAssert2(outBufResSize != NULL, -1);

    /* encode */
    for(inPos = outPos = 0; (inPos < inBufSize) && (outPos < outBufSize); ) {
        status = xmlSecBase64CtxEncodeByte(ctx, inBuf[inPos], &(outBuf[outPos]));
        switch(status) {
            case xmlSecBase64StatusConsumeAndNext:
                ++inPos;
                ++outPos;
                break;
            case xmlSecBase64StatusConsumeAndRepeat:
                ++outPos;
                break;
            case xmlSecBase64StatusNext:
            case xmlSecBase64StatusDone:
            case xmlSecBase64StatusFailed:
                xmlSecInternalError2("xmlSecBase64CtxEncodeByte", NULL,
                    "status=" XMLSEC_ENUM_FMT, XMLSEC_ENUM_CAST(status));
                return(-1);
        }
    }

    (*inBufResSize)  = inPos;
    (*outBufResSize) = outPos;

    return(0);
}

static int
xmlSecBase64CtxEncodeFinal(xmlSecBase64CtxPtr ctx, xmlSecByte* outBuf, xmlSecSize outBufSize,
    xmlSecSize* outBufResSize) {

    xmlSecBase64Status status = xmlSecBase64StatusNext;
    xmlSecSize outPos;

    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(outBuf != NULL, -1);
    xmlSecAssert2(outBufResSize != NULL, -1);

    /* encode final bytes */
    for(outPos = 0; (outPos < outBufSize) && (status != xmlSecBase64StatusDone); ) {
        status = xmlSecBase64CtxEncodeByteFinal(ctx, &(outBuf[outPos]));
        switch(status) {
            case xmlSecBase64StatusConsumeAndNext:
            case xmlSecBase64StatusConsumeAndRepeat:
                ++outPos;
                break;
            case xmlSecBase64StatusDone:
                break;
            case xmlSecBase64StatusNext:
            case xmlSecBase64StatusFailed:
                xmlSecInternalError2("xmlSecBase64CtxEncodeByteFinal", NULL,
                    "status=" XMLSEC_ENUM_FMT, XMLSEC_ENUM_CAST(status));
                return(-1);
        }
    }

    if(status != xmlSecBase64StatusDone) {
        xmlSecInvalidSizeOtherError("invalid base64 buffer size", NULL);
        return(-1);
    }
    if(outPos < outBufSize) {
        outBuf[outPos] = '\0'; /* just in case */
    }

    (*outBufResSize) = outPos;
    return(0);
}


static int
xmlSecBase64CtxDecode(xmlSecBase64CtxPtr ctx,
                     const xmlSecByte* inBuf, xmlSecSize inBufSize, xmlSecSize* inBufResSize,
                     xmlSecByte* outBuf, xmlSecSize outBufSize, xmlSecSize* outBufResSize) {
    xmlSecBase64Status status = xmlSecBase64StatusNext;
    xmlSecSize inPos, outPos;

    xmlSecAssert2(ctx != NULL, -1);
    xmlSecAssert2(inBuf != NULL, -1);
    xmlSecAssert2(inBufResSize != NULL, -1);
    xmlSecAssert2(outBuf != NULL, -1);
    xmlSecAssert2(outBufResSize != NULL, -1);

    /* decode */
    for(inPos = outPos = 0; (inPos < inBufSize) && (outPos < outBufSize) && (status != xmlSecBase64StatusDone); ) {
        status = xmlSecBase64CtxDecodeByte(ctx, inBuf[inPos], &(outBuf[outPos]));
        switch(status) {
            case xmlSecBase64StatusConsumeAndNext:
                ++inPos;
                ++outPos;
                break;
            case xmlSecBase64StatusConsumeAndRepeat:
                ++outPos;
                break;
            case xmlSecBase64StatusNext:
                ++inPos;
                break;
            case xmlSecBase64StatusDone:
                break;
            case xmlSecBase64StatusFailed:
                xmlSecInternalError2("xmlSecBase64CtxDecodeByte", NULL,
                    "status=" XMLSEC_ENUM_FMT, XMLSEC_ENUM_CAST(status));
                return(-1);
        }
    }

    /* skip spaces at the end */
    while((inPos < inBufSize) && xmlSecIsBase64Space(inBuf[inPos])) {
        ++inPos;
    }

    (*inBufResSize)  = inPos;
    (*outBufResSize) = outPos;

    return(0);
}

static int
xmlSecBase64CtxDecodeIsFinished(xmlSecBase64CtxPtr ctx) {
    xmlSecAssert2(ctx != NULL, -1);

    return((ctx->inPos == 0) ? 1 : 0);
}

static xmlSecSize
xmlSecBase64GetEncodeSize(xmlSecBase64CtxPtr ctx, xmlSecSize inLen) {
    xmlSecSize size;

    xmlSecAssert2(ctx != NULL, 0);

    size = (4 * inLen) / 3 + 4;
    if(ctx->columns > 0) {
        size += (size / ctx->columns) + 4;
    }
    return(size + 1);
}

/**
 * xmlSecBase64Encode:
 * @in:                 the input buffer.
 * @inSize:             the input buffer size.
 * @columns:            the output max line length (if 0 then no line breaks
 *                      would be inserted)
 *
 * Encodes the data from input buffer and allocates the string for the result.
 * The caller is responsible for freeing returned buffer using
 * xmlFree() function.
 *
 * Returns: newly allocated string with base64 encoded data
 * or NULL if an error occurs.
 */
xmlChar*
xmlSecBase64Encode(const xmlSecByte *in, xmlSecSize inSize, int columns) {
    xmlSecBase64Ctx ctx;
    int ctx_initialized = 0;
    xmlSecByte* ptr = NULL;
    xmlChar* res = NULL;
    xmlSecSize outSize, outUpdatedSize, outFinalSize;
    int ret;

    xmlSecAssert2(in != NULL, NULL);

    ret = xmlSecBase64CtxInitialize(&ctx, 1, columns);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBase64CtxInitialize", NULL);
        goto done;
    }
    ctx_initialized = 1;

    /* create result buffer */
    outSize = xmlSecBase64GetEncodeSize(&ctx, inSize);
    if(outSize == 0) {
        xmlSecInternalError("xmlSecBase64GetEncodeSize", NULL);
        goto done;
    }
    ptr = (xmlSecByte*)xmlMalloc(outSize);
    if(ptr == NULL) {
        xmlSecMallocError(outSize, NULL);
        goto done;
    }

    ret = xmlSecBase64CtxUpdate_ex(&ctx, in, inSize, ptr, outSize, &outUpdatedSize);
    if (ret < 0) {
        xmlSecInternalError3("xmlSecBase64CtxUpdate_ex", NULL,
            "inSize=" XMLSEC_SIZE_FMT "; outSize=" XMLSEC_SIZE_FMT, inSize, outSize);
        goto done;
    }

    ret = xmlSecBase64CtxFinal_ex(&ctx, ptr + outUpdatedSize, outSize - outUpdatedSize,
        &outFinalSize);
    if (ret < 0) {
        xmlSecInternalError("xmlSecBase64CtxFinal_ex", NULL);
        goto done;
    }

    /* success */
    ptr[outUpdatedSize + outFinalSize] = '\0';
    res = BAD_CAST(ptr);
    ptr = NULL;

done:
    if(ptr != NULL) {
        xmlFree(ptr);
    }
    if(ctx_initialized != 0) {
        xmlSecBase64CtxFinalize(&ctx);
    }
    return(res);
}

/**
 * xmlSecBase64Decode_ex:
 * @str:                the input buffer with base64 encoded string
 * @out:                the output buffer
 * @outSize:            the output buffer size
 * @outWritten:         the pointer to store the number of bytes written into the output.
 *
 * Decodes input base64 encoded string and puts result into
 * the output buffer.
 *
 * Returns: 0 on success and a negative value otherwise.
 */
int
xmlSecBase64Decode_ex(const xmlChar* str, xmlSecByte* out, xmlSecSize outSize, xmlSecSize* outWritten) {
    xmlSecBase64Ctx ctx;
    int ctx_initialized = 0;
    xmlSecSize outUpdateSize, outFinalSize;
    int ret;
    int res = -1;

    xmlSecAssert2(str != NULL, -1);
    xmlSecAssert2(out != NULL, -1);
    xmlSecAssert2(outWritten != NULL, -1);

    ret = xmlSecBase64CtxInitialize(&ctx, 0, 0);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBase64CtxInitialize", NULL);
        goto done;
    }
    ctx_initialized = 1;

    ret = xmlSecBase64CtxUpdate_ex(&ctx, (const xmlSecByte*)str, xmlSecStrlen(str),
        out, outSize, &outUpdateSize);
    if (ret < 0) {
        xmlSecInternalError("xmlSecBase64CtxUpdate_ex", NULL);
        goto done;
    }

    ret = xmlSecBase64CtxFinal_ex(&ctx, out + outUpdateSize, outSize - outUpdateSize,
        &outFinalSize);
    if (ret < 0) {
        xmlSecInternalError("xmlSecBase64CtxFinal_ex", NULL);
        goto done;
    }

    /* success */
    (*outWritten) = (outUpdateSize + outFinalSize);
    res = 0;

done:
    if(ctx_initialized != 0) {
        xmlSecBase64CtxFinalize(&ctx);
    }
    return(res);
}

/**
 * xmlSecBase64DecodeInPlace:
 * @str:                the input/output buffer
 * @outWritten:         the pointer to store the number of bytes written into the output.
 *
 * Decodes input base64 encoded string from @str "in-place" (i.e. puts results into @str buffer).
 *
 * Returns: 0 on success and a negative value otherwise.
 */
int
xmlSecBase64DecodeInPlace(xmlChar* str, xmlSecSize* outWritten) {
    xmlSecAssert2(str != NULL, -1);
    return(xmlSecBase64Decode_ex(str, (xmlSecByte*)str, xmlSecStrlen(str) + 1,outWritten));
}

/**************************************************************
 *
 * Deprecated functions for backward compatibility
 *
 **************************************************************/

/**
 * xmlSecBase64CtxUpdate:
 * @ctx:                the pointer to #xmlSecBase64Ctx structure
 * @in:                 the input buffer
 * @inSize:             the input buffer size
 * @out:                the output buffer
 * @outSize:            the output buffer size
 *
 * DEPRECATED. Encodes or decodes the next piece of data from input buffer.
 *
 * Returns: the number of bytes written to output buffer or
 * -1 if an error occurs.
 */
int
xmlSecBase64CtxUpdate(xmlSecBase64CtxPtr ctx, const xmlSecByte* in, xmlSecSize inSize,
    xmlSecByte* out, xmlSecSize outSize) {

    int ret;
    xmlSecSize outWritten;
    int res;

    ret = xmlSecBase64CtxUpdate_ex(ctx, in, inSize, out, outSize, &outWritten);
    if (ret < 0) {
        return(-1);
    }

    XMLSEC_SAFE_CAST_SIZE_TO_INT(outWritten, res, return(-1), NULL);
    return(res);
}


/**
 * xmlSecBase64CtxFinal:
 * @ctx:                the pointer to #xmlSecBase64Ctx structure
 * @out:                the output buffer
 * @outSize:            the output buffer size
 *
 * DEPRECATED. Encodes or decodes the last piece of data stored in the context
 * and finalizes the result.
 *
 * Returns: the number of bytes written to output buffer or
 * -1 if an error occurs.
 */
int
xmlSecBase64CtxFinal(xmlSecBase64CtxPtr ctx, xmlSecByte* out, xmlSecSize outSize) {
    int ret;
    xmlSecSize outWritten;
    int res;

    ret = xmlSecBase64CtxFinal_ex(ctx, out, outSize, &outWritten);
    if (ret < 0) {
        return(-1);
    }

    XMLSEC_SAFE_CAST_SIZE_TO_INT(outWritten, res, return(-1), NULL);
    return(res);
}

 /**
  * xmlSecBase64Decode:
  * @str:                the input buffer with base64 encoded string
  * @out:                the output buffer
  * @outSize:            the output buffer size
  *
  * DEPRECATED. Decodes input base64 encoded string and puts result into
  * the output buffer.
  *
  * Returns: the number of bytes written to the output buffer or
  * a negative value if an error occurs
  */
int
xmlSecBase64Decode(const xmlChar* str, xmlSecByte* out, xmlSecSize outSize) {
    int ret;
    xmlSecSize outWritten;
    int res;

    ret = xmlSecBase64Decode_ex(str, out, outSize, &outWritten);
    if (ret < 0) {
        return(-1);
    }

    XMLSEC_SAFE_CAST_SIZE_TO_INT(outWritten, res, return(-1), NULL);
    return(res);
}


/**************************************************************
 *
 * Base64 Transform
 *
 * xmlSecTransform + xmlSecBase64Ctx
 *
 **************************************************************/
XMLSEC_TRANSFORM_DECLARE(Base64, xmlSecBase64Ctx)
#define xmlSecBase64Size XMLSEC_TRANSFORM_SIZE(Base64)

static int              xmlSecBase64Initialize          (xmlSecTransformPtr transform);
static void             xmlSecBase64Finalize            (xmlSecTransformPtr transform);
static int              xmlSecBase64Execute             (xmlSecTransformPtr transform,
                                                         int last,
                                                         xmlSecTransformCtxPtr transformCtx);

static xmlSecTransformKlass xmlSecBase64Klass = {
    /* klass/object sizes */
    sizeof(xmlSecTransformKlass),               /* xmlSecSize klassSize */
    xmlSecBase64Size,                           /* xmlSecSize objSize */

    xmlSecNameBase64,                           /* const xmlChar* name; */
    xmlSecHrefBase64,                           /* const xmlChar* href; */
    xmlSecTransformUsageDSigTransform,          /* xmlSecAlgorithmUsage usage; */

    xmlSecBase64Initialize,                     /* xmlSecTransformInitializeMethod initialize; */
    xmlSecBase64Finalize,                       /* xmlSecTransformFinalizeMethod finalize; */
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
    xmlSecBase64Execute,                        /* xmlSecTransformExecuteMethod execute; */

    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecTransformBase64GetKlass:
 *
 * The Base64 transform klass (http://www.w3.org/TR/xmldsig-core/#sec-Base-64).
 * The normative specification for base64 decoding transforms is RFC 2045
 * (http://www.ietf.org/rfc/rfc2045.txt). The base64 Transform element has
 * no content. The input is decoded by the algorithms. This transform is
 * useful if an application needs to sign the raw data associated with
 * the encoded content of an element.
 *
 * Returns: base64 transform id.
 */
xmlSecTransformId
xmlSecTransformBase64GetKlass(void) {
    return(&xmlSecBase64Klass);
}

/**
 * xmlSecTransformBase64SetLineSize:
 * @transform:          the pointer to BASE64 encode transform.
 * @lineSize:           the new max line size.
 *
 * Sets the max line size to @lineSize.
 */
void
xmlSecTransformBase64SetLineSize(xmlSecTransformPtr transform, xmlSecSize lineSize) {
    xmlSecBase64CtxPtr ctx;

    xmlSecAssert(xmlSecTransformCheckId(transform, xmlSecTransformBase64Id));

    ctx = xmlSecBase64GetCtx(transform);
    xmlSecAssert(ctx != NULL);

    ctx->columns = lineSize;
}

static int
xmlSecBase64Initialize(xmlSecTransformPtr transform) {
    xmlSecBase64CtxPtr ctx;
    int ret;
    int columns;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformBase64Id), -1);

    ctx = xmlSecBase64GetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    columns = xmlSecBase64GetDefaultLineSize();
    transform->operation = xmlSecTransformOperationDecode;
    ret = xmlSecBase64CtxInitialize(ctx, 0, columns);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBase64CtxInitialize", xmlSecTransformGetName(transform));
        return(-1);
    }

    return(0);
}

static void
xmlSecBase64Finalize(xmlSecTransformPtr transform) {
    xmlSecBase64CtxPtr ctx;

    xmlSecAssert(xmlSecTransformCheckId(transform, xmlSecTransformBase64Id));

    ctx = xmlSecBase64GetCtx(transform);
    xmlSecAssert(ctx != NULL);

    xmlSecBase64CtxFinalize(ctx);
}

static int
xmlSecBase64Execute(xmlSecTransformPtr transform, int last, xmlSecTransformCtxPtr transformCtx) {
    xmlSecBase64CtxPtr ctx;
    xmlSecBufferPtr in, out;
    xmlSecSize inSize, outSize, outMaxLen, outLen;
    int ret;

    xmlSecAssert2(xmlSecTransformCheckId(transform, xmlSecTransformBase64Id), -1);
    xmlSecAssert2((transform->operation == xmlSecTransformOperationEncode) || (transform->operation == xmlSecTransformOperationDecode), -1);
    xmlSecAssert2(transformCtx != NULL, -1);

    ctx = xmlSecBase64GetCtx(transform);
    xmlSecAssert2(ctx != NULL, -1);

    in = &(transform->inBuf);
    out = &(transform->outBuf);

    if(transform->status == xmlSecTransformStatusNone) {
        ctx->encode = (transform->operation == xmlSecTransformOperationEncode) ? 1 : 0;
        transform->status = xmlSecTransformStatusWorking;
    }

    switch(transform->status) {
        case xmlSecTransformStatusWorking:
            inSize = xmlSecBufferGetSize(in);
            outSize = xmlSecBufferGetSize(out);
            if(inSize > 0) {
                if(ctx->encode != 0) {
                    outMaxLen = 4 * inSize / 3 + 8;
                    if(ctx->columns > 0) {
                        outMaxLen += inSize / ctx->columns + 4;
                    }
                } else {
                    outMaxLen = 3 * inSize / 4 + 8;
                }
                ret = xmlSecBufferSetMaxSize(out, outSize + outMaxLen);
                if(ret < 0) {
                    xmlSecInternalError2("xmlSecBufferSetMaxSize", xmlSecTransformGetName(transform),
                        "size=" XMLSEC_SIZE_FMT, (outSize + outMaxLen));
                    return(-1);
                }

                /* encode/decode the next chunk */
                ret = xmlSecBase64CtxUpdate_ex(ctx, xmlSecBufferGetData(in), inSize,
                    xmlSecBufferGetData(out) + outSize, outMaxLen, &outLen);
                if(ret < 0) {
                    xmlSecInternalError("xmlSecBase64CtxUpdate_ex", xmlSecTransformGetName(transform));
                    return(-1);
                }

                /* set correct size */
                ret = xmlSecBufferSetSize(out, outSize + outLen);
                if(ret < 0) {
                    xmlSecInternalError2("xmlSecBufferSetSize", xmlSecTransformGetName(transform),
                        "size=" XMLSEC_SIZE_FMT, (outSize + outLen));
                    return(-1);
                }

                /* remove chunk from input */
                ret = xmlSecBufferRemoveHead(in, inSize);
                if(ret < 0) {
                    xmlSecInternalError2("xmlSecBufferRemoveHead", xmlSecTransformGetName(transform),
                        "size=" XMLSEC_SIZE_FMT, inSize);
                    return(-1);
                }
            }

            if(last) {
                outSize = xmlSecBufferGetSize(out);
                outMaxLen = 16; /* last block */

                ret = xmlSecBufferSetMaxSize(out, outSize + outMaxLen);
                if(ret < 0) {
                    xmlSecInternalError2("xmlSecBufferSetMaxSize", xmlSecTransformGetName(transform),
                        "size=" XMLSEC_SIZE_FMT, (outSize + outMaxLen));
                    return(-1);
                }

                /* add from ctx buffer */
                ret = xmlSecBase64CtxFinal_ex(ctx, xmlSecBufferGetData(out) + outSize,
                    outMaxLen, &outLen);
                if (ret < 0) {
                    xmlSecInternalError("xmlSecBase64CtxFinal_ex", xmlSecTransformGetName(transform));
                    return(-1);
                }

                /* set correct size */
                ret = xmlSecBufferSetSize(out, outSize + outLen);
                if(ret < 0) {
                    xmlSecInternalError2("xmlSecBufferSetSize", xmlSecTransformGetName(transform),
                        "size=" XMLSEC_SIZE_FMT, (outSize + outLen));
                    return(-1);
                }
                transform->status = xmlSecTransformStatusFinished;
            }
            break;
        case xmlSecTransformStatusFinished:
            /* the only way we can get here is if there is no input */
            xmlSecAssert2(xmlSecBufferGetSize(in) == 0, -1);
            break;
        default:
            xmlSecInvalidTransfromStatusError(transform);
            return(-1);
    }
    return(0);
}


