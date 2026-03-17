/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Base64 encode/decode transform and utility functions.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_BASE64_H__
#define __XMLSEC_BASE64_H__

#include <libxml/tree.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/transforms.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * XMLSEC_BASE64_LINESIZE:
 *
 * The default maximum base64 encoded line size.
 */
#define XMLSEC_BASE64_LINESIZE                          64

XMLSEC_EXPORT int               xmlSecBase64GetDefaultLineSize  (void);
XMLSEC_EXPORT void              xmlSecBase64SetDefaultLineSize  (int columns);


/* Base64 Context */
typedef struct _xmlSecBase64Ctx                                 xmlSecBase64Ctx,
                                                                *xmlSecBase64CtxPtr;

XMLSEC_EXPORT xmlSecBase64CtxPtr xmlSecBase64CtxCreate          (int encode,
                                                                 int columns);
XMLSEC_EXPORT void              xmlSecBase64CtxDestroy          (xmlSecBase64CtxPtr ctx);
XMLSEC_EXPORT int               xmlSecBase64CtxInitialize       (xmlSecBase64CtxPtr ctx,
                                                                 int encode,
                                                                 int columns);
XMLSEC_EXPORT void              xmlSecBase64CtxFinalize         (xmlSecBase64CtxPtr ctx);
XMLSEC_EXPORT int               xmlSecBase64CtxUpdate_ex        (xmlSecBase64CtxPtr ctx,
                                                                 const xmlSecByte* in,
                                                                 xmlSecSize inSize,
                                                                 xmlSecByte* out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize* outWritten);

XMLSEC_EXPORT int                xmlSecBase64CtxFinal_ex        (xmlSecBase64CtxPtr ctx,
                                                                 xmlSecByte* out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize* outWritten);

/* Standalone routines to do base64 encode/decode "at once" */
XMLSEC_EXPORT xmlChar*           xmlSecBase64Encode             (const xmlSecByte* in,
                                                                 xmlSecSize inSize,
                                                                 int columns);
XMLSEC_EXPORT int                xmlSecBase64Decode_ex          (const xmlChar* str,
                                                                 xmlSecByte* out,
                                                                 xmlSecSize outSize,
                                                                 xmlSecSize* outWritten);
XMLSEC_EXPORT int                xmlSecBase64DecodeInPlace      (xmlChar* str,
                                                                 xmlSecSize* outWritten);

/* These functions are deprecated and will be removed in the future. */
XMLSEC_DEPRECATED XMLSEC_EXPORT int xmlSecBase64CtxUpdate      (xmlSecBase64CtxPtr ctx,
                                                                const xmlSecByte* in,
                                                                xmlSecSize inSize,
                                                                xmlSecByte* out,
                                                                xmlSecSize outSize);
XMLSEC_DEPRECATED XMLSEC_EXPORT int xmlSecBase64CtxFinal       (xmlSecBase64CtxPtr ctx,
                                                                xmlSecByte* out,
                                                                xmlSecSize outSize);
XMLSEC_DEPRECATED XMLSEC_EXPORT int xmlSecBase64Decode         (const xmlChar* str,
                                                                xmlSecByte* out,
                                                                xmlSecSize outSize);
#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_BASE64_H__ */

