/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Simple Big Numbers processing.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_BN_H__
#define __XMLSEC_BN_H__

#include <libxml/tree.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/buffer.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef xmlSecBuffer                                            xmlSecBn,
                                                                *xmlSecBnPtr;

/**
 * xmlSecBnFormat:
 * @xmlSecBnBase64:             the base64 decoded binary blob.
 * @xmlSecBnHex:                the hex number.
 * @xmlSecBnDec:                the decimal number.
 *
 * The big numbers formats.
 */
typedef enum {
    xmlSecBnBase64,
    xmlSecBnHex,
    xmlSecBnDec
} xmlSecBnFormat;

XMLSEC_EXPORT xmlSecBnPtr       xmlSecBnCreate                  (xmlSecSize size);
XMLSEC_EXPORT void              xmlSecBnDestroy                 (xmlSecBnPtr bn);
XMLSEC_EXPORT int               xmlSecBnInitialize              (xmlSecBnPtr bn,
                                                                 xmlSecSize size);
XMLSEC_EXPORT void              xmlSecBnFinalize                (xmlSecBnPtr bn);
XMLSEC_EXPORT xmlSecByte*       xmlSecBnGetData                 (xmlSecBnPtr bn);
XMLSEC_EXPORT int               xmlSecBnSetData                 (xmlSecBnPtr bn,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize size);
XMLSEC_EXPORT xmlSecSize        xmlSecBnGetSize                 (xmlSecBnPtr bn);
XMLSEC_EXPORT void              xmlSecBnZero                    (xmlSecBnPtr bn);

XMLSEC_EXPORT int               xmlSecBnFromString              (xmlSecBnPtr bn,
                                                                 const xmlChar* str,
                                                                 xmlSecSize base);
XMLSEC_EXPORT xmlChar*          xmlSecBnToString                (xmlSecBnPtr bn,
                                                                 xmlSecSize base);
XMLSEC_EXPORT int               xmlSecBnFromHexString           (xmlSecBnPtr bn,
                                                                 const xmlChar* str);
XMLSEC_EXPORT xmlChar*          xmlSecBnToHexString             (xmlSecBnPtr bn);

XMLSEC_EXPORT int               xmlSecBnFromDecString           (xmlSecBnPtr bn,
                                                                 const xmlChar* str);
XMLSEC_EXPORT xmlChar*          xmlSecBnToDecString             (xmlSecBnPtr bn);

XMLSEC_EXPORT int               xmlSecBnMul                     (xmlSecBnPtr bn,
                                                                 int multiplier);
XMLSEC_EXPORT int               xmlSecBnDiv                     (xmlSecBnPtr bn,
                                                                 int divider,
                                                                 int* mod);
XMLSEC_EXPORT int               xmlSecBnAdd                     (xmlSecBnPtr bn,
                                                                 int delta);
XMLSEC_EXPORT int               xmlSecBnReverse                 (xmlSecBnPtr bn);
XMLSEC_EXPORT int               xmlSecBnCompare                 (xmlSecBnPtr bn,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize);
XMLSEC_EXPORT int               xmlSecBnCompareReverse          (xmlSecBnPtr bn,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize);
XMLSEC_EXPORT int               xmlSecBnGetNodeValue            (xmlSecBnPtr bn,
                                                                 xmlNodePtr cur,
                                                                 xmlSecBnFormat format,
                                                                 int reverse);
XMLSEC_EXPORT int               xmlSecBnSetNodeValue            (xmlSecBnPtr bn,
                                                                 xmlNodePtr cur,
                                                                 xmlSecBnFormat format,
                                                                 int reverse,
                                                                 int addLineBreaks);
XMLSEC_EXPORT int               xmlSecBnBlobSetNodeValue        (const xmlSecByte* data,
                                                                 xmlSecSize dataSize,
                                                                 xmlNodePtr cur,
                                                                 xmlSecBnFormat format,
                                                                 int reverse,
                                                                 int addLineBreaks);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_BN_H__ */

