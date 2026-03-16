/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * XML Parser transform and utility functions.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_PARSER_H__
#define __XMLSEC_PARSER_H__

#include <libxml/tree.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/transforms.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

XMLSEC_EXPORT xmlDocPtr         xmlSecParseFile         (const char *filename);
XMLSEC_EXPORT xmlDocPtr         xmlSecParseMemory       (const xmlSecByte *buffer,
                                                         xmlSecSize size,
                                                         int recovery);
XMLSEC_EXPORT xmlDocPtr         xmlSecParseMemoryExt    (const xmlSecByte *prefix,
                                                         xmlSecSize prefixSize,
                                                         const xmlSecByte *buffer,
                                                         xmlSecSize bufferSize,
                                                         const xmlSecByte *postfix,
                                                         xmlSecSize postfixSize);
XMLSEC_EXPORT void              xmlSecParsePrepareCtxt  (xmlParserCtxtPtr ctxt);

XMLSEC_EXPORT int               xmlSecParserGetDefaultOptions(void);
XMLSEC_EXPORT void              xmlSecParserSetDefaultOptions(int options);


/**
 * xmlSecTransformXmlParserId:
 *
 * The XML Parser transform klass.
 */
#define xmlSecTransformXmlParserId \
        xmlSecTransformXmlParserGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformXmlParserGetKlass        (void);


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_PARSER_H__ */

