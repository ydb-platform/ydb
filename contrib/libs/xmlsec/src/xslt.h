/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * XSLT helper functions
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_PRIVATE_XSLT_H__
#define __XMLSEC_PRIVATE_XSLT_H__

#ifndef XMLSEC_PRIVATE
#error "xmlsec/private/xslt.h file contains private xmlsec definitions and should not be used outside xmlsec or xmlsec-$crypto libraries"
#endif /* XMLSEC_PRIVATE */

#ifndef XMLSEC_NO_XSLT

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

void xmlSecTransformXsltInitialize                          (void);
void xmlSecTransformXsltShutdown                            (void);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* XMLSEC_NO_XSLT */

#endif /* __XMLSEC_PRIVATE_XSLT_H__ */

