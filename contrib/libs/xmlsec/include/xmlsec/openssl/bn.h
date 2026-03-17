/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Reading/writing BIGNUM values
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_BN_H__
#define __XMLSEC_BN_H__

#include <openssl/bn.h>

#include <libxml/tree.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

XMLSEC_DEPRECATED XMLSEC_CRYPTO_EXPORT BIGNUM*    xmlSecOpenSSLNodeGetBNValue     (const xmlNodePtr cur,
                                                                 BIGNUM **a);
XMLSEC_DEPRECATED XMLSEC_CRYPTO_EXPORT int        xmlSecOpenSSLNodeSetBNValue     (xmlNodePtr cur,
                                                                 const BIGNUM *a,
                                                                 int addLineBreaks);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_BN_H__ */

