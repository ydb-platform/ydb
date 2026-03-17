/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_DL_H__
#define __XMLSEC_DL_H__

#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING

#include <libxml/tree.h>
#include <libxml/xmlIO.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/keysdata.h>
#include <xmlsec/keys.h>
#include <xmlsec/keysmngr.h>
#include <xmlsec/transforms.h>

#endif /* XMLSEC_NO_CRYPTO_DYNAMIC_LOADING */

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef struct _xmlSecCryptoDLFunctions         xmlSecCryptoDLFunctions,
                                                *xmlSecCryptoDLFunctionsPtr;

XMLSEC_EXPORT int                               xmlSecCryptoDLFunctionsRegisterKeyDataAndTransforms
                                                                            (xmlSecCryptoDLFunctionsPtr functions);

#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING

/****************************************************************************
 *
 * Dynamic load functions
 *
 ****************************************************************************/
XMLSEC_EXPORT int                               xmlSecCryptoDLInit              (void);
XMLSEC_EXPORT int                               xmlSecCryptoDLShutdown          (void);

XMLSEC_EXPORT int                               xmlSecCryptoDLLoadLibrary       (const xmlChar* crypto);
XMLSEC_EXPORT xmlSecCryptoDLFunctionsPtr        xmlSecCryptoDLGetLibraryFunctions(const xmlChar* crypto);
XMLSEC_EXPORT int                               xmlSecCryptoDLUnloadLibrary     (const xmlChar* crypto);

XMLSEC_EXPORT int                               xmlSecCryptoDLSetFunctions      (xmlSecCryptoDLFunctionsPtr functions);
XMLSEC_EXPORT xmlSecCryptoDLFunctionsPtr        xmlSecCryptoDLGetFunctions      (void);

#endif /* XMLSEC_NO_CRYPTO_DYNAMIC_LOADING */

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_APP_H__ */

