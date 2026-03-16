/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_OPENSSL_APP_H__
#define __XMLSEC_OPENSSL_APP_H__

#include <openssl/pem.h>
#include <openssl/bio.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/keys.h>
#include <xmlsec/keysmngr.h>
#include <xmlsec/transforms.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/********************************************************************
 *
 * Init/shutdown
 *
 ********************************************************************/
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppInit            (const char* config);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppShutdown        (void);

/********************************************************************
 *
 * Keys Manager
 *
 *******************************************************************/
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppDefaultKeysMngrInit(xmlSecKeysMngrPtr mngr);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppDefaultKeysMngrAdoptKey(xmlSecKeysMngrPtr mngr,
                                                                         xmlSecKeyPtr key);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppDefaultKeysMngrLoad(xmlSecKeysMngrPtr mngr,
                                                                         const char* uri);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppDefaultKeysMngrSave(xmlSecKeysMngrPtr mngr,
                                                                         const char* filename,
                                                                         xmlSecKeyDataType type);
#ifndef XMLSEC_NO_X509
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppKeysMngrCertLoad(xmlSecKeysMngrPtr mngr,
                                                                         const char *filename,
                                                                         xmlSecKeyDataFormat format,
                                                                         xmlSecKeyDataType type);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppKeysMngrCertLoadMemory(xmlSecKeysMngrPtr mngr,
                                                                         const xmlSecByte* data,
                                                                         xmlSecSize dataSize,
                                                                         xmlSecKeyDataFormat format,
                                                                         xmlSecKeyDataType type);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppKeysMngrCertLoadBIO(xmlSecKeysMngrPtr mngr,
                                                                         BIO* bio,
                                                                         xmlSecKeyDataFormat format,
                                                                         xmlSecKeyDataType type);

XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppKeysMngrAddCertsPath(xmlSecKeysMngrPtr mngr,
                                                                         const char *path);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppKeysMngrAddCertsFile(xmlSecKeysMngrPtr mngr,
                                                                         const char *filename);

#endif /* XMLSEC_NO_X509 */


/********************************************************************
 *
 * Keys
 *
 ********************************************************************/
XMLSEC_CRYPTO_EXPORT xmlSecKeyPtr       xmlSecOpenSSLAppKeyLoad         (const char *filename,
                                                                         xmlSecKeyDataFormat format,
                                                                         const char *pwd,
                                                                         void* pwdCallback,
                                                                         void* pwdCallbackCtx);
XMLSEC_CRYPTO_EXPORT xmlSecKeyPtr       xmlSecOpenSSLAppKeyLoadMemory   (const xmlSecByte* data,
                                                                         xmlSecSize dataSize,
                                                                         xmlSecKeyDataFormat format,
                                                                         const char *pwd,
                                                                         void* pwdCallback,
                                                                         void* pwdCallbackCtx);
XMLSEC_CRYPTO_EXPORT xmlSecKeyPtr       xmlSecOpenSSLAppKeyLoadBIO      (BIO* bio,
                                                                         xmlSecKeyDataFormat format,
                                                                         const char *pwd,
                                                                         void* pwdCallback,
                                                                         void* pwdCallbackCtx);

#ifndef XMLSEC_NO_X509
XMLSEC_CRYPTO_EXPORT xmlSecKeyPtr       xmlSecOpenSSLAppPkcs12Load      (const char* filename,
                                                                         const char* pwd,
                                                                         void* pwdCallback,
                                                                         void* pwdCallbackCtx);
XMLSEC_CRYPTO_EXPORT xmlSecKeyPtr       xmlSecOpenSSLAppPkcs12LoadMemory(const xmlSecByte* data,
                                                                         xmlSecSize dataSize,
                                                                         const char* pwd,
                                                                         void* pwdCallback,
                                                                         void* pwdCallbackCtx);
XMLSEC_CRYPTO_EXPORT xmlSecKeyPtr       xmlSecOpenSSLAppPkcs12LoadBIO   (BIO* bio,
                                                                         const char* pwd,
                                                                         void* pwdCallback,
                                                                         void* pwdCallbackCtx);

XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppKeyCertLoad     (xmlSecKeyPtr key,
                                                                         const char* filename,
                                                                         xmlSecKeyDataFormat format);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppKeyCertLoadMemory(xmlSecKeyPtr key,
                                                                         const xmlSecByte* data,
                                                                         xmlSecSize dataSize,
                                                                         xmlSecKeyDataFormat format);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLAppKeyCertLoadBIO  (xmlSecKeyPtr key,
                                                                         BIO* bio,
                                                                         xmlSecKeyDataFormat format);
XMLSEC_CRYPTO_EXPORT xmlSecKeyPtr       xmlSecOpenSSLAppKeyFromCertLoadBIO(BIO* bio,
                                                                         xmlSecKeyDataFormat format);
#endif /* XMLSEC_NO_X509 */

XMLSEC_CRYPTO_EXPORT void*              xmlSecOpenSSLAppGetDefaultPwdCallback(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_OPENSSL_APP_H__ */


