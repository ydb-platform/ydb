/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_OPENSSL_X509_H__
#define __XMLSEC_OPENSSL_X509_H__

#ifndef XMLSEC_NO_X509

#include <openssl/x509.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * XMLSEC_STACK_OF_X509:
 *
 * Macro. To make docbook happy.
 */
#define XMLSEC_STACK_OF_X509            STACK_OF(X509)

/**
 * XMLSEC_STACK_OF_X509_CRL:
 *
 * Macro. To make docbook happy.
 */
#define XMLSEC_STACK_OF_X509_CRL        STACK_OF(X509_CRL)

/**
 * xmlSecOpenSSLKeyDataX509Id:
 *
 * The OpenSSL X509 data klass.
 */
#define xmlSecOpenSSLKeyDataX509Id \
        xmlSecOpenSSLKeyDataX509GetKlass()
XMLSEC_CRYPTO_EXPORT xmlSecKeyDataId    xmlSecOpenSSLKeyDataX509GetKlass(void);

XMLSEC_CRYPTO_EXPORT X509*              xmlSecOpenSSLKeyDataX509GetKeyCert(xmlSecKeyDataPtr data);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLKeyDataX509AdoptKeyCert(xmlSecKeyDataPtr data,
                                                                         X509* cert);

XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLKeyDataX509AdoptCert(xmlSecKeyDataPtr data,
                                                                         X509* cert);
XMLSEC_CRYPTO_EXPORT X509*              xmlSecOpenSSLKeyDataX509GetCert (xmlSecKeyDataPtr data,
                                                                         xmlSecSize pos);
XMLSEC_CRYPTO_EXPORT xmlSecSize         xmlSecOpenSSLKeyDataX509GetCertsSize(xmlSecKeyDataPtr data);

XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLKeyDataX509AdoptCrl(xmlSecKeyDataPtr data,
                                                                         X509_CRL* crl);
XMLSEC_CRYPTO_EXPORT X509_CRL*          xmlSecOpenSSLKeyDataX509GetCrl  (xmlSecKeyDataPtr data,
                                                                         xmlSecSize pos);
XMLSEC_CRYPTO_EXPORT xmlSecSize         xmlSecOpenSSLKeyDataX509GetCrlsSize(xmlSecKeyDataPtr data);

XMLSEC_CRYPTO_EXPORT xmlSecKeyDataPtr   xmlSecOpenSSLX509CertGetKey     (X509* cert);


/**
 * xmlSecOpenSSLKeyDataRawX509CertId:
 *
 * The OpenSSL raw X509 certificate klass.
 */
#define xmlSecOpenSSLKeyDataRawX509CertId \
        xmlSecOpenSSLKeyDataRawX509CertGetKlass()
XMLSEC_CRYPTO_EXPORT xmlSecKeyDataId    xmlSecOpenSSLKeyDataRawX509CertGetKlass(void);

/**
 * xmlSecOpenSSLX509StoreId:
 *
 * The OpenSSL X509 store klass.
 */
#define xmlSecOpenSSLX509StoreId \
        xmlSecOpenSSLX509StoreGetKlass()
XMLSEC_CRYPTO_EXPORT xmlSecKeyDataStoreId xmlSecOpenSSLX509StoreGetKlass(void);
XMLSEC_CRYPTO_EXPORT X509*              xmlSecOpenSSLX509StoreFindCert  (xmlSecKeyDataStorePtr store,
                                                                         xmlChar *subjectName,
                                                                         xmlChar *issuerName,
                                                                         xmlChar *issuerSerial,
                                                                         xmlChar *ski,
                                                                         xmlSecKeyInfoCtx* keyInfoCtx);
XMLSEC_CRYPTO_EXPORT X509*              xmlSecOpenSSLX509StoreFindCert_ex(xmlSecKeyDataStorePtr store,
                                                                         xmlChar *subjectName,
                                                                         xmlChar *issuerName,
                                                                         xmlChar *issuerSerial,
                                                                         xmlSecByte * ski,
                                                                         xmlSecSize skiSize,
                                                                         xmlSecKeyInfoCtx* keyInfoCtx);
XMLSEC_CRYPTO_EXPORT X509*              xmlSecOpenSSLX509StoreVerify    (xmlSecKeyDataStorePtr store,
                                                                         XMLSEC_STACK_OF_X509* certs,
                                                                         XMLSEC_STACK_OF_X509_CRL* crls,
                                                                         xmlSecKeyInfoCtx* keyInfoCtx);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLX509StoreAdoptCert (xmlSecKeyDataStorePtr store,
                                                                         X509* cert,
                                                                         xmlSecKeyDataType type);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLX509StoreAdoptCrl  (xmlSecKeyDataStorePtr store,
                                                                         X509_CRL* crl);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLX509StoreAddCertsPath(xmlSecKeyDataStorePtr store,
                                                                         const char* path);
XMLSEC_CRYPTO_EXPORT int                xmlSecOpenSSLX509StoreAddCertsFile(xmlSecKeyDataStorePtr store,
                                                                         const char* filename);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* XMLSEC_NO_X509 */

#endif /* __XMLSEC_OPENSSL_X509_H__ */
