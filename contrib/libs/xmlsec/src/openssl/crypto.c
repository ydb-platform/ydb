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
 * SECTION:crypto
 * @Short_description: Crypto transforms implementation for OpenSSL.
 * @Stability: Stable
 *
 */

#include "globals.h"

#include <string.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/keys.h>
#include <xmlsec/keysmngr.h>
#include <xmlsec/transforms.h>
#include <xmlsec/errors.h>
#include <xmlsec/dl.h>
#include <xmlsec/private.h>

#include <openssl/x509.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <xmlsec/openssl/app.h>
#include <xmlsec/openssl/crypto.h>
#include <xmlsec/openssl/x509.h>

#include "openssl_compat.h"
#include "../cast_helpers.h"

static int              xmlSecOpenSSLErrorsInit                 (void);
static void             xmlSecOpenSSLErrorsShutdown             (void);

static xmlSecCryptoDLFunctionsPtr gXmlSecOpenSSLFunctions = NULL;
static xmlChar* gXmlSecOpenSSLTrustedCertsFolder = NULL;

#if !defined(XMLSEC_OPENSSL_API_300) && !defined(OPENSSL_IS_BORINGSSL) && !defined(OPENSSL_NO_ERR)

#define XMLSEC_OPENSSL_ERRORS_FUNCTION                  0

static int gXmlSecOpenSSLErrorsLib = 0;
static char gXmlSecOpenSSLErrorsLibName[] = "xmlsec lib";
static char gXmlSecOpenSSLErrorsDefault[] = "xmlsec routines";

static ERR_STRING_DATA xmlSecOpenSSLStrLib[2];
static ERR_STRING_DATA xmlSecOpenSSLStrDefReason[2];
static ERR_STRING_DATA xmlSecOpenSSLStrReasons[XMLSEC_ERRORS_MAX_NUMBER + 1];
#endif /* !defined(XMLSEC_OPENSSL_API_300) && !defined(OPENSSL_IS_BORINGSSL) && !defined(OPENSSL_NO_ERR) */

/**
 * xmlSecCryptoGetFunctions_openssl:
 *
 * Gets the pointer to xmlsec-openssl functions table.
 *
 * Returns: the xmlsec-openssl functions table or NULL if an error occurs.
 */
xmlSecCryptoDLFunctionsPtr
xmlSecCryptoGetFunctions_openssl(void) {
    static xmlSecCryptoDLFunctions functions;

    if(gXmlSecOpenSSLFunctions != NULL) {
        return(gXmlSecOpenSSLFunctions);
    }

    memset(&functions, 0, sizeof(functions));
    gXmlSecOpenSSLFunctions = &functions;

    /********************************************************************
     *
     * Crypto Init/shutdown
     *
     ********************************************************************/
    gXmlSecOpenSSLFunctions->cryptoInit                 = xmlSecOpenSSLInit;
    gXmlSecOpenSSLFunctions->cryptoShutdown             = xmlSecOpenSSLShutdown;
    gXmlSecOpenSSLFunctions->cryptoKeysMngrInit         = xmlSecOpenSSLKeysMngrInit;

    /********************************************************************
     *
     * Key data ids
     *
     ********************************************************************/
#ifndef XMLSEC_NO_AES
    gXmlSecOpenSSLFunctions->keyDataAesGetKlass         = xmlSecOpenSSLKeyDataAesGetKlass;
#endif /* XMLSEC_NO_AES */

#ifndef XMLSEC_NO_DES
    gXmlSecOpenSSLFunctions->keyDataDesGetKlass         = xmlSecOpenSSLKeyDataDesGetKlass;
#endif /* XMLSEC_NO_DES */

#ifndef XMLSEC_NO_DSA
    gXmlSecOpenSSLFunctions->keyDataDsaGetKlass         = xmlSecOpenSSLKeyDataDsaGetKlass;
#endif /* XMLSEC_NO_DSA */

#ifndef XMLSEC_NO_ECDSA
    gXmlSecOpenSSLFunctions->keyDataEcdsaGetKlass       = xmlSecOpenSSLKeyDataEcdsaGetKlass;
#endif /* XMLSEC_NO_ECDSA */

#ifndef XMLSEC_NO_GOST
    gXmlSecOpenSSLFunctions->keyDataGost2001GetKlass           = xmlSecOpenSSLKeyDataGost2001GetKlass;
#endif /* XMLSEC_NO_GOST */

#ifndef XMLSEC_NO_GOST2012
    gXmlSecOpenSSLFunctions->keyDataGostR3410_2012_256GetKlass = xmlSecOpenSSLKeyDataGostR3410_2012_256GetKlass;
    gXmlSecOpenSSLFunctions->keyDataGostR3410_2012_512GetKlass = xmlSecOpenSSLKeyDataGostR3410_2012_512GetKlass;
#endif /* XMLSEC_NO_GOST2012 */

#ifndef XMLSEC_NO_HMAC
    gXmlSecOpenSSLFunctions->keyDataHmacGetKlass        = xmlSecOpenSSLKeyDataHmacGetKlass;
#endif /* XMLSEC_NO_HMAC */

#ifndef XMLSEC_NO_RSA
    gXmlSecOpenSSLFunctions->keyDataRsaGetKlass         = xmlSecOpenSSLKeyDataRsaGetKlass;
#endif /* XMLSEC_NO_RSA */

#ifndef XMLSEC_NO_X509
    gXmlSecOpenSSLFunctions->keyDataX509GetKlass        = xmlSecOpenSSLKeyDataX509GetKlass;
    gXmlSecOpenSSLFunctions->keyDataRawX509CertGetKlass = xmlSecOpenSSLKeyDataRawX509CertGetKlass;
#endif /* XMLSEC_NO_X509 */

    /********************************************************************
     *
     * Key data store ids
     *
     ********************************************************************/
#ifndef XMLSEC_NO_X509
    gXmlSecOpenSSLFunctions->x509StoreGetKlass          = xmlSecOpenSSLX509StoreGetKlass;
#endif /* XMLSEC_NO_X509 */

    /********************************************************************
     *
     * Crypto transforms ids
     *
     ********************************************************************/

    /******************************* AES ********************************/
#ifndef XMLSEC_NO_AES
    gXmlSecOpenSSLFunctions->transformAes128CbcGetKlass         = xmlSecOpenSSLTransformAes128CbcGetKlass;
    gXmlSecOpenSSLFunctions->transformAes192CbcGetKlass         = xmlSecOpenSSLTransformAes192CbcGetKlass;
    gXmlSecOpenSSLFunctions->transformAes256CbcGetKlass         = xmlSecOpenSSLTransformAes256CbcGetKlass;
    gXmlSecOpenSSLFunctions->transformAes128GcmGetKlass         = xmlSecOpenSSLTransformAes128GcmGetKlass;
    gXmlSecOpenSSLFunctions->transformAes192GcmGetKlass         = xmlSecOpenSSLTransformAes192GcmGetKlass;
    gXmlSecOpenSSLFunctions->transformAes256GcmGetKlass         = xmlSecOpenSSLTransformAes256GcmGetKlass;
    gXmlSecOpenSSLFunctions->transformKWAes128GetKlass          = xmlSecOpenSSLTransformKWAes128GetKlass;
    gXmlSecOpenSSLFunctions->transformKWAes192GetKlass          = xmlSecOpenSSLTransformKWAes192GetKlass;
    gXmlSecOpenSSLFunctions->transformKWAes256GetKlass          = xmlSecOpenSSLTransformKWAes256GetKlass;
#endif /* XMLSEC_NO_AES */

    /******************************* DES ********************************/
#ifndef XMLSEC_NO_DES
    gXmlSecOpenSSLFunctions->transformDes3CbcGetKlass           = xmlSecOpenSSLTransformDes3CbcGetKlass;
    gXmlSecOpenSSLFunctions->transformKWDes3GetKlass            = xmlSecOpenSSLTransformKWDes3GetKlass;
#endif /* XMLSEC_NO_DES */

    /******************************* DSA ********************************/
#ifndef XMLSEC_NO_DSA

#ifndef XMLSEC_NO_SHA1
    gXmlSecOpenSSLFunctions->transformDsaSha1GetKlass           = xmlSecOpenSSLTransformDsaSha1GetKlass;
#endif /* XMLSEC_NO_SHA1 */

#ifndef XMLSEC_NO_SHA256
    gXmlSecOpenSSLFunctions->transformDsaSha256GetKlass         = xmlSecOpenSSLTransformDsaSha256GetKlass;
#endif /* XMLSEC_NO_SHA256 */

#endif /* XMLSEC_NO_DSA */

    /******************************* ECDSA ********************************/
#ifndef XMLSEC_NO_ECDSA

#ifndef XMLSEC_NO_SHA1
    gXmlSecOpenSSLFunctions->transformEcdsaSha1GetKlass         = xmlSecOpenSSLTransformEcdsaSha1GetKlass;
#endif /* XMLSEC_NO_SHA1 */

#ifndef XMLSEC_NO_SHA224
    gXmlSecOpenSSLFunctions->transformEcdsaSha224GetKlass       = xmlSecOpenSSLTransformEcdsaSha224GetKlass;
#endif /* XMLSEC_NO_SHA224 */

#ifndef XMLSEC_NO_SHA256
    gXmlSecOpenSSLFunctions->transformEcdsaSha256GetKlass       = xmlSecOpenSSLTransformEcdsaSha256GetKlass;
#endif /* XMLSEC_NO_SHA256 */

#ifndef XMLSEC_NO_SHA384
    gXmlSecOpenSSLFunctions->transformEcdsaSha384GetKlass       = xmlSecOpenSSLTransformEcdsaSha384GetKlass;
#endif /* XMLSEC_NO_SHA384 */

#ifndef XMLSEC_NO_SHA512
    gXmlSecOpenSSLFunctions->transformEcdsaSha512GetKlass       = xmlSecOpenSSLTransformEcdsaSha512GetKlass;
#endif /* XMLSEC_NO_SHA512 */

#endif /* XMLSEC_NO_ECDSA */

    /******************************* GOST ********************************/
#ifndef XMLSEC_NO_GOST
    gXmlSecOpenSSLFunctions->transformGost2001GostR3411_94GetKlass     = xmlSecOpenSSLTransformGost2001GostR3411_94GetKlass;
    gXmlSecOpenSSLFunctions->transformGostR3411_94GetKlass             = xmlSecOpenSSLTransformGostR3411_94GetKlass;
#endif /* XMLSEC_NO_GOST */

#ifndef XMLSEC_NO_GOST2012
    gXmlSecOpenSSLFunctions->transformGostR3410_2012GostR3411_2012_256GetKlass = xmlSecOpenSSLTransformGostR3410_2012GostR3411_2012_256GetKlass;
    gXmlSecOpenSSLFunctions->transformGostR3410_2012GostR3411_2012_512GetKlass = xmlSecOpenSSLTransformGostR3410_2012GostR3411_2012_512GetKlass;
    gXmlSecOpenSSLFunctions->transformGostR3411_2012_256GetKlass       = xmlSecOpenSSLTransformGostR3411_2012_256GetKlass;
    gXmlSecOpenSSLFunctions->transformGostR3411_2012_512GetKlass       = xmlSecOpenSSLTransformGostR3411_2012_512GetKlass;
#endif /* XMLSEC_NO_GOST2012 */

    /******************************* HMAC ********************************/
#ifndef XMLSEC_NO_HMAC

#ifndef XMLSEC_NO_MD5
    gXmlSecOpenSSLFunctions->transformHmacMd5GetKlass           = xmlSecOpenSSLTransformHmacMd5GetKlass;
#endif /* XMLSEC_NO_MD5 */

#ifndef XMLSEC_NO_RIPEMD160
    gXmlSecOpenSSLFunctions->transformHmacRipemd160GetKlass     = xmlSecOpenSSLTransformHmacRipemd160GetKlass;
#endif /* XMLSEC_NO_RIPEMD160 */

#ifndef XMLSEC_NO_SHA1
    gXmlSecOpenSSLFunctions->transformHmacSha1GetKlass          = xmlSecOpenSSLTransformHmacSha1GetKlass;
#endif /* XMLSEC_NO_SHA1 */

#ifndef XMLSEC_NO_SHA224
    gXmlSecOpenSSLFunctions->transformHmacSha224GetKlass        = xmlSecOpenSSLTransformHmacSha224GetKlass;
#endif /* XMLSEC_NO_SHA224 */

#ifndef XMLSEC_NO_SHA256
    gXmlSecOpenSSLFunctions->transformHmacSha256GetKlass        = xmlSecOpenSSLTransformHmacSha256GetKlass;
#endif /* XMLSEC_NO_SHA256 */

#ifndef XMLSEC_NO_SHA384
    gXmlSecOpenSSLFunctions->transformHmacSha384GetKlass        = xmlSecOpenSSLTransformHmacSha384GetKlass;
#endif /* XMLSEC_NO_SHA384 */

#ifndef XMLSEC_NO_SHA512
    gXmlSecOpenSSLFunctions->transformHmacSha512GetKlass        = xmlSecOpenSSLTransformHmacSha512GetKlass;
#endif /* XMLSEC_NO_SHA512 */

#endif /* XMLSEC_NO_HMAC */

    /******************************* MD5 ********************************/
#ifndef XMLSEC_NO_MD5
    gXmlSecOpenSSLFunctions->transformMd5GetKlass               = xmlSecOpenSSLTransformMd5GetKlass;
#endif /* XMLSEC_NO_MD5 */

    /******************************* RIPEMD160 ********************************/
#ifndef XMLSEC_NO_RIPEMD160
    gXmlSecOpenSSLFunctions->transformRipemd160GetKlass         = xmlSecOpenSSLTransformRipemd160GetKlass;
#endif /* XMLSEC_NO_RIPEMD160 */

    /******************************* RSA ********************************/
#ifndef XMLSEC_NO_RSA

#ifndef XMLSEC_NO_MD5
    gXmlSecOpenSSLFunctions->transformRsaMd5GetKlass            = xmlSecOpenSSLTransformRsaMd5GetKlass;
#endif /* XMLSEC_NO_MD5 */

#ifndef XMLSEC_NO_RIPEMD160
    gXmlSecOpenSSLFunctions->transformRsaRipemd160GetKlass      = xmlSecOpenSSLTransformRsaRipemd160GetKlass;
#endif /* XMLSEC_NO_RIPEMD160 */

#ifndef XMLSEC_NO_SHA1
    gXmlSecOpenSSLFunctions->transformRsaSha1GetKlass           = xmlSecOpenSSLTransformRsaSha1GetKlass;
#endif /* XMLSEC_NO_SHA1 */

#ifndef XMLSEC_NO_SHA224
    gXmlSecOpenSSLFunctions->transformRsaSha224GetKlass         = xmlSecOpenSSLTransformRsaSha224GetKlass;
#endif /* XMLSEC_NO_SHA224 */

#ifndef XMLSEC_NO_SHA256
    gXmlSecOpenSSLFunctions->transformRsaSha256GetKlass         = xmlSecOpenSSLTransformRsaSha256GetKlass;
#endif /* XMLSEC_NO_SHA256 */

#ifndef XMLSEC_NO_SHA384
    gXmlSecOpenSSLFunctions->transformRsaSha384GetKlass         = xmlSecOpenSSLTransformRsaSha384GetKlass;
#endif /* XMLSEC_NO_SHA384 */

#ifndef XMLSEC_NO_SHA512
    gXmlSecOpenSSLFunctions->transformRsaSha512GetKlass         = xmlSecOpenSSLTransformRsaSha512GetKlass;
#endif /* XMLSEC_NO_SHA512 */

    gXmlSecOpenSSLFunctions->transformRsaPkcs1GetKlass          = xmlSecOpenSSLTransformRsaPkcs1GetKlass;
    gXmlSecOpenSSLFunctions->transformRsaOaepGetKlass           = xmlSecOpenSSLTransformRsaOaepGetKlass;
#endif /* XMLSEC_NO_RSA */

    /******************************* SHA ********************************/
#ifndef XMLSEC_NO_SHA1
    gXmlSecOpenSSLFunctions->transformSha1GetKlass              = xmlSecOpenSSLTransformSha1GetKlass;
#endif /* XMLSEC_NO_SHA1 */

#ifndef XMLSEC_NO_SHA224
    gXmlSecOpenSSLFunctions->transformSha224GetKlass            = xmlSecOpenSSLTransformSha224GetKlass;
#endif /* XMLSEC_NO_SHA224 */

#ifndef XMLSEC_NO_SHA256
    gXmlSecOpenSSLFunctions->transformSha256GetKlass            = xmlSecOpenSSLTransformSha256GetKlass;
#endif /* XMLSEC_NO_SHA256 */

#ifndef XMLSEC_NO_SHA384
    gXmlSecOpenSSLFunctions->transformSha384GetKlass            = xmlSecOpenSSLTransformSha384GetKlass;
#endif /* XMLSEC_NO_SHA384 */

#ifndef XMLSEC_NO_SHA512
    gXmlSecOpenSSLFunctions->transformSha512GetKlass            = xmlSecOpenSSLTransformSha512GetKlass;
#endif /* XMLSEC_NO_SHA512 */

    /********************************************************************
     *
     * High level routines form xmlsec command line utility
     *
     ********************************************************************/
    gXmlSecOpenSSLFunctions->cryptoAppInit                      = xmlSecOpenSSLAppInit;
    gXmlSecOpenSSLFunctions->cryptoAppShutdown                  = xmlSecOpenSSLAppShutdown;
    gXmlSecOpenSSLFunctions->cryptoAppDefaultKeysMngrInit       = xmlSecOpenSSLAppDefaultKeysMngrInit;
    gXmlSecOpenSSLFunctions->cryptoAppDefaultKeysMngrAdoptKey   = xmlSecOpenSSLAppDefaultKeysMngrAdoptKey;
    gXmlSecOpenSSLFunctions->cryptoAppDefaultKeysMngrLoad       = xmlSecOpenSSLAppDefaultKeysMngrLoad;
    gXmlSecOpenSSLFunctions->cryptoAppDefaultKeysMngrSave       = xmlSecOpenSSLAppDefaultKeysMngrSave;
#ifndef XMLSEC_NO_X509
    gXmlSecOpenSSLFunctions->cryptoAppKeysMngrCertLoad          = xmlSecOpenSSLAppKeysMngrCertLoad;
    gXmlSecOpenSSLFunctions->cryptoAppKeysMngrCertLoadMemory    = xmlSecOpenSSLAppKeysMngrCertLoadMemory;
    gXmlSecOpenSSLFunctions->cryptoAppPkcs12Load                = xmlSecOpenSSLAppPkcs12Load;
    gXmlSecOpenSSLFunctions->cryptoAppPkcs12LoadMemory          = xmlSecOpenSSLAppPkcs12LoadMemory;
    gXmlSecOpenSSLFunctions->cryptoAppKeyCertLoad               = xmlSecOpenSSLAppKeyCertLoad;
    gXmlSecOpenSSLFunctions->cryptoAppKeyCertLoadMemory         = xmlSecOpenSSLAppKeyCertLoadMemory;
#endif /* XMLSEC_NO_X509 */
    gXmlSecOpenSSLFunctions->cryptoAppKeyLoad                   = xmlSecOpenSSLAppKeyLoad;
    gXmlSecOpenSSLFunctions->cryptoAppKeyLoadMemory             = xmlSecOpenSSLAppKeyLoadMemory;
    gXmlSecOpenSSLFunctions->cryptoAppDefaultPwdCallback        = (void*)xmlSecOpenSSLAppGetDefaultPwdCallback();

    return(gXmlSecOpenSSLFunctions);
}

/**
 * xmlSecOpenSSLInit:
 *
 * XMLSec library specific crypto engine initialization.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
int
xmlSecOpenSSLInit (void)  {
    /* Check loaded xmlsec library version */
    if(xmlSecCheckVersionExact() != 1) {
        xmlSecInternalError("xmlSecCheckVersionExact", NULL);
        return(-1);
    }

    if(xmlSecOpenSSLErrorsInit() < 0) {
        xmlSecInternalError("xmlSecOpenSSLErrorsInit", NULL);
        return(-1);
    }

    /* register our klasses */
    if(xmlSecCryptoDLFunctionsRegisterKeyDataAndTransforms(xmlSecCryptoGetFunctions_openssl()) < 0) {
        xmlSecInternalError("xmlSecCryptoDLFunctionsRegisterKeyDataAndTransforms", NULL);
        return(-1);
    }

    return(0);
}

/**
 * xmlSecOpenSSLShutdown:
 *
 * XMLSec library specific crypto engine shutdown.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
int
xmlSecOpenSSLShutdown(void) {
    xmlSecOpenSSLSetDefaultTrustedCertsFolder(NULL);
    xmlSecOpenSSLErrorsShutdown();
    return(0);
}

/**
 * xmlSecOpenSSLKeysMngrInit:
 * @mngr:               the pointer to keys manager.
 *
 * Adds OpenSSL specific key data stores in keys manager.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
int
xmlSecOpenSSLKeysMngrInit(xmlSecKeysMngrPtr mngr) {
    int ret;

    xmlSecAssert2(mngr != NULL, -1);

#ifndef XMLSEC_NO_X509
    /* create x509 store if needed */
    if(xmlSecKeysMngrGetDataStore(mngr, xmlSecOpenSSLX509StoreId) == NULL) {
        xmlSecKeyDataStorePtr x509Store;

        x509Store = xmlSecKeyDataStoreCreate(xmlSecOpenSSLX509StoreId);
        if(x509Store == NULL) {
            xmlSecInternalError("xmlSecKeyDataStoreCreate(xmlSecOpenSSLX509StoreId)", NULL);
            return(-1);
        }

        ret = xmlSecKeysMngrAdoptDataStore(mngr, x509Store);
        if(ret < 0) {
            xmlSecInternalError("xmlSecKeysMngrAdoptDataStore", NULL);
            xmlSecKeyDataStoreDestroy(x509Store);
            return(-1);
        }
    }
#endif /* XMLSEC_NO_X509 */
    return(0);
}

/**
 * xmlSecOpenSSLGenerateRandom:
 * @buffer:             the destination buffer.
 * @size:               the numer of bytes to generate.
 *
 * Generates @size random bytes and puts result in @buffer.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
int
xmlSecOpenSSLGenerateRandom(xmlSecBufferPtr buffer, xmlSecSize size) {
    int ret;

    xmlSecAssert2(buffer != NULL, -1);
    xmlSecAssert2(size > 0, -1);

    ret = xmlSecBufferSetSize(buffer, size);
    if(ret < 0) {
        xmlSecInternalError2("xmlSecBufferSetSize", NULL,
                             "size=" XMLSEC_SIZE_FMT, size);
        return(-1);
    }

    /* get random data */
    ret = RAND_priv_bytes_ex(xmlSecOpenSSLGetLibCtx(), (xmlSecByte*)xmlSecBufferGetData(buffer), size,
                        XMLSEEC_OPENSSL_RAND_BYTES_STRENGTH);
    if(ret != 1) {
        xmlSecOpenSSLError2("RAND_priv_bytes_ex", NULL,
                            "size=" XMLSEC_SIZE_FMT, size);
        return(-1);
    }
    return(0);
}

/**
 * xmlSecOpenSSLErrorsDefaultCallback:
 * @file:               the error location file name (__FILE__ macro).
 * @line:               the error location line number (__LINE__ macro).
 * @func:               the error location function name (__FUNCTION__ macro).
 * @errorObject:        the error specific error object
 * @errorSubject:       the error specific error subject.
 * @reason:             the error code.
 * @msg:                the additional error message.
 *
 * The errors reporting callback function.
 */
void
xmlSecOpenSSLErrorsDefaultCallback(const char* file, int line, const char* func,
                                const char* errorObject, const char* errorSubject,
                                int reason, const char* msg) {
#if !defined(XMLSEC_OPENSSL_API_300) && !defined(OPENSSL_IS_BORINGSSL) && !defined(OPENSSL_NO_ERR)
    ERR_put_error(gXmlSecOpenSSLErrorsLib,
                XMLSEC_OPENSSL_ERRORS_FUNCTION,
                reason, file, line);
#endif /* !defined(XMLSEC_OPENSSL_API_300) && !defined(OPENSSL_IS_BORINGSSL) && !defined(OPENSSL_NO_ERR) */

    xmlSecErrorsDefaultCallback(file, line, func,
                errorObject, errorSubject,
                reason, msg);
}

static int
xmlSecOpenSSLErrorsInit(void) {
#if !defined(XMLSEC_OPENSSL_API_300) && !defined(OPENSSL_IS_BORINGSSL) && !defined(OPENSSL_NO_ERR)
    xmlSecSize pos;

    /* get XMLSec library id */
    gXmlSecOpenSSLErrorsLib = ERR_get_next_error_library();

    /* initialize xmlsec lib name array */
    memset(xmlSecOpenSSLStrLib, 0, sizeof(xmlSecOpenSSLStrLib));
    xmlSecOpenSSLStrLib[0].error = ERR_PACK(gXmlSecOpenSSLErrorsLib, 0, 0);
    xmlSecOpenSSLStrLib[0].string = gXmlSecOpenSSLErrorsLibName;

    /* initialize xmlsec default error array */
    memset(xmlSecOpenSSLStrDefReason, 0, sizeof(xmlSecOpenSSLStrDefReason));
    xmlSecOpenSSLStrDefReason[0].error = ERR_PACK(gXmlSecOpenSSLErrorsLib, XMLSEC_OPENSSL_ERRORS_FUNCTION, 0);
    xmlSecOpenSSLStrDefReason[0].string = gXmlSecOpenSSLErrorsDefault;

    /* initialize reasons array */
    memset(xmlSecOpenSSLStrReasons, 0, sizeof(xmlSecOpenSSLStrReasons));
    for(pos = 0; (pos < XMLSEC_ERRORS_MAX_NUMBER) && (xmlSecErrorsGetMsg(pos) != NULL); ++pos) {
        xmlSecOpenSSLStrReasons[pos].error  = ERR_PACK(gXmlSecOpenSSLErrorsLib, XMLSEC_OPENSSL_ERRORS_FUNCTION, xmlSecErrorsGetCode(pos));
        xmlSecOpenSSLStrReasons[pos].string = xmlSecErrorsGetMsg(pos);
    }

    /* load xmlsec strings in OpenSSL */
    ERR_load_strings(gXmlSecOpenSSLErrorsLib, xmlSecOpenSSLStrLib); /* define xmlsec lib name */
    ERR_load_strings(gXmlSecOpenSSLErrorsLib, xmlSecOpenSSLStrDefReason); /* define default reason */
    ERR_load_strings(gXmlSecOpenSSLErrorsLib, xmlSecOpenSSLStrReasons);
#endif /* !defined(XMLSEC_OPENSSL_API_300) && !defined(OPENSSL_IS_BORINGSSL) && !defined(OPENSSL_NO_ERR) */

    /* and set default errors callback for xmlsec to us */
    xmlSecErrorsSetCallback(xmlSecOpenSSLErrorsDefaultCallback);

    return(0);
}


static void
xmlSecOpenSSLErrorsShutdown(void) {
    /* remove callback */
    xmlSecErrorsSetCallback(NULL);

#if !defined(XMLSEC_OPENSSL_API_300) && !defined(OPENSSL_IS_BORINGSSL) && !defined(OPENSSL_NO_ERR)
    /* unload xmlsec strings from OpenSSL */
    ERR_unload_strings(gXmlSecOpenSSLErrorsLib, xmlSecOpenSSLStrLib);
    ERR_unload_strings(gXmlSecOpenSSLErrorsLib, xmlSecOpenSSLStrDefReason);
    ERR_unload_strings(gXmlSecOpenSSLErrorsLib, xmlSecOpenSSLStrReasons);
#endif /* !defined(XMLSEC_OPENSSL_API_300) && !defined(OPENSSL_IS_BORINGSSL) && !defined(OPENSSL_NO_ERR) */
}

/**
 * xmlSecOpenSSLSetDefaultTrustedCertsFolder:
 * @path:       the default trusted certs path.
 *
 * Sets the default trusted certs folder.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecOpenSSLSetDefaultTrustedCertsFolder(const xmlChar* path) {
    if(gXmlSecOpenSSLTrustedCertsFolder != NULL) {
        xmlFree(gXmlSecOpenSSLTrustedCertsFolder);
        gXmlSecOpenSSLTrustedCertsFolder = NULL;
    }

    if(path != NULL) {
        gXmlSecOpenSSLTrustedCertsFolder = xmlStrdup(BAD_CAST path);
        if(gXmlSecOpenSSLTrustedCertsFolder == NULL) {
            xmlSecStrdupError(BAD_CAST path, NULL);
            return(-1);
        }
    }

    return(0);
}

/**
 * xmlSecOpenSSLGetDefaultTrustedCertsFolder:
 *
 * Gets the default trusted certs folder.
 *
 * Returns: the default trusted cert folder.
 */
const xmlChar*
xmlSecOpenSSLGetDefaultTrustedCertsFolder(void) {
    return(gXmlSecOpenSSLTrustedCertsFolder);
}

#ifdef XMLSEC_OPENSSL_API_300

static OSSL_LIB_CTX* gXmlSecOpenSSLLibCtx = NULL;

/**
 * xmlSecOpenSSLSetLibCtx:
 * @libctx:           the OSSL_LIB_CTX object to be used by xmlsec-openssl
 *                    or NULL to use default.
 *
 * Sets the OSSL_LIB_CTX object to be used by xmlsec-openssl. The caller is
 * responsible for lifetime of this object.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecOpenSSLSetLibCtx(OSSL_LIB_CTX* libctx) {
    gXmlSecOpenSSLLibCtx = libctx;
    return(0);
}

/**
 * xmlSecOpenSSLGetLibCtx:
 *
 * Gets the current OSSL_LIB_CTX object to be used by xmlsec-openssl or
 * NULL if the default one is used.
 *
 * Returns: the current OSSL_LIB_CTX object or NULL if default is used.
 */
OSSL_LIB_CTX*
xmlSecOpenSSLGetLibCtx(void) {
    return(gXmlSecOpenSSLLibCtx);
}
#endif /* XMLSEC_OPENSSL_API_300 */

/********************************************************************
 *
 * BIO helpers
 *
 ********************************************************************/

/**
 * xmlSecOpenSSLCreateMemBio:
 *
 * Creates a memory BIO using xmlSecOpenSSLGetLibCtx() for OpenSSL 3.0.
 *
 * Returns: the pointer to BIO object or NULL if an error occurs/
 */
BIO*
xmlSecOpenSSLCreateMemBio(void) {
    BIO* bio = NULL;

    bio = BIO_new_ex(xmlSecOpenSSLGetLibCtx(), BIO_s_mem());
    if(bio == NULL) {
        xmlSecOpenSSLError("BIO_new_ex(BIO_s_mem())", NULL);
        return(NULL);
    }
    return(bio);
}

/**
 * xmlSecOpenSSLCreateMemBufBio:
 * @buf:      the data
 * @bufSize:  the data size
 *
 * Creates a read-only memory BIO using xmlSecOpenSSLGetLibCtx() for
 * OpenSSL 3.0 containing @len bytes of data from @buf.
 *
 * Returns: the pointer to BIO object or NULL if an error occurs/
 */
BIO*
xmlSecOpenSSLCreateMemBufBio(const xmlSecByte *buf, xmlSecSize bufSize) {
    BIO* bio = NULL;
    int bufLen;

    xmlSecAssert2(buf != NULL, NULL);

    XMLSEC_SAFE_CAST_SIZE_TO_INT(bufSize, bufLen, return(NULL), NULL);
    bio = BIO_new_mem_buf((const void*)buf, bufLen);
    if(bio == NULL) {
        xmlSecOpenSSLError2("BIO_new_mem_buf", NULL,
                            "dataSize=%d", bufLen);
        return(NULL);
    }
    return(bio);
}

/**
 * xmlSecOpenSSLCreateReadFileBio:
 * @path:     the file path
 *
 * Creates a read-only file BIO using xmlSecOpenSSLGetLibCtx() for
 * OpenSSL 3.0.
 *
 * Returns: the pointer to BIO object or NULL if an error occurs/
 */
BIO*
xmlSecOpenSSLCreateReadFileBio(const char* path) {
    BIO* bio = NULL;
    xmlSecAssert2(path != NULL, NULL);

    bio = BIO_new_ex(xmlSecOpenSSLGetLibCtx(), BIO_s_file());
    if(bio == NULL) {
        xmlSecOpenSSLError("BIO_new_ex(BIO_s_file())", NULL);
        return(NULL);
    }
    if(BIO_read_filename(bio, path) != 1) {
        xmlSecOpenSSLError2("BIO_read_filename", NULL,
            "path=%s", xmlSecErrorsSafeString(path));
        return(NULL);
    }
    return(bio);
}

