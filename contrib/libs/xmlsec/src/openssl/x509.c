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
 * SECTION:x509
 * @Short_description: X509 certificates implementation for OpenSSL.
 * @Stability: Stable
 *
 */

#include "globals.h"

#ifndef XMLSEC_NO_X509

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <time.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/base64.h>
#include <xmlsec/keys.h>
#include <xmlsec/keyinfo.h>
#include <xmlsec/keysmngr.h>
#include <xmlsec/x509.h>
#include <xmlsec/base64.h>
#include <xmlsec/errors.h>
#include <xmlsec/private.h>
#include <xmlsec/xmltree.h>

#include <xmlsec/openssl/crypto.h>
#include <xmlsec/openssl/evp.h>
#include <xmlsec/openssl/x509.h>

/* Windows overwrites X509_NAME and other things that break openssl */
#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>
#include <openssl/x509v3.h>
#include <openssl/asn1.h>

#ifdef OPENSSL_IS_BORINGSSL
#error #include <openssl/mem.h>
#endif /* OPENSSL_IS_BORINGSSL */

#include "../cast_helpers.h"
#include "../keysdata_helpers.h"

#include "openssl_compat.h"

/*************************************************************************
 *
 * X509 utility functions
 *
 ************************************************************************/
static int              xmlSecOpenSSLKeyDataX509VerifyAndExtractKey(xmlSecKeyDataPtr data,
                                                                xmlSecKeyPtr key,
                                                                xmlSecKeyInfoCtxPtr keyInfoCtx);
static X509*            xmlSecOpenSSLX509CertDerRead            (const xmlSecByte* buf,
                                                                 xmlSecSize size);
static X509_CRL*        xmlSecOpenSSLX509CrlDerRead             (xmlSecByte* buf,
                                                                 xmlSecSize size);
static void             xmlSecOpenSSLX509CertDebugDump          (X509* cert,
                                                                 FILE* output);
static void             xmlSecOpenSSLX509CertDebugXmlDump       (X509* cert,
                                                                 FILE* output);

/*************************************************************************
 *
 * Internal OpenSSL X509 data CTX
 *
 ************************************************************************/
typedef struct _xmlSecOpenSSLX509DataCtx                xmlSecOpenSSLX509DataCtx,
                                                        *xmlSecOpenSSLX509DataCtxPtr;
struct _xmlSecOpenSSLX509DataCtx {
    X509*               keyCert;
    STACK_OF(X509)*     certsList;
    STACK_OF(X509_CRL)* crlsList;
};

/**************************************************************************
 *
 * <dsig:X509Data> processing (http://www.w3.org/TR/xmldsig-core/#sec-X509Data)
 *
 *************************************************************************/
XMLSEC_KEY_DATA_DECLARE(OpenSSLX509Data, xmlSecOpenSSLX509DataCtx)
#define xmlSecOpenSSLX509DataSize XMLSEC_KEY_DATA_SIZE(OpenSSLX509Data)

static int              xmlSecOpenSSLKeyDataX509Initialize      (xmlSecKeyDataPtr data);
static int              xmlSecOpenSSLKeyDataX509Duplicate       (xmlSecKeyDataPtr dst,
                                                                 xmlSecKeyDataPtr src);
static void             xmlSecOpenSSLKeyDataX509Finalize        (xmlSecKeyDataPtr data);
static int              xmlSecOpenSSLKeyDataX509XmlRead         (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 xmlNodePtr node,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
static int              xmlSecOpenSSLKeyDataX509XmlWrite        (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 xmlNodePtr node,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
static xmlSecKeyDataType xmlSecOpenSSLKeyDataX509GetType        (xmlSecKeyDataPtr data);
static const xmlChar*   xmlSecOpenSSLKeyDataX509GetIdentifier   (xmlSecKeyDataPtr data);

static void             xmlSecOpenSSLKeyDataX509DebugDump       (xmlSecKeyDataPtr data,
                                                                 FILE* output);
static void             xmlSecOpenSSLKeyDataX509DebugXmlDump    (xmlSecKeyDataPtr data,
                                                                 FILE* output);


typedef struct _xmlSecOpenSSLKeyDataX509Context {
    xmlSecSize crtPos;
    xmlSecSize crtSize;
    xmlSecSize crlPos;
    xmlSecSize crlSize;
} xmlSecOpenSSLKeyDataX509Context;

static int              xmlSecOpenSSLKeyDataX509Read            (xmlSecKeyDataPtr data,
                                                                 xmlSecKeyValueX509Ptr x509Value,
                                                                 xmlSecKeysMngrPtr keysMngr,
                                                                 unsigned int flags);
static int              xmlSecOpenSSLKeyDataX509Write           (xmlSecKeyDataPtr data,
                                                                  xmlSecKeyValueX509Ptr x509Value,
                                                                  int content,
                                                                  void* context);

static xmlSecKeyDataKlass xmlSecOpenSSLKeyDataX509Klass = {
    sizeof(xmlSecKeyDataKlass),
    xmlSecOpenSSLX509DataSize,

    /* data */
    xmlSecNameX509Data,
    xmlSecKeyDataUsageKeyInfoNode | xmlSecKeyDataUsageRetrievalMethodNodeXml,
                                                /* xmlSecKeyDataUsage usage; */
    xmlSecHrefX509Data,                         /* const xmlChar* href; */
    xmlSecNodeX509Data,                         /* const xmlChar* dataNodeName; */
    xmlSecDSigNs,                               /* const xmlChar* dataNodeNs; */

    /* constructors/destructor */
    xmlSecOpenSSLKeyDataX509Initialize,         /* xmlSecKeyDataInitializeMethod initialize; */
    xmlSecOpenSSLKeyDataX509Duplicate,          /* xmlSecKeyDataDuplicateMethod duplicate; */
    xmlSecOpenSSLKeyDataX509Finalize,           /* xmlSecKeyDataFinalizeMethod finalize; */
    NULL,                                       /* xmlSecKeyDataGenerateMethod generate; */

    /* get info */
    xmlSecOpenSSLKeyDataX509GetType,            /* xmlSecKeyDataGetTypeMethod getType; */
    NULL,                                       /* xmlSecKeyDataGetSizeMethod getSize; */
    xmlSecOpenSSLKeyDataX509GetIdentifier,      /* xmlSecKeyDataGetIdentifier getIdentifier; */

    /* read/write */
    xmlSecOpenSSLKeyDataX509XmlRead,            /* xmlSecKeyDataXmlReadMethod xmlRead; */
    xmlSecOpenSSLKeyDataX509XmlWrite,           /* xmlSecKeyDataXmlWriteMethod xmlWrite; */
    NULL,                                       /* xmlSecKeyDataBinReadMethod binRead; */
    NULL,                                       /* xmlSecKeyDataBinWriteMethod binWrite; */

    /* debug */
    xmlSecOpenSSLKeyDataX509DebugDump,          /* xmlSecKeyDataDebugDumpMethod debugDump; */
    xmlSecOpenSSLKeyDataX509DebugXmlDump,       /* xmlSecKeyDataDebugDumpMethod debugXmlDump; */

    /* reserved for the future */
    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLKeyDataX509GetKlass:
 *
 * The OpenSSL X509 key data klass (http://www.w3.org/TR/xmldsig-core/#sec-X509Data).
 *
 * Returns: the X509 data klass.
 */
xmlSecKeyDataId
xmlSecOpenSSLKeyDataX509GetKlass(void) {
    return(&xmlSecOpenSSLKeyDataX509Klass);
}

/**
 * xmlSecOpenSSLKeyDataX509GetKeyCert:
 * @data:               the pointer to X509 key data.
 *
 * Gets the certificate from which the key was extracted.
 *
 * Returns: the key's certificate or NULL if key data was not used for key
 * extraction or an error occurs.
 */
X509*
xmlSecOpenSSLKeyDataX509GetKeyCert(xmlSecKeyDataPtr data) {
    xmlSecOpenSSLX509DataCtxPtr ctx;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), NULL);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, NULL);

    return(ctx->keyCert);
}

/**
 * xmlSecOpenSSLKeyDataX509AdoptKeyCert:
 * @data:               the pointer to X509 key data.
 * @cert:               the pointer to OpenSSL X509 certificate.
 *
 * Sets the key's certificate in @data.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecOpenSSLKeyDataX509AdoptKeyCert(xmlSecKeyDataPtr data, X509* cert) {
    xmlSecOpenSSLX509DataCtxPtr ctx;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), -1);
    xmlSecAssert2(cert != NULL, -1);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, -1);

    if(ctx->keyCert != NULL) {
        X509_free(ctx->keyCert);
    }
    ctx->keyCert = cert;
    return(0);
}

/**
 * xmlSecOpenSSLKeyDataX509AdoptCert:
 * @data:               the pointer to X509 key data.
 * @cert:               the pointer to OpenSSL X509 certificate.
 *
 * Adds certificate to the X509 key data.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecOpenSSLKeyDataX509AdoptCert(xmlSecKeyDataPtr data, X509* cert) {
    xmlSecOpenSSLX509DataCtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), -1);
    xmlSecAssert2(cert != NULL, -1);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, -1);

    if(ctx->certsList == NULL) {
        ctx->certsList = sk_X509_new_null();
        if(ctx->certsList == NULL) {
            xmlSecOpenSSLError("sk_X509_new_null",
                               xmlSecKeyDataGetName(data));
            return(-1);
        }
    }

    ret = sk_X509_push(ctx->certsList, cert);
    if(ret < 1) {
        xmlSecOpenSSLError("sk_X509_push",
                           xmlSecKeyDataGetName(data));
        return(-1);
    }

    return(0);
}

/**
 * xmlSecOpenSSLKeyDataX509GetCert:
 * @data:               the pointer to X509 key data.
 * @pos:                the desired certificate position.
 *
 * Gets a certificate from X509 key data.
 *
 * Returns: the pointer to certificate or NULL if @pos is larger than the
 * number of certificates in @data or an error occurs.
 */
X509*
xmlSecOpenSSLKeyDataX509GetCert(xmlSecKeyDataPtr data, xmlSecSize pos) {
    xmlSecOpenSSLX509DataCtxPtr ctx;
    int iPos;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), NULL);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, NULL);
    xmlSecAssert2(ctx->certsList != NULL, NULL);

    XMLSEC_SAFE_CAST_SIZE_TO_INT(pos, iPos, return(NULL), NULL);
    xmlSecAssert2(iPos < sk_X509_num(ctx->certsList), NULL);
    return(sk_X509_value(ctx->certsList, iPos));
}

/**
 * xmlSecOpenSSLKeyDataX509GetCertsSize:
 * @data:               the pointer to X509 key data.
 *
 * Gets the number of certificates in @data.
 *
 * Returns: te number of certificates in @data.
 */
xmlSecSize
xmlSecOpenSSLKeyDataX509GetCertsSize(xmlSecKeyDataPtr data) {
    xmlSecOpenSSLX509DataCtxPtr ctx;
    int ret;
    xmlSecSize res;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), 0);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, 0);

    if(ctx->certsList == NULL) {
        return(0);
    }

    ret = sk_X509_num(ctx->certsList);
    if(ret < 0) {
        xmlSecOpenSSLError("sk_X509_num", NULL);
        return(0);
    }
    XMLSEC_SAFE_CAST_INT_TO_SIZE(ret, res, return(0), NULL);

    return(res);
}

/**
 * xmlSecOpenSSLKeyDataX509AdoptCrl:
 * @data:               the pointer to X509 key data.
 * @crl:                the pointer to OpenSSL X509 CRL.
 *
 * Adds CRL to the X509 key data.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecOpenSSLKeyDataX509AdoptCrl(xmlSecKeyDataPtr data, X509_CRL* crl) {
    xmlSecOpenSSLX509DataCtxPtr ctx;
    int ret;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), -1);
    xmlSecAssert2(crl != NULL, -1);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, -1);

    if(ctx->crlsList == NULL) {
        ctx->crlsList = sk_X509_CRL_new_null();
        if(ctx->crlsList == NULL) {
            xmlSecOpenSSLError("sk_X509_CRL_new_null",
                               xmlSecKeyDataGetName(data));
            return(-1);
        }
    }

    ret = sk_X509_CRL_push(ctx->crlsList, crl);
    if(ret < 1) {
        xmlSecOpenSSLError("sk_X509_CRL_push",
                           xmlSecKeyDataGetName(data));
        return(-1);
    }

    return(0);
}

/**
 * xmlSecOpenSSLKeyDataX509GetCrl:
 * @data:               the pointer to X509 key data.
 * @pos:                the desired CRL position.
 *
 * Gets a CRL from X509 key data.
 *
 * Returns: the pointer to CRL or NULL if @pos is larger than the
 * number of CRLs in @data or an error occurs.
 */
X509_CRL*
xmlSecOpenSSLKeyDataX509GetCrl(xmlSecKeyDataPtr data, xmlSecSize pos) {
    xmlSecOpenSSLX509DataCtxPtr ctx;
    int iPos;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), NULL);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, NULL);

    xmlSecAssert2(ctx->crlsList != NULL, NULL);

    XMLSEC_SAFE_CAST_SIZE_TO_INT(pos, iPos, return(NULL), NULL);
    xmlSecAssert2(iPos < sk_X509_CRL_num(ctx->crlsList), NULL);
    return(sk_X509_CRL_value(ctx->crlsList, iPos));
}

/**
 * xmlSecOpenSSLKeyDataX509GetCrlsSize:
 * @data:               the pointer to X509 key data.
 *
 * Gets the number of CRLs in @data.
 *
 * Returns: te number of CRLs in @data.
 */
xmlSecSize
xmlSecOpenSSLKeyDataX509GetCrlsSize(xmlSecKeyDataPtr data) {
    xmlSecOpenSSLX509DataCtxPtr ctx;
    int ret;
    xmlSecSize res;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), 0);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, 0);

    if(ctx->crlsList == NULL) {
        return(0);
    }

    ret = sk_X509_CRL_num(ctx->crlsList);
    if(ret < 0) {
        xmlSecOpenSSLError("sk_X509_CRL_num", NULL);
        return(0);
    }
    XMLSEC_SAFE_CAST_INT_TO_SIZE(ret, res, return(0), NULL);

    return(res);
}

static int
xmlSecOpenSSLKeyDataX509Initialize(xmlSecKeyDataPtr data) {
    xmlSecOpenSSLX509DataCtxPtr ctx;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), -1);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, -1);

    memset(ctx, 0, sizeof(xmlSecOpenSSLX509DataCtx));
    return(0);
}

static int
xmlSecOpenSSLKeyDataX509Duplicate(xmlSecKeyDataPtr dst, xmlSecKeyDataPtr src) {
    X509* certSrc;
    X509* certDst;
    X509_CRL* crlSrc;
    X509_CRL* crlDst;
    xmlSecSize size, pos;
    int ret;

    xmlSecAssert2(xmlSecKeyDataCheckId(dst, xmlSecOpenSSLKeyDataX509Id), -1);
    xmlSecAssert2(xmlSecKeyDataCheckId(src, xmlSecOpenSSLKeyDataX509Id), -1);

    /* copy certsList */
    size = xmlSecOpenSSLKeyDataX509GetCertsSize(src);
    for(pos = 0; pos < size; ++pos) {
        certSrc = xmlSecOpenSSLKeyDataX509GetCert(src, pos);
        if(certSrc == NULL) {
            xmlSecInternalError2("xmlSecOpenSSLKeyDataX509GetCert",
                                 xmlSecErrorsSafeString(xmlSecKeyDataGetName(src)),
                                 "pos=" XMLSEC_SIZE_FMT, pos);
            return(-1);
        }

        certDst = X509_dup(certSrc);
        if(certDst == NULL) {
            xmlSecOpenSSLError("X509_dup",
                               xmlSecKeyDataGetName(dst));
            return(-1);
        }

        ret = xmlSecOpenSSLKeyDataX509AdoptCert(dst, certDst);
        if(ret < 0) {
            xmlSecInternalError("xmlSecOpenSSLKeyDataX509AdoptCert",
                                xmlSecKeyDataGetName(dst));
            X509_free(certDst);
            return(-1);
        }
    }

    /* copy crls */
    size = xmlSecOpenSSLKeyDataX509GetCrlsSize(src);
    for(pos = 0; pos < size; ++pos) {
        crlSrc = xmlSecOpenSSLKeyDataX509GetCrl(src, pos);
        if(crlSrc == NULL) {
            xmlSecInternalError2("xmlSecOpenSSLKeyDataX509GetCrl",
                                 xmlSecKeyDataGetName(src),
                                 "pos=" XMLSEC_SIZE_FMT, pos);
            return(-1);
        }

        crlDst = X509_CRL_dup(crlSrc);
        if(crlDst == NULL) {
            xmlSecOpenSSLError("X509_CRL_dup",
                               xmlSecKeyDataGetName(dst));
            return(-1);
        }

        ret = xmlSecOpenSSLKeyDataX509AdoptCrl(dst, crlDst);
        if(ret < 0) {
            xmlSecInternalError("xmlSecOpenSSLKeyDataX509AdoptCrl",
                                xmlSecKeyDataGetName(dst));
            X509_CRL_free(crlDst);
            return(-1);
        }
    }

    /* copy key cert if exist */
    certSrc = xmlSecOpenSSLKeyDataX509GetKeyCert(src);
    if(certSrc != NULL) {
        certDst = X509_dup(certSrc);
        if(certDst == NULL) {
            xmlSecOpenSSLError("X509_dup",
                               xmlSecKeyDataGetName(dst));
            return(-1);
        }
        ret = xmlSecOpenSSLKeyDataX509AdoptKeyCert(dst, certDst);
        if(ret < 0) {
            xmlSecInternalError("xmlSecOpenSSLKeyDataX509AdoptKeyCert",
                                xmlSecKeyDataGetName(dst));
            X509_free(certDst);
            return(-1);
        }
    }
    return(0);
}

static void
xmlSecOpenSSLKeyDataX509Finalize(xmlSecKeyDataPtr data) {
    xmlSecOpenSSLX509DataCtxPtr ctx;

    xmlSecAssert(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id));

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert(ctx != NULL);

    if(ctx->certsList != NULL) {
        sk_X509_pop_free(ctx->certsList, X509_free);
    }
    if(ctx->crlsList != NULL) {
        sk_X509_CRL_pop_free(ctx->crlsList, X509_CRL_free);
    }
    if(ctx->keyCert != NULL) {
        X509_free(ctx->keyCert);
    }
    memset(ctx, 0, sizeof(xmlSecOpenSSLX509DataCtx));
}

static int
xmlSecOpenSSLKeyDataX509XmlRead(xmlSecKeyDataId id, xmlSecKeyPtr key,
                                xmlNodePtr node, xmlSecKeyInfoCtxPtr keyInfoCtx) {
    xmlSecKeyDataPtr data;
    int ret;

    xmlSecAssert2(id == xmlSecOpenSSLKeyDataX509Id, -1);
    xmlSecAssert2(key != NULL, -1);

    data = xmlSecKeyEnsureData(key, id);
    if(data == NULL) {
        xmlSecInternalError("xmlSecKeyEnsureData",
            xmlSecKeyDataKlassGetName(id));
        return(-1);
    }

    ret = xmlSecKeyDataX509XmlRead(data, node, keyInfoCtx,
        xmlSecOpenSSLKeyDataX509Read);
    if(ret < 0) {
        xmlSecInternalError("xmlSecKeyDataX509XmlRead",
            xmlSecKeyDataKlassGetName(id));
        return(-1);
    }

    ret = xmlSecOpenSSLKeyDataX509VerifyAndExtractKey(data, key, keyInfoCtx);
    if(ret < 0) {
        xmlSecInternalError("xmlSecOpenSSLKeyDataX509VerifyAndExtractKey",
            xmlSecKeyDataKlassGetName(id));
        return(-1);
    }
    return(0);
}

static int
xmlSecOpenSSLKeyDataX509XmlWrite(xmlSecKeyDataId id, xmlSecKeyPtr key,
                                xmlNodePtr node, xmlSecKeyInfoCtxPtr keyInfoCtx) {
    xmlSecKeyDataPtr data;
    xmlSecOpenSSLKeyDataX509Context context;
    int ret;

    xmlSecAssert2(id == xmlSecOpenSSLKeyDataX509Id, -1);
    xmlSecAssert2(key != NULL, -1);

    /* get x509 data */
    data = xmlSecKeyGetData(key, id);
    if(data == NULL) {
        /* no x509 data in the key */
        return(0);
    }

    /* setup context */
    context.crtPos = context.crlPos = 0;
    context.crtSize = xmlSecOpenSSLKeyDataX509GetCertsSize(data);
    context.crlSize = xmlSecOpenSSLKeyDataX509GetCrlsSize(data);

    ret = xmlSecKeyDataX509XmlWrite(data, node, keyInfoCtx,
        xmlSecBase64GetDefaultLineSize(), 1, /* add line breaks */
        xmlSecOpenSSLKeyDataX509Write, &context);
    if(ret < 0) {
        xmlSecInternalError3("xmlSecKeyDataX509XmlWrite",
            xmlSecKeyDataKlassGetName(id),
            "crtSize=" XMLSEC_SIZE_FMT "; crlSize=" XMLSEC_SIZE_FMT,
            context.crtSize, context.crlSize);
        return(-1);
    }

    /* success */
    return(0);
}


static xmlSecKeyDataType
xmlSecOpenSSLKeyDataX509GetType(xmlSecKeyDataPtr data) {
    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), xmlSecKeyDataTypeUnknown);

    /* TODO: return verified/not verified status */
    return(xmlSecKeyDataTypeUnknown);
}

static const xmlChar*
xmlSecOpenSSLKeyDataX509GetIdentifier(xmlSecKeyDataPtr data) {
    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), NULL);

    /* TODO */
    return(NULL);
}

static void
xmlSecOpenSSLKeyDataX509DebugDump(xmlSecKeyDataPtr data, FILE* output) {
    X509* cert;
    xmlSecSize size, pos;

    xmlSecAssert(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id));
    xmlSecAssert(output != NULL);

    fprintf(output, "=== X509 Data:\n");
    cert = xmlSecOpenSSLKeyDataX509GetKeyCert(data);
    if(cert != NULL) {
        fprintf(output, "==== Key Certificate:\n");
        xmlSecOpenSSLX509CertDebugDump(cert, output);
    }

    size = xmlSecOpenSSLKeyDataX509GetCertsSize(data);
    for(pos = 0; pos < size; ++pos) {
        cert = xmlSecOpenSSLKeyDataX509GetCert(data, pos);
        if(cert == NULL) {
            xmlSecInternalError2("xmlSecOpenSSLKeyDataX509GetCert",
                                 xmlSecKeyDataGetName(data),
                                 "pos=" XMLSEC_SIZE_FMT, pos);
            return;
        }
        fprintf(output, "==== Certificate:\n");
        xmlSecOpenSSLX509CertDebugDump(cert, output);
    }

    /* we don't print out crls */
}

static void
xmlSecOpenSSLKeyDataX509DebugXmlDump(xmlSecKeyDataPtr data, FILE* output) {
    X509* cert;
    xmlSecSize size, pos;

    xmlSecAssert(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id));
    xmlSecAssert(output != NULL);

    fprintf(output, "<X509Data>\n");
    cert = xmlSecOpenSSLKeyDataX509GetKeyCert(data);
    if(cert != NULL) {
        fprintf(output, "<KeyCertificate>\n");
        xmlSecOpenSSLX509CertDebugXmlDump(cert, output);
        fprintf(output, "</KeyCertificate>\n");
    }

    size = xmlSecOpenSSLKeyDataX509GetCertsSize(data);
    for(pos = 0; pos < size; ++pos) {
        cert = xmlSecOpenSSLKeyDataX509GetCert(data, pos);
        if(cert == NULL) {
            xmlSecInternalError2("xmlSecOpenSSLKeyDataX509GetCert",
                                 xmlSecKeyDataGetName(data),
                                 "pos=" XMLSEC_SIZE_FMT, pos);
            return;
        }
        fprintf(output, "<Certificate>\n");
        xmlSecOpenSSLX509CertDebugXmlDump(cert, output);
        fprintf(output, "</Certificate>\n");
    }

    /* we don't print out crls */
    fprintf(output, "</X509Data>\n");
}

static int
xmlSecOpenSSLKeyDataX509Read(xmlSecKeyDataPtr data, xmlSecKeyValueX509Ptr x509Value,
                             xmlSecKeysMngrPtr keysMngr, unsigned int flags) {
    xmlSecKeyDataStorePtr x509Store;
    int stopOnUnknownCert = 0;
    X509* storeCert = NULL;
    X509* cert = NULL;
    X509_CRL* crl = NULL;
    int ret;
    int res = -1;

    xmlSecAssert2(data != NULL, -1);
    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), -1);
    xmlSecAssert2(x509Value != NULL, -1);
    xmlSecAssert2(keysMngr != NULL, -1);

    x509Store = xmlSecKeysMngrGetDataStore(keysMngr, xmlSecOpenSSLX509StoreId);
    if(x509Store == NULL) {
        xmlSecInternalError("xmlSecKeysMngrGetDataStore", xmlSecKeyDataGetName(data));
        goto done;
    }

    /* determine what to do */
    if((flags & XMLSEC_KEYINFO_FLAGS_X509DATA_STOP_ON_UNKNOWN_CERT) != 0) {
        stopOnUnknownCert = 1;
    }

    if(xmlSecBufferGetSize(&(x509Value->cert)) > 0) {
        cert = xmlSecOpenSSLX509CertDerRead(xmlSecBufferGetData(&(x509Value->cert)),
            xmlSecBufferGetSize(&(x509Value->cert)));
        if(cert == NULL) {
            xmlSecInternalError("xmlSecOpenSSLX509CertDerRead", xmlSecKeyDataGetName(data));
            goto done;
        }
    } else if(xmlSecBufferGetSize(&(x509Value->crl)) > 0) {
        crl = xmlSecOpenSSLX509CrlDerRead(xmlSecBufferGetData(&(x509Value->crl)),
            xmlSecBufferGetSize(&(x509Value->crl)));
        if(crl == NULL) {
            xmlSecInternalError("xmlSecOpenSSLX509CertDerRead", xmlSecKeyDataGetName(data));
            goto done;
        }
    } else if(xmlSecBufferGetSize(&(x509Value->ski)) > 0) {
        storeCert = xmlSecOpenSSLX509StoreFindCert_ex(x509Store, NULL,  NULL, NULL,
            xmlSecBufferGetData(&(x509Value->ski)), xmlSecBufferGetSize(&(x509Value->ski)),
            NULL /* unused */);
        if((storeCert == NULL) && (stopOnUnknownCert != 0)) {
            xmlSecOtherError2(XMLSEC_ERRORS_R_CERT_NOT_FOUND, xmlSecKeyDataGetName(data),
                "skiSize=" XMLSEC_SIZE_FMT, xmlSecBufferGetSize(&(x509Value->ski)));
            goto done;
        }
    } else if(x509Value->subject != NULL) {
        storeCert = xmlSecOpenSSLX509StoreFindCert_ex(x509Store, x509Value->subject,
            NULL, NULL, NULL, 0, NULL /* unused */);
        if((storeCert == NULL) && (stopOnUnknownCert != 0)) {
            xmlSecOtherError2(XMLSEC_ERRORS_R_CERT_NOT_FOUND, xmlSecKeyDataGetName(data),
                "subject=%s", xmlSecErrorsSafeString(x509Value->subject));
            goto done;
        }
    } else if((x509Value->issuerName != NULL) && (x509Value->issuerSerial != NULL)) {
        storeCert = xmlSecOpenSSLX509StoreFindCert_ex(x509Store, NULL,
            x509Value->issuerName, x509Value->issuerSerial,
            NULL, 0, NULL /* unused */);
        if((storeCert == NULL) && (stopOnUnknownCert != 0)) {
            xmlSecOtherError3(XMLSEC_ERRORS_R_CERT_NOT_FOUND, xmlSecKeyDataGetName(data),
                "issuerName=%s;issuerSerial=%s",
                xmlSecErrorsSafeString(x509Value->issuerName),
                xmlSecErrorsSafeString(x509Value->issuerSerial));
            goto done;
        }
    }

    /* if we found cert in a store, then duplicate it for key data */
    if((cert == NULL) && (storeCert != NULL)) {
        cert = X509_dup(storeCert);
        if(cert == NULL) {
            xmlSecOpenSSLError("X509_dup", xmlSecKeyDataGetName(data));
            goto done;
        }
    }

    /* if we found a cert or a crl, then add it to the data */
    if(cert != NULL) {
        ret = xmlSecOpenSSLKeyDataX509AdoptCert(data, cert);
        if(ret < 0) {
            xmlSecInternalError("xmlSecOpenSSLKeyDataX509AdoptCert", xmlSecKeyDataGetName(data));
            goto done;
        }
        cert = NULL; /* owned by data now */
    }
    if(crl != NULL) {
        ret = xmlSecOpenSSLKeyDataX509AdoptCrl(data, crl);
        if(ret < 0) {
            xmlSecInternalError("xmlSecOpenSSLKeyDataX509AdoptCrl", xmlSecKeyDataGetName(data));
            goto done;
        }
        crl = NULL; /* owned by data now */
    }

    /* success */
    res = 0;

done:
    /* cleanup */
    if(cert != NULL) {
        X509_free(cert);
    }
    if(crl != NULL) {
        X509_CRL_free(crl);
    }
    return(res);
}

static int
xmlSecOpenSSLX509CertDerWrite(X509* cert, xmlSecBufferPtr buf) {
    BIO *mem = NULL;
    xmlSecByte *data = NULL;
    xmlSecSize size;
    long len;
    int ret;
    int res = -1;

    xmlSecAssert2(cert != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);

    mem = xmlSecOpenSSLCreateMemBio();
    if(mem == NULL) {
        xmlSecInternalError("xmlSecOpenSSLCreateMemBio", NULL);
        goto done;
    }

    ret = i2d_X509_bio(mem, cert);
    if(ret != 1) {
        xmlSecOpenSSLError("i2d_X509_bio", NULL);
        goto done;
    }
    ret = BIO_flush(mem);
    if(ret != 1) {
        xmlSecOpenSSLError("BIO_flush", NULL);
        goto done;
    }

    len = BIO_get_mem_data(mem, &data);
    if((len <= 0) || (data == NULL)){
        xmlSecOpenSSLError("BIO_get_mem_data", NULL);
        goto done;
    }
    XMLSEC_SAFE_CAST_LONG_TO_SIZE(len, size, goto done, NULL);

    ret = xmlSecBufferSetData(buf, data, size);
    if(ret < 0) {
        xmlSecInternalError("xmlSecBufferSetData", NULL);
        goto done;
    }

    /* success */
    res = 0;

done:
    if(mem != NULL) {
        BIO_free_all(mem);
    }
    return(res);
}

static int
xmlSecOpenSSLX509CrlDerWrite(X509_CRL* crl, xmlSecBufferPtr buf) {
    BIO *mem = NULL;
    xmlSecByte *data = NULL;
    xmlSecSize size;
    long len;
    int ret;
    int res = -1;

    xmlSecAssert2(crl != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);

    mem = xmlSecOpenSSLCreateMemBio();
    if(mem == NULL) {
        xmlSecInternalError("xmlSecOpenSSLCreateMemBio", NULL);
        goto done;
    }

    ret = i2d_X509_CRL_bio(mem, crl);
    if(ret != 1) {
        xmlSecOpenSSLError("i2d_X509_CRL_bio", NULL);
        goto done;
    }
    ret = BIO_flush(mem);
    if(ret != 1) {
        xmlSecOpenSSLError("BIO_flush", NULL);
        goto done;
    }

    len = BIO_get_mem_data(mem, &data);
    if((len <= 0) || (data == NULL)){
        xmlSecOpenSSLError("BIO_get_mem_data", NULL);
        goto done;
    }
    XMLSEC_SAFE_CAST_LONG_TO_SIZE(len, size, goto done, NULL);

    ret = xmlSecBufferSetData(buf, data, size);
    if(ret < 0) {
        xmlSecInternalError2("xmlSecBufferSetData", NULL,
            "size=" XMLSEC_SIZE_FMT, size);
        goto done;
    }

    /* success */
    res = 0;

done:
    if(mem != NULL) {
        BIO_free_all(mem);
    }
    return(res);
}

static int
xmlSecOpenSSLX509SKIWrite(X509* cert, xmlSecBufferPtr buf) {
    X509_EXTENSION *ext;
    ASN1_OCTET_STRING *keyId = NULL;
    const xmlSecByte* keyIdData;
    int index, keyIdLen;
    xmlSecSize keyIdSize;
    int ret;
    int res = -1;

    xmlSecAssert2(cert != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);

    index = X509_get_ext_by_NID(cert, NID_subject_key_identifier, -1);
    if (index < 0) {
        xmlSecOpenSSLError("X509_get_ext_by_NID(): Certificate without SubjectKeyIdentifier extension", NULL);
        goto done;
    }

    ext = X509_get_ext(cert, index);
    if (ext == NULL) {
        xmlSecOpenSSLError("X509_get_ext", NULL);
        goto done;
    }

    keyId = (ASN1_OCTET_STRING *)X509V3_EXT_d2i(ext);
    if (keyId == NULL) {
        xmlSecOpenSSLError("X509V3_EXT_d2i", NULL);
        goto done;
    }

    keyIdData = ASN1_STRING_get0_data(keyId);
    if(keyIdData == NULL) {
        xmlSecOpenSSLError("ASN1_STRING_get0_data", NULL);
        goto done;
    }
    keyIdLen = ASN1_STRING_length(keyId);
    if(keyIdLen <= 0) {
        xmlSecOpenSSLError("ASN1_STRING_length", NULL);
        goto done;
    }
    XMLSEC_SAFE_CAST_INT_TO_SIZE(keyIdLen, keyIdSize, goto done, NULL);

    ret = xmlSecBufferSetData(buf, keyIdData, keyIdSize);
    if(ret < 0) {
        xmlSecInternalError2("xmlSecBufferSetData", NULL,
            "keyIdSize=" XMLSEC_SIZE_FMT, keyIdSize);
        goto done;
    }

    /* success */
    res = 0;


done:
    if(keyId != NULL) {
        ASN1_OCTET_STRING_free(keyId);
    }
    return(res);
}

static xmlChar*
xmlSecOpenSSLX509NameWrite(X509_NAME* nm) {
    xmlChar* res = NULL;
    BIO *mem = NULL;
    xmlChar* buf = NULL;
    xmlSecSize sizeBuf;
    int lenBuf, lenRead;
    int ret;

    xmlSecAssert2(nm != NULL, NULL);

    mem = xmlSecOpenSSLCreateMemBio();
    if(mem == NULL) {
        xmlSecInternalError("xmlSecOpenSSLCreateMemBio", NULL);
        goto done;
    }

    if (X509_NAME_print_ex(mem, nm, 0, XN_FLAG_RFC2253) <=0) {
        xmlSecOpenSSLError("X509_NAME_print_ex", NULL);
        goto done;
    }

    ret = BIO_flush(mem);
    if(ret != 1) {
        xmlSecOpenSSLError("BIO_flush", NULL);
        goto done;
    }

    lenBuf = BIO_pending(mem);
    if(lenBuf <= 0) {
        xmlSecOpenSSLError("BIO_pending", NULL);
        goto done;
    }
    XMLSEC_SAFE_CAST_INT_TO_SIZE(lenBuf, sizeBuf, goto done, NULL);

    buf = (xmlChar *)xmlMalloc(sizeBuf + 1);
    if(buf == NULL) {
        xmlSecMallocError(sizeBuf + 1, NULL);
        goto done;
    }
    memset(buf, 0, sizeBuf + 1);

    lenRead = BIO_read(mem, buf, lenBuf);
    if(lenRead != lenBuf) {
        xmlSecOpenSSLError("BIO_read", NULL);
        goto done;
    }

    /* success */
    buf[sizeBuf] = '\0';
    res = buf;
    buf = NULL;

done:
    if(buf != NULL) {
        xmlFree(buf);
    }
    if(mem != NULL) {
        BIO_free_all(mem);
    }
    return(res);
}

static xmlChar*
xmlSecOpenSSLASN1IntegerWrite(ASN1_INTEGER *asni) {
    xmlChar *res = NULL;
    BIGNUM *bn;
    char *p;

    xmlSecAssert2(asni != NULL, NULL);

    bn = ASN1_INTEGER_to_BN(asni, NULL);
    if(bn == NULL) {
        xmlSecOpenSSLError("ASN1_INTEGER_to_BN", NULL);
        return(NULL);
    }

    p = BN_bn2dec(bn);
    if (p == NULL) {
        xmlSecOpenSSLError("BN_bn2dec", NULL);
        BN_clear_free(bn);
        return(NULL);
    }
    BN_clear_free(bn);
    bn = NULL;

    /* OpenSSL and LibXML2 can have different memory callbacks, i.e.
       when data is allocated in OpenSSL should be freed with OpenSSL
       method, not with LibXML2 method.
     */
    res = xmlCharStrdup(p);
    if(res == NULL) {
        xmlSecStrdupError(BAD_CAST p, NULL);
        OPENSSL_free(p);
        return(NULL);
    }
    OPENSSL_free(p);
    p = NULL;
    return(res);
}

static int
xmlSecOpenSSLKeyDataX509Write(xmlSecKeyDataPtr data,  xmlSecKeyValueX509Ptr x509Value,
                              int content, void* context) {
    xmlSecOpenSSLKeyDataX509Context* ctx;
    int ret;

    xmlSecAssert2(data != NULL, -1);
    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), -1);
    xmlSecAssert2(x509Value != NULL, -1);
    xmlSecAssert2(context != NULL, -1);

    ctx = (xmlSecOpenSSLKeyDataX509Context*)context;
    if(ctx->crtPos < ctx->crtSize) {
        /* write cert */
        X509* cert = xmlSecOpenSSLKeyDataX509GetCert(data, ctx->crtPos);
        if(cert == NULL) {
            xmlSecInternalError2("xmlSecOpenSSLKeyDataX509GetCert",
                xmlSecKeyDataGetName(data),
                "pos=" XMLSEC_SIZE_FMT, ctx->crtPos);
            return(-1);
        }
        if((content & XMLSEC_X509DATA_CERTIFICATE_NODE) != 0) {
            ret = xmlSecOpenSSLX509CertDerWrite(cert, &(x509Value->cert));
            if(ret < 0) {
                xmlSecInternalError2("xmlSecOpenSSLX509CertDerWrite",
                    xmlSecKeyDataGetName(data),
                    "pos=" XMLSEC_SIZE_FMT, ctx->crtPos);
                return(-1);
            }
        }
        if((content & XMLSEC_X509DATA_SKI_NODE) != 0) {
            ret = xmlSecOpenSSLX509SKIWrite(cert, &(x509Value->ski));
            if(ret < 0) {
                xmlSecInternalError2("xmlSecOpenSSLX509SKIWrite",
                    xmlSecKeyDataGetName(data),
                    "pos=" XMLSEC_SIZE_FMT, ctx->crtPos);
                return(-1);
            }
        }
        if((content & XMLSEC_X509DATA_SUBJECTNAME_NODE) != 0) {
            xmlSecAssert2(x509Value->subject == NULL, -1);

            x509Value->subject = xmlSecOpenSSLX509NameWrite(X509_get_subject_name(cert));
            if(x509Value->subject == NULL) {
                xmlSecInternalError2("xmlSecOpenSSLX509NameWrite(X509_get_subject_name)",
                    xmlSecKeyDataGetName(data),
                    "pos=" XMLSEC_SIZE_FMT, ctx->crtPos);
                return(-1);
            }
        }
        if((content & XMLSEC_X509DATA_ISSUERSERIAL_NODE) != 0) {
            xmlSecAssert2(x509Value->issuerName == NULL, -1);
            xmlSecAssert2(x509Value->issuerSerial == NULL, -1);

            x509Value->issuerName = xmlSecOpenSSLX509NameWrite(X509_get_issuer_name(cert));
            if(x509Value->issuerName == NULL) {
                xmlSecInternalError2("xmlSecOpenSSLX509NameWrite(X509_get_issuer_name)",
                    xmlSecKeyDataGetName(data),
                    "pos=" XMLSEC_SIZE_FMT, ctx->crtPos);
                return(-1);
            }
            x509Value->issuerSerial = xmlSecOpenSSLASN1IntegerWrite(X509_get_serialNumber(cert));
            if(x509Value->issuerSerial == NULL) {
                xmlSecInternalError2("xmlSecOpenSSLASN1IntegerWrite(X509_get_serialNumber))",
                    xmlSecKeyDataGetName(data),
                    "pos=" XMLSEC_SIZE_FMT, ctx->crtPos);
                return(-1);
            }
        }
        ++ctx->crtPos;
    } else if(ctx->crlPos < ctx->crlSize) {
        /* write crl */
        X509_CRL* crl = xmlSecOpenSSLKeyDataX509GetCrl(data, ctx->crlPos);
        if(crl == NULL) {
            xmlSecInternalError2("xmlSecOpenSSLKeyDataX509GetCrl",
                xmlSecKeyDataGetName(data),
                "pos=" XMLSEC_SIZE_FMT, ctx->crlPos);
            return(-1);
        }

        if((content & XMLSEC_X509DATA_CRL_NODE) != 0) {
            ret = xmlSecOpenSSLX509CrlDerWrite(crl, &(x509Value->crl));
            if(ret < 0) {
                xmlSecInternalError2("xmlSecOpenSSLX509CrlDerWrite",
                    xmlSecKeyDataGetName(data),
                    "pos=" XMLSEC_SIZE_FMT, ctx->crlPos);
                return(-1);
            }
        }
        ++ctx->crlPos;
    } else {
        /* no more certs or crls */
        return(1);
    }

    /* success */
    return(0);
}


#ifdef HAVE_TIMEGM

/* easy case */
extern time_t timegm (struct tm *tm);

#elif !defined(XMLSEC_WINDOWS)

/* Absolutely not the best way but it's the only ANSI compatible way I know.
 * If you system has a native struct tm --> GMT time_t conversion function
 * (like timegm) use it instead.
 */
static time_t
my_timegm(struct tm *t) {
    time_t tl, tb;
    struct tm *tg;

    tl = mktime (t);
    if(tl == -1) {
        t->tm_hour--;
        tl = mktime (t);
        if (tl == -1) {
            return (-1);
        }
        tl += 3600;
    }
    tg = gmtime (&tl);
    tg->tm_isdst = 0;
    tb = mktime (tg);
    if (tb == -1) {
        tg->tm_hour--;
        tb = mktime (tg);
        if (tb == -1) {
            return (-1);
        }
        tb += 3600;
    }
    return (tl - (tb - tl));
}

#define timegm(tm) my_timegm(tm)

#elif defined(_MSC_VER)

/* Windows build with MSVC */
static time_t
my_timegm(struct tm *t) {
    long seconds = 0;
    if(_get_timezone(&seconds) != 0) {
        return(-1);
    }
    return (mktime(t) - seconds);
}
#define timegm(tm) my_timegm(tm)

#else  /* defined(_MSC_VER) */

/* Windows build with MinGW, Cygwin, etc */
#define timegm(tm)      (mktime(tm) - _timezone)

#endif /* HAVE_TIMEGM */

#if (defined(XMLSEC_OPENSSL_API_110) || defined(XMLSEC_OPENSSL_API_300)) && !defined(OPENSSL_IS_BORINGSSL)

static int
xmlSecOpenSSLX509CertGetTime(const ASN1_TIME * t, time_t* res) {
    struct tm tm;
    int ret;

    xmlSecAssert2(t != NULL, -1);
    xmlSecAssert2(res != NULL, -1);

    (*res) = 0;
    if(!ASN1_TIME_check(t)) {
        xmlSecOpenSSLError("ASN1_TIME_check", NULL);
        return(-1);
    }

    memset(&tm, 0, sizeof(tm));
    ret = ASN1_TIME_to_tm(t, &tm);
    if(ret != 1) {
        xmlSecOpenSSLError("ASN1_TIME_to_tm", NULL);
        return(-1);
    }

    (*res) = timegm(&tm);
    return(0);
}

#else  /* (defined(XMLSEC_OPENSSL_API_110) || defined(XMLSEC_OPENSSL_API_300)) && !defined(OPENSSL_IS_BORINGSSL) */

static int
xmlSecOpenSSLX509CertGetTime(ASN1_TIME * t, time_t* res) {
    struct tm tm;
    int offset;

    xmlSecAssert2(t != NULL, -1);
    xmlSecAssert2(res != NULL, -1);

    (*res) = 0;
    if(!ASN1_TIME_check(t)) {
        xmlSecOpenSSLError("ASN1_TIME_check", NULL);
        return(-1);
    }

    memset(&tm, 0, sizeof(tm));

#define g2(p) (((p)[0]-'0')*10+(p)[1]-'0')
    if(t->type == V_ASN1_UTCTIME) {
        xmlSecAssert2(t->length > 12, -1);

        /* this code is copied from OpenSSL asn1/a_utctm.c file */
        tm.tm_year = g2(t->data);
        if(tm.tm_year < 50) {
            tm.tm_year += 100;
        }
        tm.tm_mon  = g2(t->data + 2) - 1;
        tm.tm_mday = g2(t->data + 4);
        tm.tm_hour = g2(t->data + 6);
        tm.tm_min  = g2(t->data + 8);
        tm.tm_sec  = g2(t->data + 10);
        if(t->data[12] == 'Z') {
            offset = 0;
        } else {
            xmlSecAssert2(t->length > 16, -1);

            offset = g2(t->data + 13) * 60 + g2(t->data + 15);
            if(t->data[12] == '-') {
                offset = -offset;
            }
        }
        tm.tm_isdst = -1;
    } else {
        xmlSecAssert2(t->length > 14, -1);

        tm.tm_year = g2(t->data) * 100 + g2(t->data + 2);
        tm.tm_mon  = g2(t->data + 4) - 1;
        tm.tm_mday = g2(t->data + 6);
        tm.tm_hour = g2(t->data + 8);
        tm.tm_min  = g2(t->data + 10);
        tm.tm_sec  = g2(t->data + 12);
        if(t->data[14] == 'Z') {
            offset = 0;
        } else {
            xmlSecAssert2(t->length > 18, -1);

            offset = g2(t->data + 15) * 60 + g2(t->data + 17);
            if(t->data[14] == '-') {
                offset = -offset;
            }
        }
        tm.tm_isdst = -1;
    }
#undef g2
    (*res) = timegm(&tm) - offset * 60;
    return(0);
}

#endif /* (defined(XMLSEC_OPENSSL_API_110) || defined(XMLSEC_OPENSSL_API_300)) && !defined(OPENSSL_IS_BORINGSSL) */

static int
xmlSecOpenSSLKeyDataX509VerifyAndExtractKey(xmlSecKeyDataPtr data, xmlSecKeyPtr key,
                                    xmlSecKeyInfoCtxPtr keyInfoCtx) {
    xmlSecOpenSSLX509DataCtxPtr ctx;
    xmlSecKeyDataStorePtr x509Store;
    int ret;

    xmlSecAssert2(xmlSecKeyDataCheckId(data, xmlSecOpenSSLKeyDataX509Id), -1);
    xmlSecAssert2(key != NULL, -1);
    xmlSecAssert2(keyInfoCtx != NULL, -1);
    xmlSecAssert2(keyInfoCtx->keysMngr != NULL, -1);

    ctx = xmlSecOpenSSLX509DataGetCtx(data);
    xmlSecAssert2(ctx != NULL, -1);

    x509Store = xmlSecKeysMngrGetDataStore(keyInfoCtx->keysMngr, xmlSecOpenSSLX509StoreId);
    if(x509Store == NULL) {
        xmlSecInternalError("xmlSecKeysMngrGetDataStore",
                            xmlSecKeyDataGetName(data));
        return(-1);
    }

    if((ctx->keyCert == NULL) && (ctx->certsList != NULL) && (xmlSecKeyGetValue(key) == NULL)) {
        X509* cert;

        cert = xmlSecOpenSSLX509StoreVerify(x509Store, ctx->certsList, ctx->crlsList, keyInfoCtx);
        if(cert != NULL) {
            xmlSecKeyDataPtr keyValue;

            ctx->keyCert = X509_dup(cert);
            if(ctx->keyCert == NULL) {
                xmlSecOpenSSLError("X509_dup",
                                   xmlSecKeyDataGetName(data));
                return(-1);
            }

            keyValue = xmlSecOpenSSLX509CertGetKey(ctx->keyCert);
            if(keyValue == NULL) {
                xmlSecInternalError("xmlSecOpenSSLX509CertGetKey",
                                    xmlSecKeyDataGetName(data));
                return(-1);
            }

            /* verify that the key matches our expectations */
            if(xmlSecKeyReqMatchKeyValue(&(keyInfoCtx->keyReq), keyValue) != 1) {
                xmlSecInternalError("xmlSecKeyReqMatchKeyValue",
                                    xmlSecKeyDataGetName(data));
                xmlSecKeyDataDestroy(keyValue);
                return(-1);
            }

            ret = xmlSecKeySetValue(key, keyValue);
            if(ret < 0) {
                xmlSecInternalError("xmlSecKeySetValue",
                                    xmlSecKeyDataGetName(data));
                xmlSecKeyDataDestroy(keyValue);
                return(-1);
            }

            if((X509_get0_notBefore(ctx->keyCert) != NULL) && (X509_get0_notAfter(ctx->keyCert) != NULL)) {
                ret = xmlSecOpenSSLX509CertGetTime(X509_get0_notBefore(ctx->keyCert), &(key->notValidBefore));
                if(ret < 0) {
                    xmlSecInternalError("xmlSecOpenSSLX509CertGetTime(notAfter)",
                                        xmlSecKeyDataGetName(data));
                    return(-1);
                }
                ret = xmlSecOpenSSLX509CertGetTime(X509_get0_notAfter(ctx->keyCert), &(key->notValidAfter));
                if(ret < 0) {
                    xmlSecInternalError("xmlSecOpenSSLX509CertGetTime(notBefore)",
                                        xmlSecKeyDataGetName(data));
                    return(-1);
                }
            } else {
                key->notValidBefore = key->notValidAfter = 0;
            }
        } else if((keyInfoCtx->flags & XMLSEC_KEYINFO_FLAGS_X509DATA_STOP_ON_INVALID_CERT) != 0) {
            xmlSecOtherError(XMLSEC_ERRORS_R_CERT_NOT_FOUND, xmlSecKeyDataGetName(data), NULL);
            return(-1);
        }
    }
    return(0);
}

/**
 * xmlSecOpenSSLX509CertGetKey:
 * @cert:               the certificate.
 *
 * Extracts public key from the @cert.
 *
 * Returns: public key value or NULL if an error occurs.
 */
xmlSecKeyDataPtr
xmlSecOpenSSLX509CertGetKey(X509* cert) {
    xmlSecKeyDataPtr data;
    EVP_PKEY *pKey = NULL;

    xmlSecAssert2(cert != NULL, NULL);

    pKey = X509_get_pubkey(cert);
    if(pKey == NULL) {
        xmlSecOpenSSLError("X509_get_pubkey", NULL);
        return(NULL);
    }

    data = xmlSecOpenSSLEvpKeyAdopt(pKey);
    if(data == NULL) {
        xmlSecInternalError("xmlSecOpenSSLEvpKeyAdopt", NULL);
        EVP_PKEY_free(pKey);
        return(NULL);
    }

    return(data);
}

static X509*
xmlSecOpenSSLX509CertDerRead(const xmlSecByte* buf, xmlSecSize size) {
    X509 *cert = NULL;
    BIO *mem = NULL;
    X509 *tmpCert = NULL;

    xmlSecAssert2(buf != NULL, NULL);
    xmlSecAssert2(size > 0, NULL);

    mem = xmlSecOpenSSLCreateMemBufBio(buf, size);
    if(mem == NULL) {
        xmlSecInternalError2("xmlSecOpenSSLCreateMemBufBio", NULL,
                             "size=" XMLSEC_SIZE_FMT, size);
        goto done;
    }

    tmpCert = X509_new_ex(xmlSecOpenSSLGetLibCtx(), NULL);
    if(tmpCert == NULL) {
        xmlSecOpenSSLError("X509_new_ex", NULL);
        goto done;
    }
    cert = d2i_X509_bio(mem, &tmpCert);
    if(cert == NULL) {
        xmlSecOpenSSLError2("d2i_X509_bio", NULL,
                            "size=" XMLSEC_SIZE_FMT, size);
        goto done;
    }

    /* sucess: tmpCert is now cert */
    tmpCert = NULL;

done:
    /*  cleanup */
    if(tmpCert != NULL) {
        X509_free(tmpCert);
    }
    if(mem != NULL) {
        BIO_free_all(mem);
    }
    return(cert);
}

static X509_CRL*
xmlSecOpenSSLX509CrlDerRead(xmlSecByte* buf, xmlSecSize size) {
    X509_CRL *tmpCrl = NULL;
    X509_CRL *crl = NULL;
    BIO *mem = NULL;

    xmlSecAssert2(buf != NULL, NULL);
    xmlSecAssert2(size > 0, NULL);

    mem = xmlSecOpenSSLCreateMemBufBio(buf, size);
    if(mem == NULL) {
        xmlSecInternalError2("xmlSecOpenSSLCreateMemBufBio", NULL,
                             "size=" XMLSEC_SIZE_FMT, size);
        goto done;
    }

    tmpCrl = X509_CRL_new_ex(xmlSecOpenSSLGetLibCtx(), NULL);
    if(tmpCrl == NULL) {
        xmlSecOpenSSLError("X509_CRL_new_ex", NULL);
        goto done;
    }

    crl = d2i_X509_CRL_bio(mem, &tmpCrl);
    if(crl == NULL) {
        xmlSecOpenSSLError("d2i_X509_CRL_bio", NULL);
        goto done;
    }

    /* success, tmpCrl is now crl */
    tmpCrl = NULL;

done:
    /* cleanup */
    if(tmpCrl != NULL) {
        X509_CRL_free(tmpCrl);
    }
    if(mem != NULL) {
        BIO_free_all(mem);
    }
    return(crl);
}

static void
xmlSecOpenSSLX509CertDebugDump(X509* cert, FILE* output) {
    char buf[1024];
    BIGNUM *bn = NULL;

    xmlSecAssert(cert != NULL);
    xmlSecAssert(output != NULL);

    fprintf(output, "==== Subject Name: %s\n",
        X509_NAME_oneline(X509_get_subject_name(cert), buf, sizeof(buf)));
    fprintf(output, "==== Issuer Name: %s\n",
        X509_NAME_oneline(X509_get_issuer_name(cert), buf, sizeof(buf)));
    fprintf(output, "==== Issuer Serial: ");
    bn = ASN1_INTEGER_to_BN(X509_get_serialNumber(cert),NULL);
    if(bn != NULL) {
        BN_print_fp(output, bn);
        BN_clear_free(bn);
        fprintf(output, "\n");
    } else {
        fprintf(output, "unknown\n");
    }
}


static void
xmlSecOpenSSLX509CertDebugXmlDump(X509* cert, FILE* output) {
    char buf[1024];
    BIGNUM *bn = NULL;

    xmlSecAssert(cert != NULL);
    xmlSecAssert(output != NULL);

    fprintf(output, "<SubjectName>");
    xmlSecPrintXmlString(output,
        BAD_CAST X509_NAME_oneline(X509_get_subject_name(cert), buf, sizeof(buf))
    );
    fprintf(output, "</SubjectName>\n");


    fprintf(output, "<IssuerName>");
    xmlSecPrintXmlString(output,
        BAD_CAST X509_NAME_oneline(X509_get_issuer_name(cert), buf, sizeof(buf)));
    fprintf(output, "</IssuerName>\n");

    fprintf(output, "<SerialNumber>");
    bn = ASN1_INTEGER_to_BN(X509_get_serialNumber(cert),NULL);
    if(bn != NULL) {
        BN_print_fp(output, bn);
        BN_clear_free(bn);
    }
    fprintf(output, "</SerialNumber>\n");
}


/**************************************************************************
 *
 * Raw X509 Certificate processing
 *
 *
 *************************************************************************/
static int              xmlSecOpenSSLKeyDataRawX509CertBinRead  (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 const xmlSecByte* buf,
                                                                 xmlSecSize bufSize,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);

static xmlSecKeyDataKlass xmlSecOpenSSLKeyDataRawX509CertKlass = {
    sizeof(xmlSecKeyDataKlass),
    sizeof(xmlSecKeyData),

    /* data */
    xmlSecNameRawX509Cert,
    xmlSecKeyDataUsageRetrievalMethodNodeBin,
                                                /* xmlSecKeyDataUsage usage; */
    xmlSecHrefRawX509Cert,                      /* const xmlChar* href; */
    NULL,                                       /* const xmlChar* dataNodeName; */
    xmlSecDSigNs,                               /* const xmlChar* dataNodeNs; */

    /* constructors/destructor */
    NULL,                                       /* xmlSecKeyDataInitializeMethod initialize; */
    NULL,                                       /* xmlSecKeyDataDuplicateMethod duplicate; */
    NULL,                                       /* xmlSecKeyDataFinalizeMethod finalize; */
    NULL,                                       /* xmlSecKeyDataGenerateMethod generate; */

    /* get info */
    NULL,                                       /* xmlSecKeyDataGetTypeMethod getType; */
    NULL,                                       /* xmlSecKeyDataGetSizeMethod getSize; */
    NULL,                                       /* xmlSecKeyDataGetIdentifier getIdentifier; */

    /* read/write */
    NULL,                                       /* xmlSecKeyDataXmlReadMethod xmlRead; */
    NULL,                                       /* xmlSecKeyDataXmlWriteMethod xmlWrite; */
    xmlSecOpenSSLKeyDataRawX509CertBinRead,     /* xmlSecKeyDataBinReadMethod binRead; */
    NULL,                                       /* xmlSecKeyDataBinWriteMethod binWrite; */

    /* debug */
    NULL,                                       /* xmlSecKeyDataDebugDumpMethod debugDump; */
    NULL,                                       /* xmlSecKeyDataDebugDumpMethod debugXmlDump; */

    /* reserved for the future */
    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecOpenSSLKeyDataRawX509CertGetKlass:
 *
 * The raw X509 certificates key data klass.
 *
 * Returns: raw X509 certificates key data klass.
 */
xmlSecKeyDataId
xmlSecOpenSSLKeyDataRawX509CertGetKlass(void) {
    return(&xmlSecOpenSSLKeyDataRawX509CertKlass);
}

static int
xmlSecOpenSSLKeyDataRawX509CertBinRead(xmlSecKeyDataId id, xmlSecKeyPtr key,
                                    const xmlSecByte* buf, xmlSecSize bufSize,
                                    xmlSecKeyInfoCtxPtr keyInfoCtx) {
    xmlSecKeyDataPtr data;
    X509* cert;
    int ret;

    xmlSecAssert2(id == xmlSecOpenSSLKeyDataRawX509CertId, -1);
    xmlSecAssert2(key != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);
    xmlSecAssert2(bufSize > 0, -1);
    xmlSecAssert2(keyInfoCtx != NULL, -1);

    cert = xmlSecOpenSSLX509CertDerRead(buf, bufSize);
    if(cert == NULL) {
        xmlSecInternalError("xmlSecOpenSSLX509CertDerRead", NULL);
        return(-1);
    }

    data = xmlSecKeyEnsureData(key, xmlSecOpenSSLKeyDataX509Id);
    if(data == NULL) {
        xmlSecInternalError("xmlSecKeyEnsureData",
                            xmlSecKeyDataKlassGetName(id));
        X509_free(cert);
        return(-1);
    }

    ret = xmlSecOpenSSLKeyDataX509AdoptCert(data, cert);
    if(ret < 0) {
        xmlSecInternalError("xmlSecOpenSSLKeyDataX509AdoptCert",
                            xmlSecKeyDataKlassGetName(id));
        X509_free(cert);
        return(-1);
    }

    ret = xmlSecOpenSSLKeyDataX509VerifyAndExtractKey(data, key, keyInfoCtx);
    if(ret < 0) {
        xmlSecInternalError("xmlSecOpenSSLKeyDataX509VerifyAndExtractKey",
                            xmlSecKeyDataKlassGetName(id));
        return(-1);
    }
    return(0);
}

#endif /* XMLSEC_NO_X509 */
