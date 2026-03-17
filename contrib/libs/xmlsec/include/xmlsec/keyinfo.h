/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * <dsig:KeyInfo> element processing
 * (http://www.w3.org/TR/xmlSec-core/#sec-KeyInfo:
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_KEYINFO_H__
#define __XMLSEC_KEYINFO_H__

#include <time.h>

#include <libxml/tree.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/list.h>
#include <xmlsec/keysdata.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/****************************************************************************
 *
 * High-level functions
 *
 ****************************************************************************/
XMLSEC_EXPORT int               xmlSecKeyInfoNodeRead           (xmlNodePtr keyInfoNode,
                                                                 xmlSecKeyPtr key,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
XMLSEC_EXPORT int               xmlSecKeyInfoNodeWrite          (xmlNodePtr keyInfoNode,
                                                                 xmlSecKeyPtr key,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);

/**
 * xmlSecKeyInfoMode:
 * @xmlSecKeyInfoModeRead: read <dsig:KeyInfo /> element.
 * @xmlSecKeyInfoModeWrite: write <dsig:KeyInfo /> element.
 *
 * The @xmlSecKeyInfoCtx operation mode (read or write).
 */
typedef enum {
    xmlSecKeyInfoModeRead = 0,
    xmlSecKeyInfoModeWrite
} xmlSecKeyInfoMode;

/**
 * XMLSEC_KEYINFO_FLAGS_DONT_STOP_ON_KEY_FOUND:
 *
 * If flag is set then we will continue reading <dsig:KeyInfo />
 * element even when key is already found.
 */
#define XMLSEC_KEYINFO_FLAGS_DONT_STOP_ON_KEY_FOUND             0x00000001

/**
 * XMLSEC_KEYINFO_FLAGS_STOP_ON_UNKNOWN_CHILD:
 *
 * If flag is set then we abort if an unknown <dsig:KeyInfo />
 * child is found.
 */
#define XMLSEC_KEYINFO_FLAGS_STOP_ON_UNKNOWN_CHILD              0x00000002

/**
 * XMLSEC_KEYINFO_FLAGS_KEYNAME_STOP_ON_UNKNOWN:
 *
 * If flags is set then we abort if an unknown key name
 * (content of <dsig:KeyName /> element) is found.
 */
#define XMLSEC_KEYINFO_FLAGS_KEYNAME_STOP_ON_UNKNOWN            0x00000004

/**
 * XMLSEC_KEYINFO_FLAGS_KEYVALUE_STOP_ON_UNKNOWN_CHILD:
 *
 * If flags is set then we abort if an unknown <dsig:KeyValue />
 * child is found.
 */
#define XMLSEC_KEYINFO_FLAGS_KEYVALUE_STOP_ON_UNKNOWN_CHILD     0x00000008

/**
 * XMLSEC_KEYINFO_FLAGS_RETRMETHOD_STOP_ON_UNKNOWN_HREF:
 *
 * If flag is set then we abort if an unknown href attribute
 * of <dsig:RetrievalMethod /> element is found.
 */
#define XMLSEC_KEYINFO_FLAGS_RETRMETHOD_STOP_ON_UNKNOWN_HREF    0x00000010

/**
 * XMLSEC_KEYINFO_FLAGS_RETRMETHOD_STOP_ON_MISMATCH_HREF:
 *
 * If flag is set then we abort if an href attribute <dsig:RetrievalMethod />
 * element does not match the real key data type.
 */
#define XMLSEC_KEYINFO_FLAGS_RETRMETHOD_STOP_ON_MISMATCH_HREF   0x00000020

/**
 * XMLSEC_KEYINFO_FLAGS_X509DATA_STOP_ON_UNKNOWN_CHILD:
 *
 * If flags is set then we abort if an unknown <dsig:X509Data />
 * child is found.
 */
#define XMLSEC_KEYINFO_FLAGS_X509DATA_STOP_ON_UNKNOWN_CHILD     0x00000100

/**
 * XMLSEC_KEYINFO_FLAGS_X509DATA_DONT_VERIFY_CERTS:
 *
 * If flag is set then we'll load certificates from <dsig:X509Data />
 * element without verification.
 */
#define XMLSEC_KEYINFO_FLAGS_X509DATA_DONT_VERIFY_CERTS         0x00000200

/**
 * XMLSEC_KEYINFO_FLAGS_X509DATA_STOP_ON_UNKNOWN_CERT:
 *
 * If flag is set then we'll stop when we could not resolve reference
 * to certificate from <dsig:X509IssuerSerial />, <dsig:X509SKI /> or
 * <dsig:X509SubjectName /> elements.
 */
#define XMLSEC_KEYINFO_FLAGS_X509DATA_STOP_ON_UNKNOWN_CERT      0x00000400

/**
 * XMLSEC_KEYINFO_FLAGS_X509DATA_STOP_ON_INVALID_CERT:
 *
 * If the flag is set then we'll stop when <dsig:X509Data /> element
 * processing does not return a verified certificate.
 */
#define XMLSEC_KEYINFO_FLAGS_X509DATA_STOP_ON_INVALID_CERT      0x00000800

/**
 * XMLSEC_KEYINFO_FLAGS_ENCKEY_DONT_STOP_ON_FAILED_DECRYPTION:
 *
 * If the flag is set then we'll stop when <enc:EncryptedKey /> element
 * processing fails.
 */
#define XMLSEC_KEYINFO_FLAGS_ENCKEY_DONT_STOP_ON_FAILED_DECRYPTION 0x00001000

/**
 * XMLSEC_KEYINFO_FLAGS_STOP_ON_EMPTY_NODE:
 *
 * If the flag is set then we'll stop when we found an empty node.
 * Otherwise we just ignore it.
 */
#define XMLSEC_KEYINFO_FLAGS_STOP_ON_EMPTY_NODE                 0x00002000

/**
 * XMLSEC_KEYINFO_FLAGS_X509DATA_SKIP_STRICT_CHECKS:
 *
 * If the flag is set then we'll skip strict checking of certs and CRLs
 */
#define XMLSEC_KEYINFO_FLAGS_X509DATA_SKIP_STRICT_CHECKS        0x00004000

/**
 * xmlSecKeyInfoCtx:
 * @userData:           the pointer to user data (xmlsec and xmlsec-crypto
 *                      never touch this).
 * @flags:              the bit mask for flags that control processin.
 * @flags2:             reserved for future.
 * @mode:               do we read or write <dsig:KeyInfo /> element.
 * @keysMngr:           the pointer to current keys manager.
 * @enabledKeyData:     the list of enabled @xmlSecKeyDataId (if list is
 *                      empty then all data ids are enabled).
 * @base64LineSize:     the max columns size for base64 encoding.
 * @retrievalMethodCtx: the transforms context for <dsig:RetrievalMethod />
 *                      element processing.
 * @maxRetrievalMethodLevel: the max recursion level when processing
 *                      <dsig:RetrievalMethod /> element; default level is 1
 *                      (see also @curRetrievalMethodLevel).
 * @encCtx:             the encryption context for <dsig:EncryptedKey /> element
 *                      processing.
 * @maxEncryptedKeyLevel: the max recursion level when processing
 *                      <enc:EncryptedKey /> element; default level is 1
 *                      (see @curEncryptedKeyLevel).
 * @certsVerificationTime: the time to use for X509 certificates verification
 *                      ("not valid before" and "not valid after" checks);
 *                      if @certsVerificationTime is equal to 0 (default)
 *                      then we verify certificates against the system's
 *                      clock "now".
 * @certsVerificationDepth: the max certifications chain length (default is 9).
 * @pgpReserved:        reserved for PGP.
 * @curRetrievalMethodLevel: the current <dsig:RetrievalMethod /> element
 *                      processing level (see @maxRetrievalMethodLevel).
 * @curEncryptedKeyLevel: the current <enc:EncryptedKey /> element
 *                      processing level (see @maxEncryptedKeyLevel).
 * @keyReq:             the current key requirements.
 * @reserved0:          reserved for the future.
 * @reserved1:          reserved for the future.
 *
 * The <dsig:KeyInfo /> reading or writing context.
 */
struct _xmlSecKeyInfoCtx {
    void*                               userData;
    unsigned int                        flags;
    unsigned int                        flags2;
    xmlSecKeysMngrPtr                   keysMngr;
    xmlSecKeyInfoMode                   mode;
    xmlSecPtrList                       enabledKeyData;
    int                                 base64LineSize;

    /* RetrievalMethod */
    xmlSecTransformCtx                  retrievalMethodCtx;
    int                                 maxRetrievalMethodLevel;

#ifndef XMLSEC_NO_XMLENC
    /* EncryptedKey */
    xmlSecEncCtxPtr                     encCtx;
    int                                 maxEncryptedKeyLevel;
#endif /* XMLSEC_NO_XMLENC */

#ifndef XMLSEC_NO_X509
    /* x509 certificates */
    time_t                              certsVerificationTime;
    int                                 certsVerificationDepth;
#endif /* XMLSEC_NO_X509 */

    /* PGP */
    void*                               pgpReserved;    /* TODO */

    /* internal data */
    int                                 curRetrievalMethodLevel;
    int                                 curEncryptedKeyLevel;
    xmlSecKeyReq                        keyReq;

    /* for the future */
    void*                               reserved0;
    void*                               reserved1;
};

XMLSEC_EXPORT xmlSecKeyInfoCtxPtr       xmlSecKeyInfoCtxCreate          (xmlSecKeysMngrPtr keysMngr);
XMLSEC_EXPORT void                      xmlSecKeyInfoCtxDestroy         (xmlSecKeyInfoCtxPtr keyInfoCtx);
XMLSEC_EXPORT int                       xmlSecKeyInfoCtxInitialize      (xmlSecKeyInfoCtxPtr keyInfoCtx,
                                                                         xmlSecKeysMngrPtr keysMngr);
XMLSEC_EXPORT void                      xmlSecKeyInfoCtxFinalize        (xmlSecKeyInfoCtxPtr keyInfoCtx);
XMLSEC_EXPORT void                      xmlSecKeyInfoCtxReset           (xmlSecKeyInfoCtxPtr keyInfoCtx);
XMLSEC_EXPORT int                       xmlSecKeyInfoCtxCopyUserPref    (xmlSecKeyInfoCtxPtr dst,
                                                                         xmlSecKeyInfoCtxPtr src);
XMLSEC_EXPORT int                       xmlSecKeyInfoCtxCreateEncCtx    (xmlSecKeyInfoCtxPtr keyInfoCtx);
XMLSEC_EXPORT void                      xmlSecKeyInfoCtxDebugDump       (xmlSecKeyInfoCtxPtr keyInfoCtx,
                                                                         FILE* output);
XMLSEC_EXPORT void                      xmlSecKeyInfoCtxDebugXmlDump    (xmlSecKeyInfoCtxPtr keyInfoCtx,
                                                                         FILE* output);
/**
 * xmlSecKeyDataNameId
 *
 * The <dsig:KeyName> processing class.
 */
#define xmlSecKeyDataNameId             xmlSecKeyDataNameGetKlass()
XMLSEC_EXPORT xmlSecKeyDataId           xmlSecKeyDataNameGetKlass       (void);

/**
 * xmlSecKeyDataValueId
 *
 * The <dsig:KeyValue> processing class.
 */
#define xmlSecKeyDataValueId            xmlSecKeyDataValueGetKlass()
XMLSEC_EXPORT xmlSecKeyDataId           xmlSecKeyDataValueGetKlass      (void);

/**
 * xmlSecKeyDataRetrievalMethodId
 *
 * The <dsig:RetrievalMethod> processing class.
 */
#define xmlSecKeyDataRetrievalMethodId  xmlSecKeyDataRetrievalMethodGetKlass()
XMLSEC_EXPORT xmlSecKeyDataId           xmlSecKeyDataRetrievalMethodGetKlass(void);

#ifndef XMLSEC_NO_XMLENC
/**
 * xmlSecKeyDataEncryptedKeyId
 *
 * The <enc:EncryptedKey> processing class.
 */
#define xmlSecKeyDataEncryptedKeyId     xmlSecKeyDataEncryptedKeyGetKlass()
XMLSEC_EXPORT xmlSecKeyDataId           xmlSecKeyDataEncryptedKeyGetKlass(void);
#endif /* XMLSEC_NO_XMLENC */

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_KEYINFO_H__ */

