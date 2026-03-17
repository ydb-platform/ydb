/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Keys.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_KEYS_H__
#define __XMLSEC_KEYS_H__

#include <time.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/list.h>
#include <xmlsec/keysdata.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * xmlSecKeyUsage:
 *
 * The key usage.
 */
typedef unsigned int                    xmlSecKeyUsage;

/**
 * xmlSecKeyUsageSign:
 *
 * Key for signing.
 */
#define xmlSecKeyUsageSign              0x00000001

/**
 * xmlSecKeyUsageVerify:
 *
 * Key for signature verification.
 */
#define xmlSecKeyUsageVerify            0x00000002

/**
 * xmlSecKeyUsageEncrypt:
 *
 * An encryption key.
 */
#define xmlSecKeyUsageEncrypt           0x00000004

/**
 * xmlSecKeyUsageDecrypt:
 *
 * A decryption key.
 */
#define xmlSecKeyUsageDecrypt           0x00000008

/**
 * xmlSecKeyUsageKeyExchange:
 *
 * The key is used for key exchange.
 */
#define xmlSecKeyUsageKeyExchange       0x00000010

/**
 * xmlSecKeyUsageAny:
 *
 * Key can be used in any way.
 */
#define xmlSecKeyUsageAny               0xFFFFFFFF


/**************************************************************************
 *
 * xmlSecKeyUseWith
 *
 *************************************************************************/
typedef struct _xmlSecKeyUseWith                xmlSecKeyUseWith, *xmlSecKeyUseWithPtr;
XMLSEC_EXPORT int       xmlSecKeyUseWithInitialize              (xmlSecKeyUseWithPtr keyUseWith);
XMLSEC_EXPORT void      xmlSecKeyUseWithFinalize                (xmlSecKeyUseWithPtr keyUseWith);
XMLSEC_EXPORT void      xmlSecKeyUseWithReset                   (xmlSecKeyUseWithPtr keyUseWith);
XMLSEC_EXPORT int       xmlSecKeyUseWithCopy                    (xmlSecKeyUseWithPtr dst,
                                                                 xmlSecKeyUseWithPtr src);
XMLSEC_EXPORT xmlSecKeyUseWithPtr xmlSecKeyUseWithCreate        (const xmlChar* application,
                                                                 const xmlChar* identifier);
XMLSEC_EXPORT xmlSecKeyUseWithPtr xmlSecKeyUseWithDuplicate     (xmlSecKeyUseWithPtr keyUseWith);
XMLSEC_EXPORT void      xmlSecKeyUseWithDestroy                 (xmlSecKeyUseWithPtr keyUseWith);
XMLSEC_EXPORT int       xmlSecKeyUseWithSet                     (xmlSecKeyUseWithPtr keyUseWith,
                                                                 const xmlChar* application,
                                                                 const xmlChar* identifier);
XMLSEC_EXPORT void      xmlSecKeyUseWithDebugDump               (xmlSecKeyUseWithPtr keyUseWith,
                                                                 FILE* output);
XMLSEC_EXPORT void      xmlSecKeyUseWithDebugXmlDump            (xmlSecKeyUseWithPtr keyUseWith,
                                                                 FILE* output);

/**
 * xmlSecKeyUseWith:
 * @application:        the application.
 * @identifier:         the identifier.
 * @reserved1:          reserved for future use.
 * @reserved2:          reserved for future use.
 *
 * Information about application and user of the key.
 */
struct _xmlSecKeyUseWith {
    xmlChar*                    application;
    xmlChar*                    identifier;

    void*                       reserved1;
    void*                       reserved2;
};

/**
 * xmlSecKeyUseWithPtrListId:
 *
 * The keys list klass.
 */
#define xmlSecKeyUseWithPtrListId       xmlSecKeyUseWithPtrListGetKlass()
XMLSEC_EXPORT xmlSecPtrListId   xmlSecKeyUseWithPtrListGetKlass (void);

/**************************************************************************
 *
 * xmlSecKeyReq - what key are we looking for?
 *
 *************************************************************************/
typedef struct _xmlSecKeyReq                    xmlSecKeyReq, *xmlSecKeyReqPtr;

/**
 * xmlSecKeyReq:
 * @keyId:              the desired key value klass.
 * @keyType:            the desired key type.
 * @keyUsage:           the desired key usage.
 * @keyBitsSize:        the desired key size (in bits!).
 * @keyUseWithList:     the desired key use with application/identifier information.
 * @reserved1:          reserved for future use.
 * @reserved2:          reserved for future use.
 *
 * The key requirements information.
 */
struct _xmlSecKeyReq {
    xmlSecKeyDataId             keyId;
    xmlSecKeyDataType           keyType;
    xmlSecKeyUsage              keyUsage;
    xmlSecSize                  keyBitsSize;
    xmlSecPtrList               keyUseWithList;

    void*                       reserved1;
    void*                       reserved2;
};

XMLSEC_EXPORT int       xmlSecKeyReqInitialize                  (xmlSecKeyReqPtr keyReq);
XMLSEC_EXPORT void      xmlSecKeyReqFinalize                    (xmlSecKeyReqPtr keyReq);
XMLSEC_EXPORT void      xmlSecKeyReqReset                       (xmlSecKeyReqPtr keyReq);
XMLSEC_EXPORT int       xmlSecKeyReqCopy                        (xmlSecKeyReqPtr dst,
                                                                 xmlSecKeyReqPtr src);
XMLSEC_EXPORT int       xmlSecKeyReqMatchKey                    (xmlSecKeyReqPtr keyReq,
                                                                 xmlSecKeyPtr key);
XMLSEC_EXPORT int       xmlSecKeyReqMatchKeyValue               (xmlSecKeyReqPtr keyReq,
                                                                 xmlSecKeyDataPtr value);
XMLSEC_EXPORT void      xmlSecKeyReqDebugDump                   (xmlSecKeyReqPtr keyReq,
                                                                 FILE* output);
XMLSEC_EXPORT void      xmlSecKeyReqDebugXmlDump                (xmlSecKeyReqPtr keyReq,
                                                                 FILE* output);

/**
 * xmlSecKey:
 * @name:               the key name.
 * @value:              the key value.
 * @dataList:           the key data list.
 * @usage:              the key usage.
 * @notValidBefore:     the start key validity interval.
 * @notValidAfter:      the end key validity interval.
 *
 * The key.
 */
struct _xmlSecKey {
    xmlChar*                            name;
    xmlSecKeyDataPtr                    value;
    xmlSecPtrListPtr                    dataList;
    xmlSecKeyUsage                      usage;
    time_t                              notValidBefore;
    time_t                              notValidAfter;
};

XMLSEC_EXPORT xmlSecKeyPtr      xmlSecKeyCreate         (void);
XMLSEC_EXPORT void              xmlSecKeyDestroy        (xmlSecKeyPtr key);
XMLSEC_EXPORT void              xmlSecKeyEmpty          (xmlSecKeyPtr key);
XMLSEC_EXPORT xmlSecKeyPtr      xmlSecKeyDuplicate      (xmlSecKeyPtr key);
XMLSEC_EXPORT int               xmlSecKeyCopy           (xmlSecKeyPtr keyDst,
                                                         xmlSecKeyPtr keySrc);

XMLSEC_EXPORT const xmlChar*    xmlSecKeyGetName        (xmlSecKeyPtr key);
XMLSEC_EXPORT int               xmlSecKeySetName        (xmlSecKeyPtr key,
                                                         const xmlChar* name);

XMLSEC_EXPORT xmlSecKeyDataType xmlSecKeyGetType        (xmlSecKeyPtr key);

XMLSEC_EXPORT xmlSecKeyDataPtr  xmlSecKeyGetValue       (xmlSecKeyPtr key);
XMLSEC_EXPORT int               xmlSecKeySetValue       (xmlSecKeyPtr key,
                                                         xmlSecKeyDataPtr value);

XMLSEC_EXPORT xmlSecKeyDataPtr  xmlSecKeyGetData        (xmlSecKeyPtr key,
                                                         xmlSecKeyDataId dataId);
XMLSEC_EXPORT xmlSecKeyDataPtr  xmlSecKeyEnsureData     (xmlSecKeyPtr key,
                                                         xmlSecKeyDataId dataId);
XMLSEC_EXPORT int               xmlSecKeyAdoptData      (xmlSecKeyPtr key,
                                                         xmlSecKeyDataPtr data);

XMLSEC_EXPORT void              xmlSecKeyDebugDump      (xmlSecKeyPtr key,
                                                         FILE *output);
XMLSEC_EXPORT void              xmlSecKeyDebugXmlDump   (xmlSecKeyPtr key,
                                                         FILE *output);
XMLSEC_EXPORT xmlSecKeyPtr      xmlSecKeyGenerate       (xmlSecKeyDataId dataId,
                                                         xmlSecSize sizeBits,
                                                         xmlSecKeyDataType type);
XMLSEC_EXPORT xmlSecKeyPtr      xmlSecKeyGenerateByName (const xmlChar* name,
                                                         xmlSecSize sizeBits,
                                                         xmlSecKeyDataType type);


XMLSEC_EXPORT int               xmlSecKeyMatch          (xmlSecKeyPtr key,
                                                         const xmlChar *name,
                                                         xmlSecKeyReqPtr keyReq);

XMLSEC_EXPORT xmlSecKeyPtr      xmlSecKeyReadBuffer     (xmlSecKeyDataId dataId,
                                                         xmlSecBuffer* buffer);
XMLSEC_EXPORT xmlSecKeyPtr      xmlSecKeyReadBinaryFile (xmlSecKeyDataId dataId,
                                                         const char* filename);
XMLSEC_EXPORT xmlSecKeyPtr      xmlSecKeyReadMemory     (xmlSecKeyDataId dataId,
                                                         const xmlSecByte* data,
                                                         xmlSecSize dataSize);


/**
 * xmlSecKeyIsValid:
 * @key: the pointer to key.
 *
 * Macro. Returns 1 if @key is not NULL and @key->id is not NULL
 * or 0 otherwise.
 */
#define xmlSecKeyIsValid(key) \
        ((( key ) != NULL) && \
         (( key )->value != NULL) && \
         ((( key )->value->id) != NULL))
/**
 * xmlSecKeyCheckId:
 * @key: the pointer to key.
 * @keyId: the key Id.
 *
 * Macro. Returns 1 if @key is valid and @key's id is equal to @keyId.
 */
#define xmlSecKeyCheckId(key, keyId) \
        (xmlSecKeyIsValid(( key )) && \
        ((( key )->value->id) == ( keyId )))


/***********************************************************************
 *
 * Keys list
 *
 **********************************************************************/
/**
 * xmlSecKeyPtrListId:
 *
 * The keys list klass.
 */
#define xmlSecKeyPtrListId      xmlSecKeyPtrListGetKlass()
XMLSEC_EXPORT xmlSecPtrListId   xmlSecKeyPtrListGetKlass                (void);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_KEYS_H__ */

