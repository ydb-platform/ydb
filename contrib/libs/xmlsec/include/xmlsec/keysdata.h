/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Key data.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_KEYSDATA_H__
#define __XMLSEC_KEYSDATA_H__

#include <libxml/tree.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/buffer.h>
#include <xmlsec/list.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/****************************************************************************
 *
 * Forward declarations
 *
 ****************************************************************************/
typedef const struct _xmlSecKeyDataKlass                xmlSecKeyDataKlass,
                                                        *xmlSecKeyDataId;
typedef const struct _xmlSecKeyDataStoreKlass           xmlSecKeyDataStoreKlass,
                                                        *xmlSecKeyDataStoreId;
typedef struct _xmlSecKeyDataList                       xmlSecKeyDataList,
                                                        *xmlSecKeyDataListPtr;


/**************************************************************************
 *
 * xmlSecKeyDataUsage
 *
 *************************************************************************/
/**
 * xmlSecKeyDataUsage:
 *
 * The bits mask that determines possible keys data usage.
 */
typedef unsigned int                                    xmlSecKeyDataUsage;

/**
 * xmlSecKeyDataUsageUnknown:
 *
 * The key data usage is unknown.
 */
#define xmlSecKeyDataUsageUnknown                       0x00000

/**
 * xmlSecKeyDataUsageKeyInfoNodeRead:
 *
 * The key data could be read from a <dsig:KeyInfo/> child.
 */
#define xmlSecKeyDataUsageKeyInfoNodeRead               0x00001

/**
 * xmlSecKeyDataUsageKeyInfoNodeWrite:
 *
 * The key data could be written to a <dsig:KeyInfo /> child.
 */
#define xmlSecKeyDataUsageKeyInfoNodeWrite              0x00002

/**
 * xmlSecKeyDataUsageKeyValueNodeRead:
 *
 * The key data could be read from a <dsig:KeyValue /> child.
 */
#define xmlSecKeyDataUsageKeyValueNodeRead              0x00004

/**
 * xmlSecKeyDataUsageKeyValueNodeWrite:
 *
 * The key data could be written to a <dsig:KeyValue /> child.
 */
#define xmlSecKeyDataUsageKeyValueNodeWrite             0x00008

/**
 * xmlSecKeyDataUsageRetrievalMethodNodeXml:
 *
 * The key data could be retrieved using <dsig:RetrievalMethod /> node
 * in XML format.
 */
#define xmlSecKeyDataUsageRetrievalMethodNodeXml        0x00010

/**
 * xmlSecKeyDataUsageRetrievalMethodNodeBin:
 *
 * The key data could be retrieved using <dsig:RetrievalMethod /> node
 * in binary format.
 */
#define xmlSecKeyDataUsageRetrievalMethodNodeBin        0x00020

/**
 * xmlSecKeyDataUsageAny:
 *
 * Any key data usage.
 */
#define xmlSecKeyDataUsageAny                           0xFFFFF

/**
 * xmlSecKeyDataUsageKeyInfoNode:
 *
 * The key data could be read and written from/to a <dsig:KeyInfo /> child.
 */
#define xmlSecKeyDataUsageKeyInfoNode                   \
        (xmlSecKeyDataUsageKeyInfoNodeRead | xmlSecKeyDataUsageKeyInfoNodeWrite)

/**
 * xmlSecKeyDataUsageKeyValueNode:
 *
 * The key data could be read and written from/to a <dsig:KeyValue /> child.
 */
#define xmlSecKeyDataUsageKeyValueNode                  \
        (xmlSecKeyDataUsageKeyValueNodeRead | xmlSecKeyDataUsageKeyValueNodeWrite)

/**
 * xmlSecKeyDataUsageRetrievalMethodNode:
 *
 * The key data could be retrieved using <dsig:RetrievalMethod /> node
 * in any format.
 */
#define xmlSecKeyDataUsageRetrievalMethodNode           \
        (xmlSecKeyDataUsageRetrievalMethodNodeXml | xmlSecKeyDataUsageRetrievalMethodNodeBin)

/**************************************************************************
 *
 * xmlSecKeyDataType
 *
 *************************************************************************/
/**
 * xmlSecKeyDataType:
 *
 * The key data type (public/private, session/permanent, etc.).
 */
typedef unsigned int                            xmlSecKeyDataType;

/**
 * xmlSecKeyDataTypeUnknown:
 *
 * The key data type is unknown (same as #xmlSecKeyDataTypeNone).
 */
#define xmlSecKeyDataTypeUnknown                        0x0000

/**
 * xmlSecKeyDataTypeNone:
 *
 * The key data type is unknown (same as #xmlSecKeyDataTypeUnknown).
 */
#define xmlSecKeyDataTypeNone                           xmlSecKeyDataTypeUnknown

/**
 * xmlSecKeyDataTypePublic:
 *
 * The key data contain a public key.
 */
#define xmlSecKeyDataTypePublic                         0x0001

/**
 * xmlSecKeyDataTypePrivate:
 *
 * The key data contain a private key.
 */
#define xmlSecKeyDataTypePrivate                        0x0002

/**
 * xmlSecKeyDataTypeSymmetric:
 *
 * The key data contain a symmetric key.
 */
#define xmlSecKeyDataTypeSymmetric                      0x0004

/**
 * xmlSecKeyDataTypeSession:
 *
 * The key data contain session key (one time key, not stored in keys manager).
 */
#define xmlSecKeyDataTypeSession                        0x0008

/**
 * xmlSecKeyDataTypePermanent:
 *
 * The key data contain permanent key (stored in keys manager).
 */
#define xmlSecKeyDataTypePermanent                      0x0010

/**
 * xmlSecKeyDataTypeTrusted:
 *
 * The key data is trusted.
 */
#define xmlSecKeyDataTypeTrusted                        0x0100

/**
 * xmlSecKeyDataTypeAny:
 *
 * Any key data.
 */
#define xmlSecKeyDataTypeAny                            0xFFFF

/**************************************************************************
 *
 * xmlSecKeyDataFormat
 *
 *************************************************************************/
/**
 * xmlSecKeyDataFormat:
 * @xmlSecKeyDataFormatUnknown:         the key data format is unknown.
 * @xmlSecKeyDataFormatBinary:          the binary key data.
 * @xmlSecKeyDataFormatPem:             the PEM key data (cert or public/private key).
 * @xmlSecKeyDataFormatDer:             the DER key data (cert or public/private key).
 * @xmlSecKeyDataFormatPkcs8Pem:        the PKCS8 PEM private key.
 * @xmlSecKeyDataFormatPkcs8Der:        the PKCS8 DER private key.
 * @xmlSecKeyDataFormatPkcs12:          the PKCS12 format (bag of keys and certs)
 * @xmlSecKeyDataFormatCertPem:         the PEM cert.
 * @xmlSecKeyDataFormatCertDer:         the DER cert.
 * @xmlSecKeyDataFormatEngine:          the crypto engine (e.g. OpenSSL ENGINE).
 *
 * The key data format (binary, der, pem, etc.).
 */
typedef enum {
    xmlSecKeyDataFormatUnknown = 0,
    xmlSecKeyDataFormatBinary,
    xmlSecKeyDataFormatPem,
    xmlSecKeyDataFormatDer,
    xmlSecKeyDataFormatPkcs8Pem,
    xmlSecKeyDataFormatPkcs8Der,
    xmlSecKeyDataFormatPkcs12,
    xmlSecKeyDataFormatCertPem,
    xmlSecKeyDataFormatCertDer,
    xmlSecKeyDataFormatEngine
} xmlSecKeyDataFormat;

/**************************************************************************
 *
 * Global xmlSecKeyDataIds methods
 *
 *************************************************************************/
XMLSEC_EXPORT xmlSecPtrListPtr  xmlSecKeyDataIdsGet             (void);
XMLSEC_EXPORT int               xmlSecKeyDataIdsInit            (void);
XMLSEC_EXPORT void              xmlSecKeyDataIdsShutdown        (void);
XMLSEC_EXPORT int               xmlSecKeyDataIdsRegisterDefault (void);
XMLSEC_EXPORT int               xmlSecKeyDataIdsRegister        (xmlSecKeyDataId id);

/**************************************************************************
 *
 * xmlSecKeyData
 *
 *************************************************************************/
/**
 * xmlSecKeyData:
 * @id:                 the data id (#xmlSecKeyDataId).
 * @reserved0:          reserved for the future.
 * @reserved1:          reserved for the future.
 *
 * The key data: key value (crypto material), x509 data, pgp data, etc.
 */
struct _xmlSecKeyData {
    xmlSecKeyDataId                     id;
    void*                               reserved0;
    void*                               reserved1;
};

XMLSEC_EXPORT xmlSecKeyDataPtr  xmlSecKeyDataCreate             (xmlSecKeyDataId id);
XMLSEC_EXPORT xmlSecKeyDataPtr  xmlSecKeyDataDuplicate          (xmlSecKeyDataPtr data);
XMLSEC_EXPORT void              xmlSecKeyDataDestroy            (xmlSecKeyDataPtr data);
XMLSEC_EXPORT int               xmlSecKeyDataGenerate           (xmlSecKeyDataPtr data,
                                                                 xmlSecSize sizeBits,
                                                                 xmlSecKeyDataType type);
XMLSEC_EXPORT xmlSecKeyDataType xmlSecKeyDataGetType            (xmlSecKeyDataPtr data);
XMLSEC_EXPORT xmlSecSize        xmlSecKeyDataGetSize            (xmlSecKeyDataPtr data);
XMLSEC_EXPORT const xmlChar*    xmlSecKeyDataGetIdentifier      (xmlSecKeyDataPtr data);
XMLSEC_EXPORT void              xmlSecKeyDataDebugDump          (xmlSecKeyDataPtr data,
                                                                 FILE *output);
XMLSEC_EXPORT void              xmlSecKeyDataDebugXmlDump       (xmlSecKeyDataPtr data,
                                                                 FILE *output);
XMLSEC_EXPORT int               xmlSecKeyDataXmlRead            (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 xmlNodePtr node,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
XMLSEC_EXPORT int               xmlSecKeyDataXmlWrite           (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 xmlNodePtr node,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
XMLSEC_EXPORT int               xmlSecKeyDataBinRead            (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 const xmlSecByte* buf,
                                                                 xmlSecSize bufSize,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
XMLSEC_EXPORT int               xmlSecKeyDataBinWrite           (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 xmlSecByte** buf,
                                                                 xmlSecSize* bufSize,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);

/**
 * xmlSecKeyDataGetName:
 * @data:               the pointer to key data.
 *
 * Macro. Returns the key data name.
 */
#define xmlSecKeyDataGetName(data) \
        ((xmlSecKeyDataIsValid((data))) ? \
          xmlSecKeyDataKlassGetName((data)->id) : NULL)

/**
 * xmlSecKeyDataIsValid:
 * @data:               the pointer to data.
 *
 * Macro. Returns 1 if @data is not NULL and @data->id is not NULL
 * or 0 otherwise.
 */
#define xmlSecKeyDataIsValid(data) \
        ((( data ) != NULL) && \
         (( data )->id != NULL) && \
         (( data )->id->klassSize >= sizeof(xmlSecKeyDataKlass)) && \
         (( data )->id->objSize >= sizeof(xmlSecKeyData)) && \
         (( data )->id->name != NULL))
/**
 * xmlSecKeyDataCheckId:
 * @data:               the pointer to data.
 * @dataId:             the data Id.
 *
 * Macro. Returns 1 if @data is valid and @data's id is equal to @dataId.
 */
#define xmlSecKeyDataCheckId(data, dataId) \
        (xmlSecKeyDataIsValid(( data )) && \
        ((( data )->id) == ( dataId )))

/**
 * xmlSecKeyDataCheckUsage:
 * @data:               the pointer to data.
 * @usg:                the data usage.
 *
 * Macro. Returns 1 if @data is valid and could be used for @usg.
 */
#define xmlSecKeyDataCheckUsage(data, usg) \
        (xmlSecKeyDataIsValid(( data )) && \
        (((( data )->id->usage) & ( usg )) != 0))

/**
 * xmlSecKeyDataCheckSize:
 * @data:               the pointer to data.
 * @size:               the expected size.
 *
 * Macro. Returns 1 if @data is valid and @data's object has at least @size bytes.
 */
#define xmlSecKeyDataCheckSize(data, size) \
        (xmlSecKeyDataIsValid(( data )) && \
         (( data )->id->objSize >= size))

/**************************************************************************
 *
 * xmlSecKeyDataKlass
 *
 *************************************************************************/
/**
 * xmlSecKeyDataIdUnknown:
 *
 * The "unknown" id.
 */
#define xmlSecKeyDataIdUnknown                  ((xmlSecKeyDataId)NULL)

/**
 * xmlSecKeyDataInitMethod:
 * @data:               the pointer to key data.
 *
 * Key data specific initialization method.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
typedef int                     (*xmlSecKeyDataInitMethod)      (xmlSecKeyDataPtr data);

/**
 * xmlSecKeyDataDuplicateMethod:
 * @dst:                the pointer to destination key data.
 * @src:                the pointer to source key data.
 *
 * Key data specific duplication (copy) method.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
typedef int                     (*xmlSecKeyDataDuplicateMethod) (xmlSecKeyDataPtr dst,
                                                                 xmlSecKeyDataPtr src);

/**
 * xmlSecKeyDataFinalizeMethod:
 * @data:               the data.
 *
 * Key data specific finalization method. All the objects and resources allocated
 * by the key data object must be freed inside this method.
 */
typedef void                    (*xmlSecKeyDataFinalizeMethod)  (xmlSecKeyDataPtr data);

/**
 * xmlSecKeyDataXmlReadMethod:
 * @id:                 the data id.
 * @key:                the key.
 * @node:               the pointer to data's value XML node.
 * @keyInfoCtx:         the <dsig:KeyInfo/> node processing context.
 *
 * Key data specific method for reading XML node.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
typedef int                     (*xmlSecKeyDataXmlReadMethod)   (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 xmlNodePtr node,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
/**
 * xmlSecKeyDataXmlWriteMethod:
 * @id:                 the data id.
 * @key:                the key.
 * @node:               the pointer to data's value XML node.
 * @keyInfoCtx:         the <dsig:KeyInfo> node processing context.
 *
 * Key data specific method for writing XML node.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
typedef int                     (*xmlSecKeyDataXmlWriteMethod)  (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 xmlNodePtr node,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
/**
 * xmlSecKeyDataBinReadMethod:
 * @id:                 the data id.
 * @key:                the key.
 * @buf:                the input buffer.
 * @bufSize:            the buffer size.
 * @keyInfoCtx:         the <dsig:KeyInfo/> node processing context.
 *
 * Key data specific method for reading binary buffer.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
typedef int                     (*xmlSecKeyDataBinReadMethod)   (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 const xmlSecByte* buf,
                                                                 xmlSecSize bufSize,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);
/**
 * xmlSecKeyDataBinWriteMethod:
 * @id:                 the data id.
 * @key:                the key.
 * @buf:                the output buffer.
 * @bufSize:            the buffer size.
 * @keyInfoCtx:         the <dsig:KeyInfo/> node processing context.
 *
 * Key data specific method for reading binary buffer.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
typedef int                     (*xmlSecKeyDataBinWriteMethod)  (xmlSecKeyDataId id,
                                                                 xmlSecKeyPtr key,
                                                                 xmlSecByte** buf,
                                                                 xmlSecSize* bufSize,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);

/**
 * xmlSecKeyDataGenerateMethod:
 * @data:               the pointer to key data.
 * @sizeBits:           the key data specific size.
 * @type:               the required key type (session/permanent, etc.)
 *
 * Key data specific method for generating new key data.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
typedef int                     (*xmlSecKeyDataGenerateMethod)  (xmlSecKeyDataPtr data,
                                                                 xmlSecSize sizeBits,
                                                                 xmlSecKeyDataType type);

/**
 * xmlSecKeyDataGetTypeMethod:
 * @data:                the data.
 *
 * Key data specific method to get the key type.
 *
 * Returns: the key type.
 */
typedef xmlSecKeyDataType       (*xmlSecKeyDataGetTypeMethod)   (xmlSecKeyDataPtr data);

/**
 * xmlSecKeyDataGetSizeMethod:
 * @data:               the pointer to key data.
 *
 * Key data specific method to get the key size.
 *
 * Returns: the key size in bits.
 */
typedef xmlSecSize              (*xmlSecKeyDataGetSizeMethod)   (xmlSecKeyDataPtr data);

/**
 * xmlSecKeyDataGetIdentifierMethod:
 * @data:               the pointer to key data.
 *
 * Key data specific method to get the key data identifier string (for example,
 * X509 data identifier is the subject of the verified cert).
 *
 * Returns: the identifier string or NULL if an error occurs.
 */
typedef const xmlChar*          (*xmlSecKeyDataGetIdentifierMethod) (xmlSecKeyDataPtr data);

/**
 * xmlSecKeyDataDebugDumpMethod:
 * @data:               the data.
 * @output:             the FILE to print debug info (should be open for writing).
 *
 * Key data specific method for printing debug info.
 */
typedef void                    (*xmlSecKeyDataDebugDumpMethod) (xmlSecKeyDataPtr data,
                                                                 FILE* output);

/**
 * xmlSecKeyDataKlass:
 * @klassSize:          the klass size.
 * @objSize:            the object size.
 * @name:               the object name.
 * @usage:              the allowed data usage.
 * @href:               the identification string (href).
 * @dataNodeName:       the data's XML node name.
 * @dataNodeNs:         the data's XML node namespace.
 * @initialize:         the initialization method.
 * @duplicate:          the duplicate (copy) method.
 * @finalize:           the finalization (destroy) method.
 * @generate:           the new data generation method.
 * @getType:            the method to access data's type information.
 * @getSize:            the method to access data's size.
 * @getIdentifier:      the method to access data's string identifier.
 * @xmlRead:            the method for reading data from XML node.
 * @xmlWrite:           the method for writing data to XML node.
 * @binRead:            the method for reading data from a binary buffer.
 * @binWrite:           the method for writing data to binary buffer.
 * @debugDump:          the method for printing debug data information.
 * @debugXmlDump:       the method for printing debug data information in XML format.
 * @reserved0:          reserved for the future.
 * @reserved1:          reserved for the future.
 *
 * The data id (klass).
 */
struct _xmlSecKeyDataKlass {
    xmlSecSize                          klassSize;
    xmlSecSize                          objSize;

    /* data */
    const xmlChar*                      name;
    xmlSecKeyDataUsage                  usage;
    const xmlChar*                      href;
    const xmlChar*                      dataNodeName;
    const xmlChar*                      dataNodeNs;

    /* constructors/destructor */
    xmlSecKeyDataInitMethod             initialize;
    xmlSecKeyDataDuplicateMethod        duplicate;
    xmlSecKeyDataFinalizeMethod         finalize;
    xmlSecKeyDataGenerateMethod         generate;

    /* get info */
    xmlSecKeyDataGetTypeMethod          getType;
    xmlSecKeyDataGetSizeMethod          getSize;
    xmlSecKeyDataGetIdentifierMethod    getIdentifier;

    /* read/write */
    xmlSecKeyDataXmlReadMethod          xmlRead;
    xmlSecKeyDataXmlWriteMethod         xmlWrite;
    xmlSecKeyDataBinReadMethod          binRead;
    xmlSecKeyDataBinWriteMethod         binWrite;

    /* debug */
    xmlSecKeyDataDebugDumpMethod        debugDump;
    xmlSecKeyDataDebugDumpMethod        debugXmlDump;

    /* for the future */
    void*                               reserved0;
    void*                               reserved1;
};

/**
 * xmlSecKeyDataKlassGetName:
 * @klass:              the data klass.
 *
 * Macro. Returns data klass name.
 */
#define xmlSecKeyDataKlassGetName(klass) \
        (((klass)) ? ((klass)->name) : NULL)



/***********************************************************************
 *
 * Helper functions for binary key data (HMAC, AES, DES, ...).
 *
 **********************************************************************/
XMLSEC_EXPORT xmlSecSize        xmlSecKeyDataBinaryValueGetSize         (xmlSecKeyDataPtr data);
XMLSEC_EXPORT xmlSecBufferPtr   xmlSecKeyDataBinaryValueGetBuffer       (xmlSecKeyDataPtr data);
XMLSEC_EXPORT int               xmlSecKeyDataBinaryValueSetBuffer       (xmlSecKeyDataPtr data,
                                                                         const xmlSecByte* buf,
                                                                         xmlSecSize bufSize);

/***********************************************************************
 *
 * Key Data list
 *
 **********************************************************************/
/**
 * xmlSecKeyDataListId:
 *
 *
 * The key data klasses list klass id.
 */
#define xmlSecKeyDataListId     xmlSecKeyDataListGetKlass()
XMLSEC_EXPORT xmlSecPtrListId   xmlSecKeyDataListGetKlass       (void);

/***********************************************************************
 *
 * Key Data Ids list
 *
 **********************************************************************/
/**
 * xmlSecKeyDataIdListId:
 *
 *
 * The key data list klass id.
 */
#define xmlSecKeyDataIdListId   xmlSecKeyDataIdListGetKlass()
XMLSEC_EXPORT xmlSecPtrListId   xmlSecKeyDataIdListGetKlass     (void);
XMLSEC_EXPORT int               xmlSecKeyDataIdListFind         (xmlSecPtrListPtr list,
                                                                 xmlSecKeyDataId dataId);
XMLSEC_EXPORT xmlSecKeyDataId   xmlSecKeyDataIdListFindByNode   (xmlSecPtrListPtr list,
                                                                 const xmlChar* nodeName,
                                                                 const xmlChar* nodeNs,
                                                                 xmlSecKeyDataUsage usage);
XMLSEC_EXPORT xmlSecKeyDataId   xmlSecKeyDataIdListFindByHref   (xmlSecPtrListPtr list,
                                                                 const xmlChar* href,
                                                                 xmlSecKeyDataUsage usage);
XMLSEC_EXPORT xmlSecKeyDataId   xmlSecKeyDataIdListFindByName   (xmlSecPtrListPtr list,
                                                                 const xmlChar* name,
                                                                 xmlSecKeyDataUsage usage);
XMLSEC_EXPORT void              xmlSecKeyDataIdListDebugDump    (xmlSecPtrListPtr list,
                                                                 FILE* output);
XMLSEC_EXPORT void              xmlSecKeyDataIdListDebugXmlDump (xmlSecPtrListPtr list,
                                                                 FILE* output);


/**************************************************************************
 *
 * xmlSecKeyDataStore
 *
 *************************************************************************/
/**
 * xmlSecKeyDataStore:
 * @id:                 the store id (#xmlSecKeyDataStoreId).
 * @reserved0:          reserved for the future.
 * @reserved1:          reserved for the future.
 *
 * The key data store. Key data store holds common key data specific information
 * required for key data processing. For example, X509 data store may hold
 * information about trusted (root) certificates.
 */
struct _xmlSecKeyDataStore {
    xmlSecKeyDataStoreId                id;

    /* for the future */
    void*                               reserved0;
    void*                               reserved1;
};

XMLSEC_EXPORT xmlSecKeyDataStorePtr xmlSecKeyDataStoreCreate    (xmlSecKeyDataStoreId id);
XMLSEC_EXPORT void              xmlSecKeyDataStoreDestroy       (xmlSecKeyDataStorePtr store);

/**
 * xmlSecKeyDataStoreGetName:
 * @store:              the pointer to store.
 *
 * Macro. Returns key data store name.
 */
#define xmlSecKeyDataStoreGetName(store) \
    ((xmlSecKeyDataStoreIsValid((store))) ? \
      xmlSecKeyDataStoreKlassGetName((store)->id) : NULL)

/**
 * xmlSecKeyDataStoreIsValid:
 * @store:              the pointer to store.
 *
 * Macro. Returns 1 if @store is not NULL and @store->id is not NULL
 * or 0 otherwise.
 */
#define xmlSecKeyDataStoreIsValid(store) \
        ((( store ) != NULL) && ((( store )->id) != NULL))
/**
 * xmlSecKeyDataStoreCheckId:
 * @store:              the pointer to store.
 * @storeId:            the store Id.
 *
 * Macro. Returns 1 if @store is valid and @store's id is equal to @storeId.
 */
#define xmlSecKeyDataStoreCheckId(store, storeId) \
        (xmlSecKeyDataStoreIsValid(( store )) && \
        ((( store )->id) == ( storeId )))

/**
 * xmlSecKeyDataStoreCheckSize:
 * @store:              the pointer to store.
 * @size:               the expected size.
 *
 * Macro. Returns 1 if @data is valid and @stores's object has at least @size bytes.
 */
#define xmlSecKeyDataStoreCheckSize(store, size) \
        (xmlSecKeyDataStoreIsValid(( store )) && \
         (( store )->id->objSize >= size))


/**************************************************************************
 *
 * xmlSecKeyDataStoreKlass
 *
 *************************************************************************/
/**
 * xmlSecKeyDataStoreIdUnknown:
 *
 * The "unknown" id.
 */
#define xmlSecKeyDataStoreIdUnknown                     NULL

/**
 * xmlSecKeyDataStoreInitializeMethod:
 * @store:              the data store.
 *
 * Key data store specific initialization method.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
typedef int                     (*xmlSecKeyDataStoreInitializeMethod)   (xmlSecKeyDataStorePtr store);

/**
 * xmlSecKeyDataStoreFinalizeMethod:
 * @store:              the data store.
 *
 * Key data store specific finalization (destroy) method.
 */
typedef void                    (*xmlSecKeyDataStoreFinalizeMethod)     (xmlSecKeyDataStorePtr store);

/**
 * xmlSecKeyDataStoreKlass:
 * @klassSize:          the data store klass size.
 * @objSize:            the data store obj size.
 * @name:               the store's name.
 * @initialize:         the store's initialization method.
 * @finalize:           the store's finalization (destroy) method.
 * @reserved0:          reserved for the future.
 * @reserved1:          reserved for the future.
 *
 * The data store id (klass).
 */
struct _xmlSecKeyDataStoreKlass {
    xmlSecSize                          klassSize;
    xmlSecSize                          objSize;

    /* data */
    const xmlChar*                      name;

    /* constructors/destructor */
    xmlSecKeyDataStoreInitializeMethod  initialize;
    xmlSecKeyDataStoreFinalizeMethod    finalize;

    /* for the future */
    void*                               reserved0;
    void*                               reserved1;
};

/**
 * xmlSecKeyDataStoreKlassGetName:
 * @klass:              the pointer to store klass.
 *
 * Macro. Returns store klass name.
 */
#define xmlSecKeyDataStoreKlassGetName(klass) \
        (((klass)) ? ((klass)->name) : NULL)

/***********************************************************************
 *
 * Key Data Store list
 *
 **********************************************************************/
/**
 * xmlSecKeyDataStorePtrListId:
 *
 * The data store list id (klass).
 */
#define xmlSecKeyDataStorePtrListId     xmlSecKeyDataStorePtrListGetKlass()
XMLSEC_EXPORT xmlSecPtrListId   xmlSecKeyDataStorePtrListGetKlass       (void);

XMLSEC_EXPORT void xmlSecImportSetPersistKey                            (void);
XMLSEC_EXPORT int xmlSecImportGetPersistKey                             (void);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_KEYSDATA_H__ */
