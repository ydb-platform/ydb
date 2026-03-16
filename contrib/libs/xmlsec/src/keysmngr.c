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
 * SECTION:keysmngr
 * @Short_description: Keys manager object functions.
 * @Stability: Stable
 *
 */
#include "globals.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <libxml/tree.h>
#include <libxml/parser.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/list.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>
#include <xmlsec/keysmngr.h>
#include <xmlsec/errors.h>
#include <xmlsec/private.h>

#include "cast_helpers.h"

/****************************************************************************
 *
 * Keys Manager
 *
 ***************************************************************************/
/**
 * xmlSecKeysMngrCreate:
 *
 * Creates new keys manager. Caller is responsible for freeing it with
 * #xmlSecKeysMngrDestroy function.
 *
 * Returns: the pointer to newly allocated keys manager or NULL if
 * an error occurs.
 */
xmlSecKeysMngrPtr
xmlSecKeysMngrCreate(void) {
    xmlSecKeysMngrPtr mngr;
    int ret;

    /* Allocate a new xmlSecKeysMngr and fill the fields. */
    mngr = (xmlSecKeysMngrPtr)xmlMalloc(sizeof(xmlSecKeysMngr));
    if(mngr == NULL) {
        xmlSecMallocError(sizeof(xmlSecKeysMngr), NULL);
        return(NULL);
    }
    memset(mngr, 0, sizeof(xmlSecKeysMngr));

    ret = xmlSecPtrListInitialize(&(mngr->storesList), xmlSecKeyDataStorePtrListId);
    if(ret < 0) {
        xmlSecInternalError("xmlSecPtrListInitialize(xmlSecKeyDataStorePtrListId)", NULL);
        return(NULL);
    }

    return(mngr);
}

/**
 * xmlSecKeysMngrDestroy:
 * @mngr:               the pointer to keys manager.
 *
 * Destroys keys manager created with #xmlSecKeysMngrCreate function.
 */
void
xmlSecKeysMngrDestroy(xmlSecKeysMngrPtr mngr) {
    xmlSecAssert(mngr != NULL);

    /* destroy keys store */
    if(mngr->keysStore != NULL) {
        xmlSecKeyStoreDestroy(mngr->keysStore);
    }

    /* destroy other data stores */
    xmlSecPtrListFinalize(&(mngr->storesList));

    memset(mngr, 0, sizeof(xmlSecKeysMngr));
    xmlFree(mngr);
}

/**
 * xmlSecKeysMngrFindKey:
 * @mngr:               the pointer to keys manager.
 * @name:               the desired key name.
 * @keyInfoCtx:         the pointer to <dsig:KeyInfo/> node processing context.
 *
 * Lookups key in the keys manager keys store. The caller is responsible
 * for destroying the returned key using #xmlSecKeyDestroy method.
 *
 * Returns: the pointer to a key or NULL if key is not found or an error occurs.
 */
xmlSecKeyPtr
xmlSecKeysMngrFindKey(xmlSecKeysMngrPtr mngr, const xmlChar* name, xmlSecKeyInfoCtxPtr keyInfoCtx) {
    xmlSecKeyStorePtr store;

    xmlSecAssert2(mngr != NULL, NULL);
    xmlSecAssert2(keyInfoCtx != NULL, NULL);

    store = xmlSecKeysMngrGetKeysStore(mngr);
    if(store == NULL) {
        /* no store. is it an error? */
        return(NULL);
    }

    return(xmlSecKeyStoreFindKey(store, name, keyInfoCtx));
}

/**
 * xmlSecKeysMngrAdoptKeysStore:
 * @mngr:               the pointer to keys manager.
 * @store:              the pointer to keys store.
 *
 * Adopts keys store in the keys manager @mngr.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecKeysMngrAdoptKeysStore(xmlSecKeysMngrPtr mngr, xmlSecKeyStorePtr store) {
    xmlSecAssert2(mngr != NULL, -1);
    xmlSecAssert2(xmlSecKeyStoreIsValid(store), -1);

    if(mngr->keysStore != NULL) {
        xmlSecKeyStoreDestroy(mngr->keysStore);
    }
    mngr->keysStore = store;

    return(0);
}

/**
 * xmlSecKeysMngrGetKeysStore:
 * @mngr:               the pointer to keys manager.
 *
 * Gets the keys store.
 *
 * Returns: the keys store in the keys manager @mngr or NULL if
 * there is no store or an error occurs.
 */
xmlSecKeyStorePtr
xmlSecKeysMngrGetKeysStore(xmlSecKeysMngrPtr mngr) {
    xmlSecAssert2(mngr != NULL, NULL);

    return(mngr->keysStore);
}

/**
 * xmlSecKeysMngrAdoptDataStore:
 * @mngr:               the pointer to keys manager.
 * @store:              the pointer to data store.
 *
 * Adopts data store in the keys manager.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecKeysMngrAdoptDataStore(xmlSecKeysMngrPtr mngr, xmlSecKeyDataStorePtr store) {
    xmlSecKeyDataStorePtr tmp;
    xmlSecSize pos, size;

    xmlSecAssert2(mngr != NULL, -1);
    xmlSecAssert2(xmlSecKeyDataStoreIsValid(store), -1);

    size = xmlSecPtrListGetSize(&(mngr->storesList));
    for(pos = 0; pos < size; ++pos) {
        tmp = (xmlSecKeyDataStorePtr)xmlSecPtrListGetItem(&(mngr->storesList), pos);
        if((tmp != NULL) && (tmp->id == store->id)) {
            return(xmlSecPtrListSet(&(mngr->storesList), store, pos));
        }
    }

    return(xmlSecPtrListAdd(&(mngr->storesList), store));
}


/**
 * xmlSecKeysMngrGetDataStore:
 * @mngr:               the pointer to keys manager.
 * @id:                 the desired data store klass.
 *
 * Lookups the data store of given klass @id in the keys manager.
 *
 * Returns: pointer to data store or NULL if it is not found or an error
 * occurs.
 */
xmlSecKeyDataStorePtr
xmlSecKeysMngrGetDataStore(xmlSecKeysMngrPtr mngr, xmlSecKeyDataStoreId id) {
    xmlSecKeyDataStorePtr tmp;
    xmlSecSize pos, size;

    xmlSecAssert2(mngr != NULL, NULL);
    xmlSecAssert2(id != xmlSecKeyDataStoreIdUnknown, NULL);

    size = xmlSecPtrListGetSize(&(mngr->storesList));
    for(pos = 0; pos < size; ++pos) {
        tmp = (xmlSecKeyDataStorePtr)xmlSecPtrListGetItem(&(mngr->storesList), pos);
        if((tmp != NULL) && (tmp->id == id)) {
            return(tmp);
        }
    }

    return(NULL);
}

/**************************************************************************
 *
 * xmlSecKeyStore functions
 *
 *************************************************************************/
/**
 * xmlSecKeyStoreCreate:
 * @id:                 the key store klass.
 *
 * Creates new store of the specified klass @klass. Caller is responsible
 * for freeing the returned store by calling #xmlSecKeyStoreDestroy function.
 *
 * Returns: the pointer to newly allocated keys store or NULL if an error occurs.
 */
xmlSecKeyStorePtr
xmlSecKeyStoreCreate(xmlSecKeyStoreId id)  {
    xmlSecKeyStorePtr store;
    int ret;

    xmlSecAssert2(id != NULL, NULL);
    xmlSecAssert2(id->objSize > 0, NULL);

    /* Allocate a new xmlSecKeyStore and fill the fields. */
    store = (xmlSecKeyStorePtr)xmlMalloc(id->objSize);
    if(store == NULL) {
        xmlSecMallocError(id->objSize,
                          xmlSecKeyStoreKlassGetName(id));
        return(NULL);
    }
    memset(store, 0, id->objSize);
    store->id = id;

    if(id->initialize != NULL) {
        ret = (id->initialize)(store);
        if(ret < 0) {
            xmlSecInternalError("id->initialize",
                                xmlSecKeyStoreKlassGetName(id));
            xmlSecKeyStoreDestroy(store);
            return(NULL);
        }
    }

    return(store);
}

/**
 * xmlSecKeyStoreDestroy:
 * @store:              the pointer to keys store.
 *
 * Destroys the store created with #xmlSecKeyStoreCreate function.
 */
void
xmlSecKeyStoreDestroy(xmlSecKeyStorePtr store) {
    xmlSecAssert(xmlSecKeyStoreIsValid(store));
    xmlSecAssert(store->id->objSize > 0);

    if(store->id->finalize != NULL) {
        (store->id->finalize)(store);
    }
    memset(store, 0, store->id->objSize);
    xmlFree(store);
}

/**
 * xmlSecKeyStoreFindKey:
 * @store:              the pointer to keys store.
 * @name:               the desired key name.
 * @keyInfoCtx:         the pointer to <dsig:KeyInfo/> node processing context.
 *
 * Lookups key in the store. The caller is responsible for destroying
 * the returned key using #xmlSecKeyDestroy method.
 *
 * Returns: the pointer to a key or NULL if key is not found or an error occurs.
 */
xmlSecKeyPtr
xmlSecKeyStoreFindKey(xmlSecKeyStorePtr store, const xmlChar* name, xmlSecKeyInfoCtxPtr keyInfoCtx) {
    xmlSecAssert2(xmlSecKeyStoreIsValid(store), NULL);
    xmlSecAssert2(store->id->findKey != NULL, NULL);
    xmlSecAssert2(keyInfoCtx != NULL, NULL);

    return(store->id->findKey(store, name, keyInfoCtx));
}

/****************************************************************************
 *
 * Simple Keys Store
 *
 * xmlSecKeyStore + xmlSecPtrList (keys list)
 *
 ***************************************************************************/
XMLSEC_KEY_STORE_DECLARE(SimpleKeysStore, xmlSecPtrList)
#define xmlSecSimpleKeysStoreSize XMLSEC_KEY_STORE_SIZE(SimpleKeysStore)

static int                      xmlSecSimpleKeysStoreInitialize (xmlSecKeyStorePtr store);
static void                     xmlSecSimpleKeysStoreFinalize   (xmlSecKeyStorePtr store);
static xmlSecKeyPtr             xmlSecSimpleKeysStoreFindKey    (xmlSecKeyStorePtr store,
                                                                 const xmlChar* name,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);

static xmlSecKeyStoreKlass xmlSecSimpleKeysStoreKlass = {
    sizeof(xmlSecKeyStoreKlass),
    xmlSecSimpleKeysStoreSize,

    /* data */
    BAD_CAST "simple-keys-store",               /* const xmlChar* name; */

    /* constructors/destructor */
    xmlSecSimpleKeysStoreInitialize,            /* xmlSecKeyStoreInitializeMethod initialize; */
    xmlSecSimpleKeysStoreFinalize,              /* xmlSecKeyStoreFinalizeMethod finalize; */
    xmlSecSimpleKeysStoreFindKey,               /* xmlSecKeyStoreFindKeyMethod findKey; */

    /* reserved for the future */
    NULL,                                       /* void* reserved0; */
    NULL,                                       /* void* reserved1; */
};

/**
 * xmlSecSimpleKeysStoreGetKlass:
 *
 * The simple list based keys store klass.
 *
 * Returns: simple list based keys store klass.
 */
xmlSecKeyStoreId
xmlSecSimpleKeysStoreGetKlass(void) {
    return(&xmlSecSimpleKeysStoreKlass);
}

/**
 * xmlSecSimpleKeysStoreAdoptKey:
 * @store:              the pointer to simple keys store.
 * @key:                the pointer to key.
 *
 * Adds @key to the @store.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecSimpleKeysStoreAdoptKey(xmlSecKeyStorePtr store, xmlSecKeyPtr key) {
    xmlSecPtrListPtr list;
    int ret;

    xmlSecAssert2(xmlSecKeyStoreCheckId(store, xmlSecSimpleKeysStoreId), -1);
    xmlSecAssert2(key != NULL, -1);

    list = xmlSecSimpleKeysStoreGetCtx(store);
    xmlSecAssert2(xmlSecPtrListCheckId(list, xmlSecKeyPtrListId), -1);

    ret = xmlSecPtrListAdd(list, key);
    if(ret < 0) {
        xmlSecInternalError("xmlSecPtrListAdd",
                            xmlSecKeyStoreGetName(store));
        return(-1);
    }

    return(0);
}

/**
 * xmlSecSimpleKeysStoreLoad:
 * @store:              the pointer to simple keys store.
 * @uri:                the filename.
 * @keysMngr:           the pointer to associated keys manager.
 *
 * Reads keys from an XML file.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecSimpleKeysStoreLoad(xmlSecKeyStorePtr store, const char *uri,
                            xmlSecKeysMngrPtr keysMngr) {
    xmlSecAssert2(xmlSecKeyStoreCheckId(store, xmlSecSimpleKeysStoreId), -1);

    return(xmlSecSimpleKeysStoreLoad_ex(store, uri, keysMngr,
        xmlSecSimpleKeysStoreAdoptKey));
}

/**
 * xmlSecSimpleKeysStoreLoad_ex:
 * @store:              the pointer to simple keys store.
 * @uri:                the filename.
 * @keysMngr:           the pointer to associated keys manager.
 * @adoptKeyFunc:       the callback to add the key to keys manager.
 *
 * Reads keys from an XML file.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecSimpleKeysStoreLoad_ex(xmlSecKeyStorePtr store, const char *uri,
                            xmlSecKeysMngrPtr keysMngr ATTRIBUTE_UNUSED,
                            xmlSecSimpleKeysStoreAdoptKeyFunc adoptKeyFunc) {
    xmlDocPtr doc;
    xmlNodePtr root;
    xmlNodePtr cur;
    xmlSecKeyPtr key;
    xmlSecKeyInfoCtx keyInfoCtx;
    int ret;

    /* don't check store ID here because it might not be simple store ID;
     * we will check for the correct store ID in the adoptKeyFunc instead */
    xmlSecAssert2(store != NULL, -1);
    xmlSecAssert2(uri != NULL, -1);
    xmlSecAssert2(adoptKeyFunc != NULL, -1);
    UNREFERENCED_PARAMETER(keysMngr);

    doc = xmlParseFile(uri);
    if(doc == NULL) {
        xmlSecXmlError2("xmlParseFile", xmlSecKeyStoreGetName(store),
                        "uri=%s", xmlSecErrorsSafeString(uri));
        return(-1);
    }

    root = xmlDocGetRootElement(doc);
    if(!xmlSecCheckNodeName(root, BAD_CAST "Keys", xmlSecNs)) {
        xmlSecInvalidNodeError(root, BAD_CAST "Keys",
            xmlSecKeyStoreGetName(store));
        xmlFreeDoc(doc);
        return(-1);
    }

    cur = xmlSecGetNextElementNode(root->children);
    while((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeKeyInfo, xmlSecDSigNs)) {
        key = xmlSecKeyCreate();
        if(key == NULL) {
            xmlSecInternalError("xmlSecKeyCreate",
                xmlSecKeyStoreGetName(store));
            xmlFreeDoc(doc);
            return(-1);
        }

        ret = xmlSecKeyInfoCtxInitialize(&keyInfoCtx, NULL);
        if(ret < 0) {
            xmlSecInternalError("xmlSecKeyInfoCtxInitialize",
                xmlSecKeyStoreGetName(store));
            xmlSecKeyDestroy(key);
            xmlFreeDoc(doc);
            return(-1);
        }

        keyInfoCtx.mode           = xmlSecKeyInfoModeRead;
        keyInfoCtx.keysMngr       = NULL;
        keyInfoCtx.flags          = XMLSEC_KEYINFO_FLAGS_DONT_STOP_ON_KEY_FOUND |
                                    XMLSEC_KEYINFO_FLAGS_X509DATA_DONT_VERIFY_CERTS;
        keyInfoCtx.keyReq.keyId   = xmlSecKeyDataIdUnknown;
        keyInfoCtx.keyReq.keyType = xmlSecKeyDataTypeAny;
        keyInfoCtx.keyReq.keyUsage= xmlSecKeyDataUsageAny;

        ret = xmlSecKeyInfoNodeRead(cur, key, &keyInfoCtx);
        if(ret < 0) {
            xmlSecInternalError("xmlSecKeyInfoNodeRead",
                xmlSecKeyStoreGetName(store));
            xmlSecKeyInfoCtxFinalize(&keyInfoCtx);
            xmlSecKeyDestroy(key);
            xmlFreeDoc(doc);
            return(-1);
        }
        xmlSecKeyInfoCtxFinalize(&keyInfoCtx);

        if(xmlSecKeyIsValid(key)) {
            ret = adoptKeyFunc(store, key);
            if(ret < 0) {
                xmlSecInternalError("adoptKeyFunc",
                    xmlSecKeyStoreGetName(store));
                xmlSecKeyDestroy(key);
                xmlFreeDoc(doc);
                return(-1);
            }
        } else {
            /* we have an unknown key in our file, just ignore it */
            xmlSecKeyDestroy(key);
        }
        cur = xmlSecGetNextElementNode(cur->next);
    }

    if(cur != NULL) {
        xmlSecUnexpectedNodeError(cur, xmlSecKeyStoreGetName(store));
        xmlFreeDoc(doc);
        return(-1);
    }

    xmlFreeDoc(doc);
    return(0);

}

/**
 * xmlSecSimpleKeysStoreSave:
 * @store:              the pointer to simple keys store.
 * @filename:           the filename.
 * @type:               the saved keys type (public, private, ...).
 *
 * Writes keys from @store to an XML file.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecSimpleKeysStoreSave(xmlSecKeyStorePtr store, const char *filename, xmlSecKeyDataType type) {
    xmlSecKeyInfoCtx keyInfoCtx;
    xmlSecPtrListPtr list;
    xmlSecKeyPtr key;
    xmlSecSize i, keysSize;
    xmlDocPtr doc;
    xmlNodePtr cur;
    xmlSecKeyDataPtr data;
    xmlSecPtrListPtr idsList;
    xmlSecKeyDataId dataId;
    xmlSecSize idsSize, j;
    int ret;

    xmlSecAssert2(xmlSecKeyStoreCheckId(store, xmlSecSimpleKeysStoreId), -1);
    xmlSecAssert2(filename != NULL, -1);

    list = xmlSecSimpleKeysStoreGetCtx(store);
    xmlSecAssert2(xmlSecPtrListCheckId(list, xmlSecKeyPtrListId), -1);

    /* create doc */
    doc = xmlSecCreateTree(BAD_CAST "Keys", xmlSecNs);
    if(doc == NULL) {
        xmlSecInternalError("xmlSecCreateTree",
                            xmlSecKeyStoreGetName(store));
        return(-1);
    }

    idsList = xmlSecKeyDataIdsGet();
    xmlSecAssert2(idsList != NULL, -1);

    keysSize = xmlSecPtrListGetSize(list);
    idsSize = xmlSecPtrListGetSize(idsList);
    for(i = 0; i < keysSize; ++i) {
        key = (xmlSecKeyPtr)xmlSecPtrListGetItem(list, i);
        xmlSecAssert2(key != NULL, -1);

        cur = xmlSecAddChild(xmlDocGetRootElement(doc), xmlSecNodeKeyInfo, xmlSecDSigNs);
        if(cur == NULL) {
            xmlSecInternalError2("xmlSecAddChild",
                                 xmlSecKeyStoreGetName(store),
                                 "node=%s",
                                 xmlSecErrorsSafeString(xmlSecNodeKeyInfo));
            xmlFreeDoc(doc);
            return(-1);
        }

        /* special data key name */
        if(xmlSecKeyGetName(key) != NULL) {
            if(xmlSecAddChild(cur, xmlSecNodeKeyName, xmlSecDSigNs) == NULL) {
                xmlSecInternalError2("xmlSecAddChild",
                                     xmlSecKeyStoreGetName(store),
                                     "node=%s",
                                     xmlSecErrorsSafeString(xmlSecNodeKeyName));
                xmlFreeDoc(doc);
                return(-1);
            }
        }

        /* create nodes for other keys data */
        for(j = 0; j < idsSize; ++j) {
            dataId = (xmlSecKeyDataId)xmlSecPtrListGetItem(idsList, j);
            xmlSecAssert2(dataId != xmlSecKeyDataIdUnknown, -1);

            if(dataId->dataNodeName == NULL) {
                continue;
            }

            data = xmlSecKeyGetData(key, dataId);
            if(data == NULL) {
                continue;
            }

            if(xmlSecAddChild(cur, dataId->dataNodeName, dataId->dataNodeNs) == NULL) {
                xmlSecInternalError2("xmlSecAddChild",
                                     xmlSecKeyStoreGetName(store),
                                    "node=%s", xmlSecErrorsSafeString(dataId->dataNodeName));
                xmlFreeDoc(doc);
                return(-1);
            }
        }

        ret = xmlSecKeyInfoCtxInitialize(&keyInfoCtx, NULL);
        if(ret < 0) {
            xmlSecInternalError("xmlSecKeyInfoCtxInitialize",
                                xmlSecKeyStoreGetName(store));
            xmlFreeDoc(doc);
            return(-1);
        }

        keyInfoCtx.mode                 = xmlSecKeyInfoModeWrite;
        keyInfoCtx.keyReq.keyId         = xmlSecKeyDataIdUnknown;
        keyInfoCtx.keyReq.keyType       = type;
        keyInfoCtx.keyReq.keyUsage      = xmlSecKeyDataUsageAny;

        /* finally write key in the node */
        ret = xmlSecKeyInfoNodeWrite(cur, key, &keyInfoCtx);
        if(ret < 0) {
            xmlSecInternalError("xmlSecKeyInfoNodeWrite",
                                xmlSecKeyStoreGetName(store));
            xmlSecKeyInfoCtxFinalize(&keyInfoCtx);
            xmlFreeDoc(doc);
            return(-1);
        }
        xmlSecKeyInfoCtxFinalize(&keyInfoCtx);
    }

    /* now write result */
    ret = xmlSaveFormatFile(filename, doc, 1);
    if(ret < 0) {
        xmlSecXmlError2("xmlSaveFormatFile", xmlSecKeyStoreGetName(store),
                        "filename=%s", xmlSecErrorsSafeString(filename));
        xmlFreeDoc(doc);
        return(-1);
    }

    xmlFreeDoc(doc);
    return(0);
}

/**
 * xmlSecSimpleKeysStoreGetKeys:
 * @store:              the pointer to simple keys store.
 *
 * Gets list of keys from simple keys store.
 *
 * Returns: pointer to the list of keys stored in the keys store or NULL
 * if an error occurs.
 */
xmlSecPtrListPtr
xmlSecSimpleKeysStoreGetKeys(xmlSecKeyStorePtr store) {
    xmlSecPtrListPtr list;

    xmlSecAssert2(xmlSecKeyStoreCheckId(store, xmlSecSimpleKeysStoreId), NULL);

    list = xmlSecSimpleKeysStoreGetCtx(store);
    xmlSecAssert2(xmlSecPtrListCheckId(list, xmlSecKeyPtrListId), NULL);

    return list;
}

static int
xmlSecSimpleKeysStoreInitialize(xmlSecKeyStorePtr store) {
    xmlSecPtrListPtr list;
    int ret;

    xmlSecAssert2(xmlSecKeyStoreCheckId(store, xmlSecSimpleKeysStoreId), -1);

    list = xmlSecSimpleKeysStoreGetCtx(store);
    xmlSecAssert2(list != NULL, -1);

    ret = xmlSecPtrListInitialize(list, xmlSecKeyPtrListId);
    if(ret < 0) {
        xmlSecInternalError("xmlSecPtrListInitialize(xmlSecKeyPtrListId)",
                            xmlSecKeyStoreGetName(store));
        return(-1);
    }

    return(0);
}

static void
xmlSecSimpleKeysStoreFinalize(xmlSecKeyStorePtr store) {
    xmlSecPtrListPtr list;

    xmlSecAssert(xmlSecKeyStoreCheckId(store, xmlSecSimpleKeysStoreId));

    list = xmlSecSimpleKeysStoreGetCtx(store);
    xmlSecAssert(list != NULL);

    xmlSecPtrListFinalize(list);
}

static xmlSecKeyPtr
xmlSecSimpleKeysStoreFindKey(xmlSecKeyStorePtr store, const xmlChar* name,
                            xmlSecKeyInfoCtxPtr keyInfoCtx) {
    xmlSecPtrListPtr list;
    xmlSecKeyPtr key;
    xmlSecSize pos, size;

    xmlSecAssert2(xmlSecKeyStoreCheckId(store, xmlSecSimpleKeysStoreId), NULL);
    xmlSecAssert2(keyInfoCtx != NULL, NULL);

    list = xmlSecSimpleKeysStoreGetCtx(store);
    xmlSecAssert2(xmlSecPtrListCheckId(list, xmlSecKeyPtrListId), NULL);

    size = xmlSecPtrListGetSize(list);
    for(pos = 0; pos < size; ++pos) {
        key = (xmlSecKeyPtr)xmlSecPtrListGetItem(list, pos);
        if((key != NULL) && (xmlSecKeyMatch(key, name, &(keyInfoCtx->keyReq)) == 1)) {
            return(xmlSecKeyDuplicate(key));
        }
    }
    return(NULL);
}

