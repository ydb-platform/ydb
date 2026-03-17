//-----------------------------------------------------------------------------
// Copyright (c) 2018, 2024, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// dpiSodaDb.c
//   Implementation of SODA database methods.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

//-----------------------------------------------------------------------------
// dpiSodaDb__checkConnected() [INTERNAL]
//   Check to see that the connection to the database is available for use.
//-----------------------------------------------------------------------------
static int dpiSodaDb__checkConnected(dpiSodaDb *db, const char *fnName,
        dpiError *error)
{
    if (dpiGen__startPublicFn(db, DPI_HTYPE_SODA_DB, fnName, error) < 0)
        return DPI_FAILURE;
    if (!db->conn->handle || db->conn->closing)
        return dpiError__set(error, "check connection", DPI_ERR_NOT_CONNECTED);
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaDb__createDocument() [INTERNAL]
//   Create a document and set the supplied values, if applicable.
//-----------------------------------------------------------------------------
static int dpiSodaDb__createDocument(dpiSodaDb *db, const char *key,
        uint32_t keyLength, const char *content, uint32_t contentLength,
        const char *mediaType, uint32_t mediaTypeLength, dpiSodaDoc **doc,
        dpiError *error)
{
    dpiSodaDoc *tempDoc;
    int detectEncoding;

    // allocate SODA document structure
    if (dpiSodaDoc__allocate(db, NULL, &tempDoc, error) < 0)
        return DPI_FAILURE;

    // set key, if applicable
    if (key && keyLength > 0) {
        if (dpiOci__attrSet(tempDoc->handle, DPI_OCI_HTYPE_SODA_DOCUMENT,
                (void*) key, keyLength, DPI_OCI_ATTR_SODA_KEY, "set key",
                error) < 0) {
            dpiSodaDoc__free(tempDoc, error);
            return DPI_FAILURE;
        }
    }

    // set binary or encoded text content, if applicable
    if (content && contentLength > 0) {
        tempDoc->binaryContent = 1;
        detectEncoding = 1;
        if (dpiOci__attrSet(tempDoc->handle, DPI_OCI_HTYPE_SODA_DOCUMENT,
                (void*) &detectEncoding, 0, DPI_OCI_ATTR_SODA_DETECT_JSON_ENC,
                "set detect encoding", error) < 0) {
            dpiSodaDoc__free(tempDoc, error);
            return DPI_FAILURE;
        }
        if (dpiOci__attrSet(tempDoc->handle, DPI_OCI_HTYPE_SODA_DOCUMENT,
                (void*) content, contentLength, DPI_OCI_ATTR_SODA_CONTENT,
                "set content", error) < 0) {
            dpiSodaDoc__free(tempDoc, error);
            return DPI_FAILURE;
        }
    }

    // set media type, if applicable
    if (mediaType && mediaTypeLength > 0) {
        if (dpiOci__attrSet(tempDoc->handle, DPI_OCI_HTYPE_SODA_DOCUMENT,
                (void*) mediaType, mediaTypeLength,
                DPI_OCI_ATTR_SODA_MEDIA_TYPE, "set media type", error) < 0) {
            dpiSodaDoc__free(tempDoc, error);
            return DPI_FAILURE;
        }
    }

    *doc = tempDoc;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaDb__getCollectionNames() [PUBLIC]
//   Internal method used for getting all collection names from the database.
// The provided cursor handle is iterated until either the limit is reached
// or there are no more collections to find.
//-----------------------------------------------------------------------------
static int dpiSodaDb__getCollectionNames(dpiSodaDb *db, void *cursorHandle,
        uint32_t limit, dpiStringList *names, dpiError *error)
{
    uint32_t numAllocatedStrings = 0, nameLength;
    void *collectionHandle;
    char *name;

    while (names->numStrings < limit || limit == 0) {

        // get next collection from cursor
        if (dpiOci__sodaCollGetNext(db->conn, cursorHandle, &collectionHandle,
                error) < 0)
            return DPI_FAILURE;
        if (!collectionHandle)
            break;

        // get name from collection
        if (dpiOci__attrGet(collectionHandle, DPI_OCI_HTYPE_SODA_COLLECTION,
                (void*) &name, &nameLength, DPI_OCI_ATTR_SODA_COLL_NAME,
                "get collection name", error) < 0) {
            dpiOci__handleFree(collectionHandle, DPI_OCI_HTYPE_SODA_COLLECTION);
            return DPI_FAILURE;
        }

        // add element to list
        if (dpiStringList__addElement(names, name, nameLength,
                &numAllocatedStrings, error) < 0) {
            dpiOci__handleFree(collectionHandle, DPI_OCI_HTYPE_SODA_COLLECTION);
            return DPI_FAILURE;
        }

        // free collection now that we have processed it successfully
        dpiOci__handleFree(collectionHandle, DPI_OCI_HTYPE_SODA_COLLECTION);

    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaDb__free() [INTERNAL]
//   Free the memory for a SODA database.
//-----------------------------------------------------------------------------
void dpiSodaDb__free(dpiSodaDb *db, dpiError *error)
{
    if (db->conn) {
        dpiGen__setRefCount(db->conn, error, -1);
        db->conn = NULL;
    }
    dpiUtils__freeMemory(db);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_addRef() [PUBLIC]
//   Add a reference to the SODA database.
//-----------------------------------------------------------------------------
int dpiSodaDb_addRef(dpiSodaDb *db)
{
    return dpiGen__addRef(db, DPI_HTYPE_SODA_DB, __func__);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_createCollection() [PUBLIC]
//   Create a new SODA collection with the given name and metadata.
//-----------------------------------------------------------------------------
int dpiSodaDb_createCollection(dpiSodaDb *db, const char *name,
        uint32_t nameLength, const char *metadata, uint32_t metadataLength,
        uint32_t flags, dpiSodaColl **coll)
{
    dpiError error;
    uint32_t mode;
    void *handle;

    // validate parameters
    if (dpiSodaDb__checkConnected(db, __func__, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(db, name)
    DPI_CHECK_PTR_AND_LENGTH(db, metadata)
    DPI_CHECK_PTR_NOT_NULL(db, coll)

    // determine OCI mode to use
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;
    if (flags & DPI_SODA_FLAGS_CREATE_COLL_MAP)
        mode |= DPI_OCI_SODA_COLL_CREATE_MAP;

    // create collection
    if (dpiOci__sodaCollCreateWithMetadata(db, name, nameLength, metadata,
            metadataLength, mode, &handle, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    if (dpiSodaColl__allocate(db, handle, coll, &error) < 0) {
        dpiOci__handleFree(handle, DPI_OCI_HTYPE_SODA_COLLECTION);
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    }
    return dpiGen__endPublicFn(db, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_createDocument() [PUBLIC]
//   Create a SODA document with binary or encoded text content that can be
// inserted in the collection or can be used to replace an existing document in
// the collection.
//-----------------------------------------------------------------------------
int dpiSodaDb_createDocument(dpiSodaDb *db, const char *key,
        uint32_t keyLength, const char *content, uint32_t contentLength,
        const char *mediaType, uint32_t mediaTypeLength, UNUSED uint32_t flags,
        dpiSodaDoc **doc)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiSodaDb__checkConnected(db, __func__, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(db, key)
    DPI_CHECK_PTR_AND_LENGTH(db, content)
    DPI_CHECK_PTR_AND_LENGTH(db, mediaType)
    DPI_CHECK_PTR_NOT_NULL(db, doc)

    // create document
    status = dpiSodaDb__createDocument(db, key, keyLength, content,
            contentLength, mediaType, mediaTypeLength, doc, &error);
    return dpiGen__endPublicFn(db, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_createJsonDocument() [PUBLIC]
//   Create a SODA document with JSON content that can be inserted in the
// collection or can be used to replace an existing document in the collection.
// This is only supported with Oracle Client 23ai and higher.
//-----------------------------------------------------------------------------
int dpiSodaDb_createJsonDocument(dpiSodaDb *db, const char *key,
        uint32_t keyLength, const dpiJsonNode *content, UNUSED uint32_t flags,
        dpiSodaDoc **doc)
{
    int status, jsonDesc;
    uint32_t tempLength;
    dpiSodaDoc *tempDoc;
    void *jsonHandle;
    dpiError error;

    // validate parameters
    if (dpiSodaDb__checkConnected(db, __func__, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(db, key)
    DPI_CHECK_PTR_NOT_NULL(db, doc)

    // only supported in Oracle Client 23ai+
    if (dpiUtils__checkClientVersion(db->env->versionInfo, 23, 4, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);

    // create document
    if (dpiSodaDb__createDocument(db, key, keyLength, NULL, 0, NULL, 0,
            &tempDoc, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);

    // populate content, if applicable
    if (content) {
        jsonDesc = 1;
        status = dpiOci__attrSet(tempDoc->handle, DPI_OCI_HTYPE_SODA_DOCUMENT,
                &jsonDesc, 0, DPI_OCI_ATTR_SODA_JSON_DESC,
                "set JSON descriptor flag", &error);
        if (status == DPI_SUCCESS) {
            status = dpiOci__attrGet(tempDoc->handle,
                    DPI_OCI_HTYPE_SODA_DOCUMENT, (void*) &jsonHandle,
                    &tempLength, DPI_OCI_ATTR_SODA_CONTENT,
                    "get JSON descriptor", &error);
        }
        if (status == DPI_SUCCESS)
            status = dpiJson__allocate(db->conn, jsonHandle, &tempDoc->json,
                    &error);
        if (status == DPI_SUCCESS)
            status = dpiJson__setValue(tempDoc->json, content, &error);
        if (status != DPI_SUCCESS) {
            dpiSodaDoc__free(tempDoc, &error);
            return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
        }
    }

    *doc = tempDoc;
    return dpiGen__endPublicFn(db, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_freeCollectionNames() [PUBLIC]
//   Free the names of the collections allocated earlier with a call to
// dpiSodaDb_getCollectionNames(). This method is deprecated and should be
// replaced with a call to dpiContext_freeStringList().
//-----------------------------------------------------------------------------
int dpiSodaDb_freeCollectionNames(dpiSodaDb *db, dpiStringList *names)
{
    dpiError error;

    // validate parameters
    if (dpiSodaDb__checkConnected(db, __func__, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(db, names)

    dpiStringList__free(names);
    return dpiGen__endPublicFn(db, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_getCollections() [PUBLIC]
//   Return a cursor to iterate over the SODA collections in the database.
//-----------------------------------------------------------------------------
int dpiSodaDb_getCollections(dpiSodaDb *db, const char *startName,
        uint32_t startNameLength, uint32_t flags, dpiSodaCollCursor **cursor)
{
    dpiError error;
    uint32_t mode;
    void *handle;

    if (dpiSodaDb__checkConnected(db, __func__, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(db, startName)
    DPI_CHECK_PTR_NOT_NULL(db, cursor)
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;
    if (dpiOci__sodaCollList(db, startName, startNameLength, &handle, mode,
            &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    if (dpiSodaCollCursor__allocate(db, handle, cursor, &error) < 0) {
        dpiOci__handleFree(handle, DPI_OCI_HTYPE_SODA_COLL_CURSOR);
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    }
    return dpiGen__endPublicFn(db, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_getCollectionNames() [PUBLIC]
//   Return the names of all collections in the provided array.
//-----------------------------------------------------------------------------
int dpiSodaDb_getCollectionNames(dpiSodaDb *db, const char *startName,
        uint32_t startNameLength, uint32_t limit, uint32_t flags,
        dpiStringList *names)
{
    dpiError error;
    uint32_t mode;
    void *handle;
    int status;

    // validate parameters
    if (dpiSodaDb__checkConnected(db, __func__, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(db, startName)
    DPI_CHECK_PTR_NOT_NULL(db, names)

    // determine OCI mode to use
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // acquire collection cursor
    if (dpiOci__sodaCollList(db, startName, startNameLength, &handle, mode,
            &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);

    // iterate over cursor to acquire collection names
    memset(names, 0, sizeof(dpiStringList));
    status = dpiSodaDb__getCollectionNames(db, handle, limit, names, &error);
    dpiOci__handleFree(handle, DPI_OCI_HTYPE_SODA_COLL_CURSOR);
    if (status < 0)
        dpiStringList__free(names);
    return dpiGen__endPublicFn(db, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_openCollection() [PUBLIC]
//   Open an existing SODA collection and return a handle to it.
//-----------------------------------------------------------------------------
int dpiSodaDb_openCollection(dpiSodaDb *db, const char *name,
        uint32_t nameLength, uint32_t flags, dpiSodaColl **coll)
{
    dpiError error;
    uint32_t mode;
    void *handle;

    if (dpiSodaDb__checkConnected(db, __func__, &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(db, name)
    DPI_CHECK_PTR_NOT_NULL(db, coll)
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;
    if (dpiOci__sodaCollOpen(db, name, nameLength, mode, &handle,
            &error) < 0)
        return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
    *coll = NULL;
    if (handle) {
        if (dpiSodaColl__allocate(db, handle, coll, &error) < 0) {
            dpiOci__handleFree(handle, DPI_OCI_HTYPE_SODA_COLLECTION);
            return dpiGen__endPublicFn(db, DPI_FAILURE, &error);
        }
    }
    return dpiGen__endPublicFn(db, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDb_release() [PUBLIC]
//   Release a reference to the SODA database.
//-----------------------------------------------------------------------------
int dpiSodaDb_release(dpiSodaDb *db)
{
    return dpiGen__release(db, DPI_HTYPE_SODA_DB, __func__);
}
