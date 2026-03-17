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
// dpiSodaColl.c
//   Implementation of SODA collections.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

// forward declarations of internal functions only used in this file
static int dpiSodaColl__populateOperOptions(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, void *handle, dpiError *error);


//-----------------------------------------------------------------------------
// dpiSodaColl__allocate() [INTERNAL]
//   Allocate and initialize a SODA collection structure.
//-----------------------------------------------------------------------------
int dpiSodaColl__allocate(dpiSodaDb *db, void *handle, dpiSodaColl **coll,
        dpiError *error)
{
    uint8_t sqlType, contentType;
    dpiSodaColl *tempColl;

    if (dpiOci__attrGet(handle, DPI_OCI_HTYPE_SODA_COLLECTION,
            (void*) &sqlType, 0, DPI_OCI_ATTR_SODA_CTNT_SQL_TYPE,
            "get content sql type", error) < 0)
        return DPI_FAILURE;
    if (dpiGen__allocate(DPI_HTYPE_SODA_COLL, db->env, (void**) &tempColl,
            error) < 0)
        return DPI_FAILURE;
    dpiGen__setRefCount(db, error, 1);
    tempColl->db = db;
    tempColl->handle = handle;
    if (sqlType == DPI_SQLT_BLOB) {
        tempColl->binaryContent = 1;
        contentType = 0;
        dpiOci__attrGet(handle, DPI_OCI_HTYPE_SODA_COLLECTION,
                (void*) &contentType, 0, DPI_OCI_ATTR_SODA_CTNT_FORMAT,
                    NULL, error);
        if (contentType == DPI_OCI_JSON_FORMAT_OSON)
            tempColl->binaryContent = 0;
    }
    *coll = tempColl;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__check() [INTERNAL]
//   Determine if the SODA collection is available to use.
//-----------------------------------------------------------------------------
static int dpiSodaColl__check(dpiSodaColl *coll, const char *fnName,
        dpiError *error)
{
    if (dpiGen__startPublicFn(coll, DPI_HTYPE_SODA_COLL, fnName, error) < 0)
        return DPI_FAILURE;
    if (!coll->db->conn->handle || coll->db->conn->closing)
        return dpiError__set(error, "check connection", DPI_ERR_NOT_CONNECTED);
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__createOperOptions() [INTERNAL]
//   Create a SODA operation options handle with the specified information.
//-----------------------------------------------------------------------------
static int dpiSodaColl__createOperOptions(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, void **handle, dpiError *error)
{
    dpiSodaOperOptions localOptions;

    // if no options specified, use default values
    if (!options) {
        dpiContext__initSodaOperOptions(&localOptions);
        options = &localOptions;
    }

    // allocate new handle
    if (dpiOci__handleAlloc(coll->env->handle, handle,
            DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
            "allocate SODA operation options handle", error) < 0)
        return DPI_FAILURE;

    // populate handle attributes
    if (dpiSodaColl__populateOperOptions(coll, options, *handle, error) < 0) {
        dpiOci__handleFree(*handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
        return DPI_FAILURE;
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__find() [INTERNAL]
//   Perform a find of SODA documents by creating an operation options handle
// and populating it with the requested options. Once the find is complete,
// return either a cursor or a document.
//-----------------------------------------------------------------------------
static int dpiSodaColl__find(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, uint32_t flags,
        dpiSodaDocCursor **cursor, dpiSodaDoc **doc, dpiError *error)
{
    uint32_t ociMode, returnHandleType, ociFlags;
    void *optionsHandle, *ociReturnHandle;
    int status;

    // determine OCI mode to pass
    ociMode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        ociMode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // create new OCI operation options handle
    if (dpiSodaColl__createOperOptions(coll, options, &optionsHandle,
            error) < 0)
        return DPI_FAILURE;

    // determine OCI flags to use
    if (coll->binaryContent || coll->env->context->sodaUseJsonDesc) {
        ociFlags = DPI_OCI_SODA_AS_STORED;
    } else {
        ociFlags = DPI_OCI_SODA_AS_AL32UTF8;
    }

    // perform actual find
    if (cursor) {
        *cursor = NULL;
        status = dpiOci__sodaFind(coll, optionsHandle, ociFlags, ociMode,
                &ociReturnHandle, error);
    } else {
        *doc = NULL;
        status = dpiOci__sodaFindOne(coll, optionsHandle, ociFlags, ociMode,
                &ociReturnHandle, error);
    }
    dpiOci__handleFree(optionsHandle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
    if (status < 0)
        return DPI_FAILURE;

    // return cursor or document, as appropriate
    if (cursor) {
        status = dpiSodaDocCursor__allocate(coll, ociReturnHandle, cursor,
                error);
        returnHandleType = DPI_OCI_HTYPE_SODA_DOC_CURSOR;
    } else if (ociReturnHandle) {
        status = dpiSodaDoc__allocate(coll->db, ociReturnHandle, doc, error);
        returnHandleType = DPI_OCI_HTYPE_SODA_DOCUMENT;
    }
    if (status < 0)
        dpiOci__handleFree(ociReturnHandle, returnHandleType);

    return status;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__free() [INTERNAL]
//   Free the memory for a SODA collection. Note that the reference to the
// database must remain until after the handle is freed; otherwise, a segfault
// can take place.
//-----------------------------------------------------------------------------
void dpiSodaColl__free(dpiSodaColl *coll, dpiError *error)
{
    if (coll->handle) {
        dpiOci__handleFree(coll->handle, DPI_OCI_HTYPE_SODA_COLLECTION);
        coll->handle = NULL;
    }
    if (coll->db) {
        dpiGen__setRefCount(coll->db, error, -1);
        coll->db = NULL;
    }
    dpiUtils__freeMemory(coll);
}


//-----------------------------------------------------------------------------
// dpiSodaColl__getDocCount() [INTERNAL]
//   Internal method for getting document count.
//-----------------------------------------------------------------------------
static int dpiSodaColl__getDocCount(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, uint32_t flags, uint64_t *count,
        dpiError *error)
{
    void *optionsHandle;
    uint32_t ociMode;
    int status;

    // determine OCI mode to pass
    ociMode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        ociMode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // create new OCI operation options handle
    if (dpiSodaColl__createOperOptions(coll, options, &optionsHandle,
            error) < 0)
        return DPI_FAILURE;

    // perform actual document count
    status = dpiOci__sodaDocCount(coll, optionsHandle, ociMode, count, error);
    dpiOci__handleFree(optionsHandle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
    return status;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__insertMany() [INTERNAL]
//   Insert multiple documents into the collection and return handles to the
// newly created documents, if desired.
//-----------------------------------------------------------------------------
static int dpiSodaColl__insertMany(dpiSodaColl *coll, uint32_t numDocs,
        void **docHandles, uint32_t flags, dpiSodaDoc **insertedDocs,
        void *operOptionsHandle, dpiError *error)
{
    void *outputOptionsHandle;
    uint32_t i, j, mode;
    uint64_t docCount;
    int status;

    // create OCI output options handle
    if (dpiOci__handleAlloc(coll->env->handle, &outputOptionsHandle,
            DPI_OCI_HTYPE_SODA_OUTPUT_OPTIONS,
            "allocate SODA output options handle", error) < 0)
        return DPI_FAILURE;

    // determine mode to pass
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // perform actual bulk insert
    if (operOptionsHandle) {
        status = dpiOci__sodaBulkInsertAndGetWithOpts(coll, docHandles,
                numDocs, operOptionsHandle, outputOptionsHandle, mode, error);
        dpiOci__handleFree(operOptionsHandle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
    } else if (insertedDocs) {
        status = dpiOci__sodaBulkInsertAndGet(coll, docHandles, numDocs,
                outputOptionsHandle, mode, error);
    } else {
        status = dpiOci__sodaBulkInsert(coll, docHandles, numDocs,
                outputOptionsHandle, mode, error);
    }

    // on failure, determine the number of documents that were successfully
    // inserted and store that information in the error buffer
    if (status < 0) {
        dpiOci__attrGet(outputOptionsHandle, DPI_OCI_HTYPE_SODA_OUTPUT_OPTIONS,
                (void*) &docCount, 0, DPI_OCI_ATTR_SODA_DOC_COUNT,
                NULL, error);
        error->buffer->offset = (uint32_t) docCount;
    }
    dpiOci__handleFree(outputOptionsHandle, DPI_OCI_HTYPE_SODA_OUTPUT_OPTIONS);

    // on failure, if using the "AndGet" variant, any document handles that
    // were created need to be freed
    if (insertedDocs && status < 0) {
        for (i = 0; i < numDocs; i++) {
            if (docHandles[i]) {
                dpiOci__handleFree(docHandles[i], DPI_OCI_HTYPE_SODA_DOCUMENT);
                docHandles[i] = NULL;
            }
        }
    }
    if (status < 0)
        return DPI_FAILURE;

    // return document handles, if desired
    if (insertedDocs) {
        for (i = 0; i < numDocs; i++) {
            if (dpiSodaDoc__allocate(coll->db, docHandles[i], &insertedDocs[i],
                    error) < 0) {
                for (j = 0; j < i; j++) {
                    dpiSodaDoc__free(insertedDocs[j], error);
                    insertedDocs[j] = NULL;
                }
                for (j = i; j < numDocs; j++) {
                    dpiOci__handleFree(docHandles[i],
                            DPI_OCI_HTYPE_SODA_DOCUMENT);
                }
                return DPI_FAILURE;
            }
        }
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__listIndexes() [INTERNAL]
//   Return the list of indexes associated with the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl__listIndexes(dpiSodaColl *coll, uint32_t flags,
        dpiStringList *list, dpiError *error)
{
    uint32_t ptrLen, numAllocatedStrings = 0;
    void *listHandle = NULL;
    void **elem, *elemInd;
    int32_t i, listLen;
    int exists, status;
    char *ptr;

    if (dpiUtils__checkClientVersionMulti(coll->env->versionInfo, 19, 13,
            21, 3, error) < 0)
        return DPI_FAILURE;
    if (dpiOci__sodaIndexList(coll, flags, &listHandle, error) < 0)
        return DPI_FAILURE;
    status = dpiOci__collSize(coll->db->conn, listHandle, &listLen, error);
    for (i = 0; i < listLen && status == DPI_SUCCESS; i++) {
        status = dpiOci__collGetElem(coll->db->conn, listHandle, i, &exists,
                (void**) &elem, &elemInd, error);
        if (status < 0)
            break;
        status = dpiOci__stringPtr(coll->env->handle, *elem, &ptr);
        if (status < 0)
            break;
        status = dpiOci__stringSize(coll->env->handle, *elem, &ptrLen);
        if (status < 0)
            break;
        status = dpiStringList__addElement(list, ptr, ptrLen,
                &numAllocatedStrings, error);
    }

    if (listHandle)
        dpiOci__objectFree(coll->env->handle, listHandle, 0, error);
    return status;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__populateOperOptions() [INTERNAL]
//   Populate the SODA operation options handle with the information found in
// the supplied structure.
//-----------------------------------------------------------------------------
static int dpiSodaColl__populateOperOptions(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, void *handle, dpiError *error)
{
    // set multiple keys, if applicable
    if (options->numKeys > 0) {
        if (dpiOci__sodaOperKeysSet(options, handle, error) < 0)
            return DPI_FAILURE;
    }

    // set single key, if applicable
    if (options->keyLength > 0) {
        if (dpiOci__attrSet(handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
                (void*) options->key, options->keyLength,
                DPI_OCI_ATTR_SODA_KEY, "set key", error) < 0)
            return DPI_FAILURE;
    }

    // set single version, if applicable
    if (options->versionLength > 0) {
        if (dpiOci__attrSet(handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
                (void*) options->version, options->versionLength,
                DPI_OCI_ATTR_SODA_VERSION, "set version", error) < 0)
            return DPI_FAILURE;
    }

    // set filter, if applicable
    if (options->filterLength > 0) {
        if (dpiOci__attrSet(handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
                (void*) options->filter, options->filterLength,
                DPI_OCI_ATTR_SODA_FILTER, "set filter", error) < 0)
            return DPI_FAILURE;
    }

    // set skip count, if applicable
    if (options->skip > 0) {
        if (dpiOci__attrSet(handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
                (void*) &options->skip, 0, DPI_OCI_ATTR_SODA_SKIP,
                "set skip count", error) < 0)
            return DPI_FAILURE;
    }

    // set limit, if applicable
    if (options->limit > 0) {
        if (dpiOci__attrSet(handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
                (void*) &options->limit, 0, DPI_OCI_ATTR_SODA_LIMIT,
                "set limit", error) < 0)
            return DPI_FAILURE;
    }

    // set fetch array size, if applicable (only available in 19.5+ client)
    if (options->fetchArraySize > 0) {
        if (dpiUtils__checkClientVersion(coll->env->versionInfo, 19, 5,
                error) < 0)
            return DPI_FAILURE;
        if (dpiOci__attrSet(handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
                (void*) &options->fetchArraySize, 0,
                DPI_OCI_ATTR_SODA_FETCH_ARRAY_SIZE, "set fetch array size",
                error) < 0)
            return DPI_FAILURE;
    }

    // set hint, if applicable (only available in 19.11+/21.3+ client)
    if (options->hintLength > 0) {
        if (dpiUtils__checkClientVersionMulti(coll->env->versionInfo, 19, 11,
                21, 3, error) < 0)
            return DPI_FAILURE;
        if (dpiOci__attrSet(handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
                (void*) options->hint, options->hintLength,
                DPI_OCI_ATTR_SODA_HINT, "set hint", error) < 0)
            return DPI_FAILURE;
    }

    // set lock, if applicable (only available in 19.11+/21.3+ client)
    if (options->lock) {
        if (dpiUtils__checkClientVersionMulti(coll->env->versionInfo, 19, 11,
                21, 3, error) < 0)
            return DPI_FAILURE;
        if (dpiOci__attrSet(handle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS,
                (void*) &options->lock, 0, DPI_OCI_ATTR_SODA_LOCK, "set lock",
                error) < 0)
            return DPI_FAILURE;
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__remove() [INTERNAL]
//   Internal method for removing documents from a collection.
//-----------------------------------------------------------------------------
static int dpiSodaColl__remove(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, uint32_t flags, uint64_t *count,
        dpiError *error)
{
    void *optionsHandle;
    uint32_t mode;
    int status;

    // determine OCI mode to pass
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // create new OCI operation options handle
    if (dpiSodaColl__createOperOptions(coll, options, &optionsHandle,
            error) < 0)
        return DPI_FAILURE;

    // remove documents from collection
    status = dpiOci__sodaRemove(coll, optionsHandle, mode, count, error);
    dpiOci__handleFree(optionsHandle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
    return status;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__replace() [INTERNAL]
//   Internal method for replacing a document in the collection.
//-----------------------------------------------------------------------------
static int dpiSodaColl__replace(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, dpiSodaDoc *doc, uint32_t flags,
        int *replaced, dpiSodaDoc **replacedDoc, dpiError *error)
{
    void *docHandle, *optionsHandle;
    int status, dummyIsReplaced;
    uint32_t mode;

    // use dummy value if the replaced flag is not desired
    if (!replaced)
        replaced = &dummyIsReplaced;

    // determine OCI mode to pass
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // create new OCI operation options handle
    if (dpiSodaColl__createOperOptions(coll, options, &optionsHandle,
            error) < 0)
        return DPI_FAILURE;

    // replace document in collection
    // use "AndGet" variant if the replaced document is requested
    docHandle = doc->handle;
    if (!replacedDoc) {
        status = dpiOci__sodaReplOne(coll, optionsHandle, docHandle, mode,
                replaced, error);
    } else {
        *replacedDoc = NULL;
        status = dpiOci__sodaReplOneAndGet(coll, optionsHandle, &docHandle,
                mode, replaced, error);
        if (status == 0 && docHandle) {
            status = dpiSodaDoc__allocate(coll->db, docHandle, replacedDoc,
                    error);
            if (status < 0)
                dpiOci__handleFree(docHandle, DPI_OCI_HTYPE_SODA_DOCUMENT);
        }
    }

    dpiOci__handleFree(optionsHandle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
    return status;
}


//-----------------------------------------------------------------------------
// dpiSodaColl__save() [INTERNAL]
//   Internal method for saving a document in the collection.
//-----------------------------------------------------------------------------
static int dpiSodaColl__save(dpiSodaColl *coll, dpiSodaDoc *doc,
        uint32_t flags, dpiSodaDoc **savedDoc, void *optionsHandle,
        dpiError *error)
{
    void *docHandle;
    uint32_t mode;
    int status;

    // determine OCI mode to pass
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // save document in collection
    // use "AndGet" variant if the saved document is requested
    docHandle = doc->handle;
    if (!savedDoc) {
        status = dpiOci__sodaSave(coll, docHandle, mode, error);
    } else {
        *savedDoc = NULL;
        if (optionsHandle) {
            status = dpiOci__sodaSaveAndGetWithOpts(coll, &docHandle,
                    optionsHandle, mode, error);
            dpiOci__handleFree(optionsHandle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
        } else {
            status = dpiOci__sodaSaveAndGet(coll, &docHandle, mode, error);
        }
        if (status == 0 && docHandle) {
            status = dpiSodaDoc__allocate(coll->db, docHandle, savedDoc,
                    error);
            if (status < 0)
                dpiOci__handleFree(docHandle, DPI_OCI_HTYPE_SODA_DOCUMENT);
        }
    }

    return status;
}


//-----------------------------------------------------------------------------
// dpiSodaColl_addRef() [PUBLIC]
//   Add a reference to the SODA collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_addRef(dpiSodaColl *coll)
{
    return dpiGen__addRef(coll, DPI_HTYPE_SODA_COLL, __func__);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_createIndex() [PUBLIC]
//   Create an index on the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_createIndex(dpiSodaColl *coll, const char *indexSpec,
        uint32_t indexSpecLength, uint32_t flags)
{
    dpiError error;
    uint32_t mode;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(coll, indexSpec)

    // determine mode to pass to OCI
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // create index
    status = dpiOci__sodaIndexCreate(coll, indexSpec, indexSpecLength, mode,
            &error);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_drop() [PUBLIC]
//   Drop the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_drop(dpiSodaColl *coll, uint32_t flags, int *isDropped)
{
    int status, dummyIsDropped;
    dpiError error;
    uint32_t mode;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    // isDropped is not a mandatory parameter, but it is for OCI
    if (!isDropped)
        isDropped = &dummyIsDropped;

    // determine mode to pass to OCI
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // drop collection
    status = dpiOci__sodaCollDrop(coll, isDropped, mode, &error);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_dropIndex() [PUBLIC]
//   Drop the index on the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_dropIndex(dpiSodaColl *coll, const char *name,
        uint32_t nameLength, uint32_t flags, int *isDropped)
{
    int status, dummyIsDropped;
    dpiError error;
    uint32_t mode;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(coll, name)

    // isDropped is not a mandatory parameter, but it is for OCI
    if (!isDropped)
        isDropped = &dummyIsDropped;

    // determine mode to pass to OCI
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;
    if (flags & DPI_SODA_FLAGS_INDEX_DROP_FORCE)
        mode |= DPI_OCI_SODA_INDEX_DROP_FORCE;

    // drop index
    status = dpiOci__sodaIndexDrop(coll, name, nameLength, mode, isDropped,
            &error);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_find() [PUBLIC]
//   Find documents in a collection and return a cursor.
//-----------------------------------------------------------------------------
int dpiSodaColl_find(dpiSodaColl *coll, const dpiSodaOperOptions *options,
        uint32_t flags, dpiSodaDocCursor **cursor)
{
    dpiError error;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, cursor)

    // perform find and return a cursor
    if (dpiSodaColl__find(coll, options, flags, cursor, NULL, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    return dpiGen__endPublicFn(coll, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_findOne() [PUBLIC]
//   Find a single document in a collection and return it.
//-----------------------------------------------------------------------------
int dpiSodaColl_findOne(dpiSodaColl *coll, const dpiSodaOperOptions *options,
        uint32_t flags, dpiSodaDoc **doc)
{
    dpiError error;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, doc)

    // perform find and return a document
    if (dpiSodaColl__find(coll, options, flags, NULL, doc, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    return dpiGen__endPublicFn(coll, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_getDataGuide() [PUBLIC]
//   Return the data guide document for the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_getDataGuide(dpiSodaColl *coll, uint32_t flags,
        dpiSodaDoc **doc)
{
    void *docHandle;
    dpiError error;
    uint32_t mode;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, doc)

    // determine mode to pass
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // get data guide
    if (dpiOci__sodaDataGuideGet(coll, &docHandle, mode, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    if (!docHandle) {
        *doc = NULL;
    } else if (dpiSodaDoc__allocate(coll->db, docHandle, doc, &error) < 0) {
        dpiOci__handleFree(docHandle, DPI_OCI_HTYPE_SODA_DOCUMENT);
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    }

    return dpiGen__endPublicFn(coll, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_getDocCount() [PUBLIC]
//   Return the number of documents in the collection that match the specified
// criteria.
//-----------------------------------------------------------------------------
int dpiSodaColl_getDocCount(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, uint32_t flags, uint64_t *count)
{
    dpiError error;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, count)

    // get document count
    if (dpiSodaColl__getDocCount(coll, options, flags, count, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    return dpiGen__endPublicFn(coll, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_getMetadata() [PUBLIC]
//   Return the metadata for the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_getMetadata(dpiSodaColl *coll, const char **value,
        uint32_t *valueLength)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, value)
    DPI_CHECK_PTR_NOT_NULL(coll, valueLength)

    // get attribute value
    status = dpiOci__attrGet(coll->handle, DPI_OCI_HTYPE_SODA_COLLECTION,
            (void*) value, valueLength, DPI_OCI_ATTR_SODA_COLL_DESCRIPTOR,
            "get value", &error);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_getName() [PUBLIC]
//   Return the name of the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_getName(dpiSodaColl *coll, const char **value,
        uint32_t *valueLength)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, value)
    DPI_CHECK_PTR_NOT_NULL(coll, valueLength)

    // get attribute value
    status = dpiOci__attrGet(coll->handle, DPI_OCI_HTYPE_SODA_COLLECTION,
            (void*) value, valueLength, DPI_OCI_ATTR_SODA_COLL_NAME,
            "get value", &error);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_insertMany() [PUBLIC]
//   Similar to dpiSodaColl_insertManyWithOptions() but passing NULL options.
//-----------------------------------------------------------------------------
int dpiSodaColl_insertMany(dpiSodaColl *coll, uint32_t numDocs,
        dpiSodaDoc **docs, uint32_t flags, dpiSodaDoc **insertedDocs)
{
    return dpiSodaColl_insertManyWithOptions(coll, numDocs, docs, NULL, flags,
            insertedDocs);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_insertManyWithOptions() [PUBLIC]
//   Insert multiple documents into the collection and return handles to the
// newly created documents, if desired.
//-----------------------------------------------------------------------------
int dpiSodaColl_insertManyWithOptions(dpiSodaColl *coll, uint32_t numDocs,
        dpiSodaDoc **docs, dpiSodaOperOptions *options, uint32_t flags,
        dpiSodaDoc **insertedDocs)
{
    void **docHandles, *optionsHandle = NULL;
    dpiError error;
    uint32_t i;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, docs)
    if (numDocs == 0) {
        dpiError__set(&error, "check num documents", DPI_ERR_ARRAY_SIZE_ZERO);
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    }
    for (i = 0; i < numDocs; i++) {
        if (dpiGen__checkHandle(docs[i], DPI_HTYPE_SODA_DOC, "check document",
                &error) < 0)
            return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    }

    // bulk insert is only supported with Oracle Client 18.5+
    if (dpiUtils__checkClientVersion(coll->env->versionInfo, 18, 5,
            &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    // if options specified and the newly created document is to be returned,
    // create the operation options handle
    if (insertedDocs && options) {
        if (dpiUtils__checkClientVersionMulti(coll->env->versionInfo, 19, 11,
                21, 3, &error) < 0)
            return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
        if (dpiSodaColl__createOperOptions(coll, options, &optionsHandle,
                &error) < 0)
            return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    }

    // create and populate array to hold document handles
    if (dpiUtils__allocateMemory(numDocs, sizeof(void*), 1,
            "allocate document handles", (void**) &docHandles, &error) < 0) {
        if (optionsHandle)
            dpiOci__handleFree(optionsHandle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    }
    for (i = 0; i < numDocs; i++)
        docHandles[i] = docs[i]->handle;

    // perform bulk insert
    status = dpiSodaColl__insertMany(coll, numDocs, docHandles, flags,
            insertedDocs, optionsHandle, &error);
    dpiUtils__freeMemory(docHandles);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_insertOne() [PUBLIC]
//   Similar to dpiSodaColl_insertOneWithOptions() but passing NULL options.
//-----------------------------------------------------------------------------
int dpiSodaColl_insertOne(dpiSodaColl *coll, dpiSodaDoc *doc, uint32_t flags,
        dpiSodaDoc **insertedDoc)
{
    return dpiSodaColl_insertOneWithOptions(coll, doc, NULL, flags,
            insertedDoc);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_insertOneWithOptions() [PUBLIC]
//   Insert a document into the collection and return a handle to the newly
// created document, if desired.
//-----------------------------------------------------------------------------
int dpiSodaColl_insertOneWithOptions(dpiSodaColl *coll, dpiSodaDoc *doc,
        dpiSodaOperOptions *options, uint32_t flags, dpiSodaDoc **insertedDoc)
{
    void *docHandle, *optionsHandle = NULL;
    dpiError error;
    uint32_t mode;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(doc, DPI_HTYPE_SODA_DOC, "check document",
            &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    // if options specified and the newly created document is to be returned,
    // create the operation options handle
    if (insertedDoc && options) {
        if (dpiUtils__checkClientVersionMulti(coll->env->versionInfo, 19, 11,
                21, 3, &error) < 0)
            return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
        if (dpiSodaColl__createOperOptions(coll, options, &optionsHandle,
                &error) < 0)
            return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    }

    // determine OCI mode to use
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;

    // insert document into collection
    // use "AndGet" variants if the inserted document is requested
    docHandle = doc->handle;
    if (!insertedDoc)
        status = dpiOci__sodaInsert(coll, docHandle, mode, &error);
    else {
        if (options) {
            status = dpiOci__sodaInsertAndGetWithOpts(coll, &docHandle,
                    optionsHandle, mode, &error);
            dpiOci__handleFree(optionsHandle, DPI_OCI_HTYPE_SODA_OPER_OPTIONS);
        } else {
            status = dpiOci__sodaInsertAndGet(coll, &docHandle, mode, &error);
        }
        if (status == 0) {
            status = dpiSodaDoc__allocate(coll->db, docHandle, insertedDoc,
                    &error);
            if (status < 0)
                dpiOci__handleFree(docHandle, DPI_OCI_HTYPE_SODA_DOCUMENT);
        }
    }

    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_listIndexes() [PUBLIC]
//   Return the list of indexes associated with the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_listIndexes(dpiSodaColl *coll, uint32_t flags,
        dpiStringList *list)
{
    dpiError error;
    uint32_t mode;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, list)

    // get indexes
    memset(list, 0, sizeof(dpiStringList));
    mode = DPI_OCI_DEFAULT;
    if (flags & DPI_SODA_FLAGS_ATOMIC_COMMIT)
        mode |= DPI_OCI_SODA_ATOMIC_COMMIT;
    status = dpiSodaColl__listIndexes(coll, mode, list, &error);
    if (status < 0)
        dpiStringList__free(list);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_release() [PUBLIC]
//   Release a reference to the SODA collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_release(dpiSodaColl *coll)
{
    return dpiGen__release(coll, DPI_HTYPE_SODA_COLL, __func__);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_remove() [PUBLIC]
//   Remove the documents from the collection that match the given criteria.
//-----------------------------------------------------------------------------
int dpiSodaColl_remove(dpiSodaColl *coll, const dpiSodaOperOptions *options,
        uint32_t flags, uint64_t *count)
{
    dpiError error;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(coll, count)

    // perform removal
    if (dpiSodaColl__remove(coll, options, flags, count, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    return dpiGen__endPublicFn(coll, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_replaceOne() [PUBLIC]
//   Replace the first document in the collection that matches the given
// criteria. Returns a handle to the newly replaced document, if desired.
//-----------------------------------------------------------------------------
int dpiSodaColl_replaceOne(dpiSodaColl *coll,
        const dpiSodaOperOptions *options, dpiSodaDoc *doc, uint32_t flags,
        int *replaced, dpiSodaDoc **replacedDoc)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(doc, DPI_HTYPE_SODA_DOC, "check document",
            &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    // perform replace
    status = dpiSodaColl__replace(coll, options, doc, flags, replaced,
            replacedDoc, &error);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_save() [PUBLIC]
//   Similar to dpiSodaColl_saveWithOptions() but passing NULL options.
//-----------------------------------------------------------------------------
int dpiSodaColl_save(dpiSodaColl *coll, dpiSodaDoc *doc, uint32_t flags,
        dpiSodaDoc **savedDoc)
{
    return dpiSodaColl_saveWithOptions(coll, doc, NULL, flags, savedDoc);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_saveWithOptions() [PUBLIC]
//   Save the document into the collection. This method is equivalent to
// dpiSodaColl_insertOne() except that if client-assigned keys are used, and
// the document with the specified key already exists in the collection, it
// will be replaced with the input document. Returns a handle to the new
// document, if desired.
//-----------------------------------------------------------------------------
int dpiSodaColl_saveWithOptions(dpiSodaColl *coll, dpiSodaDoc *doc,
        dpiSodaOperOptions *options, uint32_t flags, dpiSodaDoc **savedDoc)
{
    void *optionsHandle = NULL;
    dpiError error;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(doc, DPI_HTYPE_SODA_DOC, "check document",
            &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    // save is only supported with Oracle Client 19.9+
    if (dpiUtils__checkClientVersion(coll->env->versionInfo, 19, 9,
            &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    // if options specified and the newly created document is to be returned,
    // create the operation options handle
    if (savedDoc && options) {
        if (dpiUtils__checkClientVersionMulti(coll->env->versionInfo, 19, 11,
                21, 3, &error) < 0)
            return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
        if (dpiSodaColl__createOperOptions(coll, options, &optionsHandle,
                &error) < 0)
            return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);
    }

    // perform save
    status = dpiSodaColl__save(coll, doc, flags, savedDoc, optionsHandle,
            &error);
    return dpiGen__endPublicFn(coll, status, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaColl_truncate() [PUBLIC]
//   Remove all of the documents in the collection.
//-----------------------------------------------------------------------------
int dpiSodaColl_truncate(dpiSodaColl *coll)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiSodaColl__check(coll, __func__, &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    // truncate is only supported with Oracle Client 20+
    if (dpiUtils__checkClientVersion(coll->env->versionInfo, 20, 1,
            &error) < 0)
        return dpiGen__endPublicFn(coll, DPI_FAILURE, &error);

    // perform truncate
    status = dpiOci__sodaCollTruncate(coll, &error);
    return dpiGen__endPublicFn(coll, status, &error);
}
