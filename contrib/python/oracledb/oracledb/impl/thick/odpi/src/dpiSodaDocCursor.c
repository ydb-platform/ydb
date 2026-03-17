//-----------------------------------------------------------------------------
// Copyright (c) 2018, 2022, Oracle and/or its affiliates.
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
// dpiSodaDocCursor.c
//   Implementation of SODA document cursors.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

//-----------------------------------------------------------------------------
// dpiSodaDocCursor__allocate() [INTERNAL]
//   Allocate and initialize a SODA document cursor structure.
//-----------------------------------------------------------------------------
int dpiSodaDocCursor__allocate(dpiSodaColl *coll, void *handle,
        dpiSodaDocCursor **cursor, dpiError *error)
{
    dpiSodaDocCursor *tempCursor;

    if (dpiGen__allocate(DPI_HTYPE_SODA_DOC_CURSOR, coll->env,
            (void**) &tempCursor, error) < 0)
        return DPI_FAILURE;
    dpiGen__setRefCount(coll, error, 1);
    tempCursor->coll = coll;
    tempCursor->handle = handle;
    *cursor = tempCursor;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaDocCursor__check() [INTERNAL]
//   Determine if the SODA document cursor is available to use.
//-----------------------------------------------------------------------------
static int dpiSodaDocCursor__check(dpiSodaDocCursor *cursor,
        const char *fnName, dpiError *error)
{
    if (dpiGen__startPublicFn(cursor, DPI_HTYPE_SODA_DOC_CURSOR, fnName,
            error) < 0)
        return DPI_FAILURE;
    if (!cursor->handle)
        return dpiError__set(error, "check closed",
                DPI_ERR_SODA_CURSOR_CLOSED);
    if (!cursor->coll->db->conn->handle || cursor->coll->db->conn->closing)
        return dpiError__set(error, "check connection", DPI_ERR_NOT_CONNECTED);
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiSodaDocCursor__free() [INTERNAL]
//   Free the memory for a SODA document cursor. Note that the reference to the
// collection must remain until after the handle is freed; otherwise, a
// segfault can take place.
//-----------------------------------------------------------------------------
void dpiSodaDocCursor__free(dpiSodaDocCursor *cursor, dpiError *error)
{
    if (cursor->handle) {
        dpiOci__handleFree(cursor->handle, DPI_OCI_HTYPE_SODA_DOC_CURSOR);
        cursor->handle = NULL;
    }
    if (cursor->coll) {
        dpiGen__setRefCount(cursor->coll, error, -1);
        cursor->coll = NULL;
    }
    dpiUtils__freeMemory(cursor);
}


//-----------------------------------------------------------------------------
// dpiSodaDocCursor_addRef() [PUBLIC]
//   Add a reference to the SODA document cursor.
//-----------------------------------------------------------------------------
int dpiSodaDocCursor_addRef(dpiSodaDocCursor *cursor)
{
    return dpiGen__addRef(cursor, DPI_HTYPE_SODA_DOC_CURSOR, __func__);
}


//-----------------------------------------------------------------------------
// dpiSodaDocCursor_close() [PUBLIC]
//   Close the cursor.
//-----------------------------------------------------------------------------
int dpiSodaDocCursor_close(dpiSodaDocCursor *cursor)
{
    dpiError error;

    if (dpiSodaDocCursor__check(cursor, __func__, &error) < 0)
        return dpiGen__endPublicFn(cursor, DPI_FAILURE, &error);
    if (cursor->handle) {
        dpiOci__handleFree(cursor->handle, DPI_OCI_HTYPE_SODA_DOC_CURSOR);
        cursor->handle = NULL;
    }
    return dpiGen__endPublicFn(cursor, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDocCursor_getNext() [PUBLIC]
//   Return the next document available from the cursor.
//-----------------------------------------------------------------------------
int dpiSodaDocCursor_getNext(dpiSodaDocCursor *cursor, UNUSED uint32_t flags,
        dpiSodaDoc **doc)
{
    dpiError error;
    void *handle;

    if (dpiSodaDocCursor__check(cursor, __func__, &error) < 0)
        return dpiGen__endPublicFn(cursor, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(cursor, doc)
    if (dpiOci__sodaDocGetNext(cursor, &handle, &error) < 0)
        return dpiGen__endPublicFn(cursor, DPI_FAILURE, &error);
    *doc = NULL;
    if (handle) {
        if (dpiSodaDoc__allocate(cursor->coll->db, handle, doc, &error) < 0) {
            dpiOci__handleFree(handle, DPI_OCI_HTYPE_SODA_DOCUMENT);
            return dpiGen__endPublicFn(cursor, DPI_FAILURE, &error);
        }
        (*doc)->binaryContent = cursor->coll->binaryContent;
    }
    return dpiGen__endPublicFn(cursor, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiSodaDocCursor_release() [PUBLIC]
//   Release a reference to the SODA document cursor.
//-----------------------------------------------------------------------------
int dpiSodaDocCursor_release(dpiSodaDocCursor *cursor)
{
    return dpiGen__release(cursor, DPI_HTYPE_SODA_DOC_CURSOR, __func__);
}
