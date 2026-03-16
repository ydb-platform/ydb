//-----------------------------------------------------------------------------
// Copyright (c) 2024, Oracle and/or its affiliates.
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
// dpiVector.c
//   Implementation of vector manipulation routines.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

// forward declarations of internal functions only used in this file
static void dpiVector__clearDimensions(dpiVector *vector);

//-----------------------------------------------------------------------------
// dpiVector__allocate() [INTERNAL]
//   Allocate and initialize a vector object.
//-----------------------------------------------------------------------------
int dpiVector__allocate(dpiConn *conn, dpiVector **vector, dpiError *error)
{
    dpiVector *tempVector;

    if (dpiUtils__checkClientVersion(conn->env->versionInfo, 23, 4, error) < 0)
        return DPI_FAILURE;
    if (dpiGen__allocate(DPI_HTYPE_VECTOR, conn->env, (void**) &tempVector,
            error) < 0)
        return DPI_FAILURE;
    dpiGen__setRefCount(conn, error, 1);
    tempVector->conn = conn;
    if (dpiOci__descriptorAlloc(conn->env->handle, &tempVector->handle,
            DPI_OCI_DTYPE_VECTOR, "allocate vector descriptor", error) < 0) {
        dpiVector__free(tempVector, error);
        return DPI_FAILURE;
    }

    *vector = tempVector;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiVector__clearDimensions() [INTERNAL]
//   Clear the dimensions cached in the vector.
//-----------------------------------------------------------------------------
static void dpiVector__clearDimensions(dpiVector *vector)
{
    if (vector->dimensions) {
        dpiUtils__freeMemory(vector->dimensions);
        vector->dimensions = NULL;
    }
}


//-----------------------------------------------------------------------------
// dpiVector__free() [INTERNAL]
//   Free the vector object contents.
//-----------------------------------------------------------------------------
void dpiVector__free(dpiVector *vector, dpiError *error)
{
    if (vector->handle) {
        dpiOci__descriptorFree(vector->handle, DPI_OCI_DTYPE_VECTOR);
        vector->handle = NULL;
    }
    if (vector->conn) {
        dpiGen__setRefCount(vector->conn, error, -1);
        vector->conn = NULL;
    }
    dpiVector__clearDimensions(vector);
    dpiUtils__freeMemory(vector);
}


//-----------------------------------------------------------------------------
// dpiVector__getValue() [INTERNAL]
//   Gets information about the vector.
//-----------------------------------------------------------------------------
int dpiVector__getValue(dpiVector *vector, dpiVectorInfo *info,
        dpiError *error)
{
    uint32_t numElements;

    // only need to acquire information if it was not already cached
    if (!vector->dimensions) {

        // determine the format of the vector
        if (dpiOci__attrGet(vector->handle, DPI_OCI_DTYPE_VECTOR,
                &vector->format, 0, DPI_OCI_ATTR_VECTOR_DATA_FORMAT,
                "get vector format", error) < 0)
            return DPI_FAILURE;

        // determine the number of dimensions in the vector
        if (dpiOci__attrGet(vector->handle, DPI_OCI_DTYPE_VECTOR,
                &vector->numDimensions, 0, DPI_OCI_ATTR_VECTOR_DIMENSION,
                "get number of vector dimensions", error) < 0)
            return DPI_FAILURE;

        // determine the size of each dimension
        numElements = vector->numDimensions;
        switch (vector->format) {
            case DPI_VECTOR_FORMAT_BINARY:
                vector->dimensionSize = sizeof(uint8_t);
                numElements = (uint32_t) (vector->numDimensions / 8);
                break;
            case DPI_VECTOR_FORMAT_FLOAT32:
                vector->dimensionSize = sizeof(float);
                break;
            case DPI_VECTOR_FORMAT_FLOAT64:
                vector->dimensionSize = sizeof(double);
                break;
            case DPI_VECTOR_FORMAT_INT8:
                vector->dimensionSize = sizeof(int8_t);
                break;
            default:
                return dpiError__set(error, "check vector format",
                        DPI_ERR_UNSUPPORTED_VECTOR_FORMAT, vector->format);
        }

        // allocate a buffer for the dimensions
        if (dpiUtils__allocateMemory(numElements, vector->dimensionSize, 0,
                "allocate vector dimensions", &vector->dimensions, error) < 0)
            return DPI_FAILURE;

        // populate buffer with array data
        if (dpiOci__vectorToArray(vector, error) < 0)
            return DPI_FAILURE;

    }

    // transfer cached information to the output structure
    info->format = vector->format;
    info->numDimensions = vector->numDimensions;
    info->dimensionSize = vector->dimensionSize;
    info->dimensions.asPtr = vector->dimensions;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiVector_addRef() [PUBLIC]
//   Add a reference to the vector object.
//-----------------------------------------------------------------------------
int dpiVector_addRef(dpiVector *vector)
{
    return dpiGen__addRef(vector, DPI_HTYPE_VECTOR, __func__);
}


//-----------------------------------------------------------------------------
// dpiVector_getValue() [PUBLIC]
//   Returns information about the vector to the caller.
//-----------------------------------------------------------------------------
int dpiVector_getValue(dpiVector *vector, dpiVectorInfo *info)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(vector, DPI_HTYPE_VECTOR, __func__, &error) < 0)
        return DPI_FAILURE;
    DPI_CHECK_PTR_NOT_NULL(vector, info)
    status = dpiVector__getValue(vector, info, &error);
    return dpiGen__endPublicFn(vector, status, &error);
}


//-----------------------------------------------------------------------------
// dpiVector_release() [PUBLIC]
//   Release a reference to the vector object.
//-----------------------------------------------------------------------------
int dpiVector_release(dpiVector *vector)
{
    return dpiGen__release(vector, DPI_HTYPE_VECTOR, __func__);
}


//-----------------------------------------------------------------------------
// dpiVector_setValue() [PUBLIC]
//   Sets the vector value from the supplied information.
//-----------------------------------------------------------------------------
int dpiVector_setValue(dpiVector *vector, dpiVectorInfo *info)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(vector, DPI_HTYPE_VECTOR, __func__, &error) < 0)
        return DPI_FAILURE;
    DPI_CHECK_PTR_NOT_NULL(vector, info)
    status = dpiOci__vectorFromArray(vector, info, &error);
    return dpiGen__endPublicFn(vector, status, &error);
}
