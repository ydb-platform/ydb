//-----------------------------------------------------------------------------
// Copyright (c) 2016, 2022, Oracle and/or its affiliates.
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
// dpiEnqOptions.c
//   Implementation of AQ enqueue options.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

//-----------------------------------------------------------------------------
// dpiEnqOptions__create() [INTERNAL]
//   Create a new subscription structure and return it. In case of error NULL
// is returned.
//-----------------------------------------------------------------------------
int dpiEnqOptions__create(dpiEnqOptions *options, dpiConn *conn,
        dpiError *error)
{
    dpiGen__setRefCount(conn, error, 1);
    options->conn = conn;
    return dpiOci__descriptorAlloc(conn->env->handle, &options->handle,
            DPI_OCI_DTYPE_AQENQ_OPTIONS, "allocate descriptor", error);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions__free() [INTERNAL]
//   Free the memory for a enqueue options structure.
//-----------------------------------------------------------------------------
void dpiEnqOptions__free(dpiEnqOptions *options, dpiError *error)
{
    if (options->handle) {
        dpiOci__descriptorFree(options->handle, DPI_OCI_DTYPE_AQENQ_OPTIONS);
        options->handle = NULL;
    }
    if (options->conn) {
        dpiGen__setRefCount(options->conn, error, -1);
        options->conn = NULL;
    }
    dpiUtils__freeMemory(options);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions__getAttrValue() [INTERNAL]
//   Get the attribute value in OCI.
//-----------------------------------------------------------------------------
static int dpiEnqOptions__getAttrValue(dpiEnqOptions *options,
        uint32_t attribute, const char *fnName, void *value,
        uint32_t *valueLength)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(options, DPI_HTYPE_ENQ_OPTIONS, fnName,
            &error) < 0)
        return dpiGen__endPublicFn(options, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(options, value)
    DPI_CHECK_PTR_NOT_NULL(options, valueLength)
    status = dpiOci__attrGet(options->handle, DPI_OCI_DTYPE_AQENQ_OPTIONS,
            value, valueLength, attribute, "get attribute value", &error);
    return dpiGen__endPublicFn(options, status, &error);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions__setAttrValue() [INTERNAL]
//   Set the attribute value in OCI.
//-----------------------------------------------------------------------------
static int dpiEnqOptions__setAttrValue(dpiEnqOptions *options,
        uint32_t attribute, const char *fnName, const void *value,
        uint32_t valueLength)
{
    dpiError error;
    int status;

    if (dpiGen__startPublicFn(options, DPI_HTYPE_ENQ_OPTIONS, fnName,
            &error) < 0)
        return dpiGen__endPublicFn(options, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(options, value)
    status = dpiOci__attrSet(options->handle, DPI_OCI_DTYPE_AQENQ_OPTIONS,
            (void*) value, valueLength, attribute, "set attribute value",
            &error);
    return dpiGen__endPublicFn(options, status, &error);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions_addRef() [PUBLIC]
//   Add a reference to the enqueue options.
//-----------------------------------------------------------------------------
int dpiEnqOptions_addRef(dpiEnqOptions *options)
{
    return dpiGen__addRef(options, DPI_HTYPE_ENQ_OPTIONS, __func__);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions_getTransformation() [PUBLIC]
//   Return transformation associated with enqueue options.
//-----------------------------------------------------------------------------
int dpiEnqOptions_getTransformation(dpiEnqOptions *options, const char **value,
        uint32_t *valueLength)
{
    return dpiEnqOptions__getAttrValue(options, DPI_OCI_ATTR_TRANSFORMATION,
            __func__, (void*) value, valueLength);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions_getVisibility() [PUBLIC]
//   Return visibility associated with enqueue options.
//-----------------------------------------------------------------------------
int dpiEnqOptions_getVisibility(dpiEnqOptions *options, dpiVisibility *value)
{
    uint32_t valueLength = sizeof(uint32_t);

    return dpiEnqOptions__getAttrValue(options, DPI_OCI_ATTR_VISIBILITY,
            __func__, value, &valueLength);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions_release() [PUBLIC]
//   Release a reference to the enqueue options.
//-----------------------------------------------------------------------------
int dpiEnqOptions_release(dpiEnqOptions *options)
{
    return dpiGen__release(options, DPI_HTYPE_ENQ_OPTIONS, __func__);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions_setDeliveryMode() [PUBLIC]
//   Set the delivery mode associated with enqueue options.
//-----------------------------------------------------------------------------
int dpiEnqOptions_setDeliveryMode(dpiEnqOptions *options,
        dpiMessageDeliveryMode value)
{
    return dpiEnqOptions__setAttrValue(options, DPI_OCI_ATTR_MSG_DELIVERY_MODE,
            __func__, &value, 0);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions_setTransformation() [PUBLIC]
//   Set transformation associated with enqueue options.
//-----------------------------------------------------------------------------
int dpiEnqOptions_setTransformation(dpiEnqOptions *options, const char *value,
        uint32_t valueLength)
{
    return dpiEnqOptions__setAttrValue(options, DPI_OCI_ATTR_TRANSFORMATION,
            __func__,  value, valueLength);
}


//-----------------------------------------------------------------------------
// dpiEnqOptions_setVisibility() [PUBLIC]
//   Set visibility associated with enqueue options.
//-----------------------------------------------------------------------------
int dpiEnqOptions_setVisibility(dpiEnqOptions *options, dpiVisibility value)
{
    return dpiEnqOptions__setAttrValue(options, DPI_OCI_ATTR_VISIBILITY,
            __func__, &value, 0);
}
