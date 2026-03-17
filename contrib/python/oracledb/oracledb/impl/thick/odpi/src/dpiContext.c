//-----------------------------------------------------------------------------
// Copyright (c) 2016, 2024, Oracle and/or its affiliates.
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
// dpiContext.c
//   Implementation of context. Each context uses a specific version of the
// ODPI-C library, which is checked for compatibility before allowing its use.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

// forward declarations of internal functions only used in this file
static void dpiContext__free(dpiContext *context);


//-----------------------------------------------------------------------------
// dpiContext__create() [INTERNAL]
//   Helper function for dpiContext__create().
//-----------------------------------------------------------------------------
static int dpiContext__create(const char *fnName, unsigned int majorVersion,
        unsigned int minorVersion, dpiContextCreateParams *params,
        dpiContext **context, dpiError *error)
{
    dpiVersionInfo *versionInfo;
    dpiContext *tempContext;

    // ensure global infrastructure is initialized
    if (dpiGlobal__ensureInitialized(fnName, params, &versionInfo, error) < 0)
        return DPI_FAILURE;

    // validate context handle
    if (!context)
        return dpiError__set(error, "check context handle",
                DPI_ERR_NULL_POINTER_PARAMETER, "context");

    // verify that the supplied version is supported by the library
    if (majorVersion != DPI_MAJOR_VERSION || minorVersion > DPI_MINOR_VERSION)
        return dpiError__set(error, "check version",
                DPI_ERR_VERSION_NOT_SUPPORTED, majorVersion, majorVersion,
                minorVersion, DPI_MAJOR_VERSION, DPI_MINOR_VERSION);

    // allocate context and initialize it
    if (dpiGen__allocate(DPI_HTYPE_CONTEXT, NULL, (void**) &tempContext,
            error) < 0)
        return DPI_FAILURE;
    tempContext->dpiMinorVersion = (uint8_t) minorVersion;
    tempContext->versionInfo = versionInfo;

    // using a SODA JSON descriptor is only allowed with 23.4 and higher so
    // only set the flag if that version is being used; otherwise, ignore the
    // flag completely
    if (versionInfo->versionNum > 23 ||
            (versionInfo->versionNum == 23 && versionInfo->releaseNum >= 4)) {
        tempContext->sodaUseJsonDesc = params->sodaUseJsonDesc;
        tempContext->useJsonId = params->useJsonId;
    } else {
        params->sodaUseJsonDesc = 0;
    }

    // store default encoding, if applicable
    if (params->defaultEncoding) {
        if (dpiUtils__allocateMemory(1, strlen(params->defaultEncoding) + 1, 0,
                "allocate default encoding",
                (void**) &tempContext->defaultEncoding, error) < 0) {
            dpiContext__free(tempContext);
            return DPI_FAILURE;
        }
        strcpy(tempContext->defaultEncoding, params->defaultEncoding);
    }

    // store default driver name, if applicable
    if (params->defaultDriverName) {
        if (dpiUtils__allocateMemory(1, strlen(params->defaultDriverName) + 1,
                0, "allocate default driver name",
                (void**) &tempContext->defaultDriverName, error) < 0) {
            dpiContext__free(tempContext);
            return DPI_FAILURE;
        }
        strcpy(tempContext->defaultDriverName, params->defaultDriverName);
    }

    *context = tempContext;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiContext__free() [INTERNAL]
//   Free the memory and any resources associated with the context.
//-----------------------------------------------------------------------------
static void dpiContext__free(dpiContext *context)
{
    if (context->defaultDriverName) {
        dpiUtils__freeMemory((void*) context->defaultDriverName);
        context->defaultDriverName = NULL;
    }
    if (context->defaultEncoding) {
        dpiUtils__freeMemory((void*) context->defaultEncoding);
        context->defaultEncoding = NULL;
    }
    dpiUtils__freeMemory(context);
}


//-----------------------------------------------------------------------------
// dpiContext__initCommonCreateParams() [INTERNAL]
//   Initialize the common connection/pool creation parameters to default
// values.
//-----------------------------------------------------------------------------
void dpiContext__initCommonCreateParams(const dpiContext *context,
        dpiCommonCreateParams *params)
{
    memset(params, 0, sizeof(dpiCommonCreateParams));
    if (context->defaultEncoding) {
        params->encoding = context->defaultEncoding;
        params->nencoding = context->defaultEncoding;
    } else {
        params->encoding = DPI_CHARSET_NAME_UTF8;
        params->nencoding = DPI_CHARSET_NAME_UTF8;
    }
    if (context->defaultDriverName) {
        params->driverName = context->defaultDriverName;
        params->driverNameLength =
                (uint32_t) strlen(context->defaultDriverName);
    } else {
        params->driverName = DPI_DEFAULT_DRIVER_NAME;
        params->driverNameLength = (uint32_t) strlen(params->driverName);
    }
    params->stmtCacheSize = DPI_DEFAULT_STMT_CACHE_SIZE;
}


//-----------------------------------------------------------------------------
// dpiContext__initConnCreateParams() [INTERNAL]
//   Initialize the connection creation parameters to default values. Return
// the structure size as a convenience for calling functions which may have to
// differentiate between different ODPI-C application versions.
//-----------------------------------------------------------------------------
void dpiContext__initConnCreateParams(dpiConnCreateParams *params)
{
    memset(params, 0, sizeof(dpiConnCreateParams));
}


//-----------------------------------------------------------------------------
// dpiContext__initPoolCreateParams() [INTERNAL]
//   Initialize the pool creation parameters to default values.
//-----------------------------------------------------------------------------
void dpiContext__initPoolCreateParams(dpiPoolCreateParams *params)
{
    memset(params, 0, sizeof(dpiPoolCreateParams));
    params->minSessions = 1;
    params->maxSessions = 1;
    params->sessionIncrement = 0;
    params->homogeneous = 1;
    params->getMode = DPI_MODE_POOL_GET_NOWAIT;
    params->pingInterval = DPI_DEFAULT_PING_INTERVAL;
    params->pingTimeout = DPI_DEFAULT_PING_TIMEOUT;
}


//-----------------------------------------------------------------------------
// dpiContext__initSodaOperOptions() [INTERNAL]
//   Initialize the SODA operation options to default values.
//-----------------------------------------------------------------------------
void dpiContext__initSodaOperOptions(dpiSodaOperOptions *options)
{
    memset(options, 0, sizeof(dpiSodaOperOptions));
}


//-----------------------------------------------------------------------------
// dpiContext__initSubscrCreateParams() [INTERNAL]
//   Initialize the subscription creation parameters to default values.
//-----------------------------------------------------------------------------
void dpiContext__initSubscrCreateParams(dpiSubscrCreateParams *params)
{
    memset(params, 0, sizeof(dpiSubscrCreateParams));
    params->subscrNamespace = DPI_SUBSCR_NAMESPACE_DBCHANGE;
    params->groupingType = DPI_SUBSCR_GROUPING_TYPE_SUMMARY;
}


//-----------------------------------------------------------------------------
// dpiContext_createWithParams() [PUBLIC]
//   Create a new context for interaction with the library. The major versions
// must match and the minor version of the caller must be less than or equal to
// the minor version compiled into the library. The supplied parameters can be
// used to modify how the Oracle client library is loaded.
//-----------------------------------------------------------------------------
int dpiContext_createWithParams(unsigned int majorVersion,
        unsigned int minorVersion, dpiContextCreateParams *params,
        dpiContext **context, dpiErrorInfo *errorInfo)
{
    int status, update_use_soda_json_desc = 0;
    dpiContextCreateParams localParams;
    dpiErrorInfo localErrorInfo;
    dpiError error;

    // make a copy of the parameters so that the addition of defaults doesn't
    // modify the original parameters that were passed; then add defaults, if
    // needed
    if (params) {
        if (majorVersion < 5 || (majorVersion == 5 && minorVersion < 2)) {
            memcpy(&localParams, params, sizeof(dpiContextCreateParams__v51));
        } else {
            memcpy(&localParams, params, sizeof(localParams));
            update_use_soda_json_desc = 1;
        }
    } else {
        memset(&localParams, 0, sizeof(localParams));
    }
    if (!localParams.loadErrorUrl)
        localParams.loadErrorUrl = DPI_DEFAULT_LOAD_ERROR_URL;

    status = dpiContext__create(__func__, majorVersion, minorVersion,
            &localParams, context, &error);
    if (status < 0) {
        dpiError__getInfo(&error, &localErrorInfo);
        memcpy(errorInfo, &localErrorInfo, sizeof(dpiErrorInfo__v33));
    }
    if (update_use_soda_json_desc)
        params->sodaUseJsonDesc = localParams.sodaUseJsonDesc;
    if (dpiDebugLevel & DPI_DEBUG_LEVEL_FNS)
        dpiDebug__print("fn end %s -> %d\n", __func__, status);
    return status;
}


//-----------------------------------------------------------------------------
// dpiContext_destroy() [PUBLIC]
//   Destroy an existing context. The structure will be checked for validity
// first.
//-----------------------------------------------------------------------------
int dpiContext_destroy(dpiContext *context)
{
    char message[80];
    dpiError error;

    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    dpiUtils__clearMemory(&context->checkInt, sizeof(context->checkInt));
    if (dpiDebugLevel & DPI_DEBUG_LEVEL_REFS)
        dpiDebug__print("ref %p (%s) -> 0\n", context, context->typeDef->name);
    if (dpiDebugLevel & DPI_DEBUG_LEVEL_FNS)
        (void) sprintf(message, "fn end %s(%p) -> %d", __func__, context,
                DPI_SUCCESS);
    dpiContext__free(context);
    if (dpiDebugLevel & DPI_DEBUG_LEVEL_FNS)
        dpiDebug__print("%s\n", message);
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiContext_freeStringList() [PUBLIC]
//   Frees the contents of a string list.
//-----------------------------------------------------------------------------
int dpiContext_freeStringList(dpiContext *context, dpiStringList *list)
{
    dpiError error;

    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(context, list)
    dpiStringList__free(list);
    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiContext_getClientVersion() [PUBLIC]
//   Return the version of the Oracle client that is in use.
//-----------------------------------------------------------------------------
int dpiContext_getClientVersion(const dpiContext *context,
        dpiVersionInfo *versionInfo)
{
    dpiError error;

    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(context, versionInfo)
    memcpy(versionInfo, context->versionInfo, sizeof(dpiVersionInfo));
    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiContext_getError() [PUBLIC]
//   Return information about the error that was last populated.
//-----------------------------------------------------------------------------
void dpiContext_getError(const dpiContext *context, dpiErrorInfo *info)
{
    dpiError error;

    dpiGlobal__initError(NULL, &error);
    dpiGen__checkHandle(context, DPI_HTYPE_CONTEXT, "check handle", &error);
    dpiError__getInfo(&error, info);
}


//-----------------------------------------------------------------------------
// dpiContext_initCommonCreateParams() [PUBLIC]
//   Initialize the common connection/pool creation parameters to default
// values.
//-----------------------------------------------------------------------------
int dpiContext_initCommonCreateParams(const dpiContext *context,
        dpiCommonCreateParams *params)
{
    dpiError error;

    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(context, params)
    dpiContext__initCommonCreateParams(context, params);

    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiContext_initConnCreateParams() [PUBLIC]
//   Initialize the connection creation parameters to default values.
//-----------------------------------------------------------------------------
int dpiContext_initConnCreateParams(const dpiContext *context,
        dpiConnCreateParams *params)
{
    dpiError error;

    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(context, params)

    dpiContext__initConnCreateParams(params);
    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiContext_initPoolCreateParams() [PUBLIC]
//   Initialize the pool creation parameters to default values.
//-----------------------------------------------------------------------------
int dpiContext_initPoolCreateParams(const dpiContext *context,
        dpiPoolCreateParams *params)
{
    dpiError error;

    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(context, params)
    dpiContext__initPoolCreateParams(params);

    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiContext_initSodaOperOptions() [PUBLIC]
//   Initialize the SODA operation options to default values.
//-----------------------------------------------------------------------------
int dpiContext_initSodaOperOptions(const dpiContext *context,
        dpiSodaOperOptions *options)
{
    dpiError error;

    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(context, options)
    dpiContext__initSodaOperOptions(options);

    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiContext_initSubscrCreateParams() [PUBLIC]
//   Initialize the subscription creation parameters to default values.
//-----------------------------------------------------------------------------
int dpiContext_initSubscrCreateParams(const dpiContext *context,
        dpiSubscrCreateParams *params)
{
    dpiError error;

    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(context, params)

    dpiContext__initSubscrCreateParams(params);
    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}
