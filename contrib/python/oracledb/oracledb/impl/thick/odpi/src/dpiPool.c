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
// dpiPool.c
//   Implementation of session pools.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

//-----------------------------------------------------------------------------
// dpiPool__acquireConnection() [INTERNAL]
//   Internal method used for acquiring a connection from a pool.
//-----------------------------------------------------------------------------
int dpiPool__acquireConnection(dpiPool *pool, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        dpiConnCreateParams *params, dpiConn **conn, dpiError *error)
{
    dpiConn *tempConn;

    // allocate new connection
    if (dpiGen__allocate(DPI_HTYPE_CONN, pool->env, (void**) &tempConn,
            error) < 0)
        return DPI_FAILURE;
    error->env = pool->env;

    // create the connection
    if (dpiConn__create(tempConn, pool->env->context, userName, userNameLength,
            password, passwordLength, pool->name, pool->nameLength, pool,
            NULL, params, error) < 0) {
        dpiConn__free(tempConn, error);
        return DPI_FAILURE;
    }

    *conn = tempConn;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiPool__checkConnected() [INTERNAL]
//   Determine if the session pool is connected to the database. If not, an
// error is raised.
//-----------------------------------------------------------------------------
static int dpiPool__checkConnected(dpiPool *pool, const char *fnName,
        dpiError *error)
{
    if (dpiGen__startPublicFn(pool, DPI_HTYPE_POOL, fnName, error) < 0)
        return DPI_FAILURE;
    if (!pool->handle)
        return dpiError__set(error, "check pool", DPI_ERR_NOT_CONNECTED);
    return DPI_SUCCESS;
}


//----------------------------------------------------------------------------
// dpiPool__accessTokenCallback() [INTERNAL]
//   Callback used to execute the registered callback when the authentication
// token expires and the connection pool needs to create a new connection.
// -----------------------------------------------------------------------------
int dpiPool__accessTokenCallback(dpiPool *pool, void *authInfo,
        UNUSED uint32_t mode)
{
    dpiAccessToken accessToken;
    dpiError error;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    memset(&accessToken, 0, sizeof(dpiAccessToken));

    if ((*pool->accessTokenCallback)(pool->accessTokenCallbackContext,
            &accessToken) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    if (dpiUtils__setAccessTokenAttributes(authInfo, &accessToken,
               pool->env->versionInfo, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool__create() [INTERNAL]
//   Internal method for creating a session pool.
//-----------------------------------------------------------------------------
static int dpiPool__create(dpiPool *pool, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        const char *connectString, uint32_t connectStringLength,
        const dpiCommonCreateParams *commonParams,
        dpiPoolCreateParams *createParams, dpiError *error)
{
    int32_t pingInterval;
    uint32_t poolMode;
    uint8_t getMode;
    void *authInfo;

    // validate parameters
    if (createParams->externalAuth &&
            ((userName && userNameLength > 0) ||
             (password && passwordLength > 0)))
        return dpiError__set(error, "check mixed credentials",
                DPI_ERR_EXT_AUTH_WITH_CREDENTIALS);

    // create the session pool handle
    if (dpiOci__handleAlloc(pool->env->handle, &pool->handle,
            DPI_OCI_HTYPE_SPOOL, "allocate pool handle", error) < 0)
        return DPI_FAILURE;

    // prepare pool mode
    poolMode = DPI_OCI_SPC_STMTCACHE;
    if (createParams->homogeneous)
        poolMode |= DPI_OCI_SPC_HOMOGENEOUS;

    // create authorization handle
    if (dpiOci__handleAlloc(pool->env->handle, &authInfo,
            DPI_OCI_HTYPE_AUTHINFO, "allocate authinfo handle", error) < 0)
        return DPI_FAILURE;

    // set context attributes
    if (dpiUtils__setAttributesFromCommonCreateParams(authInfo,
            DPI_OCI_HTYPE_AUTHINFO, commonParams, error) < 0)
        return DPI_FAILURE;

    // set token based authentication attributes
    if (commonParams->accessToken) {

        // homogeneous must be set to true for token based authentication
        if (!createParams->homogeneous || !createParams->externalAuth)
            return dpiError__set(error, "check homogeneous and externalAuth",
                    DPI_ERR_POOL_TOKEN_BASED_AUTH);

        if (dpiUtils__setAccessTokenAttributes(authInfo,
                commonParams->accessToken, pool->env->versionInfo, error) < 0)
            return DPI_FAILURE;

        if (createParams->accessTokenCallback) {
            // set token based auth context callback on session handle
            if (dpiOci__attrSet(authInfo, DPI_OCI_HTYPE_SESSION,
                    (void*) pool, 0, DPI_OCI_ATTR_TOKEN_CBKCTX,
                    "set token callback context", error) < 0)
                return DPI_FAILURE;

            // set token based auth callback on session handle
            if (dpiOci__attrSet(authInfo, DPI_OCI_HTYPE_SESSION,
                    (void*) dpiPool__accessTokenCallback, 0,
                    DPI_OCI_ATTR_TOKEN_CBK, "set token callback", error) < 0)
                return DPI_FAILURE;
        }
    }

    // set PL/SQL session state fixup callback, if applicable
    if (createParams->plsqlFixupCallback &&
            createParams->plsqlFixupCallbackLength > 0) {
        if (dpiUtils__checkClientVersion(pool->env->versionInfo, 12, 2,
                error) < 0)
            return DPI_FAILURE;
        if (dpiOci__attrSet(authInfo, DPI_OCI_HTYPE_AUTHINFO,
                    (void*) createParams->plsqlFixupCallback,
                    createParams->plsqlFixupCallbackLength,
                    DPI_OCI_ATTR_FIXUP_CALLBACK,
                    "set PL/SQL session state fixup callback", error) < 0)
            return DPI_FAILURE;
    }

    // set authorization info on session pool
    if (dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL, (void*) authInfo, 0,
            DPI_OCI_ATTR_SPOOL_AUTH, "set auth info", error) < 0)
        return DPI_FAILURE;

    // set the get mode on the pool
    getMode = (uint8_t) createParams->getMode;
    if (dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL, (void*) &getMode, 0,
            DPI_OCI_ATTR_SPOOL_GETMODE, "set get mode", error) < 0)
        return DPI_FAILURE;

    // set the session timeout on the pool
    if (dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL, (void*)
            &createParams->timeout, 0, DPI_OCI_ATTR_SPOOL_TIMEOUT,
            "set timeout", error) < 0)
        return DPI_FAILURE;

    // set the wait timeout on the pool (valid in 12.2 and higher)
    if (pool->env->versionInfo->versionNum > 12 ||
            (pool->env->versionInfo->versionNum == 12 &&
             pool->env->versionInfo->releaseNum >= 2)) {
        if (dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL, (void*)
                &createParams->waitTimeout, 0, DPI_OCI_ATTR_SPOOL_WAIT_TIMEOUT,
                "set wait timeout", error) < 0)
            return DPI_FAILURE;
    }

    // set the maximum lifetime session on the pool (valid in 12.1 and higher)
    if (pool->env->versionInfo->versionNum >= 12) {
        if (dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL, (void*)
                &createParams->maxLifetimeSession, 0,
                DPI_OCI_ATTR_SPOOL_MAX_LIFETIME_SESSION,
                "set max lifetime session", error) < 0)
            return DPI_FAILURE;
    }

    // set the maximum number of sessions per shard (valid in 18.3 and higher)
    if (pool->env->versionInfo->versionNum > 18 ||
            (pool->env->versionInfo->versionNum == 18 &&
             pool->env->versionInfo->releaseNum >= 3)) {
        if (dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL, (void*)
                &createParams->maxSessionsPerShard, 0,
                DPI_OCI_ATTR_SPOOL_MAX_PER_SHARD,
                "set max sessions per shard", error) < 0)
            return DPI_FAILURE;
    }

    // create pool
    if (dpiOci__sessionPoolCreate(pool, connectString, connectStringLength,
            createParams->minSessions, createParams->maxSessions,
            createParams->sessionIncrement, userName, userNameLength, password,
            passwordLength, poolMode, error) < 0)
        return DPI_FAILURE;

    // set the statement cache size
    if (dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL,
            (void*) &commonParams->stmtCacheSize, 0,
            DPI_OCI_ATTR_SPOOL_STMTCACHESIZE, "set stmt cache size",
            error) < 0)
        return DPI_FAILURE;

    // disable ping interval in Oracle Database 23ai since it does not handle
    // ping timeout yet
    if (pool->env->versionInfo->versionNum >= 23) {
        pingInterval = -1;
        if (dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL,
                (void*) &pingInterval, 0, DPI_OCI_ATTR_PING_INTERVAL,
                "disable ping interval", error) < 0)
            return DPI_FAILURE;
    }

    // set remaining attributes directly
    pool->homogeneous = createParams->homogeneous;
    pool->externalAuth = createParams->externalAuth;
    pool->pingInterval = createParams->pingInterval;
    pool->pingTimeout = createParams->pingTimeout;
    pool->stmtCacheSize = commonParams->stmtCacheSize;

    if (commonParams->accessToken) {
        pool->accessTokenCallback = createParams->accessTokenCallback;
        pool->accessTokenCallbackContext =
                createParams->accessTokenCallbackContext;
        // force externalAuth to false for token based authentication
        pool->externalAuth = 0;
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiPool__free() [INTERNAL]
//   Free any memory associated with the pool.
//-----------------------------------------------------------------------------
void dpiPool__free(dpiPool *pool, dpiError *error)
{
    if (pool->handle) {
        dpiOci__sessionPoolDestroy(pool, DPI_OCI_SPD_FORCE, 0, error);
        pool->handle = NULL;
    }
    if (pool->env) {
        dpiEnv__free(pool->env, error);
        pool->env = NULL;
    }
    dpiUtils__freeMemory(pool);
}


//-----------------------------------------------------------------------------
// dpiPool__getAttributeUint() [INTERNAL]
//   Return the value of the attribute as an unsigned integer.
//-----------------------------------------------------------------------------
static int dpiPool__getAttributeUint(dpiPool *pool, uint32_t attribute,
        uint32_t *value, const char *fnName)
{
    dpiError error;
    int status;

    if (dpiPool__checkConnected(pool, fnName, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(pool, value)
    switch (attribute) {
        case DPI_OCI_ATTR_SPOOL_MAX_LIFETIME_SESSION:
            if (dpiUtils__checkClientVersion(pool->env->versionInfo, 12, 1,
                    &error) < 0)
                return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
            break;
        case DPI_OCI_ATTR_SPOOL_WAIT_TIMEOUT:
            if (dpiUtils__checkClientVersion(pool->env->versionInfo, 12, 2,
                    &error) < 0)
                return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
            break;
        case DPI_OCI_ATTR_SPOOL_BUSY_COUNT:
        case DPI_OCI_ATTR_SPOOL_OPEN_COUNT:
        case DPI_OCI_ATTR_SPOOL_STMTCACHESIZE:
        case DPI_OCI_ATTR_SPOOL_TIMEOUT:
            break;
        case DPI_OCI_ATTR_SPOOL_MAX_PER_SHARD:
            if (dpiUtils__checkClientVersion(pool->env->versionInfo, 18, 3,
                    &error) < 0)
                return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
            break;
        default:
            dpiError__set(&error, "get attribute value",
                    DPI_ERR_NOT_SUPPORTED);
            return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    }
    status = dpiOci__attrGet(pool->handle, DPI_OCI_HTYPE_SPOOL, value,
            NULL, attribute, "get attribute value", &error);
    return dpiGen__endPublicFn(pool, status, &error);
}


//-----------------------------------------------------------------------------
// dpiPool__setAttributeUint() [INTERNAL]
//   Set the value of the OCI attribute as an unsigned integer.
//-----------------------------------------------------------------------------
static int dpiPool__setAttributeUint(dpiPool *pool, uint32_t attribute,
        uint32_t value, const char *fnName)
{
    void *ociValue = &value;
    uint8_t shortValue;
    dpiError error;
    int status;

    // make sure session pool is connected
    if (dpiPool__checkConnected(pool, fnName, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    // determine pointer to pass (OCI uses different sizes)
    switch (attribute) {
        case DPI_OCI_ATTR_SPOOL_GETMODE:
            shortValue = (uint8_t) value;
            ociValue = &shortValue;
            break;
        case DPI_OCI_ATTR_SPOOL_MAX_LIFETIME_SESSION:
            if (dpiUtils__checkClientVersion(pool->env->versionInfo, 12, 1,
                    &error) < 0)
                return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
            break;
        case DPI_OCI_ATTR_SPOOL_WAIT_TIMEOUT:
            if (dpiUtils__checkClientVersion(pool->env->versionInfo, 12, 2,
                    &error) < 0)
                return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
            break;
        case DPI_OCI_ATTR_SPOOL_STMTCACHESIZE:
        case DPI_OCI_ATTR_SPOOL_TIMEOUT:
            break;
        case DPI_OCI_ATTR_SPOOL_MAX_PER_SHARD:
            if (dpiUtils__checkClientVersion(pool->env->versionInfo, 18, 3,
                    &error) < 0)
                return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
            break;
        default:
            dpiError__set(&error, "set attribute value",
                    DPI_ERR_NOT_SUPPORTED);
            return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    }

    // set value in the OCI
    status = dpiOci__attrSet(pool->handle, DPI_OCI_HTYPE_SPOOL, ociValue, 0,
            attribute, "set attribute value", &error);
    return dpiGen__endPublicFn(pool, status, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_acquireConnection() [PUBLIC]
//   Acquire a connection from the pool.
//-----------------------------------------------------------------------------
int dpiPool_acquireConnection(dpiPool *pool, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        dpiConnCreateParams *params, dpiConn **conn)
{
    dpiConnCreateParams localParams;
    dpiError error;
    int status;

    // validate parameters
    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(pool, userName)
    DPI_CHECK_PTR_AND_LENGTH(pool, password)
    DPI_CHECK_PTR_NOT_NULL(pool, conn)

    // use default parameters if none provided
    if (!params) {
        dpiContext__initConnCreateParams(&localParams);
        params = &localParams;
    }

    // the username must be enclosed within [] if external authentication
    // with proxy is desired
    if (pool->externalAuth && userName && userNameLength > 0 &&
            (userName[0] != '[' || userName[userNameLength - 1] != ']')) {
        dpiError__set(&error, "verify proxy user name with external auth",
                DPI_ERR_EXT_AUTH_INVALID_PROXY);
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error );
    }

    status = dpiPool__acquireConnection(pool, userName, userNameLength,
            password, passwordLength, params, conn, &error);
    return dpiGen__endPublicFn(pool, status, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_addRef() [PUBLIC]
//   Add a reference to the pool.
//-----------------------------------------------------------------------------
int dpiPool_addRef(dpiPool *pool)
{
    return dpiGen__addRef(pool, DPI_HTYPE_POOL, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_close() [PUBLIC]
//   Destroy the pool now, not when the reference count reaches zero.
//-----------------------------------------------------------------------------
int dpiPool_close(dpiPool *pool, dpiPoolCloseMode mode)
{
    dpiError error;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    if (dpiOci__sessionPoolDestroy(pool, mode, 1, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_create() [PUBLIC]
//   Create a new session pool and return it.
//-----------------------------------------------------------------------------
int dpiPool_create(const dpiContext *context, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        const char *connectString, uint32_t connectStringLength,
        const dpiCommonCreateParams *commonParams,
        dpiPoolCreateParams *createParams, dpiPool **pool)
{
    dpiCommonCreateParams localCommonParams;
    dpiPoolCreateParams localCreateParams;
    dpiPool *tempPool;
    dpiError error;

    // validate parameters
    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(context, userName)
    DPI_CHECK_PTR_AND_LENGTH(context, password)
    DPI_CHECK_PTR_AND_LENGTH(context, connectString)
    DPI_CHECK_PTR_NOT_NULL(context, pool)

    // use default parameters if none provided
    if (!commonParams) {
        dpiContext__initCommonCreateParams(context, &localCommonParams);
        commonParams = &localCommonParams;
    }
    if (!createParams) {
        dpiContext__initPoolCreateParams(&localCreateParams);
        createParams = &localCreateParams;
    }

    // allocate memory for pool
    if (dpiGen__allocate(DPI_HTYPE_POOL, NULL, (void**) &tempPool, &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);

    // initialize environment
    if (dpiEnv__init(tempPool->env, context, commonParams, NULL,
            commonParams->createMode | DPI_MODE_CREATE_THREADED, &error) < 0) {
        dpiPool__free(tempPool, &error);
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    }

    // perform remaining steps required to create pool
    if (dpiPool__create(tempPool, userName, userNameLength, password,
            passwordLength, connectString, connectStringLength, commonParams,
            createParams, &error) < 0) {
        dpiPool__free(tempPool, &error);
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    }

    createParams->outPoolName = tempPool->name;
    createParams->outPoolNameLength = tempPool->nameLength;
    *pool = tempPool;
    dpiHandlePool__release(tempPool->env->errorHandles, &error.handle);
    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_getBusyCount() [PUBLIC]
//   Return the pool's busy count.
//-----------------------------------------------------------------------------
int dpiPool_getBusyCount(dpiPool *pool, uint32_t *value)
{
    return dpiPool__getAttributeUint(pool, DPI_OCI_ATTR_SPOOL_BUSY_COUNT,
            value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_getEncodingInfo() [PUBLIC]
//   Get the encoding information from the pool.
//-----------------------------------------------------------------------------
int dpiPool_getEncodingInfo(dpiPool *pool, dpiEncodingInfo *info)
{
    dpiError error;
    int status;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(pool, info)
    status = dpiEnv__getEncodingInfo(pool->env, info);
    return dpiGen__endPublicFn(pool, status, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_getGetMode() [PUBLIC]
//   Return the pool's "get" mode.
//-----------------------------------------------------------------------------
int dpiPool_getGetMode(dpiPool *pool, dpiPoolGetMode *value)
{
    dpiError error;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(pool, value)
    if (dpiOci__attrGet(pool->handle, DPI_OCI_HTYPE_SPOOL, value, NULL,
            DPI_OCI_ATTR_SPOOL_GETMODE, "get attribute value", &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_getMaxLifetimeSession() [PUBLIC]
//   Return the pool's maximum lifetime session.
//-----------------------------------------------------------------------------
int dpiPool_getMaxLifetimeSession(dpiPool *pool, uint32_t *value)
{
    return dpiPool__getAttributeUint(pool,
            DPI_OCI_ATTR_SPOOL_MAX_LIFETIME_SESSION, value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_getMaxSessionsPerShard() [PUBLIC]
//   Return the pool's maximum sessions per shard.
//-----------------------------------------------------------------------------
int dpiPool_getMaxSessionsPerShard(dpiPool *pool, uint32_t *value)
{
    return dpiPool__getAttributeUint(pool, DPI_OCI_ATTR_SPOOL_MAX_PER_SHARD,
            value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_getOpenCount() [PUBLIC]
//   Return the pool's open count.
//-----------------------------------------------------------------------------
int dpiPool_getOpenCount(dpiPool *pool, uint32_t *value)
{
    return dpiPool__getAttributeUint(pool, DPI_OCI_ATTR_SPOOL_OPEN_COUNT,
            value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_getSodaMetadataCache() [PUBLIC]
//   Return whether the SODA metadata cache is enabled or not.
//-----------------------------------------------------------------------------
int dpiPool_getSodaMetadataCache(dpiPool *pool, int *enabled)
{
    dpiError error;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(pool, enabled)
    if (dpiUtils__checkClientVersionMulti(pool->env->versionInfo, 19, 11, 21,
            3, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    if (dpiOci__attrGet(pool->env->handle, DPI_OCI_HTYPE_ENV, enabled, NULL,
            DPI_OCI_ATTR_SODA_METADATA_CACHE, "get SODA metadata cache",
            &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_getStmtCacheSize() [PUBLIC]
//   Return the pool's default statement cache size.
//-----------------------------------------------------------------------------
int dpiPool_getStmtCacheSize(dpiPool *pool, uint32_t *value)
{
    return dpiPool__getAttributeUint(pool, DPI_OCI_ATTR_SPOOL_STMTCACHESIZE,
            value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_getTimeout() [PUBLIC]
//   Return the pool's timeout value.
//-----------------------------------------------------------------------------
int dpiPool_getTimeout(dpiPool *pool, uint32_t *value)
{
    return dpiPool__getAttributeUint(pool, DPI_OCI_ATTR_SPOOL_TIMEOUT, value,
            __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_getWaitTimeout() [PUBLIC]
//   Return the pool's wait timeout value.
//-----------------------------------------------------------------------------
int dpiPool_getWaitTimeout(dpiPool *pool, uint32_t *value)
{
    return dpiPool__getAttributeUint(pool, DPI_OCI_ATTR_SPOOL_WAIT_TIMEOUT,
            value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_getPingInterval() [PUBLIC]
//   Return the pool's ping-interval value.
//-----------------------------------------------------------------------------
int dpiPool_getPingInterval(dpiPool *pool, int *value)
{
    dpiError error;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(pool, value);
    *value = pool->pingInterval;
    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_release() [PUBLIC]
//   Release a reference to the pool.
//-----------------------------------------------------------------------------
int dpiPool_release(dpiPool *pool)
{
    return dpiGen__release(pool, DPI_HTYPE_POOL, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_reconfigure() [PUBLIC]
//   Reconfigure the pool - OCI only allows poolMin, poolMax, poolIncr
//   properties
//-----------------------------------------------------------------------------
int dpiPool_reconfigure(dpiPool *pool, uint32_t minSessions,
        uint32_t maxSessions, uint32_t sessionIncrement)
{
    dpiError error;
    uint32_t mode = DPI_OCI_SPC_REINITIALIZE;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    if (dpiOci__sessionPoolCreate(pool, NULL, 0, minSessions, maxSessions,
            sessionIncrement, NULL, 0, NULL, 0, mode, &error) < 0)
        return DPI_FAILURE;

    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_setGetMode() [PUBLIC]
//   Set the pool's "get" mode.
//-----------------------------------------------------------------------------
int dpiPool_setGetMode(dpiPool *pool, dpiPoolGetMode value)
{
    return dpiPool__setAttributeUint(pool, DPI_OCI_ATTR_SPOOL_GETMODE, value,
            __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_setAccessToken() [PUBLIC]
//   Sets the token and private key for token based authentication
//-----------------------------------------------------------------------------
int dpiPool_setAccessToken(dpiPool *pool, dpiAccessToken *accessToken)
{
    dpiError error;
    void * authInfo;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(pool, accessToken)

    if (dpiOci__attrGet(pool->handle, DPI_OCI_HTYPE_SPOOL, (void *)&authInfo,
            NULL, DPI_OCI_ATTR_SPOOL_AUTH,
            "get attribute value", &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    if (dpiUtils__setAccessTokenAttributes(authInfo, accessToken,
            pool->env->versionInfo, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_setMaxLifetimeSession() [PUBLIC]
//   Set the pool's maximum lifetime session.
//-----------------------------------------------------------------------------
int dpiPool_setMaxLifetimeSession(dpiPool *pool, uint32_t value)
{
    return dpiPool__setAttributeUint(pool,
            DPI_OCI_ATTR_SPOOL_MAX_LIFETIME_SESSION, value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_setMaxSessionsPerShard() [PUBLIC]
//   Set the pool's maximum sessions per shard.
//-----------------------------------------------------------------------------
int dpiPool_setMaxSessionsPerShard(dpiPool *pool, uint32_t value)
{
    return dpiPool__setAttributeUint(pool, DPI_OCI_ATTR_SPOOL_MAX_PER_SHARD,
            value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_setSodaMetadataCache() [PUBLIC]
//   Set whether the SODA metadata cache is enabled or not.
//-----------------------------------------------------------------------------
int dpiPool_setSodaMetadataCache(dpiPool *pool, int enabled)
{
    dpiError error;

    if (dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    if (dpiUtils__checkClientVersionMulti(pool->env->versionInfo, 19, 11, 21,
            3, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    if (dpiOci__attrSet(pool->env->handle, DPI_OCI_HTYPE_ENV, &enabled, 0,
            DPI_OCI_ATTR_SODA_METADATA_CACHE, "set SODA metadata cache",
            &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);

    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiPool_setStmtCacheSize() [PUBLIC]
//   Set the pool's default statement cache size.
//-----------------------------------------------------------------------------
int dpiPool_setStmtCacheSize(dpiPool *pool, uint32_t value)
{
    pool->stmtCacheSize = value;
    return dpiPool__setAttributeUint(pool, DPI_OCI_ATTR_SPOOL_STMTCACHESIZE,
            value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_setTimeout() [PUBLIC]
//   Set the pool's timeout value.
//-----------------------------------------------------------------------------
int dpiPool_setTimeout(dpiPool *pool, uint32_t value)
{
    return dpiPool__setAttributeUint(pool, DPI_OCI_ATTR_SPOOL_TIMEOUT, value,
            __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_setWaitTimeout() [PUBLIC]
//   Set the pool's wait timeout value.
//-----------------------------------------------------------------------------
int dpiPool_setWaitTimeout(dpiPool *pool, uint32_t value)
{
    return dpiPool__setAttributeUint(pool, DPI_OCI_ATTR_SPOOL_WAIT_TIMEOUT,
            value, __func__);
}


//-----------------------------------------------------------------------------
// dpiPool_setPingInterval() [PUBLIC]
//   Set the pool-ping-interval value.
//-----------------------------------------------------------------------------
int dpiPool_setPingInterval(dpiPool *pool, int value)
{
    dpiError error;

    if(dpiPool__checkConnected(pool, __func__, &error) < 0)
        return dpiGen__endPublicFn(pool, DPI_FAILURE, &error);
    pool->pingInterval = value;
    return dpiGen__endPublicFn(pool, DPI_SUCCESS, &error);
}
