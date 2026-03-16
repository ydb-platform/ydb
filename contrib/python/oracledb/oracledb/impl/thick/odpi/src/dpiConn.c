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
// dpiConn.c
//   Implementation of connection.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"
#include <time.h>

// forward declarations of internal functions only used in this file
static int dpiConn__attachExternal(dpiConn *conn, void *externalHandle,
        dpiError *error);
static int dpiConn__createStandalone(dpiConn *conn, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        const char *connectString, uint32_t connectStringLength,
        const dpiCommonCreateParams *commonParams,
        const dpiConnCreateParams *createParams, dpiError *error);
static int dpiConn__get(dpiConn *conn, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        const char *connectString, uint32_t connectStringLength,
        const dpiCommonCreateParams *commonParams,
        dpiConnCreateParams *createParams, dpiPool *pool, dpiError *error);
static int dpiConn__getHandles(dpiConn *conn, dpiError *error);
static int dpiConn__getServerCharset(dpiConn *conn, dpiError *error);
static int dpiConn__getSession(dpiConn *conn, uint32_t mode,
        const char *connectString, uint32_t connectStringLength,
        dpiConnCreateParams *params, void *authInfo, dpiError *error);
static int dpiConn__setAttributesFromCreateParams(dpiConn *conn, void *handle,
        uint32_t handleType, const char *userName, uint32_t userNameLength,
        const char *password, uint32_t passwordLength,
        const dpiCommonCreateParams *commonParams,
        const dpiConnCreateParams *params, int *used, dpiError *error);
static int dpiConn__setShardingKey(dpiConn *conn, void **shardingKey,
        void *handle, uint32_t handleType, uint32_t attribute,
        const char *action, dpiShardingKeyColumn *columns, uint8_t numColumns,
        dpiError *error);
static int dpiConn__setShardingKeyValue(dpiConn *conn, void *shardingKey,
        dpiShardingKeyColumn *column, dpiError *error);


//-----------------------------------------------------------------------------
// dpiConn__attachExternal() [INTERNAL]
//   Attach to the server and session of an existing service context handle.
//-----------------------------------------------------------------------------
static int dpiConn__attachExternal(dpiConn *conn, void *externalHandle,
        dpiError *error)
{
    // mark connection as using an external handle so that no attempts are
    // made to close it
    conn->externalHandle = 1;

    // acquire handles from existing service context handle
    conn->handle = externalHandle;
    if (dpiConn__getHandles(conn, error) < 0) {
        conn->handle = NULL;
        return DPI_FAILURE;
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__check() [INTERNAL]
//   Validate the connection handle and that it is still connected to the
// database.
//-----------------------------------------------------------------------------
static int dpiConn__check(dpiConn *conn, const char *fnName, dpiError *error)
{
    if (dpiGen__startPublicFn(conn, DPI_HTYPE_CONN, fnName, error) < 0)
        return DPI_FAILURE;
    return dpiConn__checkConnected(conn, error);
}


//-----------------------------------------------------------------------------
// dpiConn__checkConnected() [INTERNAL]
//   Check to see if the connection is still open and raise an exception if it
// is not.
// Maintainers: keep these checks in sync with dpiConn_getIsHealthy()
//-----------------------------------------------------------------------------
int dpiConn__checkConnected(dpiConn *conn, dpiError *error)
{
    if (!conn->handle || conn->closing || conn->deadSession ||
            (conn->pool && !conn->pool->handle))
        return dpiError__set(error, "check connected", DPI_ERR_NOT_CONNECTED);
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__clearTransaction() [INTERNAL]
//   Clears the service context of any associated transaction.
//-----------------------------------------------------------------------------
int dpiConn__clearTransaction(dpiConn *conn, dpiError *error)
{
    return dpiOci__attrSet(conn->handle, DPI_OCI_HTYPE_SVCCTX, NULL, 0,
            DPI_OCI_ATTR_TRANS, "clear transaction", error);
}


//-----------------------------------------------------------------------------
// dpiConn__close() [INTERNAL]
//   Internal method used for closing the connection. Any transaction is rolled
// back and any handles allocated are freed. For connections acquired from a
// pool and that aren't marked as needed to be dropped, the last time used is
// updated. This is called from dpiConn_close() where errors are expected to be
// propagated and from dpiConn__free() where errors are ignored.
//-----------------------------------------------------------------------------
static int dpiConn__close(dpiConn *conn, uint32_t mode, const char *tag,
        uint32_t tagLength, int propagateErrors, dpiError *error)
{
    int status, txnInProgress;
    uint32_t serverStatus, i;
    time_t *lastTimeUsed;
    dpiObject *obj;
    dpiStmt *stmt;
    dpiLob *lob;

    // rollback any outstanding transaction, if one is in progress; drop the
    // session if any errors take place
    txnInProgress = 0;
    if (!conn->deadSession && !conn->externalHandle && conn->sessionHandle) {
        txnInProgress = 1;
        if (conn->env->versionInfo->versionNum >= 12)
            dpiOci__attrGet(conn->sessionHandle, DPI_OCI_HTYPE_SESSION,
                    &txnInProgress, NULL, DPI_OCI_ATTR_TRANSACTION_IN_PROGRESS,
                    NULL, error);
    }
    if (txnInProgress &&
            dpiOci__transRollback(conn, propagateErrors, error) < 0)
        conn->deadSession = 1;

    // close all objects; note that no references are retained by the
    // handle list (otherwise all objects would be left until an explicit
    // close of the connection was made) so a reference needs to be acquired
    // first, as otherwise the object may be freed while the close is being
    // performed!
    if (conn->objects && !conn->externalHandle) {
        for (i = 0; i < conn->objects->numSlots; i++) {
            obj = (dpiObject*) conn->objects->handles[i];
            if (!obj)
                continue;
            if (conn->env->threaded) {
                dpiMutex__acquire(conn->env->mutex);
                status = dpiGen__checkHandle(obj, DPI_HTYPE_OBJECT, NULL,
                        NULL);
                if (status == DPI_SUCCESS)
                    obj->refCount += 1;
                dpiMutex__release(conn->env->mutex);
                if (status < 0)
                    continue;
            }
            status = dpiObject__close(obj, propagateErrors, error);
            if (conn->env->threaded)
                dpiGen__setRefCount(obj, error, -1);
            if (status < 0)
                return DPI_FAILURE;
        }
    }

    // close all open statements; note that no references are retained by the
    // handle list (otherwise all statements would be left open until an
    // explicit close was made of either the statement or the connection) so
    // a reference needs to be acquired first, as otherwise the statement may
    // be freed while the close is being performed!
    if (conn->openStmts && !conn->externalHandle) {
        for (i = 0; i < conn->openStmts->numSlots; i++) {
            stmt = (dpiStmt*) conn->openStmts->handles[i];
            if (!stmt)
                continue;
            if (conn->env->threaded) {
                dpiMutex__acquire(conn->env->mutex);
                status = dpiGen__checkHandle(stmt, DPI_HTYPE_STMT, NULL, NULL);
                if (status == DPI_SUCCESS)
                    stmt->refCount += 1;
                dpiMutex__release(conn->env->mutex);
                if (status < 0)
                    continue;
            }
            status = dpiStmt__close(stmt, NULL, 0, propagateErrors, error);
            if (conn->env->threaded)
                dpiGen__setRefCount(stmt, error, -1);
            if (status < 0)
                return DPI_FAILURE;
        }
    }

    // close all open LOBs; the same comments apply as for statements
    // NOTE: Oracle Client 20 automatically closes all open LOBs which makes
    // this code redundant; as such, it can be removed once the minimum version
    // supported by ODPI-C is 20
    if (conn->openLobs && !conn->externalHandle) {
        for (i = 0; i < conn->openLobs->numSlots; i++) {
            lob = (dpiLob*) conn->openLobs->handles[i];
            if (!lob)
                continue;
            if (conn->env->threaded) {
                dpiMutex__acquire(conn->env->mutex);
                status = dpiGen__checkHandle(lob, DPI_HTYPE_LOB, NULL, NULL);
                if (status == DPI_SUCCESS)
                    lob->refCount += 1;
                dpiMutex__release(conn->env->mutex);
                if (status < 0)
                    continue;
            }
            status = dpiLob__close(lob, propagateErrors, error);
            if (conn->env->threaded)
                dpiGen__setRefCount(lob, error, -1);
            if (status < 0)
                return DPI_FAILURE;
        }
    }

    // handle connections created with an external handle
    if (conn->externalHandle) {
        conn->sessionHandle = NULL;

    // handle standalone connections
    } else if (conn->standalone) {

        // end session and free session handle
        if (dpiOci__sessionEnd(conn, propagateErrors, error) < 0)
            return DPI_FAILURE;
        dpiOci__handleFree(conn->sessionHandle, DPI_OCI_HTYPE_SESSION);
        conn->sessionHandle = NULL;

        // detach from server and free server handle
        if (dpiOci__serverDetach(conn, propagateErrors, error) < 0)
            return DPI_FAILURE;
        dpiOci__handleFree(conn->serverHandle, DPI_OCI_HTYPE_SERVER);

        // free service context handle
        dpiOci__handleFree(conn->handle, DPI_OCI_HTYPE_SVCCTX);

    // handle pooled connections
    } else {

        // if session is to be dropped, mark it as a dead session
        if (mode & DPI_OCI_SESSRLS_DROPSESS) {
            conn->deadSession = 1;

        // otherwise, check server status; if not connected, ensure session is
        // dropped
        } else if (conn->serverHandle) {
            if (dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                    &serverStatus, NULL, DPI_OCI_ATTR_SERVER_STATUS,
                    "get server status", error) < 0 ||
                    serverStatus != DPI_OCI_SERVER_NORMAL)
                conn->deadSession = 1;
        }

        // update last time used (if the session isn't going to be dropped)
        // clear last time used (if the session is going to be dropped)
        // do nothing, however, if not using a pool or the pool is being closed
        if (conn->sessionHandle && conn->pool && conn->pool->handle) {

            // get the pointer from the context associated with the session
            lastTimeUsed = NULL;
            if (dpiOci__contextGetValue(conn, DPI_CONTEXT_LAST_TIME_USED,
                    (uint32_t) (sizeof(DPI_CONTEXT_LAST_TIME_USED) - 1),
                    (void**) &lastTimeUsed, propagateErrors, error) < 0)
                return DPI_FAILURE;

            // if pointer available and session is going to be dropped, clear
            // memory in order to avoid memory leak in OCI
            if (lastTimeUsed && conn->deadSession) {
                dpiOci__contextSetValue(conn, DPI_CONTEXT_LAST_TIME_USED,
                        (uint32_t) (sizeof(DPI_CONTEXT_LAST_TIME_USED) - 1),
                        NULL, 0, error);
                dpiOci__memoryFree(conn, lastTimeUsed, error);
                lastTimeUsed = NULL;

            // otherwise, if the pointer is not available, allocate a new
            // pointer and set it
            } else if (!lastTimeUsed && !conn->deadSession) {
                if (dpiOci__memoryAlloc(conn, (void**) &lastTimeUsed,
                        sizeof(time_t), propagateErrors, error) < 0)
                    return DPI_FAILURE;
                if (dpiOci__contextSetValue(conn, DPI_CONTEXT_LAST_TIME_USED,
                        (uint32_t) (sizeof(DPI_CONTEXT_LAST_TIME_USED) - 1),
                        lastTimeUsed, propagateErrors, error) < 0) {
                    dpiOci__memoryFree(conn, lastTimeUsed, error);
                    lastTimeUsed = NULL;
                }
            }

            // set last time used (used when acquiring a session to determine
            // if ping is required)
            if (lastTimeUsed)
                *lastTimeUsed = time(NULL);

        }

        // release session
        if (conn->deadSession)
            mode |= DPI_OCI_SESSRLS_DROPSESS;
        else if (dpiUtils__checkClientVersion(conn->env->versionInfo, 12, 2,
                NULL) == DPI_SUCCESS && (mode & DPI_MODE_CONN_CLOSE_RETAG) &&
                tag && tagLength > 0)
            mode |= DPI_OCI_SESSRLS_MULTIPROPERTY_TAG;
        if (dpiOci__sessionRelease(conn, tag, tagLength, mode, propagateErrors,
                error) < 0)
            return DPI_FAILURE;
        conn->sessionHandle = NULL;

    }
    conn->handle = NULL;
    conn->serverHandle = NULL;

    // destroy sharding and super sharding key descriptors, if applicable
    if (conn->shardingKey) {
        dpiOci__descriptorFree(conn->shardingKey, DPI_OCI_DTYPE_SHARDING_KEY);
        conn->shardingKey = NULL;
    }
    if (conn->superShardingKey) {
        dpiOci__descriptorFree(conn->superShardingKey,
                DPI_OCI_DTYPE_SHARDING_KEY);
        conn->superShardingKey = NULL;
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__commit() [PRIVATE]
//   Internal method used to commit the transaction associated with the
// connection. Once the commit has taken place, the transaction handle
// associated with the connection is cleared.
//-----------------------------------------------------------------------------
int dpiConn__commit(dpiConn *conn, dpiError *error)
{
    if (dpiOci__transCommit(conn, conn->commitMode, error) < 0)
        return DPI_FAILURE;
    if (dpiConn__clearTransaction(conn, error) < 0)
        return DPI_FAILURE;
    conn->commitMode = DPI_OCI_DEFAULT;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__create() [PRIVATE]
//   Perform internal initialization of the connection.
//-----------------------------------------------------------------------------
int dpiConn__create(dpiConn *conn, const dpiContext *context,
        const char *userName, uint32_t userNameLength, const char *password,
        uint32_t passwordLength, const char *connectString,
        uint32_t connectStringLength, dpiPool *pool,
        const dpiCommonCreateParams *commonParams,
        dpiConnCreateParams *createParams, dpiError *error)
{
    void *envHandle = NULL;
    int status;

    // mark connection as being created so that errors that are raised do not
    // do dead connection detection
    conn->creating = 1;

    // allocate handle lists for statements, LOBs and objects
    if (dpiHandleList__create(&conn->openStmts, error) < 0)
        return DPI_FAILURE;
    if (dpiHandleList__create(&conn->openLobs, error) < 0)
        return DPI_FAILURE;
    if (dpiHandleList__create(&conn->objects, error) < 0)
        return DPI_FAILURE;

    // if an external service context handle is provided, acquire the
    // environment handle from it; need a temporary environment handle in order
    // to do so
    if (createParams->externalHandle) {
        error->env = conn->env;
        if (dpiOci__envNlsCreate(&conn->env->handle, DPI_OCI_DEFAULT, 0, 0,
                error) < 0)
            return DPI_FAILURE;
        if (dpiOci__handleAlloc(conn->env->handle, &error->handle,
                DPI_OCI_HTYPE_ERROR, "allocate temp OCI error", error) < 0)
            return DPI_FAILURE;
        if (dpiOci__attrGet(createParams->externalHandle, DPI_OCI_HTYPE_SVCCTX,
                &envHandle, NULL, DPI_OCI_ATTR_ENV, "get env handle",
                error) < 0)
            return DPI_FAILURE;
        dpiOci__handleFree(conn->env->handle, DPI_OCI_HTYPE_ENV);
        error->handle = NULL;
        conn->env->handle = NULL;
    }

    // initialize environment (for non-pooled connections)
    if (!pool && dpiEnv__init(conn->env, context, commonParams, envHandle,
            commonParams->createMode, error) < 0)
        return DPI_FAILURE;

    // if a handle is specified, use it
    if (createParams->externalHandle)
        return dpiConn__attachExternal(conn, createParams->externalHandle,
                error);

    // connection class, sharding and the use of session pools require the use
    // of the OCISessionGet() method; all other cases use the OCISessionBegin()
    // method which is more capable
    if (pool || (createParams->connectionClass &&
            createParams->connectionClassLength > 0) ||
            createParams->shardingKeyColumns ||
            createParams->superShardingKeyColumns) {
        status = dpiConn__get(conn, userName, userNameLength, password,
                passwordLength, connectString, connectStringLength,
                commonParams, createParams, pool, error);
    } else {
        status = dpiConn__createStandalone(conn, userName, userNameLength,
                password, passwordLength, connectString, connectStringLength,
                commonParams, createParams, error);
    }

    // mark connection as no longer being created so that subsequent errors
    // that are raised do perform dead connection detection
    conn->creating = 0;

    return status;
}


//-----------------------------------------------------------------------------
// dpiConn__createStandalone() [PRIVATE]
//   Create a standalone connection to the database using the parameters
// specified.
//-----------------------------------------------------------------------------
static int dpiConn__createStandalone(dpiConn *conn, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        const char *connectString, uint32_t connectStringLength,
        const dpiCommonCreateParams *commonParams,
        const dpiConnCreateParams *createParams, dpiError *error)
{
    uint32_t credentialType, authMode;
    int used = 0;

    // mark the connection as a standalone connection
    conn->standalone = 1;

    // allocate the server handle
    if (dpiOci__handleAlloc(conn->env->handle, &conn->serverHandle,
            DPI_OCI_HTYPE_SERVER, "allocate server handle", error) < 0)
        return DPI_FAILURE;

    // attach to the server
    if (dpiOci__serverAttach(conn, connectString, connectStringLength,
            error) < 0)
        return DPI_FAILURE;

    // allocate the service context handle
    if (dpiOci__handleAlloc(conn->env->handle, &conn->handle,
            DPI_OCI_HTYPE_SVCCTX, "allocate service context handle",
            error) < 0)
        return DPI_FAILURE;

    // set attribute for server handle
    if (dpiOci__attrSet(conn->handle, DPI_OCI_HTYPE_SVCCTX, conn->serverHandle,
            0, DPI_OCI_ATTR_SERVER, "set server handle", error) < 0)
        return DPI_FAILURE;

    // allocate the session handle
    if (dpiOci__handleAlloc(conn->env->handle, &conn->sessionHandle,
            DPI_OCI_HTYPE_SESSION, "allocate session handle", error) < 0)
        return DPI_FAILURE;

    // driver name and edition are only relevant for standalone connections
    if (dpiUtils__setAttributesFromCommonCreateParams(conn->sessionHandle,
            DPI_OCI_HTYPE_SESSION, commonParams, error) < 0)
        return DPI_FAILURE;

    // set access token for token based authentication
    if (commonParams->accessToken) {
        if (dpiUtils__setAccessTokenAttributes(conn->sessionHandle,
                commonParams->accessToken, conn->env->versionInfo, error) < 0)
            return DPI_FAILURE;
    }

    // populate attributes on the session handle
    if (dpiConn__setAttributesFromCreateParams(conn, conn->sessionHandle,
            DPI_OCI_HTYPE_SESSION, userName, userNameLength, password,
            passwordLength, commonParams, createParams, &used, error) < 0)
        return DPI_FAILURE;

    // set the session handle on the service context handle
    if (dpiOci__attrSet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
            conn->sessionHandle, 0, DPI_OCI_ATTR_SESSION, "set session handle",
            error) < 0)
        return DPI_FAILURE;

    // if a new password is specified, change it (this also creates the session
    // so a call to OCISessionBegin() is not needed)
    if (createParams->newPassword && createParams->newPasswordLength > 0) {
        authMode = DPI_OCI_AUTH;
        if (createParams->authMode & DPI_MODE_AUTH_SYSDBA)
            authMode |= DPI_OCI_CPW_SYSDBA;
        if (createParams->authMode & DPI_MODE_AUTH_SYSOPER)
            authMode |= DPI_OCI_CPW_SYSOPER;
        if (createParams->authMode & DPI_MODE_AUTH_SYSASM)
            authMode |= DPI_OCI_CPW_SYSASM;
        if (createParams->authMode & DPI_MODE_AUTH_SYSBKP)
            authMode |= DPI_OCI_CPW_SYSBKP;
        if (createParams->authMode & DPI_MODE_AUTH_SYSDGD)
            authMode |= DPI_OCI_CPW_SYSDGD;
        if (createParams->authMode & DPI_MODE_AUTH_SYSKMT)
            authMode |= DPI_OCI_CPW_SYSKMT;
        return dpiOci__passwordChange(conn, userName, userNameLength, password,
                passwordLength, createParams->newPassword,
                createParams->newPasswordLength, authMode, error);
    }

    // begin the session
    credentialType = (createParams->externalAuth) ? DPI_OCI_CRED_EXT :
            DPI_OCI_CRED_RDBMS;
    authMode = createParams->authMode | DPI_OCI_STMT_CACHE;
    if (dpiOci__sessionBegin(conn, credentialType, authMode, error) < 0)
        return DPI_FAILURE;
    if (dpiConn__getServerCharset(conn, error) < 0)
        return DPI_FAILURE;

    // set the statement cache size
    if (dpiOci__attrSet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
            (void*) &commonParams->stmtCacheSize, 0,
            DPI_OCI_ATTR_STMTCACHESIZE, "set stmt cache size", error) < 0)
        return DPI_FAILURE;

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__free() [INTERNAL]
//   Free the memory and any resources associated with the connection.
//-----------------------------------------------------------------------------
void dpiConn__free(dpiConn *conn, dpiError *error)
{
    if (conn->handle)
        dpiConn__close(conn, DPI_MODE_CONN_CLOSE_DEFAULT, NULL, 0, 0,
                error);
    if (conn->pool) {
        dpiGen__setRefCount(conn->pool, error, -1);
        conn->pool = NULL;
        conn->env = NULL;
    }
    if (conn->transactionHandle) {
        dpiOci__handleFree(conn->transactionHandle, DPI_OCI_HTYPE_TRANS);
        conn->transactionHandle = NULL;
    }
    if (conn->env) {
        dpiEnv__free(conn->env, error);
        conn->env = NULL;
    }
    if (conn->releaseString) {
        dpiUtils__freeMemory((void*) conn->releaseString);
        conn->releaseString = NULL;
    }
    if (conn->openStmts) {
        dpiHandleList__free(conn->openStmts);
        conn->openStmts = NULL;
    }
    if (conn->openLobs) {
        dpiHandleList__free(conn->openLobs);
        conn->openLobs = NULL;
    }
    if (conn->objects) {
        dpiHandleList__free(conn->objects);
        conn->objects = NULL;
    }
    if (conn->info) {
        dpiUtils__freeMemory(conn->info);
        conn->info = NULL;
    }
    dpiUtils__freeMemory(conn);
}


//-----------------------------------------------------------------------------
// dpiConn__get() [INTERNAL]
//   Create a connection to the database using the parameters specified. This
// method uses the simplified OCI session creation protocol which is required
// when using pools and session tagging.
//-----------------------------------------------------------------------------
static int dpiConn__get(dpiConn *conn, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        const char *connectString, uint32_t connectStringLength,
        const dpiCommonCreateParams *commonParams,
        dpiConnCreateParams *createParams, dpiPool *pool, dpiError *error)
{
    int externalAuth, status;
    void *authInfo;
    uint32_t mode;
    int used = 0;

    // clear pointers if length is 0
    if (userNameLength == 0)
        userName = NULL;
    if (passwordLength == 0)
        password = NULL;

    // set things up for the call to acquire a session
    if (pool) {
        dpiGen__setRefCount(pool, error, 1);
        conn->pool = pool;
        mode = DPI_OCI_SESSGET_SPOOL;
        externalAuth = pool->externalAuth;
        if (userName && pool->homogeneous)
            return dpiError__set(error, "check proxy", DPI_ERR_INVALID_PROXY);

        // if the userName is provided but no password is provided and external
        // authentication is not being used, proxy authentication is taking
        // place
        if (userName && !password && !externalAuth)
            mode |= DPI_OCI_SESSGET_CREDPROXY;
        if (createParams->matchAnyTag)
            mode |= DPI_OCI_SESSGET_SPOOL_MATCHANY;
        if (dpiUtils__checkClientVersion(conn->env->versionInfo, 12, 2,
                NULL) == DPI_SUCCESS && createParams->tag &&
                createParams->tagLength > 0)
            mode |= DPI_OCI_SESSGET_MULTIPROPERTY_TAG;
    } else {
        mode = DPI_OCI_SESSGET_STMTCACHE;
        externalAuth = createParams->externalAuth;
    }
    if (createParams->authMode & DPI_MODE_AUTH_SYSDBA)
        mode |= DPI_OCI_SESSGET_SYSDBA;
    if (externalAuth)
        mode |= DPI_OCI_SESSGET_CREDEXT;

    // create authorization handle
    if (dpiOci__handleAlloc(conn->env->handle, &authInfo,
            DPI_OCI_HTYPE_AUTHINFO, "allocate authinfo handle", error) < 0)
        return DPI_FAILURE;

    // set attributes for create parameters
    if (dpiConn__setAttributesFromCreateParams(conn, authInfo,
            DPI_OCI_HTYPE_AUTHINFO, userName, userNameLength, password,
            passwordLength, commonParams, createParams, &used, error) < 0) {
        dpiOci__handleFree(authInfo, DPI_OCI_HTYPE_AUTHINFO);
        return DPI_FAILURE;
    }

    // get a session from the pool
    status = dpiConn__getSession(conn, mode, connectString,
            connectStringLength, createParams, (used) ? authInfo : NULL,
            error);
    dpiOci__handleFree(authInfo, DPI_OCI_HTYPE_AUTHINFO);
    if (status < 0)
        return status;
    return dpiConn__getServerCharset(conn, error);
}


//-----------------------------------------------------------------------------
// dpiConn__getAttributeText() [INTERNAL]
//   Get the value of the OCI attribute from a text string.
//-----------------------------------------------------------------------------
static int dpiConn__getAttributeText(dpiConn *conn, uint32_t attribute,
        const char **value, uint32_t *valueLength, const char *fnName)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiConn__check(conn, fnName, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, value)
    DPI_CHECK_PTR_NOT_NULL(conn, valueLength)

    // determine pointer to pass (OCI uses different sizes)
    switch (attribute) {
        case DPI_OCI_ATTR_CURRENT_SCHEMA:
        case DPI_OCI_ATTR_LTXID:
        case DPI_OCI_ATTR_EDITION:
            status = dpiOci__attrGet(conn->sessionHandle,
                    DPI_OCI_HTYPE_SESSION, (void*) value, valueLength,
                    attribute, "get session value", &error);
            break;
        case DPI_OCI_ATTR_INSTNAME:
        case DPI_OCI_ATTR_INTERNAL_NAME:
        case DPI_OCI_ATTR_EXTERNAL_NAME:
        case DPI_OCI_ATTR_DBNAME:
        case DPI_OCI_ATTR_DBDOMAIN:
        case DPI_OCI_ATTR_SERVICENAME:
            status = dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                    (void*) value, valueLength, attribute, "get server value",
                    &error);
            break;
        default:
            status = dpiError__set(&error, "get attribute text",
                    DPI_ERR_NOT_SUPPORTED);
            break;
    }

    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn__getHandles() [INTERNAL]
//   Get the server and session handle from the service context handle.
//-----------------------------------------------------------------------------
static int dpiConn__getHandles(dpiConn *conn, dpiError *error)
{
    if (dpiOci__attrGet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
            (void*) &conn->sessionHandle, NULL, DPI_OCI_ATTR_SESSION,
            "get session handle", error) < 0)
        return DPI_FAILURE;
    if (dpiOci__attrGet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
            (void*) &conn->serverHandle, NULL, DPI_OCI_ATTR_SERVER,
            "get server handle", error) < 0)
        return DPI_FAILURE;

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__getInfo() [INTERNAL]
//   Return information about the connection in the provided structure.
//-----------------------------------------------------------------------------
static int dpiConn__getInfo(dpiConn *conn, dpiError *error)
{
    uint8_t temp8;

    // if the cache has been populated and we are not using DRCP, no need to do
    // anything further
    if (conn->info && conn->info->serverType != DPI_SERVER_TYPE_UNKNOWN &&
            conn->info->serverType != DPI_SERVER_TYPE_POOLED)
        return DPI_SUCCESS;

    // allocate memory for the cached information, if needed
    if (!conn->info) {
        if (dpiUtils__allocateMemory(1, sizeof(dpiConnInfo), 1,
                "allocate connection info", (void**) &conn->info, error) < 0)
            return DPI_FAILURE;
    }

    // determine database domain
    if (dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
            (void*) &conn->info->dbDomain, &conn->info->dbDomainLength,
            DPI_OCI_ATTR_DBDOMAIN, "get database domain", error) < 0)
        return DPI_FAILURE;

    // determine database name
    if (dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
            (void*) &conn->info->dbName, &conn->info->dbNameLength,
            DPI_OCI_ATTR_DBNAME, "get database name", error) < 0)
        return DPI_FAILURE;

    // determine instance name
    if (dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
            (void*) &conn->info->instanceName, &conn->info->instanceNameLength,
            DPI_OCI_ATTR_INSTNAME, "get instance name", error) < 0)
        return DPI_FAILURE;

    // determine service name
    if (dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
            (void*) &conn->info->serviceName, &conn->info->serviceNameLength,
            DPI_OCI_ATTR_SERVICENAME, "get service name", error) < 0)
        return DPI_FAILURE;

    // determine max identifier length; this is only available with Oracle
    // Client 12.2 and higher; databases older than 12.2 are known to be 30;
    // databases newer than that cannot be determined so zero is used.
    if (dpiUtils__checkClientVersion(conn->env->versionInfo, 12, 2,
            NULL) == DPI_SUCCESS) {
        if (dpiOci__attrGet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
                &conn->info->maxIdentifierLength, NULL,
                DPI_OCI_ATTR_MAX_IDENTIFIER_LEN, "get max identifier length",
                error) < 0)
            return DPI_FAILURE;
    } else if (conn->versionInfo.versionNum < 12 ||
            (conn->versionInfo.versionNum == 12 &&
            conn->versionInfo.releaseNum < 2)) {
        conn->info->maxIdentifierLength = 30;
    }

    // determine max open cursors
    if (dpiOci__attrGet(conn->sessionHandle, DPI_OCI_HTYPE_SESSION,
            &conn->info->maxOpenCursors, NULL, DPI_OCI_ATTR_MAX_OPEN_CURSORS,
            "get max open cursors", error) < 0)
        return DPI_FAILURE;

    // determine the server type, if possible; it is determined last in order
    // to ensure that only completely cached information is returned
    if (dpiUtils__checkClientVersion(conn->env->versionInfo, 23, 4,
            NULL) == DPI_SUCCESS) {
        if (dpiOci__attrGet(conn->handle, DPI_OCI_HTYPE_SVCCTX, &temp8,
                NULL, DPI_OCI_ATTR_SERVER_TYPE, "get server type", error) < 0)
            return DPI_FAILURE;
        conn->info->serverType = temp8;
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__getJsonTDO() [INTERNAL]
//   Internal method used for ensuring that the JSON TDO has been cached on the
// connection.
//-----------------------------------------------------------------------------
int dpiConn__getJsonTDO(dpiConn *conn, dpiError *error)
{
    if (conn->jsonTDO)
        return DPI_SUCCESS;
    return dpiOci__typeByName(conn, "SYS", 3, "JSON", 4, &conn->jsonTDO,
            error);
}


//-----------------------------------------------------------------------------
// dpiConn__getRawTDO() [INTERNAL]
//   Internal method used for ensuring that the RAW TDO has been cached on the
// connection.
//-----------------------------------------------------------------------------
int dpiConn__getRawTDO(dpiConn *conn, dpiError *error)
{
    if (conn->rawTDO)
        return DPI_SUCCESS;
    return dpiOci__typeByName(conn, "SYS", 3, "RAW", 3, &conn->rawTDO, error);
}


//-----------------------------------------------------------------------------
// dpiConn__getServerCharset() [INTERNAL]
//   Internal method used for retrieving the server character set. This is used
// to determine if any conversion is required when transferring strings between
// the client and the server.
//-----------------------------------------------------------------------------
static int dpiConn__getServerCharset(dpiConn *conn, dpiError *error)
{
    return dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
            &conn->charsetId, NULL, DPI_OCI_ATTR_CHARSET_ID,
            "get server charset id", error);
}


//-----------------------------------------------------------------------------
// dpiConn__getServerVersion() [INTERNAL]
//   Internal method used for ensuring that the server version has been cached
// on the connection.
//-----------------------------------------------------------------------------
int dpiConn__getServerVersion(dpiConn *conn, int wantReleaseString,
        dpiError *error)
{
    char buffer[512], *releaseString;
    dpiVersionInfo *tempVersionInfo;
    uint32_t serverRelease, mode;
    uint32_t releaseStringLength;
    int ociCanCache;

    // nothing to do if the server version has been cached earlier
    if (conn->releaseString ||
            (conn->versionInfo.versionNum > 0 && !wantReleaseString))
        return DPI_SUCCESS;

    // the server version is not available in the cache so it must be
    // determined; as of Oracle Client 20.3, a special mode is available that
    // causes OCI to cache the server version information, so this mode can be
    // used if the release string information is not desired and the client
    // supports it
    ociCanCache = ((conn->env->versionInfo->versionNum > 20 ||
            (conn->env->versionInfo->versionNum == 20 &&
             conn->env->versionInfo->releaseNum >= 3)) && !wantReleaseString);

    // for earlier versions where the OCI cache is not available, pooled
    // connections can cache the information on the session in order to avoid
    // the round trip, but again only if the release string is not desired;
    // nothing further needs to be done if this cache is present
    if (conn->pool && !ociCanCache && !wantReleaseString) {
        tempVersionInfo = NULL;
        if (dpiOci__contextGetValue(conn, DPI_CONTEXT_SERVER_VERSION,
                (uint32_t) (sizeof(DPI_CONTEXT_SERVER_VERSION) - 1),
                (void**) &tempVersionInfo, 1, error) < 0)
            return DPI_FAILURE;
        if (tempVersionInfo) {
            memcpy(&conn->versionInfo, tempVersionInfo,
                    sizeof(conn->versionInfo));
            return DPI_SUCCESS;
        }
    }

    // calculate the server version by making the appropriate call
    if (ociCanCache) {
        mode = DPI_OCI_SRVRELEASE2_CACHED;
        releaseString = NULL;
        releaseStringLength = 0;
    } else {
        mode = DPI_OCI_DEFAULT;
        releaseString = buffer;
        releaseStringLength = sizeof(buffer);
    }
    if (dpiOci__serverRelease(conn, releaseString, releaseStringLength,
            &serverRelease, mode, error) < 0)
        return DPI_FAILURE;

    // store release string, if applicable
    if (releaseString) {
        conn->releaseStringLength = (uint32_t) strlen(releaseString);
        if (dpiUtils__allocateMemory(1, conn->releaseStringLength, 0,
                "allocate release string", (void**) &conn->releaseString,
                error) < 0)
            return DPI_FAILURE;
        strncpy( (char*) conn->releaseString, releaseString,
                conn->releaseStringLength);
    }

    // process version number
    conn->versionInfo.versionNum = (int)((serverRelease >> 24) & 0xFF);
    if (conn->versionInfo.versionNum >= 18) {
        conn->versionInfo.releaseNum = (int)((serverRelease >> 16) & 0xFF);
        conn->versionInfo.updateNum = (int)((serverRelease >> 12) & 0x0F);
        conn->versionInfo.portReleaseNum = (int)((serverRelease >> 4) & 0xFF);
        conn->versionInfo.portUpdateNum = (int)((serverRelease) & 0xF);
    } else {
        conn->versionInfo.releaseNum = (int)((serverRelease >> 20) & 0x0F);
        conn->versionInfo.updateNum = (int)((serverRelease >> 12) & 0xFF);
        conn->versionInfo.portReleaseNum = (int)((serverRelease >> 8) & 0x0F);
        conn->versionInfo.portUpdateNum = (int)((serverRelease) & 0xFF);
    }
    conn->versionInfo.fullVersionNum = (uint32_t)
            DPI_ORACLE_VERSION_TO_NUMBER(conn->versionInfo.versionNum,
                    conn->versionInfo.releaseNum,
                    conn->versionInfo.updateNum,
                    conn->versionInfo.portReleaseNum,
                    conn->versionInfo.portUpdateNum);

    // for earlier versions where the OCI cache is not available, store the
    // version information on the session in order to avoid the round-trip the
    // next time the pooled session is acquired from the pool
    if (conn->pool && !ociCanCache) {
        if (dpiOci__memoryAlloc(conn, (void**) &tempVersionInfo,
                sizeof(conn->versionInfo), 1, error) < 0)
            return DPI_FAILURE;
        memcpy(tempVersionInfo, &conn->versionInfo, sizeof(conn->versionInfo));
        if (dpiOci__contextSetValue(conn, DPI_CONTEXT_SERVER_VERSION,
                (uint32_t) (sizeof(DPI_CONTEXT_SERVER_VERSION) - 1),
                tempVersionInfo, 1, error) < 0) {
            dpiOci__memoryFree(conn, tempVersionInfo, error);
        }
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__getSession() [INTERNAL]
//   Ping and loop until we get a good session. When a database instance goes
// down, it can leave several bad connections that need to be flushed out
// before a good connection can be acquired. If the connection is brand new
// (ping time context value has not been set) there is no need to do a ping.
// This also ensures that the loop cannot run forever!
//-----------------------------------------------------------------------------
static int dpiConn__getSession(dpiConn *conn, uint32_t mode,
        const char *connectString, uint32_t connectStringLength,
        dpiConnCreateParams *params, void *authInfo, dpiError *error)
{
    uint8_t savedBreakOnTimeout, breakOnTimeout;
    uint32_t savedTimeout;
    time_t *lastTimeUsed;

    while (1) {

        // acquire the new session
        params->outNewSession = 0;
        if (dpiOci__sessionGet(conn->env->handle, &conn->handle, authInfo,
                connectString, connectStringLength, params->tag,
                params->tagLength, &params->outTag, &params->outTagLength,
                &params->outTagFound, mode, error) < 0)
            return DPI_FAILURE;

        // get session and server handles
        if (dpiConn__getHandles(conn, error) < 0)
            return DPI_FAILURE;

        // for standalone connections, nothing more needs to be done
        if (!conn->pool) {
            params->outNewSession = 1;
            break;
        }

        // remainder of the loop is for pooled connections only; get last time
        // used from session context; if value is not found, a new connection
        // has been created and there is no need to perform a ping
        lastTimeUsed = NULL;
        if (dpiOci__contextGetValue(conn, DPI_CONTEXT_LAST_TIME_USED,
                (uint32_t) (sizeof(DPI_CONTEXT_LAST_TIME_USED) - 1),
                (void**) &lastTimeUsed, 1, error) < 0)
            return DPI_FAILURE;
        if (!lastTimeUsed) {
            params->outNewSession = 1;

            // for pooled connections, set the statement cache size; when a
            // pool is created, the minSessions value is used to create
            // connections and these use the default statement cache size, not
            // the statement cache size specified for the pool; setting the
            // value here eliminates that discrepancy
            if (dpiOci__attrSet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
                    &conn->pool->stmtCacheSize, 0, DPI_OCI_ATTR_STMTCACHESIZE,
                    "set stmt cache size", error) < 0)
                return DPI_FAILURE;

            break;
        }

        // if ping interval is negative or the ping interval (in seconds)
        // has not been exceeded yet, there is also no need to perform a ping
        if (conn->pool->pingInterval < 0 ||
                *lastTimeUsed + conn->pool->pingInterval > time(NULL))
            break;

        // ping needs to be done at this point; set parameters to ensure that
        // the ping does not take too long to complete; keep original values so
        // that they can be restored after the ping is completed
        dpiOci__attrGet(conn->serverHandle,
                DPI_OCI_HTYPE_SERVER, &savedTimeout, NULL,
                DPI_OCI_ATTR_RECEIVE_TIMEOUT, NULL, error);
        dpiOci__attrSet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                &conn->pool->pingTimeout, 0, DPI_OCI_ATTR_RECEIVE_TIMEOUT,
                NULL, error);
        if (conn->env->versionInfo->versionNum >= 12) {
            dpiOci__attrGet(conn->serverHandle,
                    DPI_OCI_HTYPE_SERVER, &savedBreakOnTimeout, NULL,
                    DPI_OCI_ATTR_BREAK_ON_NET_TIMEOUT, NULL, error);
            breakOnTimeout = 0;
            dpiOci__attrSet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                    &breakOnTimeout, 0, DPI_OCI_ATTR_BREAK_ON_NET_TIMEOUT,
                    NULL, error);
        }

        // if ping is successful, the connection is valid and can be returned;
        // restore original network parameters
        if (dpiOci__ping(conn, error) == 0) {
            dpiOci__attrSet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                    &savedTimeout, 0, DPI_OCI_ATTR_RECEIVE_TIMEOUT, NULL,
                    error);
            if (conn->env->versionInfo->versionNum >= 12)
                dpiOci__attrSet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                        &savedBreakOnTimeout, 0,
                        DPI_OCI_ATTR_BREAK_ON_NET_TIMEOUT, NULL, error);
            break;
        }

        // session is bad, need to release and drop it
        dpiOci__sessionRelease(conn, NULL, 0, DPI_OCI_SESSRLS_DROPSESS, 0,
                error);
        conn->handle = NULL;
        conn->serverHandle = NULL;
        conn->sessionHandle = NULL;
        conn->deadSession = 0;

    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__newVector() [INTERNAL]
//   Internal method for creating a vector. If vector information is supplied
// the vector is populated with this information.
//-----------------------------------------------------------------------------
int dpiConn__newVector(dpiConn *conn, dpiVectorInfo *info, dpiVector **vector,
        dpiError *error)
{
    dpiVector *tempVector;

    if (dpiVector__allocate(conn, &tempVector, error) < 0)
        return DPI_FAILURE;
    if (info) {
        if (dpiOci__vectorFromArray(tempVector, info, error) < 0) {
            dpiVector__free(tempVector, error);
            return DPI_FAILURE;
        }
    }

    *vector = tempVector;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__rollback() [PUBLIC]
//   Internal method for rolling back the transaction associated with the
// connection. Once the rollback has taken place, the transaction handle
// associated with the connection is cleared.
//-----------------------------------------------------------------------------
int dpiConn__rollback(dpiConn *conn, dpiError *error)
{
    if (dpiOci__transRollback(conn, 1, error) < 0)
        return DPI_FAILURE;
    if (dpiConn__clearTransaction(conn, error) < 0)
        return DPI_FAILURE;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__setAppContext() [INTERNAL]
//   Populate the session handle with the application context.
//-----------------------------------------------------------------------------
static int dpiConn__setAppContext(void *handle, uint32_t handleType,
        const dpiConnCreateParams *params, dpiError *error)
{
    void *listHandle, *entryHandle;
    dpiAppContext *entry;
    uint32_t i;

    // set the number of application context entries
    if (dpiOci__attrSet(handle, handleType, (void*) &params->numAppContext,
            sizeof(params->numAppContext), DPI_OCI_ATTR_APPCTX_SIZE,
            "set app context size", error) < 0)
        return DPI_FAILURE;

    // get the application context list handle
    if (dpiOci__attrGet(handle, handleType, &listHandle, NULL,
            DPI_OCI_ATTR_APPCTX_LIST, "get context list handle", error) < 0)
        return DPI_FAILURE;

    // set each application context entry
    for (i = 0; i < params->numAppContext; i++) {
        entry = &params->appContext[i];

        // retrieve the context element descriptor
        if (dpiOci__paramGet(listHandle, DPI_OCI_DTYPE_PARAM,
                &entryHandle, i + 1, "get context entry handle", error) < 0)
            return DPI_FAILURE;

        // set the namespace name
        if (dpiOci__attrSet(entryHandle, DPI_OCI_DTYPE_PARAM,
                (void*) entry->namespaceName, entry->namespaceNameLength,
                DPI_OCI_ATTR_APPCTX_NAME, "set namespace name", error) < 0)
            return DPI_FAILURE;

        // set the name
        if (dpiOci__attrSet(entryHandle, DPI_OCI_DTYPE_PARAM,
                (void*) entry->name, entry->nameLength,
                DPI_OCI_ATTR_APPCTX_ATTR, "set name", error) < 0)
            return DPI_FAILURE;

        // set the value
        if (dpiOci__attrSet(entryHandle, DPI_OCI_DTYPE_PARAM,
                (void*) entry->value, entry->valueLength,
                DPI_OCI_ATTR_APPCTX_VALUE, "set value", error) < 0)
            return DPI_FAILURE;

    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__setAttributesFromCreateParams() [INTERNAL]
//   Populate the authorization info structure or session handle using the
// create parameters specified.
//-----------------------------------------------------------------------------
static int dpiConn__setAttributesFromCreateParams(dpiConn *conn, void *handle,
        uint32_t handleType, const char *userName, uint32_t userNameLength,
        const char *password, uint32_t passwordLength,
        const dpiCommonCreateParams *commonParams,
        const dpiConnCreateParams *params, int *used, dpiError *error)
{
    uint32_t purity;

    // the handle is required for all external authentication scenarios except
    // when token authentication is being used
    if (params->externalAuth && (!commonParams || !commonParams->accessToken))
        *used = 1;

    // set credentials
    if (userName && userNameLength > 0) {
        if (dpiOci__attrSet(handle, handleType, (void*) userName,
                userNameLength, DPI_OCI_ATTR_USERNAME, "set user name",
                error) < 0)
            return DPI_FAILURE;
        *used = 1;
    }
    if (password && passwordLength > 0) {
        if (dpiOci__attrSet(handle, handleType, (void*) password,
                passwordLength, DPI_OCI_ATTR_PASSWORD, "set password",
                error) < 0)
            return DPI_FAILURE;
        *used = 1;
    }

    // set connection class and purity parameters
    if (params->connectionClass && params->connectionClassLength > 0) {
        if (dpiOci__attrSet(handle, handleType,
                (void*) params->connectionClass, params->connectionClassLength,
                DPI_OCI_ATTR_CONNECTION_CLASS, "set connection class",
                error) < 0)
            return DPI_FAILURE;
        *used = 1;
    }
    if (params->purity != DPI_OCI_ATTR_PURITY_DEFAULT) {
        purity = params->purity;
        if (dpiOci__attrSet(handle, handleType, &purity, sizeof(purity),
                DPI_OCI_ATTR_PURITY, "set purity", error) < 0)
            return DPI_FAILURE;
        *used = 1;
    }

    // set sharding key and super sharding key parameters
    if (params->shardingKeyColumns && params->numShardingKeyColumns > 0) {
        if (dpiConn__setShardingKey(conn, &conn->shardingKey, handle,
                handleType, DPI_OCI_ATTR_SHARDING_KEY, "set sharding key",
                params->shardingKeyColumns, params->numShardingKeyColumns,
                error) < 0)
            return DPI_FAILURE;
        *used = 1;
    }
    if (params->superShardingKeyColumns &&
            params->numSuperShardingKeyColumns > 0) {
        if (params->numShardingKeyColumns == 0)
            return dpiError__set(error, "ensure sharding key",
                    DPI_ERR_MISSING_SHARDING_KEY);
        if (dpiConn__setShardingKey(conn, &conn->superShardingKey, handle,
                handleType, DPI_OCI_ATTR_SUPER_SHARDING_KEY,
                "set super sharding key", params->superShardingKeyColumns,
                params->numSuperShardingKeyColumns, error) < 0)
            return DPI_FAILURE;
        *used = 1;
    }

    // set application context, if applicable
    if (handleType == DPI_OCI_HTYPE_SESSION && params->numAppContext > 0)
        return dpiConn__setAppContext(handle, handleType, params, error);

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__setAttributeText() [INTERNAL]
//   Set the value of the OCI attribute from a text string.
//-----------------------------------------------------------------------------
static int dpiConn__setAttributeText(dpiConn *conn, uint32_t attribute,
        const char *value, uint32_t valueLength, const char *fnName)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiConn__check(conn, fnName, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(conn, value)

    // determine pointer to pass (OCI uses different sizes)
    switch (attribute) {
        case DPI_OCI_ATTR_ACTION:
        case DPI_OCI_ATTR_CLIENT_IDENTIFIER:
        case DPI_OCI_ATTR_CLIENT_INFO:
        case DPI_OCI_ATTR_CURRENT_SCHEMA:
        case DPI_OCI_ATTR_ECONTEXT_ID:
        case DPI_OCI_ATTR_EDITION:
        case DPI_OCI_ATTR_MODULE:
        case DPI_OCI_ATTR_DBOP:
            status = dpiOci__attrSet(conn->sessionHandle,
                    DPI_OCI_HTYPE_SESSION, (void*) value, valueLength,
                    attribute, "set session value", &error);
            break;
        case DPI_OCI_ATTR_INTERNAL_NAME:
        case DPI_OCI_ATTR_EXTERNAL_NAME:
            status = dpiOci__attrSet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                    (void*) value, valueLength, attribute, "set server value",
                    &error);
            break;
        default:
            status = dpiError__set(&error, "set attribute text",
                    DPI_ERR_NOT_SUPPORTED);
            break;
    }

    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn__setShardingKey() [INTERNAL]
//   Using the specified columns, create a sharding key and set it on the given
// handle.
//-----------------------------------------------------------------------------
static int dpiConn__setShardingKey(dpiConn *conn, void **shardingKey,
        void *handle, uint32_t handleType, uint32_t attribute,
        const char *action, dpiShardingKeyColumn *columns, uint8_t numColumns,
        dpiError *error)
{
    uint8_t i;

    // this is only supported on 12.2 and higher clients
    if (dpiUtils__checkClientVersion(conn->env->versionInfo, 12, 2,
            error) < 0)
        return DPI_FAILURE;

    // create sharding key descriptor, if necessary
    if (dpiOci__descriptorAlloc(conn->env->handle, shardingKey,
            DPI_OCI_DTYPE_SHARDING_KEY, "allocate sharding key", error) < 0)
        return DPI_FAILURE;

    // add each column to the sharding key
    for (i = 0; i < numColumns; i++) {
        if (dpiConn__setShardingKeyValue(conn, *shardingKey, &columns[i],
                error) < 0)
            return DPI_FAILURE;
    }

    // add the sharding key to the handle
    if (dpiOci__attrSet(handle, handleType, *shardingKey, 0, attribute, action,
            error) < 0)
        return DPI_FAILURE;

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__setShardingKeyValue() [INTERNAL]
//   Using the specified columns, create a sharding key and set it on the given
// handle.
//-----------------------------------------------------------------------------
static int dpiConn__setShardingKeyValue(dpiConn *conn, void *shardingKey,
        dpiShardingKeyColumn *column, dpiError *error)
{
    dpiShardingOciDate shardingDateValue;
    uint32_t colLen = 0, descType = 0;
    const dpiOracleType *oracleType;
    dpiOciNumber numberValue;
    int convertOk, status;
    dpiOciDate dateValue;
    void *col = NULL;
    uint16_t colType;

    oracleType = dpiOracleType__getFromNum(column->oracleTypeNum, error);
    if (!oracleType)
        return DPI_FAILURE;
    convertOk = 0;
    colType = oracleType->oracleType;
    switch (column->oracleTypeNum) {
        case DPI_ORACLE_TYPE_VARCHAR:
        case DPI_ORACLE_TYPE_CHAR:
        case DPI_ORACLE_TYPE_RAW:
            if (column->nativeTypeNum == DPI_NATIVE_TYPE_BYTES) {
                col = column->value.asBytes.ptr;
                colLen = column->value.asBytes.length;
                convertOk = 1;
            }
            break;
        case DPI_ORACLE_TYPE_NUMBER:
            col = &numberValue;
            colLen = sizeof(numberValue);
            if (column->nativeTypeNum == DPI_NATIVE_TYPE_DOUBLE) {
                if (dpiDataBuffer__toOracleNumberFromDouble(&column->value,
                        error, &numberValue) < 0)
                    return DPI_FAILURE;
                convertOk = 1;
            } else if (column->nativeTypeNum == DPI_NATIVE_TYPE_INT64) {
                if (dpiDataBuffer__toOracleNumberFromInteger(&column->value,
                        error, &numberValue) < 0)
                    return DPI_FAILURE;
                convertOk = 1;
            } else if (column->nativeTypeNum == DPI_NATIVE_TYPE_UINT64) {
                if (dpiDataBuffer__toOracleNumberFromUnsignedInteger(
                        &column->value, error, &numberValue) < 0)
                    return DPI_FAILURE;
                convertOk = 1;
            } else if (column->nativeTypeNum == DPI_NATIVE_TYPE_BYTES) {
                if (dpiDataBuffer__toOracleNumberFromText(&column->value,
                        conn->env, error, &numberValue) < 0)
                    return DPI_FAILURE;
                convertOk = 1;
            }
            break;
        case DPI_ORACLE_TYPE_DATE:
            if (column->nativeTypeNum == DPI_NATIVE_TYPE_TIMESTAMP) {
                if (dpiDataBuffer__toOracleDate(&column->value,
                        &dateValue) < 0)
                    return DPI_FAILURE;
                convertOk = 1;
            } else if (column->nativeTypeNum == DPI_NATIVE_TYPE_DOUBLE) {
                if (dpiDataBuffer__toOracleDateFromDouble(&column->value,
                        conn->env, error, &dateValue) < 0)
                    return DPI_FAILURE;
                convertOk = 1;
            }

            // for sharding only, the type must be SQLT_DAT, which uses a
            // different format for storing the date values
            if (convertOk) {
                col = &shardingDateValue;
                colLen = sizeof(shardingDateValue);
                colType = DPI_SQLT_DAT;
                shardingDateValue.century =
                        ((uint8_t) (dateValue.year / 100)) + 100;
                shardingDateValue.year = (dateValue.year % 100) + 100;
                shardingDateValue.month = dateValue.month;
                shardingDateValue.day = dateValue.day;
                shardingDateValue.hour = dateValue.hour + 1;
                shardingDateValue.minute = dateValue.minute + 1;
                shardingDateValue.second = dateValue.second + 1;
            }
            break;
        case DPI_ORACLE_TYPE_TIMESTAMP:
        case DPI_ORACLE_TYPE_TIMESTAMP_TZ:
        case DPI_ORACLE_TYPE_TIMESTAMP_LTZ:
            colLen = sizeof(void*);
            colType = DPI_SQLT_TIMESTAMP;
            if (column->nativeTypeNum == DPI_NATIVE_TYPE_TIMESTAMP) {
                descType = DPI_OCI_DTYPE_TIMESTAMP;
                if (dpiOci__descriptorAlloc(conn->env->handle, &col, descType,
                        "alloc timestamp", error) < 0)
                    return DPI_FAILURE;
                if (dpiDataBuffer__toOracleTimestamp(&column->value, conn->env,
                        error, col, 0) < 0) {
                    dpiOci__descriptorFree(col, descType);
                    return DPI_FAILURE;
                }
                convertOk = 1;
            } else if (column->nativeTypeNum == DPI_NATIVE_TYPE_DOUBLE) {
                descType = DPI_OCI_DTYPE_TIMESTAMP_LTZ;
                if (dpiOci__descriptorAlloc(conn->env->handle, &col, descType,
                        "alloc LTZ timestamp", error) < 0)
                    return DPI_FAILURE;
                if (dpiDataBuffer__toOracleTimestampFromDouble(&column->value,
                        DPI_ORACLE_TYPE_TIMESTAMP_LTZ, conn->env, error,
                        col) < 0) {
                    dpiOci__descriptorFree(col, descType);
                    return DPI_FAILURE;
                }
                convertOk = 1;
            }
            break;
        default:
            break;
    }
    if (!convertOk)
        return dpiError__set(error, "check type", DPI_ERR_NOT_SUPPORTED);

    status = dpiOci__shardingKeyColumnAdd(shardingKey, col, colLen, colType,
            error);
    if (descType)
        dpiOci__descriptorFree(col, descType);
    return status;
}


//-----------------------------------------------------------------------------
// dpiConn__setXid() [INTERNAL]
//   Internal method for associating an XID with the connection.
//-----------------------------------------------------------------------------
static int dpiConn__setXid(dpiConn *conn, dpiXid *xid, dpiError *error)
{
    dpiOciXID ociXid;

    // validate XID
    if (xid->globalTransactionIdLength > 0 && !xid->globalTransactionId)
        return dpiError__set(error, "check XID transaction id ptr",
                DPI_ERR_PTR_LENGTH_MISMATCH, "xid->globalTransactionId");
    if (xid->branchQualifierLength > 0 && !xid->branchQualifier)
        return dpiError__set(error, "check XID branch id ptr",
                DPI_ERR_PTR_LENGTH_MISMATCH, "xid->branchQualifier");
    if (xid->globalTransactionIdLength > DPI_XA_MAXGTRIDSIZE)
        return dpiError__set(error, "check size of XID transaction id",
                DPI_ERR_TRANS_ID_TOO_LARGE, xid->globalTransactionIdLength,
                DPI_XA_MAXGTRIDSIZE);
    if (xid->branchQualifierLength > DPI_XA_MAXBQUALSIZE)
        return dpiError__set(error, "check size of XID branch qualifier",
                DPI_ERR_BRANCH_ID_TOO_LARGE, xid->branchQualifierLength,
                DPI_XA_MAXBQUALSIZE);

    // if a transaction handle does not exist, create one
    if (!conn->transactionHandle) {
        if (dpiOci__handleAlloc(conn->env->handle, &conn->transactionHandle,
                DPI_OCI_HTYPE_TRANS, "create transaction handle", error) < 0)
            return DPI_FAILURE;
    }

    // associate the transaction with the connection
    if (dpiOci__attrSet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
            conn->transactionHandle, 0, DPI_OCI_ATTR_TRANS,
            "associate transaction", error) < 0)
        return DPI_FAILURE;

    // associate the XID with the transaction
    ociXid.formatID = xid->formatId;
    ociXid.gtrid_length = xid->globalTransactionIdLength;
    ociXid.bqual_length = xid->branchQualifierLength;
    if (xid->globalTransactionIdLength > 0)
        memcpy(ociXid.data, xid->globalTransactionId,
                xid->globalTransactionIdLength);
    if (xid->branchQualifierLength > 0)
        memcpy(&ociXid.data[xid->globalTransactionIdLength],
                xid->branchQualifier, xid->branchQualifierLength);
    if (dpiOci__attrSet(conn->transactionHandle, DPI_OCI_HTYPE_TRANS,
            &ociXid, sizeof(dpiOciXID), DPI_OCI_ATTR_XID, "set XID",
            error) < 0)
        return DPI_FAILURE;

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiConn__startupDatabase() [INTERNAL]
//   Internal method for starting up a database. This is equivalent to
// "startup nomount" in SQL*Plus.
//-----------------------------------------------------------------------------
static int dpiConn__startupDatabase(dpiConn *conn, const char *pfile,
        uint32_t pfileLength, dpiStartupMode mode, dpiError *error)
{
    void *adminHandle = NULL;
    int status;

    // if a PFILE has been specified, create an admin handle and populate it
    if (pfileLength > 0) {
        if (dpiOci__handleAlloc(conn->env->handle, &adminHandle,
                DPI_OCI_HTYPE_ADMIN, "create admin handle", error) < 0)
            return DPI_FAILURE;
        if (dpiOci__attrSet(adminHandle, DPI_OCI_HTYPE_ADMIN,
                (void*) pfile, pfileLength, DPI_OCI_ATTR_ADMIN_PFILE,
                "associate PFILE", error) < 0) {
            dpiOci__handleFree(adminHandle, DPI_OCI_HTYPE_ADMIN);
            return DPI_FAILURE;
        }
    }

    // perform actual startup call
    status = dpiOci__dbStartup(conn, adminHandle, mode, error);

    // destroy admin handle, if needed
    if (pfileLength > 0)
        dpiOci__handleFree(adminHandle, DPI_OCI_HTYPE_ADMIN);

    return status;
}


//-----------------------------------------------------------------------------
// dpiConn_addRef() [PUBLIC]
//   Add a reference to the connection.
//-----------------------------------------------------------------------------
int dpiConn_addRef(dpiConn *conn)
{
    return dpiGen__addRef(conn, DPI_HTYPE_CONN, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_breakExecution() [PUBLIC]
//   Break (interrupt) the currently executing operation.
//-----------------------------------------------------------------------------
int dpiConn_breakExecution(dpiConn *conn)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiOci__break(conn, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_changePassword() [PUBLIC]
//   Change the password for the specified user.
//-----------------------------------------------------------------------------
int dpiConn_changePassword(dpiConn *conn, const char *userName,
        uint32_t userNameLength, const char *oldPassword,
        uint32_t oldPasswordLength, const char *newPassword,
        uint32_t newPasswordLength)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(conn, userName)
    DPI_CHECK_PTR_AND_LENGTH(conn, oldPassword)
    DPI_CHECK_PTR_AND_LENGTH(conn, newPassword)
    status = dpiOci__passwordChange(conn, userName, userNameLength,
            oldPassword, oldPasswordLength, newPassword, newPasswordLength,
            DPI_OCI_DEFAULT, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_close() [PUBLIC]
//   Close the connection and ensure it can no longer be used.
//-----------------------------------------------------------------------------
int dpiConn_close(dpiConn *conn, dpiConnCloseMode mode, const char *tag,
        uint32_t tagLength)
{
    int propagateErrors = !(mode & DPI_MODE_CONN_CLOSE_DROP);
    dpiError error;
    int closing;

    // validate parameters
    if (dpiGen__startPublicFn(conn, DPI_HTYPE_CONN, __func__, &error) < 0)
        return DPI_FAILURE;
    if (!conn->handle || conn->closing ||
            (conn->pool && !conn->pool->handle)) {
        dpiError__set(&error, "check connected", DPI_ERR_NOT_CONNECTED);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    DPI_CHECK_PTR_AND_LENGTH(conn, tag)
    if (mode && !conn->pool) {
        dpiError__set(&error, "check in pool", DPI_ERR_CONN_NOT_IN_POOL);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    if (conn->externalHandle) {
        dpiError__set(&error, "check external", DPI_ERR_CONN_IS_EXTERNAL);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    // determine whether connection is already being closed and if not, mark
    // connection as being closed; this MUST be done while holding the lock
    // (if in threaded mode) to avoid race conditions!
    if (conn->env->threaded)
        dpiMutex__acquire(conn->env->mutex);
    closing = conn->closing;
    conn->closing = 1;
    if (conn->env->threaded)
        dpiMutex__release(conn->env->mutex);

    // if connection is already being closed, raise an exception
    if (closing) {
        dpiError__set(&error, "check closing", DPI_ERR_NOT_CONNECTED);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    // if actual close fails, reset closing flag; again, this must be done
    // while holding the lock (if in threaded mode) in order to avoid race
    // conditions!
    if (dpiConn__close(conn, mode, tag, tagLength, propagateErrors,
            &error) < 0) {
        if (conn->env->threaded)
            dpiMutex__acquire(conn->env->mutex);
        conn->closing = 0;
        if (conn->env->threaded)
            dpiMutex__release(conn->env->mutex);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_commit() [PUBLIC]
//   Commit the transaction associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_commit(dpiConn *conn)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiConn__commit(conn, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_create() [PUBLIC]
//   Create a standalone connection to the database using the parameters
// specified.
//-----------------------------------------------------------------------------
int dpiConn_create(const dpiContext *context, const char *userName,
        uint32_t userNameLength, const char *password, uint32_t passwordLength,
        const char *connectString, uint32_t connectStringLength,
        const dpiCommonCreateParams *commonParams,
        dpiConnCreateParams *createParams, dpiConn **conn)
{
    dpiCommonCreateParams localCommonParams;
    dpiConnCreateParams localCreateParams;
    dpiConn *tempConn;
    dpiError error;
    int status;

    // validate parameters
    if (dpiGen__startPublicFn(context, DPI_HTYPE_CONTEXT, __func__,
            &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(context, conn)
    DPI_CHECK_PTR_AND_LENGTH(context, userName)
    DPI_CHECK_PTR_AND_LENGTH(context, password)
    DPI_CHECK_PTR_AND_LENGTH(context, connectString)

    // use default parameters if none provided
    if (!commonParams) {
        dpiContext__initCommonCreateParams(context, &localCommonParams);
        commonParams = &localCommonParams;
    }
    if (!createParams) {
        dpiContext__initConnCreateParams(&localCreateParams);
        createParams = &localCreateParams;
    }

    // password must not be specified if external authentication is desired
    if (createParams->externalAuth && password && passwordLength > 0) {
        dpiError__set(&error, "verify no password with external auth",
                DPI_ERR_EXT_AUTH_WITH_CREDENTIALS);
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    }

    // the username must be enclosed within [] if external authentication
    // with proxy is desired
    if (createParams->externalAuth && userName && userNameLength > 0 &&
            (userName[0] != '[' || userName[userNameLength - 1] != ']')) {
        dpiError__set(&error, "verify proxy user name with external auth",
                DPI_ERR_EXT_AUTH_INVALID_PROXY);
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error );
    }

    if (commonParams->accessToken) {

        // externalAuth must be set to true for token based authentication
        if (!createParams->externalAuth)
            return dpiError__set(&error, "check externalAuth value",
                    DPI_ERR_STANDALONE_TOKEN_BASED_AUTH);

        // cannot set username for token based authentication
        if (userName && userNameLength > 0)
            return dpiError__set(&error, "verify user in token based auth",
                DPI_ERR_EXT_AUTH_WITH_CREDENTIALS);
    }

    // connectionClass and edition cannot be specified at the same time
    if (createParams->connectionClass &&
            createParams->connectionClassLength > 0 &&
            commonParams->edition && commonParams->editionLength > 0) {
        dpiError__set(&error, "check edition/conn class",
                DPI_ERR_NO_EDITION_WITH_CONN_CLASS);
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    }

    // newPassword and edition cannot be specified at the same time
    if (createParams->newPassword && createParams->newPasswordLength > 0 &&
            commonParams->edition && commonParams->editionLength > 0) {
        dpiError__set(&error, "check edition/new password",
                DPI_ERR_NO_EDITION_WITH_NEW_PASSWORD);
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    }

    // handle case where pool is specified
    if (createParams->pool) {
        if (dpiGen__checkHandle(createParams->pool, DPI_HTYPE_POOL,
                "verify pool", &error) < 0)
            return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
        if (!createParams->pool->handle) {
            dpiError__set(&error, "check pool", DPI_ERR_NOT_CONNECTED);
            return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
        }
        status = dpiPool__acquireConnection(createParams->pool, userName,
                userNameLength, password, passwordLength, createParams, conn,
                &error);
        return dpiGen__endPublicFn(context, status, &error);
    }

    // create connection
    if (dpiGen__allocate(DPI_HTYPE_CONN, NULL, (void**) &tempConn, &error) < 0)
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    if (dpiConn__create(tempConn, context, userName, userNameLength,
            password, passwordLength, connectString, connectStringLength,
            NULL, commonParams, createParams, &error) < 0) {
        dpiConn__free(tempConn, &error);
        return dpiGen__endPublicFn(context, DPI_FAILURE, &error);
    }

    *conn = tempConn;
    dpiHandlePool__release(tempConn->env->errorHandles, &error.handle);
    return dpiGen__endPublicFn(context, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getSodaDb() [PUBLIC]
//   Create a new SODA collection with the given name and metadata.
//-----------------------------------------------------------------------------
int dpiConn_getSodaDb(dpiConn *conn, dpiSodaDb **db)
{
    dpiError error;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiUtils__checkClientVersion(conn->env->versionInfo, 18, 3,
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiUtils__checkDatabaseVersion(conn, 18, 0, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiGen__allocate(DPI_HTYPE_SODA_DB, conn->env, (void**) db,
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    dpiGen__setRefCount(conn, &error, 1);
    (*db)->conn = conn;
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_deqObject() [PUBLIC]
//   Dequeue a message from the specified queue.
//-----------------------------------------------------------------------------
int dpiConn_deqObject(dpiConn *conn, const char *queueName,
        uint32_t queueNameLength, dpiDeqOptions *options, dpiMsgProps *props,
        dpiObject *payload, const char **msgId, uint32_t *msgIdLength)
{
    dpiError error;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(options, DPI_HTYPE_DEQ_OPTIONS, "verify options",
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(props, DPI_HTYPE_MSG_PROPS,
            "verify message properties", &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(payload, DPI_HTYPE_OBJECT, "verify payload",
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(conn, queueName)
    DPI_CHECK_PTR_NOT_NULL(conn, msgId)
    DPI_CHECK_PTR_NOT_NULL(conn, msgIdLength)

    // dequeue message
    if (dpiOci__aqDeq(conn, queueName, options->handle, props->handle,
            payload->type->tdo, &payload->instance, &payload->indicator,
            &props->msgIdRaw, &error) < 0) {
        if (error.buffer->code == 25228) {
            *msgId = NULL;
            *msgIdLength = 0;
            return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
        }
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    dpiMsgProps__extractMsgId(props, msgId, msgIdLength);
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_enqObject() [PUBLIC]
//   Enqueue a message to the specified queue.
//-----------------------------------------------------------------------------
int dpiConn_enqObject(dpiConn *conn, const char *queueName,
        uint32_t queueNameLength, dpiEnqOptions *options, dpiMsgProps *props,
        dpiObject *payload, const char **msgId, uint32_t *msgIdLength)
{
    dpiError error;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(options, DPI_HTYPE_ENQ_OPTIONS, "verify options",
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(props, DPI_HTYPE_MSG_PROPS,
            "verify message properties", &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(payload, DPI_HTYPE_OBJECT, "verify payload",
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(conn, queueName)
    DPI_CHECK_PTR_NOT_NULL(conn, msgId)
    DPI_CHECK_PTR_NOT_NULL(conn, msgIdLength)

    // enqueue message
    if (dpiOci__aqEnq(conn, queueName, options->handle, props->handle,
            payload->type->tdo, &payload->instance, &payload->indicator,
            &props->msgIdRaw, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    dpiMsgProps__extractMsgId(props, msgId, msgIdLength);
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getCallTimeout() [PUBLIC]
//   Return the call timeout (in milliseconds) used for round-trips to the
// database. This is only valid in Oracle Client 18c and higher.
//-----------------------------------------------------------------------------
int dpiConn_getCallTimeout(dpiConn *conn, uint32_t *value)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, value)
    if (dpiUtils__checkClientVersion(conn->env->versionInfo, 18, 1,
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);

    // get call timeout
    status = dpiOci__attrGet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
            (void*) value, 0, DPI_OCI_ATTR_CALL_TIMEOUT, "get call timeout",
            &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getCurrentSchema() [PUBLIC]
//   Return the current schema associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_getCurrentSchema(dpiConn *conn, const char **value,
        uint32_t *valueLength)
{
    return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_CURRENT_SCHEMA, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getDbDomain() [PUBLIC]
//   Returns the name of the database domain.
//-----------------------------------------------------------------------------
int dpiConn_getDbDomain(dpiConn *conn, const char **value,
        uint32_t *valueLength)
{
  return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_DBDOMAIN, value,
          valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getDbName() [PUBLIC]
//   Returns the name of the database.
//-----------------------------------------------------------------------------
int dpiConn_getDbName(dpiConn *conn, const char **value, uint32_t *valueLength)
{
    return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_DBNAME, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getEdition() [PUBLIC]
//   Return the edition associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_getEdition(dpiConn *conn, const char **value,
        uint32_t *valueLength)
{
    return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_EDITION, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getEncodingInfo() [PUBLIC]
//   Get the encodings from the connection.
//-----------------------------------------------------------------------------
int dpiConn_getEncodingInfo(dpiConn *conn, dpiEncodingInfo *info)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiEnv__getEncodingInfo(conn->env, info);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getExternalName() [PUBLIC]
//   Return the external name associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_getExternalName(dpiConn *conn, const char **value,
        uint32_t *valueLength)
{
    return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_EXTERNAL_NAME, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getHandle() [PUBLIC]
//   Get the OCI service context handle associated with the connection. This is
// available in order to allow for extensions to the library using OCI
// directly.
//-----------------------------------------------------------------------------
int dpiConn_getHandle(dpiConn *conn, void **handle)
{
    dpiError error;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, handle)
    *handle = conn->handle;
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getInfo() [PUBLIC]
//   Return information about the connection in the provided structure.
//-----------------------------------------------------------------------------
int dpiConn_getInfo(dpiConn *conn, dpiConnInfo *info)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, info)
    status = dpiConn__getInfo(conn, &error);
    if (status == DPI_SUCCESS)
        memcpy(info, conn->info, sizeof(dpiConnInfo));
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getInstanceName() [PUBLIC]
//   Return the instance name associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_getInstanceName(dpiConn *conn, const char **value,
        uint32_t *valueLength)
{
    return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_INSTNAME, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getInternalName() [PUBLIC]
//   Return the internal name associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_getInternalName(dpiConn *conn, const char **value,
        uint32_t *valueLength)
{
    return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_INTERNAL_NAME, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getIsHealthy() [PUBLIC]
//   Return the health of the connection.
//-----------------------------------------------------------------------------
int dpiConn_getIsHealthy(dpiConn *conn, int *isHealthy)
{
    dpiError error;
    int status;
    uint32_t serverStatus;

    if (dpiGen__startPublicFn(conn, DPI_HTYPE_CONN, __func__, &error) < 0)
        return DPI_FAILURE;
    if (!conn->handle || !conn->serverHandle || conn->closing ||
            conn->deadSession || (conn->pool && !conn->pool->handle)) {
        *isHealthy = 0;
        status = DPI_SUCCESS;
    } else {
        DPI_CHECK_PTR_NOT_NULL(conn, isHealthy)
        status = dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                &serverStatus, NULL, DPI_OCI_ATTR_SERVER_STATUS,
                "get server status", &error);
        *isHealthy = (serverStatus == DPI_OCI_SERVER_NORMAL);
    }
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getLTXID() [PUBLIC]
//   Return the logical transaction id associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_getLTXID(dpiConn *conn, const char **value, uint32_t *valueLength)
{
    return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_LTXID, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getMaxOpenCursors() [PUBLIC]
//   Returns the maximum number of cursors that can be opened by the database.
// This is the value of the "open_cursors" parameter in init.ora.
//-----------------------------------------------------------------------------
int dpiConn_getMaxOpenCursors(dpiConn *conn, uint32_t *maxOpenCursors)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, maxOpenCursors)

    status = dpiOci__attrGet(conn->sessionHandle, DPI_OCI_HTYPE_SESSION,
            maxOpenCursors, NULL, DPI_OCI_ATTR_MAX_OPEN_CURSORS,
            "get max open cursors", &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getObjectType() [PUBLIC]
//   Look up an object type given its name and return it.
//-----------------------------------------------------------------------------
int dpiConn_getObjectType(dpiConn *conn, const char *name, uint32_t nameLength,
        dpiObjectType **objType)
{
    void *describeHandle, *param, *tdo;
    int status, useTypeByFullName;
    dpiError error;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, name)
    DPI_CHECK_PTR_NOT_NULL(conn, objType)

    // allocate describe handle
    if (dpiOci__handleAlloc(conn->env->handle, &describeHandle,
            DPI_OCI_HTYPE_DESCRIBE, "allocate describe handle", &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);

    // Oracle Client 12.1 is capable of using OCITypeByFullName() but will
    // fail if accessing an Oracle 11.2 database
    useTypeByFullName = 1;
    if (conn->env->versionInfo->versionNum < 12)
        useTypeByFullName = 0;
    else if (dpiConn__getServerVersion(conn, 0, &error) < 0)
        return DPI_FAILURE;
    else if (conn->versionInfo.versionNum < 12)
        useTypeByFullName = 0;

    // new API is supported so use it
    if (useTypeByFullName) {
        if (dpiOci__typeByFullName(conn, name, nameLength, &tdo, &error) < 0) {
            dpiOci__handleFree(describeHandle, DPI_OCI_HTYPE_DESCRIBE);
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
        }
        if (dpiOci__describeAny(conn, tdo, 0, DPI_OCI_OTYPE_PTR,
                describeHandle, &error) < 0) {
            dpiOci__handleFree(describeHandle, DPI_OCI_HTYPE_DESCRIBE);
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
        }

    // use older API
    } else {
        if (dpiOci__describeAny(conn, (void*) name, nameLength,
                DPI_OCI_OTYPE_NAME, describeHandle, &error) < 0) {
            dpiOci__handleFree(describeHandle, DPI_OCI_HTYPE_DESCRIBE);
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
        }
    }

    // get the parameter handle
    if (dpiOci__attrGet(describeHandle,
            DPI_OCI_HTYPE_DESCRIBE, &param, 0, DPI_OCI_ATTR_PARAM,
            "get param", &error) < 0) {
        dpiOci__handleFree(describeHandle, DPI_OCI_HTYPE_DESCRIBE);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    // create object type
    status = dpiObjectType__allocate(conn, param, DPI_OCI_HTYPE_DESCRIBE,
            objType, &error);
    dpiOci__handleFree(describeHandle, DPI_OCI_HTYPE_DESCRIBE);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getOciAttr() [PUBLIC]
//   Get the OCI attribute directly. This is intended for testing of attributes
// not currently exposed by ODPI-C and should only be used for that purpose.
//-----------------------------------------------------------------------------
int dpiConn_getOciAttr(dpiConn *conn, uint32_t handleType,
        uint32_t attribute, dpiDataBuffer *value, uint32_t *valueLength)
{
    const void *handle;
    dpiError error;
    int status;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, value)
    DPI_CHECK_PTR_NOT_NULL(conn, valueLength)
    switch (handleType) {
        case DPI_OCI_HTYPE_SVCCTX:
            handle = conn->handle;
            break;
        case DPI_OCI_HTYPE_SERVER:
            handle = conn->serverHandle;
            break;
        case DPI_OCI_HTYPE_SESSION:
            handle = conn->sessionHandle;
            break;
        default:
            dpiError__set(&error, "check handle type", DPI_ERR_NOT_SUPPORTED);
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    // get attribute value
    status = dpiOci__attrGet(handle, handleType, &value->asRaw, valueLength,
            attribute, "generic get OCI attribute", &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getServerVersion() [PUBLIC]
//   Get the server version string from the database.
//-----------------------------------------------------------------------------
int dpiConn_getServerVersion(dpiConn *conn, const char **releaseString,
        uint32_t *releaseStringLength, dpiVersionInfo *versionInfo)
{
    dpiError error;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, versionInfo)

    // get server version
    if (dpiConn__getServerVersion(conn, (releaseString != NULL), &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (releaseString)
        *releaseString = conn->releaseString;
    if (releaseStringLength)
        *releaseStringLength = conn->releaseStringLength;
    memcpy(versionInfo, &conn->versionInfo, sizeof(dpiVersionInfo));
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getServiceName() [PUBLIC]
//   Returns the name of the service used to connect to the database.
//-----------------------------------------------------------------------------
int dpiConn_getServiceName(dpiConn *conn, const char **value,
        uint32_t *valueLength)
{
    return dpiConn__getAttributeText(conn, DPI_OCI_ATTR_SERVICENAME, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_getStmtCacheSize() [PUBLIC]
//   Return the current size of the statement cache.
//-----------------------------------------------------------------------------
int dpiConn_getStmtCacheSize(dpiConn *conn, uint32_t *cacheSize)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, cacheSize)
    status = dpiOci__attrGet(conn->handle, DPI_OCI_HTYPE_SVCCTX, cacheSize,
            NULL, DPI_OCI_ATTR_STMTCACHESIZE, "get stmt cache size", &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newDeqOptions() [PUBLIC]
//   Create a new dequeue options object and return it.
//-----------------------------------------------------------------------------
int dpiConn_newDeqOptions(dpiConn *conn, dpiDeqOptions **options)
{
    dpiDeqOptions *tempOptions;
    dpiError error;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, options)
    if (dpiGen__allocate(DPI_HTYPE_DEQ_OPTIONS, conn->env,
            (void**) &tempOptions, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiDeqOptions__create(tempOptions, conn, &error) < 0) {
        dpiDeqOptions__free(tempOptions, &error);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    *options = tempOptions;
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newEnqOptions() [PUBLIC]
//   Create a new enqueue options object and return it.
//-----------------------------------------------------------------------------
int dpiConn_newEnqOptions(dpiConn *conn, dpiEnqOptions **options)
{
    dpiEnqOptions *tempOptions;
    dpiError error;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, options)
    if (dpiGen__allocate(DPI_HTYPE_ENQ_OPTIONS, conn->env,
            (void**) &tempOptions, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiEnqOptions__create(tempOptions, conn, &error) < 0) {
        dpiEnqOptions__free(tempOptions, &error);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    *options = tempOptions;
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newJson() [PUBLIC]
//   Create a new JSON object and return it.
//-----------------------------------------------------------------------------
int dpiConn_newJson(dpiConn *conn, dpiJson **json)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, json);
    status = dpiJson__allocate(conn, NULL, json, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newJsonQueue() [PUBLIC]
//   Create a new AQ queue object with JSON payload and return it.
//-----------------------------------------------------------------------------
int dpiConn_newJsonQueue(dpiConn *conn, const char *name, uint32_t nameLength,
        dpiQueue **queue)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(conn, name)
    DPI_CHECK_PTR_NOT_NULL(conn, queue)
    status = dpiQueue__allocate(conn, name, nameLength, NULL, queue, 1,
            &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newTempLob() [PUBLIC]
//   Create a new temporary LOB and return it.
//-----------------------------------------------------------------------------
int dpiConn_newTempLob(dpiConn *conn, dpiOracleTypeNum lobType, dpiLob **lob)
{
    const dpiOracleType *type;
    dpiLob *tempLob;
    dpiError error;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, lob)
    switch (lobType) {
        case DPI_ORACLE_TYPE_CLOB:
        case DPI_ORACLE_TYPE_BLOB:
        case DPI_ORACLE_TYPE_NCLOB:
            type = dpiOracleType__getFromNum(lobType, &error);
            break;
        default:
            dpiError__set(&error, "check lob type",
                    DPI_ERR_INVALID_ORACLE_TYPE, lobType);
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    if (dpiLob__allocate(conn, type, &tempLob, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiOci__lobCreateTemporary(tempLob, &error) < 0) {
        dpiLob__free(tempLob, &error);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    *lob = tempLob;
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newMsgProps() [PUBLIC]
//   Create a new message properties object and return it.
//-----------------------------------------------------------------------------
int dpiConn_newMsgProps(dpiConn *conn, dpiMsgProps **props)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, props)
    status = dpiMsgProps__allocate(conn, props, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newQueue() [PUBLIC]
//   Create a new AQ queue object and return it.
//-----------------------------------------------------------------------------
int dpiConn_newQueue(dpiConn *conn, const char *name, uint32_t nameLength,
        dpiObjectType *payloadType, dpiQueue **queue)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(conn, name)
    DPI_CHECK_PTR_NOT_NULL(conn, queue)
    status = dpiQueue__allocate(conn, name, nameLength, payloadType, queue,
            0, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newVar() [PUBLIC]
//   Create a new variable and return it.
//-----------------------------------------------------------------------------
int dpiConn_newVar(dpiConn *conn, dpiOracleTypeNum oracleTypeNum,
        dpiNativeTypeNum nativeTypeNum, uint32_t maxArraySize, uint32_t size,
        int sizeIsBytes, int isArray, dpiObjectType *objType, dpiVar **var,
        dpiData **data)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, var)
    DPI_CHECK_PTR_NOT_NULL(conn, data)
    status = dpiVar__allocate(conn, oracleTypeNum, nativeTypeNum, maxArraySize,
            size, sizeIsBytes, isArray, objType, var, data, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_newVector() [PUBLIC]
//   Create a new variable and return it.
//-----------------------------------------------------------------------------
int dpiConn_newVector(dpiConn *conn, dpiVectorInfo *info, dpiVector **vector)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, vector)
    status = dpiConn__newVector(conn, info, vector, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_ping() [PUBLIC]
//   Makes a round trip call to the server to confirm that the connection and
// server are still active.
//-----------------------------------------------------------------------------
int dpiConn_ping(dpiConn *conn)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiOci__ping(conn, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_prepareStmt() [PUBLIC]
//   Create a new statement and return it after preparing the specified SQL.
//-----------------------------------------------------------------------------
int dpiConn_prepareStmt(dpiConn *conn, int scrollable, const char *sql,
        uint32_t sqlLength, const char *tag, uint32_t tagLength,
        dpiStmt **stmt)
{
    dpiStmt *tempStmt;
    dpiError error;

    *stmt = NULL;
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(conn, sql)
    DPI_CHECK_PTR_AND_LENGTH(conn, tag)
    DPI_CHECK_PTR_NOT_NULL(conn, stmt)
    if (dpiStmt__allocate(conn, scrollable, &tempStmt, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiStmt__prepare(tempStmt, sql, sqlLength, tag, tagLength,
            &error) < 0) {
        dpiStmt__free(tempStmt, &error);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    *stmt = tempStmt;
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_release() [PUBLIC]
//   Release a reference to the connection.
//-----------------------------------------------------------------------------
int dpiConn_release(dpiConn *conn)
{
    return dpiGen__release(conn, DPI_HTYPE_CONN, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_rollback() [PUBLIC]
//   Rollback the transaction associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_rollback(dpiConn *conn)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiConn__rollback(conn, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_setAction() [PUBLIC]
//   Set the action associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setAction(dpiConn *conn, const char *value, uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_ACTION, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setCallTimeout() [PUBLIC]
//   Set the call timeout (in milliseconds) used for round-trips to the
// database. This is only valid in Oracle Client 18c and higher.
//-----------------------------------------------------------------------------
int dpiConn_setCallTimeout(dpiConn *conn, uint32_t value)
{
    dpiError error;
    int status;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiUtils__checkClientVersion(conn->env->versionInfo, 18, 1,
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);

    // set call timeout
    status = dpiOci__attrSet(conn->handle, DPI_OCI_HTYPE_SVCCTX, &value,
            0, DPI_OCI_ATTR_CALL_TIMEOUT, "set call timeout", &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_setClientIdentifier() [PUBLIC]
//   Set the client identifier associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setClientIdentifier(dpiConn *conn, const char *value,
        uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_CLIENT_IDENTIFIER,
            value, valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setClientInfo() [PUBLIC]
//   Set the client info associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setClientInfo(dpiConn *conn, const char *value,
        uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_CLIENT_INFO, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setCurrentSchema() [PUBLIC]
//   Set the current schema associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setCurrentSchema(dpiConn *conn, const char *value,
        uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_CURRENT_SCHEMA, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setDbOp() [PUBLIC]
//   Set the database operation associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setDbOp(dpiConn *conn, const char *value, uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_DBOP, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setEcontextId() [PUBLIC]
//   Set the execute context id associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setEcontextId(dpiConn *conn, const char *value,
        uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_ECONTEXT_ID, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setExternalName() [PUBLIC]
//   Set the external name associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setExternalName(dpiConn *conn, const char *value,
        uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_EXTERNAL_NAME, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setInternalName() [PUBLIC]
//   Set the internal name associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setInternalName(dpiConn *conn, const char *value,
        uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_INTERNAL_NAME, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setModule() [PUBLIC]
//   Set the module associated with the connection.
//-----------------------------------------------------------------------------
int dpiConn_setModule(dpiConn *conn, const char *value, uint32_t valueLength)
{
    return dpiConn__setAttributeText(conn, DPI_OCI_ATTR_MODULE, value,
            valueLength, __func__);
}


//-----------------------------------------------------------------------------
// dpiConn_setOciAttr() [PUBLIC]
//   Set the OCI attribute directly. This is intended for testing of attributes
// not currently exposed by ODPI-C and should only be used for that purpose.
//-----------------------------------------------------------------------------
int dpiConn_setOciAttr(dpiConn *conn, uint32_t handleType,
        uint32_t attribute, void *value, uint32_t valueLength)
{
    dpiError error;
    void *handle;
    int status;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, value)
    switch (handleType) {
        case DPI_OCI_HTYPE_SVCCTX:
            handle = conn->handle;
            break;
        case DPI_OCI_HTYPE_SERVER:
            handle = conn->serverHandle;
            break;
        case DPI_OCI_HTYPE_SESSION:
            handle = conn->sessionHandle;
            break;
        default:
            dpiError__set(&error, "check handle type", DPI_ERR_NOT_SUPPORTED);
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    // set attribute value
    status = dpiOci__attrSet(handle, handleType, value, valueLength,
            attribute, "generic set OCI attribute", &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_setStmtCacheSize() [PUBLIC]
//   Set the size of the statement cache.
//-----------------------------------------------------------------------------
int dpiConn_setStmtCacheSize(dpiConn *conn, uint32_t cacheSize)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiOci__attrSet(conn->handle, DPI_OCI_HTYPE_SVCCTX, &cacheSize, 0,
            DPI_OCI_ATTR_STMTCACHESIZE, "set stmt cache size", &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_getTransactionInProgress() [PUBLIC]
//   Returns whether or not a transaction is in progress. This can be used to
// determine if a COMMIT is required or not.
//----------------------------------------------------------------------------_
int dpiConn_getTransactionInProgress(dpiConn *conn, int *value)
{
    dpiError error;
    uint32_t temp;
    int status;

    // validate parameters
    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, value);

    status = dpiOci__attrGet(conn->sessionHandle, DPI_OCI_HTYPE_SESSION,
            &temp, NULL, DPI_OCI_ATTR_TRANSACTION_IN_PROGRESS,
            "get Transaction in progress", &error);
    *value = (temp == 0) ? 0: 1;
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_shutdownDatabase() [PUBLIC]
//   Shutdown the database. Note that this must be done in two phases except in
// the situation where the instance is being aborted.
//-----------------------------------------------------------------------------
int dpiConn_shutdownDatabase(dpiConn *conn, dpiShutdownMode mode)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiOci__dbShutdown(conn, mode, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_startupDatabase() [PUBLIC]
//   Startup the database. This is equivalent to "startup nomount" in SQL*Plus.
//-----------------------------------------------------------------------------
int dpiConn_startupDatabase(dpiConn *conn, dpiStartupMode mode)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiConn__startupDatabase(conn, NULL, 0, mode, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_startupDatabaseWithPfile() [PUBLIC]
//   Startup the database with a parameter file (PFILE). This is equivalent to
// "startup nomount pfile=<pfile_location>" in SQL*Plus.
//-----------------------------------------------------------------------------
int dpiConn_startupDatabaseWithPfile(dpiConn *conn, const char *pfile,
        uint32_t pfileLength, dpiStartupMode mode)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_AND_LENGTH(conn, pfile)
    status = dpiConn__startupDatabase(conn, pfile, pfileLength, mode, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_subscribe() [PUBLIC]
//   Subscribe to events in the database. A subscription is created and
// returned. This replaces dpiConn_newSubscription().
//-----------------------------------------------------------------------------
int dpiConn_subscribe(dpiConn *conn, dpiSubscrCreateParams *params,
        dpiSubscr **subscr)
{
    dpiSubscr *tempSubscr;
    dpiError error;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, params)
    DPI_CHECK_PTR_NOT_NULL(conn, subscr)
    if (!conn->env->events) {
        dpiError__set(&error, "subscribe", DPI_ERR_EVENTS_MODE_REQUIRED);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    if (dpiGen__allocate(DPI_HTYPE_SUBSCR, conn->env, (void**) &tempSubscr,
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiSubscr__create(tempSubscr, conn, params, &error) < 0) {
        dpiSubscr__free(tempSubscr, &error);
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }

    *subscr = tempSubscr;
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_tpcBegin() [PUBLIC]
//   Begin a TPC (two-phase commit) transaction.
//-----------------------------------------------------------------------------
int dpiConn_tpcBegin(dpiConn *conn, dpiXid *xid, uint32_t transactionTimeout,
        uint32_t flags)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, xid)
    if (dpiConn__setXid(conn, xid, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiOci__transStart(conn, transactionTimeout, flags, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_tpcCommit() [PUBLIC]
//   Commit a TPC (two-phase commit) transaction. This method is equivalent to
// calling dpiConn_commit() if no XID is specified.
//-----------------------------------------------------------------------------
int dpiConn_tpcCommit(dpiConn *conn, dpiXid *xid, int onePhase)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (xid) {
        if (dpiConn__setXid(conn, xid, &error) < 0)
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
        conn->commitMode = (onePhase) ? DPI_OCI_DEFAULT :
                DPI_OCI_TRANS_TWOPHASE;
    }
    status = dpiConn__commit(conn, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_tpcEnd() [PUBLIC]
//   End (detach from) a TPC (two-phase commit) transaction.
//-----------------------------------------------------------------------------
int dpiConn_tpcEnd(dpiConn *conn, dpiXid *xid, uint32_t flags)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (xid) {
        if (dpiConn__setXid(conn, xid, &error) < 0)
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    status = dpiOci__transDetach(conn, flags, &error);
    if (status == DPI_SUCCESS)
        status = dpiConn__clearTransaction(conn, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_tpcForget() [PUBLIC]
//   Forget a TPC (two-phase commit) transaction.
//-----------------------------------------------------------------------------
int dpiConn_tpcForget(dpiConn *conn, dpiXid *xid)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, xid)
    if (dpiConn__setXid(conn, xid, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    status = dpiOci__transForget(conn, &error);
    if (status == DPI_SUCCESS)
        status = dpiConn__clearTransaction(conn, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_tpcPrepare() [PUBLIC]
//   Prepare a TPC (two-phase commit) transaction for commit. A boolean is
// returned indicating if a commit is actually needed as an attempt to perform
// a commit when nothing is actually prepared results in ORA-24756 (transaction
// does not exist). This is determined by the return value from
// OCITransPrepare() which is OCI_SUCCESS_WITH_INFO if there is no transaction
// requiring commit.
//-----------------------------------------------------------------------------
int dpiConn_tpcPrepare(dpiConn *conn, dpiXid *xid, int *commitNeeded)
{
    dpiError error;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    DPI_CHECK_PTR_NOT_NULL(conn, commitNeeded)
    if (xid) {
        if (dpiConn__setXid(conn, xid, &error) < 0)
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    if (dpiOci__transPrepare(conn, commitNeeded, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (*commitNeeded)
        conn->commitMode = DPI_OCI_TRANS_TWOPHASE;
    return dpiGen__endPublicFn(conn, DPI_SUCCESS, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_tpcRollback() [PUBLIC]
//   Rollback a TPC (two-phase commit) transaction. This method is equivalent
// to calling dpiConn_rollback() if no XID is specified.
//-----------------------------------------------------------------------------
int dpiConn_tpcRollback(dpiConn *conn, dpiXid *xid)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (xid) {
        if (dpiConn__setXid(conn, xid, &error) < 0)
            return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    }
    status = dpiConn__rollback(conn, &error);
    return dpiGen__endPublicFn(conn, status, &error);
}


//-----------------------------------------------------------------------------
// dpiConn_unsubscribe() [PUBLIC]
//   Unsubscribe from events in the database. Once this call completes
// successfully no further notifications will be sent.
//-----------------------------------------------------------------------------
int dpiConn_unsubscribe(dpiConn *conn, dpiSubscr *subscr)
{
    dpiError error;
    int status;

    if (dpiConn__check(conn, __func__, &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (dpiGen__checkHandle(subscr, DPI_HTYPE_SUBSCR, "check subscription",
            &error) < 0)
        return dpiGen__endPublicFn(conn, DPI_FAILURE, &error);
    if (subscr->registered) {
        dpiMutex__acquire(subscr->mutex);
        status = dpiOci__subscriptionUnRegister(conn, subscr, &error);
        if (status == DPI_SUCCESS)
            subscr->registered = 0;
        dpiMutex__release(subscr->mutex);
        if (status < 0)
            return dpiGen__endPublicFn(subscr, DPI_FAILURE, &error);
    }

    dpiGen__setRefCount(subscr, &error, -1);
    return dpiGen__endPublicFn(subscr, DPI_SUCCESS, &error);
}
