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
// dpiError.c
//   Implementation of error.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"
#include "dpiErrorMessages.h"

//-----------------------------------------------------------------------------
// dpiError__getInfo() [INTERNAL]
//   Get the error state from the error structure. Returns DPI_FAILURE as a
// convenience to the caller.
//-----------------------------------------------------------------------------
int dpiError__getInfo(dpiError *error, dpiErrorInfo *info)
{
    if (!info)
        return DPI_FAILURE;
    info->code = error->buffer->code;
    info->offset = error->buffer->offset;
    info->offset16 = (uint16_t) error->buffer->offset;
    info->message = error->buffer->message;
    info->messageLength = error->buffer->messageLength;
    info->fnName = error->buffer->fnName;
    info->action = error->buffer->action;
    info->isRecoverable = error->buffer->isRecoverable;
    info->encoding = error->buffer->encoding;
    info->isWarning = error->buffer->isWarning;
    if (info->code == 12154) {
        info->sqlState = "42S02";
    } else if (error->buffer->errorNum == DPI_ERR_CONN_CLOSED) {
        info->sqlState = "01002";
    } else if (error->buffer->code == 0 &&
            error->buffer->errorNum == (dpiErrorNum) 0) {
        info->sqlState = "00000";
    } else {
        info->sqlState = "HY000";
    }
    return DPI_FAILURE;
}


//-----------------------------------------------------------------------------
// dpiError__initHandle() [INTERNAL]
//   Retrieve the OCI error handle to use for error handling, from a pool of
// error handles common to the environment handle stored on the error. This
// environment also controls the encoding of OCI errors (which uses the CHAR
// encoding of the environment).
//-----------------------------------------------------------------------------
int dpiError__initHandle(dpiError *error)
{
    if (dpiHandlePool__acquire(error->env->errorHandles, &error->handle,
            error) < 0)
        return DPI_FAILURE;
    if (!error->handle) {
        if (dpiOci__handleAlloc(error->env->handle, &error->handle,
                DPI_OCI_HTYPE_ERROR, "allocate OCI error", error) < 0)
            return DPI_FAILURE;
    }
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiError__set() [INTERNAL]
//   Set the error buffer to the specified DPI error. Returns DPI_FAILURE as a
// convenience to the caller.
//-----------------------------------------------------------------------------
int dpiError__set(dpiError *error, const char *action, dpiErrorNum errorNum,
        ...)
{
    va_list varArgs;

    if (error) {
        error->buffer->code = 0;
        error->buffer->isRecoverable = 0;
        error->buffer->isWarning = 0;
        error->buffer->offset = 0;
        strcpy(error->buffer->encoding, DPI_CHARSET_NAME_UTF8);
        error->buffer->action = action;
        error->buffer->errorNum = errorNum;
        va_start(varArgs, errorNum);
        error->buffer->messageLength =
                (uint32_t) vsnprintf(error->buffer->message,
                sizeof(error->buffer->message),
                dpiErrorMessages[errorNum - DPI_ERR_NO_ERR], varArgs);
        va_end(varArgs);
        if (dpiDebugLevel & DPI_DEBUG_LEVEL_ERRORS)
            dpiDebug__print("internal error %.*s (%s / %s)\n",
                    error->buffer->messageLength, error->buffer->message,
                    error->buffer->fnName, action);
    }
    return DPI_FAILURE;
}


//-----------------------------------------------------------------------------
// dpiError__setFromOCI() [INTERNAL]
//   Called when an OCI error has occurred and sets the error structure with
// the contents of that error. Note that trailing newlines and spaces are
// truncated from the message if they exist. If the connection is not NULL a
// check is made to see if the connection is no longer viable. The value
// DPI_FAILURE is returned as a convenience to the caller, except when the
// status of the call is DPI_OCI_SUCCESS_WITH_INFO, which is treated as a
// successful call.
//-----------------------------------------------------------------------------
int dpiError__setFromOCI(dpiError *error, int status, dpiConn *conn,
        const char *action)
{
    uint32_t callTimeout, serverStatus;

    // special error cases
    if (status == DPI_OCI_INVALID_HANDLE)
        return dpiError__set(error, action, DPI_ERR_INVALID_HANDLE, "OCI");
    else if (!error)
        return DPI_FAILURE;
    else if (!error->handle)
        return dpiError__set(error, action, DPI_ERR_ERR_NOT_INITIALIZED);
    else if (status != DPI_OCI_ERROR && status != DPI_OCI_NO_DATA &&
            status != DPI_OCI_SUCCESS_WITH_INFO)
        return dpiError__set(error, action,
                DPI_ERR_UNEXPECTED_OCI_RETURN_VALUE, status,
                error->buffer->fnName);

    // fetch OCI error
    error->buffer->action = action;
    strcpy(error->buffer->encoding, error->env->encoding);
    if (dpiOci__errorGet(error->handle, DPI_OCI_HTYPE_ERROR,
            error->env->charsetId, action, error) < 0)
        return DPI_FAILURE;
    if (dpiDebugLevel & DPI_DEBUG_LEVEL_ERRORS)
        dpiDebug__print("OCI error %.*s (%s / %s)\n",
                error->buffer->messageLength, error->buffer->message,
                error->buffer->fnName, action);
    if (status == DPI_OCI_SUCCESS_WITH_INFO) {
        error->buffer->isWarning = 1;
        return DPI_SUCCESS;
    }

    // determine if error is recoverable (Transaction Guard)
    // if the attribute cannot be read properly, simply leave it as false;
    // otherwise, that error will mask the one that we really want to see
    error->buffer->isRecoverable = 0;
    dpiOci__attrGet(error->handle, DPI_OCI_HTYPE_ERROR,
            (void*) &error->buffer->isRecoverable, 0,
            DPI_OCI_ATTR_ERROR_IS_RECOVERABLE, NULL, error);

    // check the health of the connection (if one was specified in this call
    // and we are not in the middle of creating that connection)
    if (conn && !conn->creating && !conn->deadSession) {

        // first check the attribute specifically designed to check the health
        // of the connection, if possible
        if (conn->serverHandle) {
            if (dpiOci__attrGet(conn->serverHandle, DPI_OCI_HTYPE_SERVER,
                    &serverStatus, NULL, DPI_OCI_ATTR_SERVER_STATUS,
                    "get server status", error) < 0 ||
                    serverStatus != DPI_OCI_SERVER_NORMAL) {
                conn->deadSession = 1;
            }
        }

        // check for certain errors which indicate that the session is dead
        if (!conn->deadSession) {
            switch (error->buffer->code) {
                case    22: // invalid session ID; access denied
                case    28: // your session has been killed
                case    31: // your session has been marked for kill
                case    45: // your session has been terminated with no replay
                case   378: // buffer pools cannot be created as specified
                case   602: // internal programming exception
                case   603: // ORACLE server session terminated by fatal error
                case   609: // could not attach to incoming connection
                case  1012: // not logged on
                case  1041: // internal error. hostdef extension doesn't exist
                case  1043: // user side memory corruption
                case  1089: // immediate shutdown or close in progress
                case  1092: // ORACLE instance terminated. Disconnection forced
                case  2396: // exceeded maximum idle time, please connect again
                case  3113: // end-of-file on communication channel
                case  3114: // not connected to ORACLE
                case  3122: // attempt to close ORACLE-side window on user side
                case  3135: // connection lost contact
                case 12153: // TNS:not connected
                case 12537: // TNS:connection closed
                case 12547: // TNS:lost contact
                case 12570: // TNS:packet reader failure
                case 12583: // TNS:no reader
                case 27146: // post/wait initialization failed
                case 28511: // lost RPC connection
                case 56600: // an illegal OCI function call was issued
                    conn->deadSession = 1;
                    break;
            }
        }

        // if session is marked as dead, return a unified error message
        if (conn->deadSession)
            return dpiError__wrap(error, DPI_ERR_CONN_CLOSED,
                    error->buffer->code);

        // check for call timeout and return a unified message instead
        switch (error->buffer->code) {
            case  3136: // inbound connection timed out
            case  3156: // OCI call timed out
            case 12161: // TNS:internal error: partial data received
                callTimeout = 0;
                if (conn->env->versionInfo->versionNum >= 18)
                    dpiOci__attrGet(conn->handle, DPI_OCI_HTYPE_SVCCTX,
                            (void*) &callTimeout, 0, DPI_OCI_ATTR_CALL_TIMEOUT,
                            NULL, error);
                if (callTimeout > 0)
                    return dpiError__wrap(error, DPI_ERR_CALL_TIMEOUT,
                            callTimeout, error->buffer->code);
                break;
        }

    }

    return DPI_FAILURE;
}


//-----------------------------------------------------------------------------
// dpiError__setFromOS() [INTERNAL]
//   Set the error buffer to a general OS error. Returns DPI_FAILURE as a
// convenience to the caller.
//-----------------------------------------------------------------------------
int dpiError__setFromOS(dpiError *error, const char *action)
{
    char *message;

#ifdef _WIN32

    size_t messageLength = 0;

    message = NULL;
    if (dpiUtils__getWindowsError(GetLastError(), &message, &messageLength,
            error) < 0)
        return DPI_FAILURE;
    dpiError__set(error, action, DPI_ERR_OS, message);
    dpiUtils__freeMemory(message);

#else

    char buffer[512];
    int err = errno;
#if defined(__GLIBC__) || defined(__CYGWIN__)
    message = strerror_r(err, buffer, sizeof(buffer));
#else
    message = (strerror_r(err, buffer, sizeof(buffer)) == 0) ? buffer : NULL;
#endif
    if (!message) {
        (void) sprintf(buffer, "unable to get OS error %d", err);
        message = buffer;
    }
    dpiError__set(error, action, DPI_ERR_OS, message);

#endif
    return DPI_FAILURE;
}


//-----------------------------------------------------------------------------
// dpiError__wrap() [INTERNAL]
//   Set the error buffer to the specified DPI error but retain the error that
// was already set and concatenate it to the new error. It is assumed that the
// error that is wrapping accepts one argument: the original error number that
// was raised. Returns DPI_FAILURE as a convenience to the caller.
// -----------------------------------------------------------------------------
int dpiError__wrap(dpiError *error, dpiErrorNum errorNum, ...)
{
    uint32_t origMessageLength;
    char *origMessage, *ptr;
    size_t ptrLength;
    va_list varArgs;

    // retain copy of original message, if possible
    origMessageLength = error->buffer->messageLength;
    origMessage = malloc(origMessageLength);
    if (origMessage)
        memcpy(origMessage, error->buffer->message, origMessageLength);

    // clear original error and set new error
    error->buffer->code = 0;
    error->buffer->isRecoverable = 0;
    error->buffer->isWarning = 0;
    error->buffer->offset = 0;
    error->buffer->errorNum = errorNum;
    va_start(varArgs, errorNum);
    error->buffer->messageLength =
            (uint32_t) vsnprintf(error->buffer->message,
            sizeof(error->buffer->message),
            dpiErrorMessages[errorNum - DPI_ERR_NO_ERR], varArgs);
    va_end(varArgs);

    // concatenate original message to new one (separated by line feed)
    if (origMessage) {
        ptr = error->buffer->message + error->buffer->messageLength;
        ptrLength = sizeof(error->buffer->message) -
                error->buffer->messageLength;
        error->buffer->messageLength += (uint32_t) snprintf(ptr, ptrLength,
                "\n%*s", origMessageLength, origMessage);
        free(origMessage);
    }

    // log message, if applicable
    if (dpiDebugLevel & DPI_DEBUG_LEVEL_ERRORS)
        dpiDebug__print("internal error %.*s (%s / %s)\n",
                error->buffer->messageLength, error->buffer->message,
                error->buffer->fnName, error->buffer->action);

    return DPI_FAILURE;
}
