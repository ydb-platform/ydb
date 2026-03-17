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
// dpiGlobal.c
//   Global environment used for managing errors in a thread safe manner as
// well as for looking up encodings.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

// cross platform way of defining an initializer that runs at application
// startup (similar to what is done for the constructor calls for static C++
// objects)
#if defined(_MSC_VER)
    #pragma section(".CRT$XCU", read)
    #define DPI_INITIALIZER_HELPER(f, p) \
        static void f(void); \
        __declspec(allocate(".CRT$XCU")) void (*f##_)(void) = f; \
        __pragma(comment(linker,"/include:" p #f "_")) \
        static void f(void)
    #ifdef _WIN64
        #define DPI_INITIALIZER(f) DPI_INITIALIZER_HELPER(f, "")
    #else
        #define DPI_INITIALIZER(f) DPI_INITIALIZER_HELPER(f, "_")
    #endif
#else
    #define DPI_INITIALIZER(f) \
        static void f(void) __attribute__((constructor)); \
        static void f(void)
#endif

// a global OCI environment is used for managing error buffers in a thread-safe
// manner; each thread is given its own error buffer; OCI error handles,
// though, must be created within the OCI environment created for use by
// standalone connections and session pools
static void *dpiGlobalEnvHandle = NULL;
static void *dpiGlobalErrorHandle = NULL;
static void *dpiGlobalThreadKey = NULL;
static dpiErrorBuffer dpiGlobalErrorBuffer;
static dpiVersionInfo dpiGlobalClientVersionInfo;
static int dpiGlobalInitialized = 0;

// a global mutex is used to ensure that only one thread is used to perform
// initialization of ODPI-C
static dpiMutexType dpiGlobalMutex;

// forward declarations of internal functions only used in this file
static int dpiGlobal__extendedInitialize(dpiContextCreateParams *params,
        const char *fnName, dpiError *error);
static void dpiGlobal__finalize(void);
static int dpiGlobal__getErrorBuffer(const char *fnName, dpiError *error);


//-----------------------------------------------------------------------------
// dpiGlobal__ensureInitialized() [INTERNAL]
//   Ensure that all initializations of the global infrastructure used by
// ODPI-C have been performed.  This is done by the first thread to execute
// this function and includes loading the Oracle Call Interface (OCI) library
// and creating a thread key used for managing error buffers in a thread-safe
// manner.
//-----------------------------------------------------------------------------
int dpiGlobal__ensureInitialized(const char *fnName,
        dpiContextCreateParams *params, dpiVersionInfo **clientVersionInfo,
        dpiError *error)
{
    // initialize error buffer output to global error buffer structure; this is
    // the value that is used if an error takes place before the thread local
    // error structure can be returned
    error->handle = NULL;
    error->buffer = &dpiGlobalErrorBuffer;
    error->buffer->fnName = fnName;

    // perform global initializations, if needed
    if (!dpiGlobalInitialized) {
        dpiMutex__acquire(dpiGlobalMutex);
        if (!dpiGlobalInitialized)
            dpiGlobal__extendedInitialize(params, fnName, error);
        dpiMutex__release(dpiGlobalMutex);
        if (!dpiGlobalInitialized)
            return DPI_FAILURE;
    }

    *clientVersionInfo = &dpiGlobalClientVersionInfo;
    return dpiGlobal__getErrorBuffer(fnName, error);
}


//-----------------------------------------------------------------------------
// dpiGlobal__extendedInitialize() [INTERNAL]
//   Create the global environment used for managing error buffers in a
// thread-safe manner. This environment is solely used for implementing thread
// local storage for the error buffers and for looking up encodings given an
// IANA or Oracle character set name.
//-----------------------------------------------------------------------------
static int dpiGlobal__extendedInitialize(dpiContextCreateParams *params,
        const char *fnName, dpiError *error)
{
    int status;

    // initialize debugging
    dpiDebug__initialize();
    if (dpiDebugLevel & DPI_DEBUG_LEVEL_FNS)
        dpiDebug__print("fn start %s\n", fnName);

    // load OCI library
    if (dpiOci__loadLib(params, &dpiGlobalClientVersionInfo, error) < 0)
        return DPI_FAILURE;

    // create threaded OCI environment for storing error buffers and for
    // looking up character sets; use character set AL32UTF8 solely to avoid
    // the overhead of processing the environment variables; no error messages
    // from this environment are ever used (ODPI-C specific error messages are
    // used)
    if (dpiOci__envNlsCreate(&dpiGlobalEnvHandle, DPI_OCI_THREADED,
            DPI_CHARSET_ID_UTF8, DPI_CHARSET_ID_UTF8, error) < 0)
        return DPI_FAILURE;

    // create global error handle
    if (dpiOci__handleAlloc(dpiGlobalEnvHandle, &dpiGlobalErrorHandle,
            DPI_OCI_HTYPE_ERROR, "create global error", error) < 0) {
        dpiOci__handleFree(dpiGlobalEnvHandle, DPI_OCI_HTYPE_ENV);
        return DPI_FAILURE;
    }

    // create global thread key
    status = dpiOci__threadKeyInit(dpiGlobalEnvHandle, dpiGlobalErrorHandle,
            &dpiGlobalThreadKey, (void*) dpiUtils__freeMemory, error);
    if (status < 0) {
        dpiOci__handleFree(dpiGlobalEnvHandle, DPI_OCI_HTYPE_ENV);
        return DPI_FAILURE;
    }

    // mark library as fully initialized
    dpiGlobalInitialized = 1;

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiGlobal__finalize() [INTERNAL]
//   Called when the process terminates and ensures that everything is cleaned
// up.
//-----------------------------------------------------------------------------
static void dpiGlobal__finalize(void)
{
    void *errorBuffer = NULL;
    dpiError error;

    dpiMutex__acquire(dpiGlobalMutex);
    dpiGlobalInitialized = 0;
    error.buffer = &dpiGlobalErrorBuffer;
    if (dpiGlobalThreadKey) {
        dpiOci__threadKeyGet(dpiGlobalEnvHandle, dpiGlobalErrorHandle,
                dpiGlobalThreadKey, &errorBuffer, &error);
        if (errorBuffer) {
            dpiOci__threadKeySet(dpiGlobalEnvHandle, dpiGlobalErrorHandle,
                    dpiGlobalThreadKey, NULL, &error);
            dpiUtils__freeMemory(errorBuffer);
        }
        dpiOci__threadKeyDestroy(dpiGlobalEnvHandle, dpiGlobalErrorHandle,
                &dpiGlobalThreadKey, &error);
        dpiGlobalThreadKey = NULL;
    }
    if (dpiGlobalEnvHandle) {
        dpiOci__handleFree(dpiGlobalEnvHandle, DPI_OCI_HTYPE_ENV);
        dpiGlobalEnvHandle = NULL;
    }
    dpiMutex__release(dpiGlobalMutex);
}


//-----------------------------------------------------------------------------
// dpiGlobal__getErrorBuffer() [INTERNAL]
//   Get the thread local error buffer. This will replace use of the global
// error buffer which is used until this function has completed successfully.
// At this point it is assumed that the global infrastructure has been
// initialialized successfully.
//-----------------------------------------------------------------------------
static int dpiGlobal__getErrorBuffer(const char *fnName, dpiError *error)
{
    dpiErrorBuffer *tempErrorBuffer;

    // look up the error buffer specific to this thread
    if (dpiOci__threadKeyGet(dpiGlobalEnvHandle, dpiGlobalErrorHandle,
            dpiGlobalThreadKey, (void**) &tempErrorBuffer, error) < 0)
        return DPI_FAILURE;

    // if NULL, key has never been set for this thread, allocate new error
    // buffer and set it
    if (!tempErrorBuffer) {
        if (dpiUtils__allocateMemory(1, sizeof(dpiErrorBuffer), 1,
                "allocate error buffer", (void**) &tempErrorBuffer, error) < 0)
            return DPI_FAILURE;
        if (dpiOci__threadKeySet(dpiGlobalEnvHandle, dpiGlobalErrorHandle,
                dpiGlobalThreadKey, tempErrorBuffer, error) < 0) {
            dpiUtils__freeMemory(tempErrorBuffer);
            return DPI_FAILURE;
        }
    }

    // if a function name has been specified, clear error
    // the only time a function name is not specified is for
    // dpiContext_getError() when the error information is being retrieved
    if (fnName) {
        tempErrorBuffer->code = 0;
        tempErrorBuffer->offset = 0;
        tempErrorBuffer->errorNum = (dpiErrorNum) 0;
        tempErrorBuffer->isRecoverable = 0;
        tempErrorBuffer->messageLength = 0;
        tempErrorBuffer->fnName = fnName;
        tempErrorBuffer->action = "start";
        tempErrorBuffer->isWarning = 0;
        strcpy(tempErrorBuffer->encoding, DPI_CHARSET_NAME_UTF8);
    }

    error->buffer = tempErrorBuffer;
    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiGlobal__initError() [INTERNAL]
//   Get the thread local error structure for use in all other functions. If
// an error structure cannot be determined for some reason, the global error
// buffer structure is returned instead.
//-----------------------------------------------------------------------------
int dpiGlobal__initError(const char *fnName, dpiError *error)
{
    // initialize error buffer output to global error buffer structure; this is
    // the value that is used if an error takes place before the thread local
    // error structure can be returned
    error->handle = NULL;
    error->buffer = &dpiGlobalErrorBuffer;
    if (fnName)
        error->buffer->fnName = fnName;

    // check to see if global environment has been initialized; if not, no call
    // to dpiContext_createWithParams() was made successfully
    if (!dpiGlobalInitialized)
        return dpiError__set(error, "check context creation",
                DPI_ERR_CONTEXT_NOT_CREATED);

    // acquire error buffer for the thread, if possible
    return dpiGlobal__getErrorBuffer(fnName, error);
}


//-----------------------------------------------------------------------------
// dpiGlobal__initialize() [INTERNAL]
//   Initialization function that runs at process startup or when the library
// is first loaded. Some operating systems have limits on what can be run in
// this function, so most work is done in the dpiGlobal__extendedInitialize()
// function that runs when the first call to dpiContext_createWithParams() is
// made.
//-----------------------------------------------------------------------------
DPI_INITIALIZER(dpiGlobal__initialize)
{
    memset(&dpiGlobalErrorBuffer, 0, sizeof(dpiGlobalErrorBuffer));
    strcpy(dpiGlobalErrorBuffer.encoding, DPI_CHARSET_NAME_UTF8);
    dpiMutex__initialize(dpiGlobalMutex);
    atexit(dpiGlobal__finalize);
}


//-----------------------------------------------------------------------------
// dpiGlobal__lookupCharSet() [INTERNAL]
//   Lookup the character set id that can be used in the call to
// OCINlsEnvCreate().
//-----------------------------------------------------------------------------
int dpiGlobal__lookupCharSet(const char *name, uint16_t *charsetId,
        dpiError *error)
{
    char oraCharsetName[DPI_OCI_NLS_MAXBUFSZ];

    // check for well-known encodings first
    if (strcmp(name, DPI_CHARSET_NAME_UTF8) == 0)
        *charsetId = DPI_CHARSET_ID_UTF8;
    else if (strcmp(name, DPI_CHARSET_NAME_UTF16) == 0)
        *charsetId = DPI_CHARSET_ID_UTF16;
    else if (strcmp(name, DPI_CHARSET_NAME_ASCII) == 0)
        *charsetId = DPI_CHARSET_ID_ASCII;
    else if (strcmp(name, DPI_CHARSET_NAME_UTF16LE) == 0 ||
            strcmp(name, DPI_CHARSET_NAME_UTF16BE) == 0)
        return dpiError__set(error, "check encoding", DPI_ERR_NOT_SUPPORTED);

    // perform lookup; check for the Oracle character set name first and if
    // that fails, lookup using the IANA character set name
    else {
        if (dpiOci__nlsCharSetNameToId(dpiGlobalEnvHandle, name, charsetId,
                error) < 0)
            return DPI_FAILURE;
        if (!*charsetId) {
            if (dpiOci__nlsNameMap(dpiGlobalEnvHandle, oraCharsetName,
                    sizeof(oraCharsetName), name, DPI_OCI_NLS_CS_IANA_TO_ORA,
                    error) < 0)
                return dpiError__set(error, "lookup charset",
                        DPI_ERR_INVALID_CHARSET, name);
            dpiOci__nlsCharSetNameToId(dpiGlobalEnvHandle, oraCharsetName,
                    charsetId, error);
        }
    }

    return DPI_SUCCESS;
}


//-----------------------------------------------------------------------------
// dpiGlobal__lookupEncoding() [INTERNAL]
//   Get the IANA character set name (encoding) given the Oracle character set
// id.
//-----------------------------------------------------------------------------
int dpiGlobal__lookupEncoding(uint16_t charsetId, char *encoding,
        dpiError *error)
{
    char oracleName[DPI_OCI_NLS_MAXBUFSZ];

    // check for well-known encodings first
    switch (charsetId) {
        case DPI_CHARSET_ID_UTF8:
            strcpy(encoding, DPI_CHARSET_NAME_UTF8);
            return DPI_SUCCESS;
        case DPI_CHARSET_ID_UTF16:
            strcpy(encoding, DPI_CHARSET_NAME_UTF16);
            return DPI_SUCCESS;
        case DPI_CHARSET_ID_ASCII:
            strcpy(encoding, DPI_CHARSET_NAME_ASCII);
            return DPI_SUCCESS;
    }

    // get character set name
    if (dpiOci__nlsCharSetIdToName(dpiGlobalEnvHandle, oracleName,
            sizeof(oracleName), charsetId, error) < 0)
        return dpiError__set(error, "lookup Oracle character set name",
                DPI_ERR_INVALID_CHARSET_ID, charsetId);

    // get IANA character set name
    if (dpiOci__nlsNameMap(dpiGlobalEnvHandle, encoding, DPI_OCI_NLS_MAXBUFSZ,
            oracleName, DPI_OCI_NLS_CS_ORA_TO_IANA, error) < 0)
        return dpiError__set(error, "lookup IANA name",
                DPI_ERR_INVALID_CHARSET_ID, charsetId);

    return DPI_SUCCESS;
}
