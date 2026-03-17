// Copyright (c) 2017 Ryan Leckey
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include "common.h"
#include "exception.h"
#include "utils.h"

#include <xmlsec/xmlsec.h>
#include <xmlsec/errors.h>

#include <pythread.h>

#include <stdio.h>

// default error class
PyObject* PyXmlSec_Error;
PyObject* PyXmlSec_InternalError;
PyObject* PyXmlSec_VerificationError;

#if PY_MINOR_VERSION >= 7
static Py_tss_t PyXmlSec_LastErrorKey;
#else
static int PyXmlSec_LastErrorKey = 0;
#endif

static int PyXmlSec_PrintErrorMessage = 0;

typedef struct {
    const xmlChar* file;
    const xmlChar* func;
    const xmlChar* object;
    const xmlChar* subject;
    const xmlChar* msg;
    int line;
    int reason;
} PyXmlSec_ErrorHolder;

PyXmlSec_ErrorHolder* PyXmlSec_ErrorHolderCreate(const char* file, int line, const char* func, const char* object, const char* subject, int reason, const char* msg) {
    PyXmlSec_ErrorHolder* h = (PyXmlSec_ErrorHolder*)xmlMalloc(sizeof(PyXmlSec_ErrorHolder));

    // file and func is __FILE__ and __FUNCTION__ macro, so it can be stored as is.
    h->file = XSTR(file);
    h->line = line;
    h->func = XSTR(func);
    h->reason = reason;
    // there is no guarantee that object and subject will not be deallocate after exit from function,
    // so make a copy
    // xmlCharStrdup returns NULL if arg is NULL
    h->object = xmlCharStrdup(object);
    h->subject = xmlCharStrdup(subject);
    h->msg = xmlCharStrdup(msg);

    PYXMLSEC_DEBUGF("new error %p", h);
    return h;
}

void PyXmlSec_ErrorHolderFree(PyXmlSec_ErrorHolder* h) {
    if (h != NULL) {
        PYXMLSEC_DEBUGF("free error %p", h);
        xmlFree((void*)(h->object));
        xmlFree((void*)(h->subject));
        xmlFree((void*)(h->msg));
        xmlFree((void*)(h));
    }
}

// saves new error in TLS and returns previous
static PyXmlSec_ErrorHolder* PyXmlSec_ExchangeLastError(PyXmlSec_ErrorHolder* e) {
    PyXmlSec_ErrorHolder* v;
    int r;

    #if PY_MINOR_VERSION >= 7
    if (PyThread_tss_is_created(&PyXmlSec_LastErrorKey) == 0) {
    #else
    if (PyXmlSec_LastErrorKey == 0) {
    #endif
        PYXMLSEC_DEBUG("WARNING: There is no error key.");
        PyXmlSec_ErrorHolderFree(e);
        return NULL;
    }

    // get_key_value and set_key_value are gil free
    #if PY_MINOR_VERSION >= 7
    v = (PyXmlSec_ErrorHolder*)PyThread_tss_get(&PyXmlSec_LastErrorKey);
    //PyThread_tss_delete(&PyXmlSec_LastErrorKey);
    r = PyThread_tss_set(&PyXmlSec_LastErrorKey, (void*)e);
    #else
    v = (PyXmlSec_ErrorHolder*)PyThread_get_key_value(PyXmlSec_LastErrorKey);
    PyThread_delete_key_value(PyXmlSec_LastErrorKey);
    r = PyThread_set_key_value(PyXmlSec_LastErrorKey, (void*)e);
    #endif
    PYXMLSEC_DEBUGF("set_key_value returns %d", r);
    return v;
}

// xmlsec library error callback
static void PyXmlSec_ErrorCallback(const char* file, int line, const char* func, const char* object, const char* subject, int reason, const char* msg) {
    // TODO do not allocate error object each time.
    PyXmlSec_ErrorHolderFree(PyXmlSec_ExchangeLastError(PyXmlSec_ErrorHolderCreate(file, line, func, object, subject, reason, msg)));

    if (PyXmlSec_PrintErrorMessage) {
        const char* error_msg = NULL;
        xmlSecSize i;
        for (i = 0; (i < XMLSEC_ERRORS_MAX_NUMBER) && (xmlSecErrorsGetMsg(i) != NULL); ++i) {
            if(xmlSecErrorsGetCode(i) == reason) {
                error_msg = xmlSecErrorsGetMsg(i);
                break;
            }
        }

        fprintf(stderr,
            "func=%s:file=%s:line=%d:obj=%s:subj=%s:error=%d:%s:%s\n",
            (func != NULL) ? func : "unknown",
            (file != NULL) ? file : "unknown",
            line,
            (object != NULL) ? object : "unknown",
            (subject != NULL) ? subject : "unknown",
            reason,
            (error_msg != NULL) ? error_msg : "",
            (msg != NULL) ? msg : "");
    }
}

// pops the last error which was occurred in current thread
// the gil should be acquired
static PyObject* PyXmlSec_GetLastError(PyObject* type, const char* msg) {
    PyXmlSec_ErrorHolder* h = PyXmlSec_ExchangeLastError(NULL);
    PyObject* exc;

    if (h == NULL) {
        return NULL;
    }

    exc = PyObject_CallFunction(type, "is", h->reason, msg);
    if (exc == NULL) goto ON_FAIL;

    PyXmlSec_SetLongAttr(exc, "code", h->reason);
    PyXmlSec_SetStringAttr(exc, "message", msg);
    PyXmlSec_SetStringAttr(exc, "details", (const char*)xmlSecErrorsSafeString(h->msg));
    PyXmlSec_SetStringAttr(exc, "file", (const char*)xmlSecErrorsSafeString(h->file));
    PyXmlSec_SetLongAttr(exc, "line", h->line);
    PyXmlSec_SetStringAttr(exc, "func", (const char*)xmlSecErrorsSafeString(h->func));
    PyXmlSec_SetStringAttr(exc, "object", (const char*)xmlSecErrorsSafeString(h->object));
    PyXmlSec_SetStringAttr(exc, "subject", (const char*)xmlSecErrorsSafeString(h->subject));

ON_FAIL:
    PyXmlSec_ErrorHolderFree(h);
    return exc;
}

void PyXmlSec_SetLastError2(PyObject* type, const char* msg) {
    PyObject* last = PyXmlSec_GetLastError(type, msg);
    if (last == NULL) {
        PYXMLSEC_DEBUG("WARNING: no xmlsec error");
        last = PyObject_CallFunction(PyXmlSec_InternalError, "is", (int)-1, msg);
        if (last == NULL) {
            return;
        }
    }
    PyErr_SetObject(type, last);
    Py_DECREF(last);
}

void PyXmlSec_SetLastError(const char* msg) {
    PyXmlSec_SetLastError2(PyXmlSec_Error, msg);
}

void PyXmlSec_ClearError(void) {
    PyXmlSec_ErrorHolderFree(PyXmlSec_ExchangeLastError(NULL));
}

void PyXmlSecEnableDebugTrace(int v) {
    PyXmlSec_PrintErrorMessage = v;
}

void PyXmlSec_InstallErrorCallback() {
    #if PY_MINOR_VERSION >= 7
    if (PyThread_tss_is_created(&PyXmlSec_LastErrorKey) != 0) {
    #else
    if (PyXmlSec_LastErrorKey != 0) {
    #endif
        xmlSecErrorsSetCallback(PyXmlSec_ErrorCallback);
    }
}

// initializes errors module
int PyXmlSec_ExceptionsModule_Init(PyObject* package) {
    PyXmlSec_Error = NULL;
    PyXmlSec_InternalError = NULL;
    PyXmlSec_VerificationError = NULL;

    if ((PyXmlSec_Error = PyErr_NewExceptionWithDoc(
            STRINGIFY(MODULE_NAME) ".Error",  "The common exception class.", PyExc_Exception, 0)) == NULL) goto ON_FAIL;

    if ((PyXmlSec_InternalError = PyErr_NewExceptionWithDoc(
            STRINGIFY(MODULE_NAME) ".InternalError",  "The internal exception class.", PyXmlSec_Error, 0)) == NULL) goto ON_FAIL;

    if ((PyXmlSec_VerificationError = PyErr_NewExceptionWithDoc(
            STRINGIFY(MODULE_NAME) ".VerificationError",  "The verification exception class.", PyXmlSec_Error, 0)) == NULL) goto ON_FAIL;

    if (PyModule_AddObject(package, "Error", PyXmlSec_Error) < 0) goto ON_FAIL;
    if (PyModule_AddObject(package, "InternalError", PyXmlSec_InternalError) < 0) goto ON_FAIL;
    if (PyModule_AddObject(package, "VerificationError", PyXmlSec_VerificationError) < 0) goto ON_FAIL;

    #if PY_MINOR_VERSION >= 7
    if (PyThread_tss_create(&PyXmlSec_LastErrorKey) == 0) {
        PyXmlSec_InstallErrorCallback();
    }
    #else
    PyXmlSec_LastErrorKey = PyThread_create_key();
    PyXmlSec_InstallErrorCallback();
    #endif

    return 0;

ON_FAIL:
    Py_XDECREF(PyXmlSec_Error);
    Py_XDECREF(PyXmlSec_InternalError);
    Py_XDECREF(PyXmlSec_VerificationError);
    return -1;
}
