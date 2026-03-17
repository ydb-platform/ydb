// Copyright (c) 2017 Ryan Leckey
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include "common.h"
#include "platform.h"
#include "exception.h"
#include "lxml.h"

#include <xmlsec/xmlsec.h>
#include <xmlsec/crypto.h>
#include <xmlsec/errors.h>
#include <xmlsec/base64.h>
#include <xmlsec/io.h>

#define _PYXMLSEC_FREE_NONE 0
#define _PYXMLSEC_FREE_XMLSEC 1
#define _PYXMLSEC_FREE_CRYPTOLIB 2
#define _PYXMLSEC_FREE_ALL 3

static int free_mode = _PYXMLSEC_FREE_NONE;

#define MODULE_DOC "The tiny python wrapper around xmlsec1 (" XMLSEC_VERSION ") library"

#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING
static const xmlChar* PyXmlSec_GetCryptoLibName() {
#if XMLSEC_VERSION_HEX > 0x10214
    // xmlSecGetDefaultCrypto was introduced in version 1.2.21
    const xmlChar* cryptoLib = xmlSecGetDefaultCrypto();
#else
    const xmlChar* cryptoLib = (const xmlChar*) XMLSEC_CRYPTO;
#endif
    PYXMLSEC_DEBUGF("dynamic crypto library: %s", cryptoLib);
    return cryptoLib;
}
#endif // !XMLSEC_NO_CRYPTO_DYNAMIC_LOADING

static void PyXmlSec_Free(int what) {
    PYXMLSEC_DEBUGF("free resources %d", what);
    switch (what) {
    case _PYXMLSEC_FREE_ALL:
        xmlSecCryptoAppShutdown();
    case _PYXMLSEC_FREE_CRYPTOLIB:
#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING
        xmlSecCryptoDLUnloadLibrary(PyXmlSec_GetCryptoLibName());
#endif
    case _PYXMLSEC_FREE_XMLSEC:
        xmlSecShutdown();
    }
    free_mode = _PYXMLSEC_FREE_NONE;
}

static int PyXmlSec_Init(void) {
    if (xmlSecInit() < 0) {
        PyXmlSec_SetLastError("cannot initialize xmlsec library.");
        PyXmlSec_Free(_PYXMLSEC_FREE_NONE);
        return -1;
    }

    if (xmlSecCheckVersion() != 1) {
        PyXmlSec_SetLastError("xmlsec library version mismatch.");
        PyXmlSec_Free(_PYXMLSEC_FREE_XMLSEC);
        return -1;
    }

#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING
    if (xmlSecCryptoDLLoadLibrary(PyXmlSec_GetCryptoLibName()) < 0) {
        PyXmlSec_SetLastError("cannot load crypto library for xmlsec.");
        PyXmlSec_Free(_PYXMLSEC_FREE_XMLSEC);
        return -1;
    }
#endif /* XMLSEC_CRYPTO_DYNAMIC_LOADING */

  /* Init crypto library */
    if (xmlSecCryptoAppInit(NULL) < 0) {
        PyXmlSec_SetLastError("cannot initialize crypto library application.");
        PyXmlSec_Free(_PYXMLSEC_FREE_CRYPTOLIB);
        return -1;
    }

  /* Init xmlsec-crypto library */
    if (xmlSecCryptoInit() < 0) {
        PyXmlSec_SetLastError("cannot initialize crypto library.");
        PyXmlSec_Free(_PYXMLSEC_FREE_ALL);
        return -1;
    }
    // xmlsec will install default callback in xmlSecCryptoInit,
    // overwriting any custom callbacks.
    // We thus reinstall our callback now.
    PyXmlSec_InstallErrorCallback();

    free_mode = _PYXMLSEC_FREE_ALL;
    return 0;
}

static char PyXmlSec_PyInit__doc__[] = \
    "init() -> None\n"
    "Initializes the library for general operation.\n\n"
    "This is called upon library import and does not need to be called\n"
    "again :func:`~.shutdown` is called explicitly).\n";
static PyObject* PyXmlSec_PyInit(PyObject *self) {
   if (PyXmlSec_Init() < 0) {
        return NULL;
   }
   Py_RETURN_NONE;
}

static char PyXmlSec_PyShutdown__doc__[] = \
    "shutdown() -> None\n"
    "Shutdowns the library and cleanup any leftover resources.\n\n"
    "This is called automatically upon interpreter termination and\n"
    "should not need to be called explicitly.";
static PyObject* PyXmlSec_PyShutdown(PyObject* self) {
    PyXmlSec_Free(free_mode);
    Py_RETURN_NONE;
}

static char PyXmlSec_GetLibXmlSecVersion__doc__[] = \
    "get_libxmlsec_version() -> tuple\n"
    "Returns Version tuple of wrapped libxmlsec library.";
static PyObject* PyXmlSec_GetLibXmlSecVersion() {
    return Py_BuildValue("(iii)", XMLSEC_VERSION_MAJOR, XMLSEC_VERSION_MINOR, XMLSEC_VERSION_SUBMINOR);
}

static char PyXmlSec_GetLibXmlVersion__doc__[] = \
    "get_libxml_version() -> tuple[int, int, int]\n"
    "Returns version tuple of libxml2 library xmlsec is using.";
static PyObject* PyXmlSec_GetLibXmlVersion() {
    return Py_BuildValue(
        "(iii)",
        PyXmlSec_GetLibXmlVersionMajor(),
        PyXmlSec_GetLibXmlVersionMinor(),
        PyXmlSec_GetLibXmlVersionPatch()
    );
}

static char PyXmlSec_GetLibXmlCompiledVersion__doc__[] = \
    "get_libxml_compiled_version() -> tuple[int, int, int]\n"
    "Returns version tuple of libxml2 library xmlsec was compiled with.";
static PyObject* PyXmlSec_GetLibXmlCompiledVersion() {
    return Py_BuildValue(
        "(iii)",
        PyXmlSec_GetLibXmlCompiledVersionMajor(),
        PyXmlSec_GetLibXmlCompiledVersionMinor(),
        PyXmlSec_GetLibXmlCompiledVersionPatch()
    );
}

static char PyXmlSec_PyEnableDebugOutput__doc__[] = \
    "enable_debug_trace(enabled) -> None\n"
    "Enables or disables calling LibXML2 callback from the default errors callback.\n\n"
    ":param enabled: flag, debug trace is enabled or disabled\n"
    ":type enabled: :class:`bool`";
static PyObject* PyXmlSec_PyEnableDebugOutput(PyObject *self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "enabled", NULL};
    PyObject* enabled = Py_True;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O:enable_debug_trace", kwlist, &enabled)) {
        return NULL;
    }
    PyXmlSecEnableDebugTrace(PyObject_IsTrue(enabled));
    Py_RETURN_NONE;
}

// NB: This whole thing assumes that the `xmlsec` callbacks are not re-entrant
// (i.e. that xmlsec won't come across a link in the reference it's processing
// and try to open that with these callbacks too).
typedef struct CbList {
  PyObject* match_cb;
  PyObject* open_cb;
  PyObject* read_cb;
  PyObject* close_cb;
  struct CbList* next;
} CbList;

static CbList* registered_callbacks = NULL;

static void RCBListCons(CbList* cb_list_item) {
    cb_list_item->next = registered_callbacks;
    registered_callbacks = cb_list_item;
}

static void RCBListClear() {
    CbList* cb_list_item = registered_callbacks;
    while (cb_list_item) {
        Py_DECREF(cb_list_item->match_cb);
        Py_DECREF(cb_list_item->open_cb);
        Py_DECREF(cb_list_item->read_cb);
        Py_DECREF(cb_list_item->close_cb);
        CbList* next = cb_list_item->next;
        free(cb_list_item);
        cb_list_item = next;
    }
    registered_callbacks = NULL;
}

// The currently executing set of Python callbacks:
static CbList* cur_cb_list_item;

static int PyXmlSec_MatchCB(const char* filename) {
    cur_cb_list_item = registered_callbacks;
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* args = Py_BuildValue("(y)", filename);
    while (cur_cb_list_item) {
        PyObject* result = PyObject_CallObject(cur_cb_list_item->match_cb, args);
        if (result && PyObject_IsTrue(result)) {
            Py_DECREF(result);
            Py_DECREF(args);
            PyGILState_Release(state);
            return 1;
        }
        Py_XDECREF(result);
        cur_cb_list_item = cur_cb_list_item->next;
    }
    Py_DECREF(args);
    PyGILState_Release(state);
    return 0;
}

static void* PyXmlSec_OpenCB(const char* filename) {
    PyGILState_STATE state = PyGILState_Ensure();

    // NB: Assumes the match callback left the current callback list item in the
    // right place:
    PyObject* args = Py_BuildValue("(y)", filename);
    PyObject* result = PyObject_CallObject(cur_cb_list_item->open_cb, args);
    Py_DECREF(args);

    PyGILState_Release(state);
    return result;
}

static int PyXmlSec_ReadCB(void* context, char* buffer, int len) {
    PyGILState_STATE state = PyGILState_Ensure();

    // NB: Assumes the match callback left the current callback list item in the
    // right place:
    PyObject* py_buffer = PyMemoryView_FromMemory(buffer, (Py_ssize_t) len, PyBUF_WRITE);
    PyObject* args = Py_BuildValue("(OO)", context, py_buffer);
    PyObject* py_bytes_read = PyObject_CallObject(cur_cb_list_item->read_cb, args);
    Py_DECREF(args);
    Py_DECREF(py_buffer);
    int result;
    if (py_bytes_read && PyLong_Check(py_bytes_read)) {
        result = (int)PyLong_AsLong(py_bytes_read);
    } else {
        result = EOF;
    }
    Py_XDECREF(py_bytes_read);

    PyGILState_Release(state);
    return result;
}

static int PyXmlSec_CloseCB(void* context) {
    PyGILState_STATE state = PyGILState_Ensure();

    PyObject* args = Py_BuildValue("(O)", context);
    PyObject* result = PyObject_CallObject(cur_cb_list_item->close_cb, args);
    Py_DECREF(args);
    Py_DECREF(context);
    Py_DECREF(result);

    PyGILState_Release(state);
    return 0;
}

static char PyXmlSec_PyIOCleanupCallbacks__doc__[] = \
    "Unregister globally all sets of IO callbacks from xmlsec.";
static PyObject* PyXmlSec_PyIOCleanupCallbacks(PyObject *self) {
    xmlSecIOCleanupCallbacks();
    // We always have callbacks registered to delegate to any Python callbacks
    // we have registered within these bindings:
    if (xmlSecIORegisterCallbacks(
            PyXmlSec_MatchCB, PyXmlSec_OpenCB, PyXmlSec_ReadCB,
            PyXmlSec_CloseCB) < 0) {
        return NULL;
    }
    RCBListClear();
    Py_RETURN_NONE;
}

static char PyXmlSec_PyIORegisterDefaultCallbacks__doc__[] = \
    "Register globally xmlsec's own default set of IO callbacks.";
static PyObject* PyXmlSec_PyIORegisterDefaultCallbacks(PyObject *self) {
    // NB: The default callbacks (specifically libxml2's `xmlFileMatch`) always
    // match, and callbacks are called in the reverse order to that which they
    // were added. So, there's no point in holding onto any previously registered
    // callbacks, because they will never be run:
    xmlSecIOCleanupCallbacks();
    RCBListClear();
    if (xmlSecIORegisterDefaultCallbacks() < 0) {
        return NULL;
    }
    // We need to make sure we can continue trying to match any newly added
    // Python callbacks:
    if (xmlSecIORegisterCallbacks(
            PyXmlSec_MatchCB, PyXmlSec_OpenCB, PyXmlSec_ReadCB,
            PyXmlSec_CloseCB) < 0) {
        return NULL;
    };
    Py_RETURN_NONE;
}

static char PyXmlSec_PyIORegisterCallbacks__doc__[] = \
    "register_callbacks(input_match_callback, input_open_callback, input_read_callback, input_close_callback) -> None\n"
    "Register globally a custom set of IO callbacks with xmlsec.\n\n"
    ":param input_match_callback: A callable that takes a filename `bytestring` and "
    "returns a boolean as to whether the other callbacks in this set can handle that name.\n"
    ":type input_match_callback: ~collections.abc.Callable[[bytes], bool]\n"
    ":param input_open_callback: A callable that takes a filename and returns some "
    "context object (e.g. a file object) that the remaining callables in this set will be passed "
    "during handling.\n"
    ":type input_open_callback: ~collections.abc.Callable[[bytes], Any]\n"
    // FIXME: How do we handle failures in ^^ (e.g. can't find the file)?
    ":param input_read_callback: A callable that that takes the context object from the "
    "open callback and a buffer, and should fill the buffer with data (e.g. BytesIO.readinto()). "
    "xmlsec will call this function several times until there is no more data returned.\n"
    ":type input_read_callback: ~collections.abc.Callable[[Any, memoryview], int]\n"
    ":param input_close_callback: A callable that takes the context object from the "
    "open callback and can do any resource cleanup necessary.\n"
    ":type input_close_callback: ~collections.abc.Callable[[Any], None]\n"
    ;
static PyObject* PyXmlSec_PyIORegisterCallbacks(PyObject *self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {
        "input_match_callback",
        "input_open_callback",
        "input_read_callback",
        "input_close_callback",
        NULL
    };
    CbList* cb_list_item = malloc(sizeof(CbList));
    if (cb_list_item == NULL) {
      return NULL;
    }
    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "OOOO:register_callbacks", kwlist,
            &cb_list_item->match_cb, &cb_list_item->open_cb, &cb_list_item->read_cb,
            &cb_list_item->close_cb)) {
        free(cb_list_item);
        return NULL;
    }
    if (!PyCallable_Check(cb_list_item->match_cb)) {
        PyErr_SetString(PyExc_TypeError, "input_match_callback must be a callable");
        free(cb_list_item);
        return NULL;
    }
    if (!PyCallable_Check(cb_list_item->open_cb)) {
        PyErr_SetString(PyExc_TypeError, "input_open_callback must be a callable");
        free(cb_list_item);
        return NULL;
    }
    if (!PyCallable_Check(cb_list_item->read_cb)) {
        PyErr_SetString(PyExc_TypeError, "input_read_callback must be a callable");
        free(cb_list_item);
        return NULL;
    }
    if (!PyCallable_Check(cb_list_item->close_cb)) {
        PyErr_SetString(PyExc_TypeError, "input_close_callback must be a callable");
        free(cb_list_item);
        return NULL;
    }
    Py_INCREF(cb_list_item->match_cb);
    Py_INCREF(cb_list_item->open_cb);
    Py_INCREF(cb_list_item->read_cb);
    Py_INCREF(cb_list_item->close_cb);
    cb_list_item->next = NULL;
    RCBListCons(cb_list_item);
    // NB: We don't need to register the callbacks with `xmlsec` here, because
    // we've already registered our helper functions that will trawl through our
    // list of callbacks.
    Py_RETURN_NONE;
}

static char PyXmlSec_PyBase64DefaultLineSize__doc__[] = \
    "base64_default_line_size(size = None)\n"
    "Configures the default maximum columns size for base64 encoding.\n\n"
    "If ``size`` is not given, this function returns the current default size, acting as a getter. "
    "If ``size`` is given, a new value is applied and this function returns nothing, acting as a setter.\n"
    ":param size: new default size value (optional)\n"
    ":type size: :class:`int` or :data:`None`";
static PyObject* PyXmlSec_PyBase64DefaultLineSize(PyObject *self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "size", NULL };
    PyObject *pySize = NULL;
    int size;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O:base64_default_line_size", kwlist, &pySize)) {
        return NULL;
    }
    if (pySize == NULL) {
        return PyLong_FromLong(xmlSecBase64GetDefaultLineSize());
    }
    size = (int)PyLong_AsLong(pySize);
    if (PyErr_Occurred()) {
        return NULL;
    }
    if (size < 0) {
        PyErr_SetString(PyExc_ValueError, "size must be positive");
        return NULL;
    }
    xmlSecBase64SetDefaultLineSize(size);
    Py_RETURN_NONE;
}

static PyMethodDef PyXmlSec_MainMethods[] = {
    {
        "init",
        (PyCFunction)PyXmlSec_PyInit,
        METH_NOARGS,
        PyXmlSec_PyInit__doc__
    },
    {
        "shutdown",
        (PyCFunction)PyXmlSec_PyShutdown,
        METH_NOARGS,
        PyXmlSec_PyShutdown__doc__
    },
    {
        "get_libxmlsec_version",
        (PyCFunction)PyXmlSec_GetLibXmlSecVersion,
        METH_NOARGS,
        PyXmlSec_GetLibXmlSecVersion__doc__
    },
    {
        "get_libxml_version",
        (PyCFunction)PyXmlSec_GetLibXmlVersion,
        METH_NOARGS,
        PyXmlSec_GetLibXmlVersion__doc__
    },
    {
        "get_libxml_compiled_version",
        (PyCFunction)PyXmlSec_GetLibXmlCompiledVersion,
        METH_NOARGS,
        PyXmlSec_GetLibXmlCompiledVersion__doc__
    },
    {
        "enable_debug_trace",
        (PyCFunction)PyXmlSec_PyEnableDebugOutput,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_PyEnableDebugOutput__doc__
    },
    {
        "cleanup_callbacks",
        (PyCFunction)PyXmlSec_PyIOCleanupCallbacks,
        METH_NOARGS,
        PyXmlSec_PyIOCleanupCallbacks__doc__
    },
    {
        "register_default_callbacks",
        (PyCFunction)PyXmlSec_PyIORegisterDefaultCallbacks,
        METH_NOARGS,
        PyXmlSec_PyIORegisterDefaultCallbacks__doc__
    },
    {
        "register_callbacks",
        (PyCFunction)PyXmlSec_PyIORegisterCallbacks,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_PyIORegisterCallbacks__doc__
    },
    {
        "base64_default_line_size",
        (PyCFunction)PyXmlSec_PyBase64DefaultLineSize,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_PyBase64DefaultLineSize__doc__
    },
    {NULL, NULL} /* sentinel */
};

// modules entry points
// loads lxml module
int PyXmlSec_InitLxmlModule(void);
// constants
int PyXmlSec_ConstantsModule_Init(PyObject* package);
// exceptions
int PyXmlSec_ExceptionsModule_Init(PyObject* package);
// keys management
int PyXmlSec_KeyModule_Init(PyObject* package);
// init lxml.tree integration
int PyXmlSec_TreeModule_Init(PyObject* package);
// digital signature management
int PyXmlSec_DSModule_Init(PyObject* package);
// encryption management
int PyXmlSec_EncModule_Init(PyObject* package);
// templates management
int PyXmlSec_TemplateModule_Init(PyObject* package);

static int PyXmlSec_PyClear(PyObject *self) {
    PyXmlSec_Free(free_mode);
    return 0;
}

static PyModuleDef PyXmlSecModule = {
    PyModuleDef_HEAD_INIT,
    STRINGIFY(MODULE_NAME), /* name of module */
    MODULE_DOC,             /* module documentation, may be NULL */
    -1,                     /* size of per-interpreter state of the module,
                               or -1 if the module keeps state in global variables. */
    PyXmlSec_MainMethods,   /* m_methods */
    NULL,                   /* m_slots */
    NULL,                   /* m_traverse */
    PyXmlSec_PyClear,       /* m_clear */
    NULL,                   /* m_free */
};

#define PYENTRY_FUNC_NAME JOIN(PyInit_, MODULE_NAME)
#define PY_MOD_RETURN(m) return m

PyMODINIT_FUNC
PYENTRY_FUNC_NAME(void)
{
    PyObject *module = NULL;
    module = PyModule_Create(&PyXmlSecModule);
    if (!module) {
        PY_MOD_RETURN(NULL); /* this really should never happen */
    }
    PYXMLSEC_DEBUGF("%p", module);

    // init first, since PyXmlSec_Init may raise XmlSecError
    if (PyXmlSec_ExceptionsModule_Init(module) < 0) goto ON_FAIL;

    if (PyXmlSec_Init() < 0) goto ON_FAIL;

    if (PyModule_AddStringConstant(module, "__version__", STRINGIFY(MODULE_VERSION)) < 0) goto ON_FAIL;

    if (PyXmlSec_InitLxmlModule() < 0) goto ON_FAIL;
    /* Populate final object settings */
    if (PyXmlSec_ConstantsModule_Init(module) < 0) goto ON_FAIL;
    if (PyXmlSec_KeyModule_Init(module) < 0) goto ON_FAIL;
    if (PyXmlSec_TreeModule_Init(module) < 0) goto ON_FAIL;
    if (PyXmlSec_DSModule_Init(module) < 0) goto ON_FAIL;
    if (PyXmlSec_EncModule_Init(module) < 0) goto ON_FAIL;
    if (PyXmlSec_TemplateModule_Init(module) < 0) goto ON_FAIL;

    PY_MOD_RETURN(module);
ON_FAIL:
    PY_MOD_RETURN(NULL);
}
