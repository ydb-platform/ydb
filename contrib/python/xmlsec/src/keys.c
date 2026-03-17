// Copyright (c) 2017 Ryan Leckey
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include "common.h"
#include "constants.h"
#include "exception.h"
#include "keys.h"
#include "utils.h"

#include <xmlsec/crypto.h>


static PyObject* PyXmlSec_Key__new__(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyXmlSec_Key* key = (PyXmlSec_Key*)PyType_GenericNew(type, args, kwargs);
    PYXMLSEC_DEBUGF("%p: new key", key);
    if (key != NULL) {
        key->handle = NULL;
        key->is_own = 0;
    }
    return (PyObject*)(key);
}

static void PyXmlSec_Key__del__(PyObject* self) {
    PyXmlSec_Key* key = (PyXmlSec_Key*)self;
    PYXMLSEC_DEBUGF("%p: delete key", self);
    if (key->is_own) {
        PYXMLSEC_DEBUGF("%p: delete handle - %p", self, key->handle);
        xmlSecKeyDestroy(key->handle);
    }
    Py_TYPE(self)->tp_free(self);
}

static PyXmlSec_Key* PyXmlSec_NewKey1(PyTypeObject* type) {
    return (PyXmlSec_Key*)PyObject_CallFunctionObjArgs((PyObject*)type, NULL);
}

static PyObject* PyXmlSec_Key__copy__(PyObject* self) {
    xmlSecKeyPtr handle = ((PyXmlSec_Key*)self)->handle;
    PyXmlSec_Key* key2;

    PYXMLSEC_DEBUGF("%p: copy key", self);

    key2 = PyXmlSec_NewKey1(Py_TYPE(self));

    if (handle == NULL || key2 == NULL) {
        PYXMLSEC_DEBUGF("%p: null key", self);
        return (PyObject*)key2;
    }

    Py_BEGIN_ALLOW_THREADS;
    key2->handle = xmlSecKeyDuplicate(handle);
    Py_END_ALLOW_THREADS;

    if (key2->handle == NULL) {
        PYXMLSEC_DEBUGF("%p: failed to duplicate key", self);
        PyXmlSec_SetLastError("cannot duplicate key");
        Py_DECREF(key2);
        return NULL;
    }
    key2->is_own = 1;
    return (PyObject*)key2;
}

static const char PyXmlSec_KeyFromMemory__doc__[] = \
    "from_memory(data, format, password = None) -> xmlsec.Key\n"
    "Loads PKI key from memory.\n\n"
    ":param data: the binary key data\n"
    ":type data: :class:`str` or :class:`bytes`\n"
    ":param format: the key file format\n"
    ":type format: :class:`int`\n"
    ":param password: the key file password (optional)\n"
    ":type password: :class:`str` or :data:`None`\n"
    ":return: pointer to newly created key\n"
    ":rtype: :class:`~xmlsec.Key`";
static PyObject* PyXmlSec_KeyFromMemory(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "data", "format", "password", NULL};

    const char* data = NULL;
    Py_ssize_t data_size = 0;
    const char* password = NULL;
    unsigned int format = 0;

    PyXmlSec_Key* key = NULL;

    PYXMLSEC_DEBUG("load key from memory - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#I|z:from_memory", kwlist, &data, &data_size, &format, &password)) {
        goto ON_FAIL;
    }

    if ((key = PyXmlSec_NewKey1((PyTypeObject*)self)) == NULL) goto ON_FAIL;

    Py_BEGIN_ALLOW_THREADS;
    key->handle = xmlSecCryptoAppKeyLoadMemory((const xmlSecByte*)data, (xmlSecSize)data_size, format, password, NULL, NULL);
    Py_END_ALLOW_THREADS;

    if (key->handle == NULL) {
        PyXmlSec_SetLastError("cannot load key");
        goto ON_FAIL;
    }

    key->is_own = 1;

    PYXMLSEC_DEBUG("load key from memory - ok");

    return (PyObject*)key;

ON_FAIL:
    PYXMLSEC_DEBUG("load key from memory - fail");
    Py_XDECREF(key);
    return NULL;
}

static const char PyXmlSec_KeyFromFile__doc__[] = \
    "from_file(file, format, password = None) -> xmlsec.Key\n"
    "Loads PKI key from a file.\n\n"
    ":param file: the file object or file path\n"
    ":type file: :class:`str`, :class:`bytes`, any :class:`~os.PathLike`, "
    ":class:`~typing.BinaryIO` or :class:`~typing.TextIO`\n"
    ":param format: the key file format\n"
    ":type format: :class:`int`\n"
    ":param password: the key file password (optional)\n"
    ":type password: :class:`str` or :data:`None`\n"
    ":return: pointer to newly created key\n"
    ":rtype: :class:`~xmlsec.Key`";
static PyObject* PyXmlSec_KeyFromFile(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "file", "format", "password", NULL};

    PyObject* file = NULL;
    const char* password = NULL;
    unsigned int format = 0;

    PyXmlSec_Key* key = NULL;
    PyObject* bytes = NULL;
    int is_content = 0;
    const char* data = NULL;
    Py_ssize_t data_size = 0;

    PYXMLSEC_DEBUG("load key from file - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OI|z:from_file", kwlist, &file, &format, &password)) {
        goto ON_FAIL;
    }

    bytes = PyXmlSec_GetFilePathOrContent(file, &is_content);
    if (bytes == NULL) goto ON_FAIL;

    if (is_content == 1) {
        data = PyBytes_AsStringAndSize2(bytes, &data_size);
    } else {
        data = PyBytes_AsString(bytes);
    }

    if (data == NULL) goto ON_FAIL;

    if ((key = PyXmlSec_NewKey1((PyTypeObject*)self)) == NULL) goto ON_FAIL;

    Py_BEGIN_ALLOW_THREADS;
    if (is_content) {
        key->handle = xmlSecCryptoAppKeyLoadMemory((const xmlSecByte*)data, (xmlSecSize)data_size, format, password, NULL, NULL);
    } else {
        #if XMLSEC_VERSION_HEX >= 0x10303
            // from version 1.3.3 (inclusive)
            key->handle = xmlSecCryptoAppKeyLoadEx(data, xmlSecKeyDataTypePrivate, format, password, NULL, NULL);
        #else
            key->handle = xmlSecCryptoAppKeyLoad(data, format, password, NULL, NULL);
        #endif
    }
    Py_END_ALLOW_THREADS;

    if (key->handle == NULL) {
        PyXmlSec_SetLastError("cannot read key");
        goto ON_FAIL;
    }

    key->is_own = 1;
    Py_DECREF(bytes);

    PYXMLSEC_DEBUG("load key from file - ok");
    return (PyObject*)key;

ON_FAIL:
    PYXMLSEC_DEBUG("load key from file - fail");
    Py_XDECREF(key);
    Py_XDECREF(bytes);
    return NULL;
}

static const char PyXmlSec_KeyFromEngine__doc__[] = \
    "from_engine(engine_and_key_id) -> xmlsec.Key\n"
    "Loads PKI key from an engine.\n\n"
    ":param engine_and_key_id: engine and key id, i.e. 'pkcs11;pkcs11:token=XmlsecToken;object=XmlsecKey;pin-value=password'\n"
    ":type engine_and_key_id: :class:`str`, "
    ":return: pointer to newly created key\n"
    ":rtype: :class:`~xmlsec.Key`";
static PyObject* PyXmlSec_KeyFromEngine(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = {"engine_and_key_id", NULL};

    const char* engine_and_key_id = NULL;
    PyXmlSec_Key* key = NULL;

    PYXMLSEC_DEBUG("load key from engine - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s:from_engine", kwlist, &engine_and_key_id)) {
        goto ON_FAIL;
    }

    if ((key = PyXmlSec_NewKey1((PyTypeObject*)self)) == NULL) goto ON_FAIL;

    Py_BEGIN_ALLOW_THREADS;
    #if XMLSEC_VERSION_HEX >= 0x10303
        // from version 1.3.3 (inclusive)
        key->handle = xmlSecCryptoAppKeyLoadEx(engine_and_key_id, xmlSecKeyDataTypePrivate, xmlSecKeyDataFormatEngine, NULL, xmlSecCryptoAppGetDefaultPwdCallback(), (void*)engine_and_key_id);
    #else
        key->handle = xmlSecCryptoAppKeyLoad(engine_and_key_id, xmlSecKeyDataFormatEngine, NULL, xmlSecCryptoAppGetDefaultPwdCallback(), (void*)engine_and_key_id);
    #endif
    Py_END_ALLOW_THREADS;

    if (key->handle == NULL) {
        PyXmlSec_SetLastError("cannot read key");
        goto ON_FAIL;
    }

    key->is_own = 1;

    PYXMLSEC_DEBUG("load key from engine - ok");
    return (PyObject*)key;

ON_FAIL:
    PYXMLSEC_DEBUG("load key from engine - fail");
    Py_XDECREF(key);
    return NULL;
}

static const char PyXmlSec_KeyGenerate__doc__[] = \
    "generate(klass, size, type) -> xmlsec.Key\n"
    "Generates key of kind ``klass`` with ``size`` and ``type``.\n\n"
    ":param klass: the requested key klass (rsa, dsa, aes, ...)\n"
    ":type klass: :class:`__KeyData`\n"
    ":param size: the new key size (in bits!)\n"
    ":type size: :class:`int`\n"
    ":param type: the new key type (session, permanent, ...)\n"
    ":type type: :class:`int`\n"
    ":return: pointer to newly created key\n"
    ":rtype: :class:`~xmlsec.Key`";
static PyObject* PyXmlSec_KeyGenerate(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "klass", "size", "type", NULL};

    PyXmlSec_KeyData* keydata = NULL;
    short unsigned int keysize = 0;
    unsigned int keytype = 0;

    PyXmlSec_Key* key = NULL;

    PYXMLSEC_DEBUG("generate new key - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!HI:generate", kwlist, PyXmlSec_KeyDataType, &keydata, &keysize, &keytype)) {
        goto ON_FAIL;
    }
    if ((key = PyXmlSec_NewKey1((PyTypeObject*)self)) == NULL) goto ON_FAIL;

    Py_BEGIN_ALLOW_THREADS;
    key->handle = xmlSecKeyGenerate(keydata->id, keysize, keytype);
    Py_END_ALLOW_THREADS;

    if (key->handle == NULL) {
        PyXmlSec_SetLastError("cannot generate key");
        goto ON_FAIL;
    }
    key->is_own = 1;
    PYXMLSEC_DEBUG("generate new key - ok");
    return (PyObject*)key;

ON_FAIL:
    PYXMLSEC_DEBUG("generate new key - fail");
    Py_XDECREF(key);
    return NULL;
}

static const char PyXmlSec_KeyFromBinaryFile__doc__[] = \
    "from_binary_file(klass, filename) -> xmlsec.Key\n"
    "Loads (symmetric) key of kind ``klass`` from ``filename``.\n\n"
    ":param klass: the key value data klass\n"
    ":type klass: :class:`__KeyData`\n"
    ":param filename: the key binary filename\n"
    ":type filename: :class:`str`, :class:`bytes` or any :class:`~os.PathLike`\n"
    ":return: pointer to newly created key\n"
    ":rtype: :class:`~xmlsec.Key`";
static PyObject* PyXmlSec_KeyFromBinaryFile(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "klass", "filename", NULL};

    PyXmlSec_KeyData* keydata = NULL;
    PyObject* filepath = NULL;

    PyXmlSec_Key* key = NULL;
    const char* filename;

    PYXMLSEC_DEBUG("load symmetric key - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!O&:from_binary_file", kwlist,
        PyXmlSec_KeyDataType, &keydata, PyUnicode_FSConverter, &filepath))
    {
        goto ON_FAIL;
    }

    filename = PyBytes_AsString(filepath);
    if (filename == NULL) goto ON_FAIL;
    if ((key = PyXmlSec_NewKey1((PyTypeObject*)self)) == NULL) goto ON_FAIL;

    Py_BEGIN_ALLOW_THREADS;
    key->handle = xmlSecKeyReadBinaryFile(keydata->id, filename);
    Py_END_ALLOW_THREADS;

    if (key->handle == NULL) {
        PyXmlSec_SetLastError("cannot read key");
        goto ON_FAIL;
    }

    key->is_own = 1;
    Py_DECREF(filepath);

    PYXMLSEC_DEBUG("load symmetric key - ok");
    return (PyObject*)key;

ON_FAIL:
    PYXMLSEC_DEBUG("load symmetric key - fail");
    Py_XDECREF(key);
    Py_XDECREF(filepath);
    return NULL;
}

static const char PyXmlSec_KeyFromBinaryData__doc__[] = \
    "from_binary_data(klass, data) -> xmlsec.Key\n"
    "Loads (symmetric) key of kind ``klass`` from ``data``.\n\n"
    ":param klass: the key value data klass\n"
    ":type klass: :class:`__KeyData`\n"
    ":param data: the key binary data\n"
    ":type data: :class:`str` or :class:`bytes`\n"
    ":return: pointer to newly created key\n"
    ":rtype: :class:`~xmlsec.Key`";
static PyObject* PyXmlSec_KeyFromBinaryData(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "klass", "data", NULL};

    PyXmlSec_KeyData* keydata = NULL;
    const char* data = NULL;
    Py_ssize_t data_size = 0;

    PyXmlSec_Key* key = NULL;

    PYXMLSEC_DEBUG("load symmetric key from memory - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!s#:from_binary_data", kwlist,
        PyXmlSec_KeyDataType, &keydata,  &data, &data_size))
    {
        goto ON_FAIL;
    }

    if ((key = PyXmlSec_NewKey1((PyTypeObject*)self)) == NULL) goto ON_FAIL;

    Py_BEGIN_ALLOW_THREADS;
    key->handle = xmlSecKeyReadMemory(keydata->id, (const xmlSecByte*)data, (xmlSecSize)data_size);
    Py_END_ALLOW_THREADS;

    if (key->handle == NULL) {
        PyXmlSec_SetLastError("cannot read key");
        goto ON_FAIL;
    }

    key->is_own = 1;

    PYXMLSEC_DEBUG("load symmetric key from memory - ok");
    return (PyObject*)key;

ON_FAIL:
    PYXMLSEC_DEBUG("load symmetric key from memory - fail");
    Py_XDECREF(key);
    return NULL;
}

static const char PyXmlSec_KeyCertFromMemory__doc__[] = \
    "load_cert_from_memory(data, format) -> None\n"
    "Loads certificate from memory.\n\n"
    ":param data: the certificate binary data\n"
    ":type data: :class:`str` or :class:`bytes`\n"
    ":param format: the certificate file format\n"
    ":type format: :class:`int`";
static PyObject* PyXmlSec_KeyCertFromMemory(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "data", "format", NULL};

    PyXmlSec_Key* key = (PyXmlSec_Key*)self;
    const char* data = NULL;
    Py_ssize_t data_size = 0;
    unsigned int format = 0;

    PyObject* tmp = NULL;
    int rv = 0;

    PYXMLSEC_DEBUGF("%p: load certificate from memory - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#I:load_cert_from_memory", kwlist, &data, &data_size, &format)) {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecCryptoAppKeyCertLoadMemory(key->handle, (const xmlSecByte*)data, (xmlSecSize)data_size, format);
    Py_END_ALLOW_THREADS;
    if (rv < 0) {
        PyXmlSec_SetLastError("cannot load cert");
        goto ON_FAIL;
    }
    Py_XDECREF(tmp);
    PYXMLSEC_DEBUGF("%p: load certificate from memory - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: load certificate from memory - fail", self);
    Py_XDECREF(tmp);
    return NULL;
}

static const char PyXmlSec_KeyCertFromFile__doc__[] = \
    "load_cert_from_file(file, format) -> None\n"
    "Loads certificate from file.\n\n"
    ":param file: the file object or file path\n"
    ":type file: :class:`str`, :class:`bytes`, any :class:`~os.PathLike`, "
    ":class:`~typing.BinaryIO` or :class:`~typing.TextIO`\n"
    ":param format: the certificate file format\n"
    ":type format: :class:`int`";
static PyObject* PyXmlSec_KeyCertFromFile(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "file", "format", NULL};

    PyXmlSec_Key* key = (PyXmlSec_Key*)self;

    PyObject* file = NULL;
    unsigned int format = 0;

    PyObject* bytes = NULL;
    int is_content = 0;
    const char* data = NULL;
    Py_ssize_t data_size = 0;
    int rv = 0;

    PYXMLSEC_DEBUGF("%p: load certificate from memory - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OI:load_cert_from_file", kwlist, &file, &format)) {
        goto ON_FAIL;
    }
    bytes = PyXmlSec_GetFilePathOrContent(file, &is_content);
    if (bytes == NULL) goto ON_FAIL;

    if (is_content == 1) {
        data = PyBytes_AsStringAndSize2(bytes, &data_size);
    } else {
        data = PyBytes_AsString(bytes);
    }

    if (data == NULL) goto ON_FAIL;

    Py_BEGIN_ALLOW_THREADS;
    if (is_content) {
        rv = xmlSecCryptoAppKeyCertLoadMemory(key->handle, (const xmlSecByte*)data, (xmlSecSize)data_size, format);
    } else {
        rv = xmlSecCryptoAppKeyCertLoad(key->handle, data, format);
    }
    Py_END_ALLOW_THREADS;
    if (rv < 0) {
        PyXmlSec_SetLastError("cannot load cert");
        goto ON_FAIL;
    }
    Py_DECREF(bytes);

    PYXMLSEC_DEBUGF("%p: load certificate from file - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: load certificate from file - fail", self);
    Py_XDECREF(bytes);
    return NULL;
}

static const char PyXmlSec_KeyName__doc__[] = "the name of this key.\n";
static PyObject* PyXmlSec_KeyNameGet(PyObject* self, void* closure) {
    PyXmlSec_Key* key = (PyXmlSec_Key*)self;
    const char* cname;

    PYXMLSEC_DEBUGF("%p: get name of key", self);
    if (key->handle == NULL) {
        PyErr_SetString(PyExc_ValueError, "key is not ready");
        return NULL;
    }
    cname = (const char*)xmlSecKeyGetName(key->handle);
    if (cname != NULL) {
        return PyUnicode_FromString(cname);
    }
    Py_RETURN_NONE;
}

static int PyXmlSec_KeyNameSet(PyObject* self, PyObject* value, void* closure) {
    PyXmlSec_Key* key = (PyXmlSec_Key*)self;
    const char* name;

    PYXMLSEC_DEBUGF("%p: set name of key %p", self, value);

    if (key->handle == NULL) {
        PyErr_SetString(PyExc_ValueError, "key is not ready");
        return -1;
    }

    if (value == NULL) {
        if (xmlSecKeySetName(key->handle, NULL) < 0) {
            PyXmlSec_SetLastError("cannot delete name");
            return -1;
        }
        return 0;
    }

    name = PyUnicode_AsUTF8(value);
    if (name == NULL) return -1;

    if (xmlSecKeySetName(key->handle, XSTR(name)) < 0) {
        PyXmlSec_SetLastError("cannot set name");
        return -1;
    }
    return 0;
}

static PyGetSetDef PyXmlSec_KeyGetSet[] = {
    {
        "name",
        (getter)PyXmlSec_KeyNameGet,
        (setter)PyXmlSec_KeyNameSet,
        (char*)PyXmlSec_KeyName__doc__,
        NULL
    },
    {NULL} /* Sentinel */
};

static PyMethodDef PyXmlSec_KeyMethods[] = {
    {
        "from_memory",
        (PyCFunction)PyXmlSec_KeyFromMemory,
        METH_CLASS|METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeyFromMemory__doc__,
    },
    {
        "from_file",
        (PyCFunction)PyXmlSec_KeyFromFile,
        METH_CLASS|METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeyFromFile__doc__
    },
    {
        "from_engine",
        (PyCFunction)PyXmlSec_KeyFromEngine,
        METH_CLASS|METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeyFromEngine__doc__
    },
    {
        "generate",
        (PyCFunction)PyXmlSec_KeyGenerate,
        METH_CLASS|METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeyGenerate__doc__
    },
    {
        "from_binary_file",
        (PyCFunction)PyXmlSec_KeyFromBinaryFile,
        METH_CLASS|METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeyFromBinaryFile__doc__
    },
    {
        "from_binary_data",
        (PyCFunction)PyXmlSec_KeyFromBinaryData,
        METH_CLASS|METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeyFromBinaryData__doc__
    },
    {
        "load_cert_from_memory",
        (PyCFunction)PyXmlSec_KeyCertFromMemory,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeyCertFromMemory__doc__
    },
    {
        "load_cert_from_file",
        (PyCFunction)PyXmlSec_KeyCertFromFile,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeyCertFromFile__doc__
    },
    {
        "__copy__",
        (PyCFunction)PyXmlSec_Key__copy__,
        METH_NOARGS,
        "",
    },
    {
        "__deepcopy__",
        (PyCFunction)PyXmlSec_Key__copy__,
        METH_NOARGS,
        "",
    },
    {NULL, NULL} /* sentinel */
};

static PyTypeObject _PyXmlSec_KeyType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    STRINGIFY(MODULE_NAME) ".Key",              /* tp_name */
    sizeof(PyXmlSec_Key),                       /* tp_basicsize */
    0,                                          /* tp_itemsize */
    PyXmlSec_Key__del__,                        /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash  */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,     /* tp_flags */
    "Key",                                      /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    PyXmlSec_KeyMethods,                        /* tp_methods */
    0,                                          /* tp_members */
    PyXmlSec_KeyGetSet,                         /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                                          /* tp_init */
    0,                                          /* tp_alloc */
    PyXmlSec_Key__new__,                        /* tp_new */
    0,                                          /* tp_free */
};

PyTypeObject* PyXmlSec_KeyType = &_PyXmlSec_KeyType;

// creates a new key object
PyXmlSec_Key* PyXmlSec_NewKey(void) {
    return PyXmlSec_NewKey1(PyXmlSec_KeyType);
}

/// key manager class

static PyObject* PyXmlSec_KeysManager__new__(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyXmlSec_KeysManager* mgr = (PyXmlSec_KeysManager*)PyType_GenericNew(type, args, kwargs);
    PYXMLSEC_DEBUGF("%p: new manager", mgr);
    if (mgr != NULL) {
        mgr->handle = NULL;
    }
    return (PyObject*)(mgr);
}

static int PyXmlSec_KeysManager__init__(PyObject* self, PyObject* args, PyObject* kwargs) {
    xmlSecKeysMngrPtr handle = xmlSecKeysMngrCreate();

    PYXMLSEC_DEBUGF("%p: init key manager", self);
    if (handle == NULL) {
        PyXmlSec_SetLastError("failed to create xmlsecKeyManager");
        return -1;
    }
    if (xmlSecCryptoAppDefaultKeysMngrInit(handle) < 0) {
        xmlSecKeysMngrDestroy(handle);
        PyXmlSec_SetLastError("failed to initialize xmlsecKeyManager");
        return -1;
    }
    PYXMLSEC_DEBUGF("%p: init key manager - done: %p", self, handle);
    ((PyXmlSec_KeysManager*)self)->handle = handle;
    return 0;
}

static void PyXmlSec_KeysManager__del__(PyObject* self) {
    PyXmlSec_KeysManager* mgr = (PyXmlSec_KeysManager*)self;

    PYXMLSEC_DEBUGF("%p: delete KeysManager", self);

    if (mgr->handle != NULL) {
        PYXMLSEC_DEBUGF("%p: delete KeysManager handle - %p", self, mgr->handle);
        xmlSecKeysMngrDestroy(mgr->handle);
    }
    Py_TYPE(self)->tp_free(self);
}

static const char PyXmlSec_KeysManagerAddKey__doc__[] = \
    "add_key(key: xmlsec.Key) -> None\n"
    "Adds a copy of ``key`` to keys manager\n\n"
    ":param key: the pointer to key\n"
    ":type key: :class:`~xmlsec.Key`";
static PyObject* PyXmlSec_KeysManagerAddKey(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "key", NULL};

    PyXmlSec_KeysManager* mgr = (PyXmlSec_KeysManager*)self;
    PyXmlSec_Key* key = NULL;
    xmlSecKeyPtr key2;
    int rv;

    PYXMLSEC_DEBUGF("%p(%p): add key - start", self, ((PyXmlSec_KeysManager*)self)->handle);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!:add_key", kwlist, PyXmlSec_KeyType, &key)) {
        goto ON_FAIL;
    }

    if (key->handle == NULL) {
        PyErr_SetString(PyExc_ValueError, "the provided key is invalid");
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS
    key2 = xmlSecKeyDuplicate(key->handle);
    Py_END_ALLOW_THREADS;

    if (key2 == NULL) {
        PyXmlSec_SetLastError("cannot make copy of key");
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecCryptoAppDefaultKeysMngrAdoptKey(mgr->handle, key2);
    Py_END_ALLOW_THREADS;
    if (rv < 0) {
        PyXmlSec_SetLastError("cannot add key");
        xmlSecKeyDestroy(key2);
        goto ON_FAIL;
    }
    PYXMLSEC_DEBUGF("%p: add key - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: add key - fail", self);
    return NULL;
}

static const char PyXmlSec_KeysManagerLoadCert__doc__[] = \
    "load_cert(filename, format, type) -> None\n"
    "Loads certificate from ``filename``.\n\n"
    ":param filename: the certificate file\n"
    ":type filename: :class:`str`, :class:`bytes` or any :class:`~os.PathLike`\n"
    ":param format: the certificate file format\n"
    ":type format: :class:`int`\n"
    ":param type: the flag that indicates is the certificate in filename trusted or not\n"
    ":type type: :class:`int`";
static PyObject* PyXmlSec_KeysManagerLoadCert(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "filename", "format", "type", NULL};

    PyXmlSec_KeysManager* mgr = (PyXmlSec_KeysManager*)self;
    PyObject* filepath = NULL;
    unsigned int format = 0;
    unsigned int type = 0;

    const char* filename;
    int rv;

    PYXMLSEC_DEBUGF("%p: load cert - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&II:load_cert", kwlist,
        PyUnicode_FSConverter, &filepath, &format, &type)) {
        goto ON_FAIL;
    }

    filename = PyBytes_AsString(filepath);

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecCryptoAppKeysMngrCertLoad(mgr->handle, filename, format, type);
    Py_END_ALLOW_THREADS;
    if (rv < 0) {
        PyXmlSec_SetLastError("cannot load cert");
        goto ON_FAIL;
    }
    Py_DECREF(filepath);
    PYXMLSEC_DEBUGF("%p: load cert - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: load cert - fail", self);
    Py_XDECREF(filepath);
    return NULL;
}

static const char PyXmlSec_KeysManagerLoadCertFromMemory__doc__[] = \
    "load_cert_from_memory(data, format, type) -> None\n"
    "Loads certificate from ``data``\n\n"
    ":param data: the certificate binary data\n"
    ":type data: :class:`str` or :class:`bytes`\n"
    ":param format: the certificate file format\n"
    ":type format: :class:`int`\n"
    ":param type: the flag that indicates is the certificate in filename trusted or not\n"
    ":type type: :class:`int`";
static PyObject* PyXmlSec_KeysManagerLoadCertFromMemory(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "data", "format", "type", NULL};

    PyXmlSec_KeysManager* mgr = (PyXmlSec_KeysManager*)self;

    const char* data = NULL;
    unsigned int type = 0;
    unsigned int format = 0;
    Py_ssize_t data_size = 0;
    int rv;

    PYXMLSEC_DEBUGF("%p: load cert from memory - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#II:load_cert", kwlist, &data, &data_size, &format, &type)) {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecCryptoAppKeysMngrCertLoadMemory(mgr->handle, (const xmlSecByte*)data, (xmlSecSize)data_size, format, type);
    Py_END_ALLOW_THREADS;
    if (rv < 0) {
        PyXmlSec_SetLastError("cannot load cert from memory");
        goto ON_FAIL;
    }
    PYXMLSEC_DEBUGF("%p: load cert from memory - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: load cert from memory - fail", self);
    return NULL;
}

static PyMethodDef PyXmlSec_KeysManagerMethods[] = {
    {
        "add_key",
        (PyCFunction)PyXmlSec_KeysManagerAddKey,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeysManagerAddKey__doc__
    },
    {
        "load_cert",
        (PyCFunction)PyXmlSec_KeysManagerLoadCert,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeysManagerLoadCert__doc__
    },
    {
        "load_cert_from_memory",
        (PyCFunction)PyXmlSec_KeysManagerLoadCertFromMemory,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_KeysManagerLoadCertFromMemory__doc__
    },
    {NULL, NULL} /* sentinel */
};

static PyTypeObject _PyXmlSec_KeysManagerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    STRINGIFY(MODULE_NAME) ".KeysManager",      /* tp_name */
    sizeof(PyXmlSec_KeysManager),               /* tp_basicsize */
    0,                                          /* tp_itemsize */
    PyXmlSec_KeysManager__del__,                /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash  */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,     /* tp_flags */
    "Keys Manager",                             /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    PyXmlSec_KeysManagerMethods,                /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    PyXmlSec_KeysManager__init__,               /* tp_init */
    0,                                          /* tp_alloc */
    PyXmlSec_KeysManager__new__,                /* tp_new */
    0,                                          /* tp_free */
};

PyTypeObject* PyXmlSec_KeysManagerType = &_PyXmlSec_KeysManagerType;


int PyXmlSec_KeysManagerConvert(PyObject* o, PyXmlSec_KeysManager** p) {
    if (o == Py_None) {
        *p = NULL;
        return 1;
    }
    if (!PyObject_IsInstance(o, (PyObject*)PyXmlSec_KeysManagerType)) {
        PyErr_SetString(PyExc_TypeError, "KeysManager required");
        return 0;
    }
    *p = (PyXmlSec_KeysManager*)(o);
    Py_INCREF(o);
    return 1;
}

int PyXmlSec_KeyModule_Init(PyObject* package) {
    if (PyType_Ready(PyXmlSec_KeyType) < 0) goto ON_FAIL;
    if (PyType_Ready(PyXmlSec_KeysManagerType) < 0) goto ON_FAIL;

    // since objects is created as static objects, need to increase refcount to prevent deallocate
    Py_INCREF(PyXmlSec_KeyType);
    Py_INCREF(PyXmlSec_KeysManagerType);

    if (PyModule_AddObject(package, "Key", (PyObject*)PyXmlSec_KeyType) < 0) goto ON_FAIL;
    if (PyModule_AddObject(package, "KeysManager", (PyObject*)PyXmlSec_KeysManagerType) < 0) goto ON_FAIL;

    return 0;
ON_FAIL:
    return -1;
}
