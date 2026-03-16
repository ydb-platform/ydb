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
#include "constants.h"
#include "keys.h"
#include "lxml.h"

#include <xmlsec/xmldsig.h>

typedef struct {
    PyObject_HEAD
    xmlSecDSigCtxPtr handle;
    PyXmlSec_KeysManager* manager;
} PyXmlSec_SignatureContext;

static PyObject* PyXmlSec_SignatureContext__new__(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)PyType_GenericNew(type, args, kwargs);
    PYXMLSEC_DEBUGF("%p: new sign context", ctx);
    if (ctx != NULL) {
        ctx->handle = NULL;
        ctx->manager = NULL;
    }
    return (PyObject*)(ctx);
}

static int PyXmlSec_SignatureContext__init__(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "manager", NULL};
    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyXmlSec_KeysManager* manager = NULL;

    PYXMLSEC_DEBUGF("%p: init sign context", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O&:__init__", kwlist, PyXmlSec_KeysManagerConvert, &manager)) {
        goto ON_FAIL;
    }
    ctx->handle = xmlSecDSigCtxCreate(manager != NULL ? manager->handle : NULL);
    if (ctx->handle == NULL) {
        PyXmlSec_SetLastError("failed to create the digital signature context");
        goto ON_FAIL;
    }
    ctx->manager = manager;
    PYXMLSEC_DEBUGF("%p: signMethod: %p", self, ctx->handle->signMethod);
    PYXMLSEC_DEBUGF("%p: init sign context - ok, manager - %p", self, manager);
    return 0;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: init sign context - failed", self);
    Py_XDECREF(manager);
    return -1;
}

static void PyXmlSec_SignatureContext__del__(PyObject* self) {
    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PYXMLSEC_DEBUGF("%p: delete sign context", self);
    if (ctx->handle != NULL) {
        xmlSecDSigCtxDestroy(ctx->handle);
    }
    // release manager object
    Py_XDECREF(ctx->manager);
    Py_TYPE(self)->tp_free(self);
}

static const char PyXmlSec_SignatureContextKey__doc__[] = "Signature key.\n";
static PyObject* PyXmlSec_SignatureContextKeyGet(PyObject* self, void* closure) {
    PyXmlSec_SignatureContext* ctx = ((PyXmlSec_SignatureContext*)self);
    PyXmlSec_Key* key;

    if (ctx->handle->signKey == NULL) {
        Py_RETURN_NONE;
    }

    key = PyXmlSec_NewKey();
    key->handle = ctx->handle->signKey;
    key->is_own = 0;
    return (PyObject*)key;
}

static int PyXmlSec_SignatureContextKeySet(PyObject* self, PyObject* value, void* closure) {
    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyXmlSec_Key* key;

    PYXMLSEC_DEBUGF("%p, %p", self, value);

    if (value == NULL) {  // key deletion
        if (ctx->handle->signKey != NULL) {
            xmlSecKeyDestroy(ctx->handle->signKey);
            ctx->handle->signKey = NULL;
        }
        return 0;
    }

    if (!PyObject_IsInstance(value, (PyObject*)PyXmlSec_KeyType)) {
        PyErr_SetString(PyExc_TypeError, "instance of *xmlsec.Key* expected.");
        return -1;
    }
    key = (PyXmlSec_Key*)value;

    if (key->handle == NULL) {
        PyErr_SetString(PyExc_TypeError, "empty key.");
        return -1;
    }

    if (ctx->handle->signKey != NULL) {
        xmlSecKeyDestroy(ctx->handle->signKey);
    }

    ctx->handle->signKey = xmlSecKeyDuplicate(key->handle);
    if (ctx->handle->signKey == NULL) {
        PyXmlSec_SetLastError("failed to duplicate key");
        return -1;
    }
    return 0;
}

static const char PyXmlSec_SignatureContextRegisterId__doc__[] = \
    "register_id(node, id_attr = 'ID', id_ns = None) -> None\n"
    "Registers new id.\n\n"
    ":param node: the pointer to XML node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param id_attr: the attribute\n"
    ":type id_attr: :class:`str`\n"
    ":param id_ns: the namespace (optional)\n"
    ":type id_ns: :class:`str` or :data:`None`";
static PyObject* PyXmlSec_SignatureContextRegisterId(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "node", "id_attr", "id_ns", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    const char* id_attr = "ID";
    const char* id_ns = NULL;

    xmlChar* name = NULL;
    xmlAttrPtr attr;
    xmlAttrPtr tmpAttr;

    PYXMLSEC_DEBUGF("%p: register id - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&|sz:register_id", kwlist,
        PyXmlSec_LxmlElementConverter, &node, &id_attr, &id_ns))
    {
        goto ON_FAIL;
    }

    if (id_ns != NULL) {
        attr = xmlHasNsProp(node->_c_node, XSTR(id_attr), XSTR(id_ns));
    } else {
        attr = xmlHasProp(node->_c_node, XSTR(id_attr));
    }

    if (attr == NULL || attr->children == NULL) {
        PyErr_SetString(PyXmlSec_Error, "missing attribute.");
        goto ON_FAIL;
    }

    name = xmlNodeListGetString(node->_c_node->doc, attr->children, 1);
    tmpAttr = xmlGetID(node->_c_node->doc, name);
    if (tmpAttr != attr) {
        if (tmpAttr != NULL) {
            PyErr_SetString(PyXmlSec_Error, "duplicated id.");
            goto ON_FAIL;
        }

        Py_BEGIN_ALLOW_THREADS;
        xmlAddID(NULL, node->_c_node->doc, name, attr);
        Py_END_ALLOW_THREADS;
    }

    xmlFree(name);
    PYXMLSEC_DEBUGF("%p: register id - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    xmlFree(name);
    PYXMLSEC_DEBUGF("%p: register id - fail", self);
    return NULL;
}

static const char PyXmlSec_SignatureContextSign__doc__[] = \
    "sign(node) -> None\n"
    "Signs according to the signature template.\n\n"
    ":param node: the pointer to :xml:`<dsig:Signature/>` node with signature template\n"
    ":type node: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_SignatureContextSign(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyXmlSec_LxmlElementPtr node = NULL;
    int rv;

    PYXMLSEC_DEBUGF("%p: sign - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:sign", kwlist, PyXmlSec_LxmlElementConverter, &node)) {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecDSigCtxSign(ctx->handle, node->_c_node);
    PYXMLSEC_DUMP(xmlSecDSigCtxDebugDump, ctx->handle);
    Py_END_ALLOW_THREADS;
    if (rv < 0) {
        PyXmlSec_SetLastError("failed to sign");
        goto ON_FAIL;
    }
    PYXMLSEC_DEBUGF("%p: sign - ok", self);
    Py_RETURN_NONE;

ON_FAIL:
    PYXMLSEC_DEBUGF("%p: sign - fail", self);
    return NULL;
}

static const char PyXmlSec_SignatureContextVerify__doc__[] = \
    "verify(node) -> None\n"
    "Verifies according to the signature template.\n\n"
    ":param node: the pointer with :xml:`<dsig:Signature/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: :data:`None` on success\n"
    ":raise VerificationError: on failure\n";
static PyObject* PyXmlSec_SignatureContextVerify(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyXmlSec_LxmlElementPtr node = NULL;
    int rv;

    PYXMLSEC_DEBUGF("%p: verify - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:verify", kwlist, PyXmlSec_LxmlElementConverter, &node)) {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecDSigCtxVerify(ctx->handle, node->_c_node);
    PYXMLSEC_DUMP(xmlSecDSigCtxDebugDump, ctx->handle);
    Py_END_ALLOW_THREADS;

    if (rv < 0) {
        PyXmlSec_SetLastError("failed to verify");
        goto ON_FAIL;
    }
    if (ctx->handle->status != xmlSecDSigStatusSucceeded) {
        PyErr_SetString(PyXmlSec_VerificationError, "Signature is invalid.");
        goto ON_FAIL;
    }
    PYXMLSEC_DEBUGF("%p: verify - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: verify - fail", self);
    return NULL;
}

// common helper for operations binary_verify and binary_sign
static int PyXmlSec_ProcessSignBinary(PyXmlSec_SignatureContext* ctx, const xmlSecByte* data, xmlSecSize data_size, xmlSecTransformId method) {
    int rv;

    if (!(method->usage & xmlSecTransformUsageSignatureMethod)) {
        PyErr_SetString(PyXmlSec_Error, "incompatible signature method");
        return -1;
    }

    if (ctx->handle->signKey == NULL) {
        PyErr_SetString(PyXmlSec_Error, "Sign key is not specified.");
        return -1;
    }

    if (ctx->handle->signMethod != NULL) {
        PYXMLSEC_DEBUGF("%p: signMethod: %p", ctx, ctx->handle->signMethod);
        PyErr_SetString(PyXmlSec_Error, "Signature context already used; it is designed for one use only.");
        return -1;
    }

    ctx->handle->signMethod = xmlSecTransformCtxCreateAndAppend(&(ctx->handle->transformCtx), method);
    if (ctx->handle->signMethod == NULL) {
        PyXmlSec_SetLastError("could not create signature transform.");
        return -1;
    }

    ctx->handle->signMethod->operation = ctx->handle->operation;
    xmlSecTransformSetKeyReq(ctx->handle->signMethod, &(ctx->handle->keyInfoReadCtx.keyReq));
    rv = xmlSecKeyMatch(ctx->handle->signKey, NULL, &(ctx->handle->keyInfoReadCtx.keyReq));
    if (rv != 1) {
        PyXmlSec_SetLastError("inappropriate key type.");
        return -1;
    }

    rv = xmlSecTransformSetKey(ctx->handle->signMethod, ctx->handle->signKey);
    if (rv < 0) {
        PyXmlSec_SetLastError("cannot set key.");
        return -1;
    }
    ctx->handle->transformCtx.result = NULL;
    ctx->handle->transformCtx.status = xmlSecTransformStatusNone;

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecTransformCtxBinaryExecute(&(ctx->handle->transformCtx), data, data_size);
    Py_END_ALLOW_THREADS;

    if (rv < 0) {
        PyXmlSec_SetLastError("failed to transform.");
        return -1;
    }
    ctx->handle->result = ctx->handle->transformCtx.result;

    return 0;
}

static const char PyXmlSec_SignatureContextSignBinary__doc__[] = \
    "sign_binary(bytes, transform) -> bytes\n"
    "Signs binary data ``data`` with algorithm ``transform``.\n\n"
    ":param bytes: the binary data\n"
    ":type bytes: :class:`bytes`\n"
    ":param transform: the signature algorithm\n"
    ":type transform: :class:`__Transform`\n"
    ":return: the signature\n"
    ":rtype: :class:`bytes`";
static PyObject* PyXmlSec_SignatureContextSignBinary(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "bytes", "transform", NULL};
    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyXmlSec_Transform* transform = NULL;
    const char* data = NULL;
    Py_ssize_t data_size = 0;

    PYXMLSEC_DEBUGF("%p: sign_binary - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#O!:sign_binary", kwlist,
        &data, &data_size, PyXmlSec_TransformType, &transform))
    {
        goto ON_FAIL;
    }

    ctx->handle->operation = xmlSecTransformOperationSign;

    if (PyXmlSec_ProcessSignBinary(ctx, (const xmlSecByte*)data, (xmlSecSize)data_size, transform->id) != 0) {
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUGF("%p: sign_binary - ok", self);
    return PyBytes_FromStringAndSize(
        (const char*)xmlSecBufferGetData(ctx->handle->result),
        (Py_ssize_t)xmlSecBufferGetSize(ctx->handle->result)
    );
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: sign_binary - fail", self);
    return NULL;
}

static const char PyXmlSec_SignatureContextVerifyBinary__doc__[] = \
    "verify_binary(bytes, transform, signature) -> None\n"
    "Verifies signature for binary data.\n\n"
    ":param bytes: the binary data\n"
    ":type bytes: :class:`bytes`\n"
    ":param transform: the signature algorithm\n"
    ":type transform: :class:`__Transform`\n"
    ":param signature: the signature\n"
    ":type signature: :class:`bytes`\n"
    ":return: :data:`None` on success\n"
    ":raise VerificationError: on failure";
static PyObject* PyXmlSec_SignatureContextVerifyBinary(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "bytes", "transform", "signature", NULL};

    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyXmlSec_Transform* transform = NULL;
    const char* data = NULL;
    Py_ssize_t data_size = 0;
    const char* sign = NULL;
    Py_ssize_t sign_size = 0;
    int rv;

    PYXMLSEC_DEBUGF("%p: verify binary - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#O!s#:verify_binary", kwlist,
        &data, &data_size, PyXmlSec_TransformType, &transform, &sign, &sign_size))
    {
        goto ON_FAIL;
    }

    ctx->handle->operation = xmlSecTransformOperationVerify;
    if (PyXmlSec_ProcessSignBinary(ctx, (const xmlSecByte*)data, (xmlSecSize)data_size, transform->id) != 0) {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecTransformVerify(ctx->handle->signMethod, (const xmlSecByte*)sign, (xmlSecSize)sign_size, &(ctx->handle->transformCtx));
    Py_END_ALLOW_THREADS;

    if (rv < 0) {
        PyXmlSec_SetLastError2(PyXmlSec_VerificationError, "Cannot verify signature.");
        goto ON_FAIL;
    }

    if (ctx->handle->signMethod->status != xmlSecTransformStatusOk) {
        PyXmlSec_SetLastError2(PyXmlSec_VerificationError, "Signature is invalid.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUGF("%p: verify binary - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: verify binary - fail", self);
    return NULL;
}

static const char PyXmlSec_SignatureContextEnableReferenceTransform__doc__[] = \
    "enable_reference_transform(transform) -> None\n"
    "Enables use of ``transform`` as reference transform.\n\n"
    ".. note:: by default, all transforms are enabled. The first call of "
    ":meth:`~SignatureContext.enable_reference_transform` will switch to explicitly enabled transforms.\n\n"
    ":param transform: the transform klass.\n"
    ":type transform: :class:`__Transform`";
static PyObject* PyXmlSec_SignatureContextEnableReferenceTransform(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "transform", NULL};

    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyXmlSec_Transform* transform = NULL;
    int rv;

    PYXMLSEC_DEBUGF("%p: enable_reference_transform - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!:enable_reference_transform", kwlist, PyXmlSec_TransformType, &transform))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecDSigCtxEnableReferenceTransform(ctx->handle, transform->id);
    Py_END_ALLOW_THREADS;

    if (rv < 0) {
        PyXmlSec_SetLastError("cannot enable reference transform.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUGF("%p: enable_reference_transform - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: enable_reference_transform - fail", self);
    return NULL;
}

static const char PyXmlSec_SignatureContextEnableSignatureTransform__doc__[] = \
    "enable_signature_transform(transform) -> None\n"
    "Enables use of ``transform`` as signature transform.\n\n"
    ".. note:: by default, all transforms are enabled. The first call of "
    ":meth:`~SignatureContext.enable_signature_transform` will switch to explicitly enabled transforms.\n\n"
    ":param transform: the transform klass.\n"
    ":type transform: :class:`__Transform`\n";
static PyObject* PyXmlSec_SignatureContextEnableSignatureTransform(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "transform", NULL};

    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyXmlSec_Transform* transform = NULL;
    int rv;

    PYXMLSEC_DEBUGF("%p: enable_signature_transform - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!:enable_signature_transform", kwlist, PyXmlSec_TransformType, &transform)) {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecDSigCtxEnableSignatureTransform(ctx->handle, transform->id);
    Py_END_ALLOW_THREADS;

    if (rv < 0) {
        PyXmlSec_SetLastError("cannot enable signature transform.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUGF("%p: enable_signature_transform - ok", self);
    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: enable_signature_transform - fail", self);
    return NULL;
}

static const char PyXmlSec_SignatureContextSetEnabledKeyData__doc__[] = \
    "set_enabled_key_data(keydata_list) -> None\n"
    "Adds selected :class:`__KeyData` to the list of enabled key data list.\n\n"
    ":param keydata_list: the list\n"
    ":type keydata_list: :class:`list` of :class:`__KeyData`";
static PyObject* PyXmlSec_SignatureContextSetEnabledKeyData(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "keydata_list", NULL};

    PyXmlSec_SignatureContext* ctx = (PyXmlSec_SignatureContext*)self;
    PyObject* keydata_list = NULL;
    PyObject* iter = NULL;
    PyObject* item = NULL;
    xmlSecPtrListPtr enabled_list;

    PYXMLSEC_DEBUGF("%p: set_enabled_key_data - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O:set_enabled_key_data", kwlist, &keydata_list)) {
        goto ON_FAIL;
    }
    if ((iter = PyObject_GetIter(keydata_list)) == NULL) goto ON_FAIL;

    enabled_list = &(ctx->handle->keyInfoReadCtx.enabledKeyData);
    xmlSecPtrListEmpty(enabled_list);

    while ((item = PyIter_Next(iter)) != NULL) {
        if (!PyObject_IsInstance(item, (PyObject*)PyXmlSec_KeyDataType)) {
            PyErr_SetString(PyExc_TypeError, "expected list of KeyData constants.");
            goto ON_FAIL;
        }
        if (xmlSecPtrListAdd(enabled_list, (xmlSecPtr)((PyXmlSec_KeyData*)item)->id) < 0) {
            PyXmlSec_SetLastError("cannot set enabled key.");
            goto ON_FAIL;
        }
        Py_DECREF(item);
    }
    Py_DECREF(iter);

    PYXMLSEC_DEBUGF("%p: set_enabled_key_data - ok", self);

    Py_RETURN_NONE;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: set_enabled_key_data - fail", self);
    Py_XDECREF(item);
    Py_XDECREF(iter);
    return NULL;
}

static PyGetSetDef PyXmlSec_SignatureContextGetSet[] = {
    {
        "key",
        (getter)PyXmlSec_SignatureContextKeyGet,
        (setter)PyXmlSec_SignatureContextKeySet,
        (char*)PyXmlSec_SignatureContextKey__doc__,
        NULL
    },
    {NULL} /* Sentinel */
};

static PyMethodDef PyXmlSec_SignatureContextMethods[] = {
    {
        "register_id",
        (PyCFunction)PyXmlSec_SignatureContextRegisterId,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_SignatureContextRegisterId__doc__,
    },
    {
        "sign",
        (PyCFunction)PyXmlSec_SignatureContextSign,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_SignatureContextSign__doc__
    },
    {
        "verify",
        (PyCFunction)PyXmlSec_SignatureContextVerify,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_SignatureContextVerify__doc__
    },
    {
        "sign_binary",
        (PyCFunction)PyXmlSec_SignatureContextSignBinary,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_SignatureContextSignBinary__doc__
    },
    {
        "verify_binary",
        (PyCFunction)PyXmlSec_SignatureContextVerifyBinary,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_SignatureContextVerifyBinary__doc__
    },
    {
        "enable_reference_transform",
        (PyCFunction)PyXmlSec_SignatureContextEnableReferenceTransform,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_SignatureContextEnableReferenceTransform__doc__
    },
    {
        "enable_signature_transform",
        (PyCFunction)PyXmlSec_SignatureContextEnableSignatureTransform,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_SignatureContextEnableSignatureTransform__doc__,
    },
    {
        "set_enabled_key_data",
        (PyCFunction)PyXmlSec_SignatureContextSetEnabledKeyData,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_SignatureContextSetEnabledKeyData__doc__,
    },
    {NULL, NULL} /* sentinel */
};

static PyTypeObject _PyXmlSec_SignatureContextType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    STRINGIFY(MODULE_NAME) ".SignatureContext", /* tp_name */
    sizeof(PyXmlSec_SignatureContext),          /* tp_basicsize */
    0,                                          /* tp_itemsize */
    PyXmlSec_SignatureContext__del__,           /* tp_dealloc */
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
    "XML Digital Signature implementation",     /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    PyXmlSec_SignatureContextMethods,           /* tp_methods */
    0,                                          /* tp_members */
    PyXmlSec_SignatureContextGetSet,            /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    PyXmlSec_SignatureContext__init__,          /* tp_init */
    0,                                          /* tp_alloc */
    PyXmlSec_SignatureContext__new__,           /* tp_new */
    0,                                          /* tp_free */
};

PyTypeObject* PyXmlSec_SignatureContextType = &_PyXmlSec_SignatureContextType;

int PyXmlSec_DSModule_Init(PyObject* package) {
    if (PyType_Ready(PyXmlSec_SignatureContextType) < 0) goto ON_FAIL;

    // since objects is created as static objects, need to increase refcount to prevent deallocate
    Py_INCREF(PyXmlSec_SignatureContextType);

    if (PyModule_AddObject(package, "SignatureContext", (PyObject*)PyXmlSec_SignatureContextType) < 0) goto ON_FAIL;
    return 0;
ON_FAIL:
    return -1;
}
