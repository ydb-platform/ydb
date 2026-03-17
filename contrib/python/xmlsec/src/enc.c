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

#include <xmlsec/xmlenc.h>
#include <xmlsec/xmltree.h>

// Backwards compatibility with xmlsec 1.2
#ifndef XMLSEC_KEYINFO_FLAGS_LAX_KEY_SEARCH
#define XMLSEC_KEYINFO_FLAGS_LAX_KEY_SEARCH 0x00008000
#endif

typedef struct {
    PyObject_HEAD
    xmlSecEncCtxPtr handle;
    PyXmlSec_KeysManager* manager;
} PyXmlSec_EncryptionContext;

static PyObject* PyXmlSec_EncryptionContext__new__(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)PyType_GenericNew(type, args, kwargs);
    PYXMLSEC_DEBUGF("%p: new enc context", ctx);
    if (ctx != NULL) {
        ctx->handle = NULL;
        ctx->manager = NULL;
    }
    return (PyObject*)(ctx);
}

static int PyXmlSec_EncryptionContext__init__(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "manager", NULL};

    PyXmlSec_KeysManager* manager = NULL;
    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)self;

    PYXMLSEC_DEBUGF("%p: init enc context", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O&:__init__", kwlist, PyXmlSec_KeysManagerConvert, &manager)) {
        goto ON_FAIL;
    }
    ctx->handle = xmlSecEncCtxCreate(manager != NULL ? manager->handle : NULL);
    if (ctx->handle == NULL) {
        PyXmlSec_SetLastError("failed to create the encryption context");
        goto ON_FAIL;
    }
    ctx->manager = manager;
    PYXMLSEC_DEBUGF("%p: init enc context - ok, manager - %p", self, manager);

    // xmlsec 1.3 changed the key search to strict mode, causing various examples
    // in the docs to fail. For backwards compatibility, this changes it back to
    // lax mode for now.
    ctx->handle->keyInfoReadCtx.flags = XMLSEC_KEYINFO_FLAGS_LAX_KEY_SEARCH;
    ctx->handle->keyInfoWriteCtx.flags = XMLSEC_KEYINFO_FLAGS_LAX_KEY_SEARCH;

    return 0;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: init enc context - failed", self);
    Py_XDECREF(manager);
    return -1;
}

static void PyXmlSec_EncryptionContext__del__(PyObject* self) {
    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)self;

    PYXMLSEC_DEBUGF("%p: delete enc context", self);

    if (ctx->handle != NULL) {
        xmlSecEncCtxDestroy(ctx->handle);
    }
    // release manager object
    Py_XDECREF(ctx->manager);
    Py_TYPE(self)->tp_free(self);
}

static const char PyXmlSec_EncryptionContextKey__doc__[] = "Encryption key.\n";
static PyObject* PyXmlSec_EncryptionContextKeyGet(PyObject* self, void* closure) {
    PyXmlSec_EncryptionContext* ctx = ((PyXmlSec_EncryptionContext*)self);
    PyXmlSec_Key* key;

    if (ctx->handle->encKey == NULL) {
        Py_RETURN_NONE;
    }

    key = PyXmlSec_NewKey();
    key->handle = ctx->handle->encKey;
    key->is_own = 0;
    return (PyObject*)key;
}

static int PyXmlSec_EncryptionContextKeySet(PyObject* self, PyObject* value, void* closure) {
    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)self;
    PyXmlSec_Key* key;

    PYXMLSEC_DEBUGF("%p, %p", self, value);

    if (value == NULL) {  // key deletion
        if (ctx->handle->encKey != NULL) {
            xmlSecKeyDestroy(ctx->handle->encKey);
            ctx->handle->encKey = NULL;
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

    if (ctx->handle->encKey != NULL) {
        xmlSecKeyDestroy(ctx->handle->encKey);
    }

    ctx->handle->encKey = xmlSecKeyDuplicate(key->handle);
    if (ctx->handle->encKey == NULL) {
        PyXmlSec_SetLastError("failed to duplicate key");
        return -1;
    }
    return 0;
}

static const char PyXmlSec_EncryptionContextReset__doc__[] = \
    "reset() -> None\n"\
    "Reset this context, user settings are not touched.\n";
static PyObject* PyXmlSec_EncryptionContextReset(PyObject* self, PyObject* args, PyObject* kwargs) {
    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)self;

    PYXMLSEC_DEBUGF("%p: reset context - start", self);
    Py_BEGIN_ALLOW_THREADS;
    xmlSecEncCtxReset(ctx->handle);
    PYXMLSEC_DUMP(xmlSecEncCtxDebugDump, ctx->handle);
    Py_END_ALLOW_THREADS;
    PYXMLSEC_DEBUGF("%p: reset context - ok", self);
    Py_RETURN_NONE;
}

static const char PyXmlSec_EncryptionContextEncryptBinary__doc__[] = \
    "encrypt_binary(template, data) -> lxml.etree._Element\n"
    "Encrypts binary ``data`` according to ``EncryptedData`` template ``template``.\n\n"
    ".. note:: ``template`` is modified in place.\n\n"
    ":param template: the pointer to :xml:`<enc:EncryptedData/>` template node\n"
    ":type template: :class:`lxml.etree._Element`\n"
    ":param data: the data\n"
    ":type data: :class:`bytes`\n"
    ":return: the resulting :xml:`<enc:EncryptedData/>` subtree\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_EncryptionContextEncryptBinary(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "template", "data", NULL};

    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)self;
    PyXmlSec_LxmlElementPtr template = NULL;
    const char* data = NULL;
    Py_ssize_t data_size = 0;
    int rv;

    PYXMLSEC_DEBUGF("%p: encrypt_binary - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&s#:encrypt_binary", kwlist,
        PyXmlSec_LxmlElementConverter, &template, &data, &data_size))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecEncCtxBinaryEncrypt(ctx->handle, template->_c_node, (const xmlSecByte*)data, (xmlSecSize)data_size);
    PYXMLSEC_DUMP(xmlSecEncCtxDebugDump, ctx->handle);
    Py_END_ALLOW_THREADS;

    if (rv < 0) {
        PyXmlSec_SetLastError("failed to encrypt binary");
        goto ON_FAIL;
    }
    Py_INCREF(template);
    PYXMLSEC_DEBUGF("%p: encrypt_binary - ok", self);

    return (PyObject*)template;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: encrypt_binary - fail", self);
    return NULL;
}

// release the replaced nodes in a way safe for `lxml`
static void PyXmlSec_ClearReplacedNodes(xmlSecEncCtxPtr ctx, PyXmlSec_LxmlDocumentPtr doc) {
    PyXmlSec_LxmlElementPtr elem;
    // release the replaced nodes in a way safe for `lxml`
    xmlNodePtr n = ctx->replacedNodeList;
    xmlNodePtr nn;

    while (n != NULL) {
        PYXMLSEC_DEBUGF("clear replaced node %p", n);
        nn = n->next;
        // if n has references, it will not be deleted
        elem = (PyXmlSec_LxmlElementPtr)PyXmlSec_elementFactory(doc, n);
        if (NULL == elem)
            xmlFreeNode(n);
        else
            Py_DECREF(elem);
        n = nn;
    }
    ctx->replacedNodeList = NULL;
}

static const char PyXmlSec_EncryptionContextEncryptXml__doc__[] = \
    "encrypt_xml(template, node) -> lxml.etree._Element\n"
    "Encrypts ``node`` using ``template``.\n\n"
    ".. note:: The ``\"Type\"`` attribute of ``template`` decides whether ``node`` itself "
    "(``http://www.w3.org/2001/04/xmlenc#Element``) or its content (``http://www.w3.org/2001/04/xmlenc#Content``) is encrypted.\n"
    "   It must have one of these two values (or an exception is raised).\n"
    "   The operation modifies the tree and removes replaced nodes.\n\n"
    ":param template: the pointer to :xml:`<enc:EncryptedData/>` template node\n\n"
    ":type template: :class:`lxml.etree._Element`\n"
    ":param node: the pointer to node for encryption\n\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to newly created :xml:`<enc:EncryptedData/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_EncryptionContextEncryptXml(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "template", "node", NULL};

    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)self;
    PyXmlSec_LxmlElementPtr template = NULL;
    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr xnew_node = NULL;
    xmlChar* tmpType = NULL;
    int rv = 0;

    PYXMLSEC_DEBUGF("%p: encrypt_xml - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&O&:encrypt_xml", kwlist,
        PyXmlSec_LxmlElementConverter, &template, PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }
    tmpType = xmlGetProp(template->_c_node, XSTR("Type"));
    if (tmpType == NULL || !(xmlStrEqual(tmpType, xmlSecTypeEncElement) || xmlStrEqual(tmpType, xmlSecTypeEncContent))) {
        PyErr_SetString(PyXmlSec_Error, "unsupported `Type`, it should be `element` or `content`");
        goto ON_FAIL;
    }

    // `xmlSecEncCtxXmlEncrypt` will replace the subtree rooted
    //  at `node._c_node` or its children by an extended subtree rooted at "c_node".
    //  We set `XMLSEC_ENC_RETURN_REPLACED_NODE` to prevent deallocation
    //  of the replaced node. This is important as `node` is still referencing it
    ctx->handle->flags = XMLSEC_ENC_RETURN_REPLACED_NODE;

    // try to do all actions whithin single python-free section
    // rv has the following codes, 1 - failed to copy node, -1 - op failed, 0 - success
    Py_BEGIN_ALLOW_THREADS;
    if (template->_doc->_c_doc != node->_doc->_c_doc) {
        // `xmlSecEncCtxEncrypt` expects *template* to belong to the document of *node*
        // if this is not the case, we copy the `libxml2` subtree there.
        xnew_node = xmlDocCopyNode(template->_c_node, node->_doc->_c_doc, 1); // recursive
        if (xnew_node == NULL) {
            rv = 1;
        }
    }
    if (rv == 0 && xmlSecEncCtxXmlEncrypt(ctx->handle, xnew_node != NULL ? xnew_node: template->_c_node, node->_c_node) < 0) {
        rv = -1;
        if (xnew_node != NULL) {
            xmlFreeNode(xnew_node);
            xnew_node = NULL;
        }
    }
    PYXMLSEC_DUMP(xmlSecEncCtxDebugDump, ctx->handle);
    Py_END_ALLOW_THREADS;

    PyXmlSec_ClearReplacedNodes(ctx->handle, node->_doc);
    if (NULL != PyErr_Occurred()) {
        goto ON_FAIL;
    }

    if (rv != 0) {
        if (rv > 0) {
            PyErr_SetString(PyXmlSec_InternalError, "could not copy template tree");
        } else {
            PyXmlSec_SetLastError("failed to encrypt xml");
        }
        goto ON_FAIL;
    }

    xmlFree(tmpType);

    PYXMLSEC_DEBUGF("%p: encrypt_xml - ok", self);
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, xnew_node != NULL ? xnew_node : template->_c_node);
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: encrypt_xml - fail", self);
    xmlFree(tmpType);
    return NULL;
}

static const char PyXmlSec_EncryptionContextEncryptUri__doc__[] = \
    "encrypt_uri(template, uri) -> lxml.etree._Element\n"
    "Encrypts binary data obtained from ``uri`` according to ``template``.\n\n"
    ".. note:: ``template`` is modified in place.\n\n"
    ":param template: the pointer to :xml:`<enc:EncryptedData/>` template node\n"
    ":type template: :class:`lxml.etree._Element`\n"
    ":param uri: the URI\n"
    ":type uri: :class:`str`\n"
    ":return: the resulting :xml:`<enc:EncryptedData/>` subtree\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_EncryptionContextEncryptUri(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "template", "uri", NULL};

    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)self;
    PyXmlSec_LxmlElementPtr template = NULL;
    const char* uri = NULL;
    int rv;

    PYXMLSEC_DEBUGF("%p: encrypt_uri - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&s:encrypt_uri", kwlist, PyXmlSec_LxmlElementConverter, &template, &uri)) {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rv = xmlSecEncCtxUriEncrypt(ctx->handle, template->_c_node, (const xmlSecByte*)uri);
    PYXMLSEC_DUMP(xmlSecEncCtxDebugDump, ctx->handle);
    Py_END_ALLOW_THREADS;

    if (rv < 0) {
        PyXmlSec_SetLastError("failed to encrypt URI");
        goto ON_FAIL;
    }
    PYXMLSEC_DEBUGF("%p: encrypt_uri - ok", self);
    Py_INCREF(template);
    return (PyObject*)template;
ON_FAIL:
    PYXMLSEC_DEBUGF("%p: encrypt_uri - fail", self);
    return NULL;
}

static const char PyXmlSec_EncryptionContextDecrypt__doc__[] = \
    "decrypt(node)\n"
    "Decrypts ``node`` (an ``EncryptedData`` or ``EncryptedKey`` element) and returns the result. "
    "The decryption may result in binary data or an XML subtree. "
    "In the former case, the binary data is returned. In the latter case, "
    "the input tree is modified and a reference to the decrypted XML subtree is returned.\n"
    "If the operation modifies the tree, it removes replaced nodes.\n\n"
    ":param node: the pointer to :xml:`<enc:EncryptedData/>` or :xml:`<enc:EncryptedKey/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: depends on input parameters\n"
    ":rtype: :class:`lxml.etree._Element` or :class:`bytes`";
static PyObject* PyXmlSec_EncryptionContextDecrypt(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_EncryptionContext* ctx = (PyXmlSec_EncryptionContext*)self;
    PyXmlSec_LxmlElementPtr node = NULL;

    PyObject* node_num = NULL;
    PyObject* parent = NULL;

    PyObject* tmp;
    xmlNodePtr root;
    xmlNodePtr xparent;
    int rv;
    xmlChar* ttype;
    int notContent;

    PYXMLSEC_DEBUGF("%p: decrypt - start", self);
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:decrypt", kwlist, PyXmlSec_LxmlElementConverter, &node)) {
        goto ON_FAIL;
    }

    xparent = node->_c_node->parent;
    if (xparent != NULL && !PyXmlSec_IsElement(xparent)) {
        xparent = NULL;
    }

    if (xparent != NULL) {
        parent = (PyObject*)PyXmlSec_elementFactory(node->_doc, xparent);
        if (parent == NULL) {
            PyErr_SetString(PyXmlSec_InternalError, "failed to construct parent");
            goto ON_FAIL;
        }
        // get index of node
        node_num = PyObject_CallMethod(parent, "index", "O", node);
        PYXMLSEC_DEBUGF("parent: %p, %p", parent, node_num);
    }

    Py_BEGIN_ALLOW_THREADS;
    ctx->handle->flags = XMLSEC_ENC_RETURN_REPLACED_NODE;
    ctx->handle->mode = xmlSecCheckNodeName(node->_c_node, xmlSecNodeEncryptedKey, xmlSecEncNs) ? xmlEncCtxModeEncryptedKey : xmlEncCtxModeEncryptedData;
    PYXMLSEC_DEBUGF("mode: %d", ctx->handle->mode);
    rv = xmlSecEncCtxDecrypt(ctx->handle, node->_c_node);
    PYXMLSEC_DUMP(xmlSecEncCtxDebugDump, ctx->handle);
    Py_END_ALLOW_THREADS;

    PyXmlSec_ClearReplacedNodes(ctx->handle, node->_doc);

    if (rv < 0) {
        PyXmlSec_SetLastError("failed to decrypt");
        goto ON_FAIL;
    }

    if (!ctx->handle->resultReplaced) {
        Py_XDECREF(node_num);
        Py_XDECREF(parent);
        PYXMLSEC_DEBUGF("%p: binary.decrypt - ok", self);
        return PyBytes_FromStringAndSize(
            (const char*)xmlSecBufferGetData(ctx->handle->result),
            (Py_ssize_t)xmlSecBufferGetSize(ctx->handle->result)
        );
    }

    if (xparent != NULL) {
        ttype = xmlGetProp(node->_c_node, XSTR("Type"));
        notContent = (ttype == NULL || !xmlStrEqual(ttype, xmlSecTypeEncContent));
        xmlFree(ttype);

        if (notContent) {
            tmp = PyObject_GetItem(parent, node_num);
            if (tmp == NULL) goto ON_FAIL;
            Py_DECREF(parent);
            parent = tmp;
        }
        Py_DECREF(node_num);
        PYXMLSEC_DEBUGF("%p: parent.decrypt - ok", self);
        return parent;
    }

    // root has been replaced
    root = xmlDocGetRootElement(node->_doc->_c_doc);
    if (root == NULL) {
        PyErr_SetString(PyXmlSec_Error, "decryption resulted in a non well formed document");
        goto ON_FAIL;
    }

    Py_XDECREF(node_num);
    Py_XDECREF(parent);

    PYXMLSEC_DEBUGF("%p: decrypt - ok", self);
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, root);

ON_FAIL:
    PYXMLSEC_DEBUGF("%p: decrypt - fail", self);
    Py_XDECREF(node_num);
    Py_XDECREF(parent);
    return NULL;
}

static PyGetSetDef PyXmlSec_EncryptionContextGetSet[] = {
    {
        "key",
        (getter)PyXmlSec_EncryptionContextKeyGet,
        (setter)PyXmlSec_EncryptionContextKeySet,
        (char*)PyXmlSec_EncryptionContextKey__doc__,
        NULL
    },
    {NULL} /* Sentinel */
};

static PyMethodDef PyXmlSec_EncryptionContextMethods[] = {
    {
        "reset",
        (PyCFunction)PyXmlSec_EncryptionContextReset,
        METH_NOARGS,
        PyXmlSec_EncryptionContextReset__doc__,
    },
    {
        "encrypt_binary",
        (PyCFunction)PyXmlSec_EncryptionContextEncryptBinary,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_EncryptionContextEncryptBinary__doc__,
    },
    {
        "encrypt_xml",
        (PyCFunction)PyXmlSec_EncryptionContextEncryptXml,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_EncryptionContextEncryptXml__doc__
    },
    {
        "encrypt_uri",
        (PyCFunction)PyXmlSec_EncryptionContextEncryptUri,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_EncryptionContextEncryptUri__doc__
    },
    {
        "decrypt",
        (PyCFunction)PyXmlSec_EncryptionContextDecrypt,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_EncryptionContextDecrypt__doc__
    },
    {NULL, NULL} /* sentinel */
};

static PyTypeObject _PyXmlSec_EncryptionContextType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    STRINGIFY(MODULE_NAME) ".EncryptionContext", /* tp_name */
    sizeof(PyXmlSec_EncryptionContext),          /* tp_basicsize */
    0,                                           /* tp_itemsize */
    PyXmlSec_EncryptionContext__del__,           /* tp_dealloc */
    0,                                           /* tp_print */
    0,                                           /* tp_getattr */
    0,                                           /* tp_setattr */
    0,                                           /* tp_reserved */
    0,                                           /* tp_repr */
    0,                                           /* tp_as_number */
    0,                                           /* tp_as_sequence */
    0,                                           /* tp_as_mapping */
    0,                                           /* tp_hash  */
    0,                                           /* tp_call */
    0,                                           /* tp_str */
    0,                                           /* tp_getattro */
    0,                                           /* tp_setattro */
    0,                                           /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,      /* tp_flags */
    "XML Encryption implementation",             /* tp_doc */
    0,                                           /* tp_traverse */
    0,                                           /* tp_clear */
    0,                                           /* tp_richcompare */
    0,                                           /* tp_weaklistoffset */
    0,                                           /* tp_iter */
    0,                                           /* tp_iternext */
    PyXmlSec_EncryptionContextMethods,           /* tp_methods */
    0,                                           /* tp_members */
    PyXmlSec_EncryptionContextGetSet,            /* tp_getset */
    0,                                           /* tp_base */
    0,                                           /* tp_dict */
    0,                                           /* tp_descr_get */
    0,                                           /* tp_descr_set */
    0,                                           /* tp_dictoffset */
    PyXmlSec_EncryptionContext__init__,          /* tp_init */
    0,                                           /* tp_alloc */
    PyXmlSec_EncryptionContext__new__,           /* tp_new */
    0                                            /* tp_free */
};

PyTypeObject* PyXmlSec_EncryptionContextType = &_PyXmlSec_EncryptionContextType;

int PyXmlSec_EncModule_Init(PyObject* package) {
    if (PyType_Ready(PyXmlSec_EncryptionContextType) < 0) goto ON_FAIL;

    PYXMLSEC_DEBUGF("%p", PyXmlSec_EncryptionContextType);
    // since objects is created as static objects, need to increase refcount to prevent deallocate
    Py_INCREF(PyXmlSec_EncryptionContextType);

    if (PyModule_AddObject(package, "EncryptionContext", (PyObject*)PyXmlSec_EncryptionContextType) < 0) goto ON_FAIL;
    return 0;
ON_FAIL:
    return -1;
}
