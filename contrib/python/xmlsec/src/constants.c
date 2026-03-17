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

#define PYXMLSEC_CONSTANTS_DOC "Various constants used by the library.\n"

// destructor
static void PyXmlSec_Transform__del__(PyObject* self) {
    PYXMLSEC_DEBUGF("%p", self);
    Py_TYPE(self)->tp_free(self);
}

// __str__ method
static PyObject* PyXmlSec_Transform__str__(PyObject* self) {
    char buf[300];
    PyXmlSec_Transform* transform = (PyXmlSec_Transform*)(self);
    if (transform->id->href != NULL)
        snprintf(buf, sizeof(buf), "%s, %s", transform->id->name, transform->id->href);
    else
        snprintf(buf, sizeof(buf), "%s, None", transform->id->name);

    return PyUnicode_FromString(buf);
}

// __repr__ method
static PyObject* PyXmlSec_Transform__repr__(PyObject* self) {
    char buf[300];
    PyXmlSec_Transform* transform = (PyXmlSec_Transform*)(self);
    if (transform->id->href != NULL)
        snprintf(buf, sizeof(buf), "__Transform('%s', '%s', %d)", transform->id->name, transform->id->href, transform->id->usage);
    else
        snprintf(buf, sizeof(buf), "__Transform('%s', None, %d)", transform->id->name, transform->id->usage);
    return PyUnicode_FromString(buf);
}

static const char PyXmlSec_TransformNameGet__doc__[] = "The transform's name.";
static PyObject* PyXmlSec_TransformNameGet(PyXmlSec_Transform* self, void* closure) {
    return PyUnicode_FromString((const char*)self->id->name);
}

static const char PyXmlSec_TransformHrefGet__doc__[] = "The transform's identification string (href).";
static PyObject* PyXmlSec_TransformHrefGet(PyXmlSec_Transform* self, void* closure) {
    if (self->id->href != NULL)
        return PyUnicode_FromString((const char*)self->id->href);
    Py_RETURN_NONE;
}

static const char PyXmlSec_TransformUsageGet__doc__[] = "The allowed transforms usages.";
static PyObject* PyXmlSec_TransformUsageGet(PyXmlSec_Transform* self, void* closure) {
    return PyLong_FromUnsignedLong(self->id->usage);
}

static PyGetSetDef PyXmlSec_TransformGetSet[] = {
    {
        "name",
        (getter)PyXmlSec_TransformNameGet,
        NULL,
        (char*)PyXmlSec_TransformNameGet__doc__,
        NULL
    },
    {
        "href",
        (getter)PyXmlSec_TransformHrefGet,
        NULL,
        (char*)PyXmlSec_TransformHrefGet__doc__,
        NULL
    },
    {
        "usage",
        (getter)PyXmlSec_TransformUsageGet,
        NULL,
        (char*)PyXmlSec_TransformUsageGet__doc__,
        NULL
    },
    {NULL} /* Sentinel */
};

static PyTypeObject _PyXmlSec_TransformType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    STRINGIFY(MODULE_NAME) ".constants.__Transform", /* tp_name */
    sizeof(PyXmlSec_Transform),                      /* tp_basicsize */
    0,                                               /* tp_itemsize */
    PyXmlSec_Transform__del__,                       /* tp_dealloc */
    0,                                               /* tp_print */
    0,                                               /* tp_getattr */
    0,                                               /* tp_setattr */
    0,                                               /* tp_reserved */
    PyXmlSec_Transform__repr__,                      /* tp_repr */
    0,                                               /* tp_as_number */
    0,                                               /* tp_as_sequence */
    0,                                               /* tp_as_mapping */
    0,                                               /* tp_hash  */
    0,                                               /* tp_call */
    PyXmlSec_Transform__str__,                       /* tp_str */
    0,                                               /* tp_getattro */
    0,                                               /* tp_setattro */
    0,                                               /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                              /* tp_flags */
    "The xmlSecTransformId reflection",              /* tp_doc */
    0,                                               /* tp_traverse */
    0,                                               /* tp_clear */
    0,                                               /* tp_richcompare */
    0,                                               /* tp_weaklistoffset */
    0,                                               /* tp_iter */
    0,                                               /* tp_iternext */
    0,                                               /* tp_methods */
    0,                                               /* tp_members */
    PyXmlSec_TransformGetSet,                        /* tp_getset */
    0,                                               /* tp_base */
    0,                                               /* tp_dict */
    0,                                               /* tp_descr_get */
    0,                                               /* tp_descr_set */
    0,                                               /* tp_dictoffset */
    0,                                               /* tp_init */
    0,                                               /* tp_alloc */
    0,                                               /* tp_new */
    0,                                               /* tp_free */
};

PyTypeObject* PyXmlSec_TransformType = &_PyXmlSec_TransformType;

static PyObject* PyXmlSec_TransformNew(xmlSecTransformId id) {
    PyXmlSec_Transform* transform = PyObject_New(PyXmlSec_Transform, PyXmlSec_TransformType);
    if (transform != NULL) {
        transform->id = id;
    }
    return (PyObject*)transform;
}

// destructor
static void PyXmlSec_KeyData__del__(PyObject* self) {
    PYXMLSEC_DEBUGF("%p", self);
    Py_TYPE(self)->tp_free(self);
}

// __str__ method
static PyObject* PyXmlSec_KeyData__str__(PyObject* self) {
    char buf[300];
    PyXmlSec_KeyData* keydata = (PyXmlSec_KeyData*)(self);
    if (keydata->id->href != NULL)
        snprintf(buf, sizeof(buf), "%s, %s", keydata->id->name, keydata->id->href);
    else
        snprintf(buf, sizeof(buf), "%s, None", keydata->id->name);
    return PyUnicode_FromString(buf);
}

// __repr__ method
static PyObject* PyXmlSec_KeyData__repr__(PyObject* self) {
    char buf[300];
    PyXmlSec_KeyData* keydata = (PyXmlSec_KeyData*)(self);
    if (keydata->id->href != NULL)
        snprintf(buf, sizeof(buf), "__KeyData('%s', '%s')", keydata->id->name, keydata->id->href);
    else
        snprintf(buf, sizeof(buf), "__KeyData('%s', None)", keydata->id->name);
    return PyUnicode_FromString(buf);
}

static const char PyXmlSec_KeyDataNameGet__doc__[] = "The key data's name.";
static PyObject* PyXmlSec_KeyDataNameGet(PyXmlSec_KeyData* self, void* closure) {
    return PyUnicode_FromString((const char*)self->id->name);
}

static const char PyXmlSec_KeyDataHrefGet__doc__[] = "The key data's identification string (href).";
static PyObject* PyXmlSec_KeyDataHrefGet(PyXmlSec_KeyData* self, void* closure) {
    if (self->id->href != NULL)
        return PyUnicode_FromString((const char*)self->id->href);
    Py_RETURN_NONE;
}

static PyGetSetDef PyXmlSec_KeyDataGetSet[] = {
    {
        "name",
        (getter)PyXmlSec_KeyDataNameGet,
        NULL,
        (char*)PyXmlSec_KeyDataNameGet__doc__,
        NULL
    },
    {
        "href",
        (getter)PyXmlSec_KeyDataHrefGet,
        NULL,
        (char*)PyXmlSec_KeyDataHrefGet__doc__,
        NULL
    },
    {NULL} /* Sentinel */
};

static PyTypeObject _PyXmlSec_KeyDataType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    STRINGIFY(MODULE_NAME) ".constants.__KeyData",  /* tp_name */
    sizeof(PyXmlSec_KeyData),                       /* tp_basicsize */
    0,                                              /* tp_itemsize */
    PyXmlSec_KeyData__del__,                        /* tp_dealloc */
    0,                                              /* tp_print */
    0,                                              /* tp_getattr */
    0,                                              /* tp_setattr */
    0,                                              /* tp_reserved */
    PyXmlSec_KeyData__repr__,                       /* tp_repr */
    0,                                              /* tp_as_number */
    0,                                              /* tp_as_sequence */
    0,                                              /* tp_as_mapping */
    0,                                              /* tp_hash  */
    0,                                              /* tp_call */
    PyXmlSec_KeyData__str__,                        /* tp_str */
    0,                                              /* tp_getattro */
    0,                                              /* tp_setattro */
    0,                                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                             /* tp_flags */
    "The xmlSecKeyDataId reflection",               /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    0,                                              /* tp_methods */
    0,                                              /* tp_members */
    PyXmlSec_KeyDataGetSet,                         /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    0,                                              /* tp_init */
    0,                                              /* tp_alloc */
    0,                                              /* tp_new */
    0,                                              /* tp_free */
};

PyTypeObject* PyXmlSec_KeyDataType = &_PyXmlSec_KeyDataType;

static PyObject* PyXmlSec_KeyDataNew(xmlSecKeyDataId id) {
    PyXmlSec_KeyData* keydata = PyObject_New(PyXmlSec_KeyData, PyXmlSec_KeyDataType);
    if (keydata != NULL) {
        keydata->id = id;
    }
    return (PyObject*)keydata;
}

static PyModuleDef PyXmlSec_ConstantsModule =
{
    PyModuleDef_HEAD_INIT,
    STRINGIFY(MODULE_NAME) ".constants",
    PYXMLSEC_CONSTANTS_DOC,
    -1, NULL, NULL, NULL, NULL, NULL
};

// initialize constants module and registers it base package
int PyXmlSec_ConstantsModule_Init(PyObject* package) {
    PyObject* constants = NULL;
    PyObject* nsCls = NULL;
    PyObject* nodeCls = NULL;
    PyObject* transformCls = NULL;
    PyObject* encryptionTypeCls = NULL;
    PyObject* keyFormatCls = NULL;
    PyObject* keyDataCls = NULL;
    PyObject* keyDataTypeCls = NULL;
    PyObject* tmp = NULL;

    constants = PyModule_Create(&PyXmlSec_ConstantsModule);

    if (!constants) return -1;

    if (PyType_Ready(PyXmlSec_TransformType) < 0) goto ON_FAIL;
    if (PyType_Ready(PyXmlSec_KeyDataType) < 0) goto ON_FAIL;

#define PYXMLSEC_ADD_INT_CONSTANT(name) PyModule_AddIntConstant(constants, STRINGIFY(name), JOIN(xmlSec, name))

    if (PYXMLSEC_ADD_INT_CONSTANT(TransformUsageUnknown) < 0) goto ON_FAIL;
    if (PYXMLSEC_ADD_INT_CONSTANT(TransformUsageDSigTransform) < 0) goto ON_FAIL;
    if (PYXMLSEC_ADD_INT_CONSTANT(TransformUsageC14NMethod) < 0) goto ON_FAIL;
    if (PYXMLSEC_ADD_INT_CONSTANT(TransformUsageDigestMethod) < 0) goto ON_FAIL;
    if (PYXMLSEC_ADD_INT_CONSTANT(TransformUsageSignatureMethod) < 0) goto ON_FAIL;
    if (PYXMLSEC_ADD_INT_CONSTANT(TransformUsageEncryptionMethod) < 0) goto ON_FAIL;
    if (PYXMLSEC_ADD_INT_CONSTANT(TransformUsageAny) < 0) goto ON_FAIL;

#undef PYXMLSEC_ADD_INT_CONSTANT

#define PYXMLSEC_DECLARE_NAMESPACE(var, name) \
    if (!(var = PyModule_New(name))) goto ON_FAIL; \
    if (PyModule_AddObject(package, name, var) < 0) goto ON_FAIL; \
    Py_INCREF(var); // add object steels reference

#define PYXMLSEC_CLOSE_NAMESPACE(var) \
    Py_DECREF(var); var = NULL // compensate add ref from declare namespace

#define PYXMLSEC_ADD_CONSTANT(ns, name, lname) \
    if (tmp == NULL) goto ON_FAIL; \
    if (PyModule_AddObject(constants, STRINGIFY(name), tmp) < 0) goto ON_FAIL; \
    Py_INCREF(tmp); \
    if (PyModule_AddObject(ns, lname, tmp) < 0) goto ON_FAIL; \
    tmp = NULL;


#define PYXMLSEC_ADD_NS_CONSTANT(name, lname) \
    tmp = PyUnicode_FromString((const char*)(JOIN(xmlSec, name))); \
    PYXMLSEC_ADD_CONSTANT(nsCls, name, lname);

    // namespaces
    PYXMLSEC_DECLARE_NAMESPACE(nsCls, "Namespace");

    PYXMLSEC_ADD_NS_CONSTANT(Ns, "BASE");
    PYXMLSEC_ADD_NS_CONSTANT(DSigNs, "DS");
    PYXMLSEC_ADD_NS_CONSTANT(EncNs, "ENC");
#ifndef XMLSEC_NO_XKMS
    PYXMLSEC_ADD_NS_CONSTANT(XkmsNs, "XKMS");
#endif
    PYXMLSEC_ADD_NS_CONSTANT(XPathNs, "XPATH");
    PYXMLSEC_ADD_NS_CONSTANT(XPath2Ns, "XPATH2");
    PYXMLSEC_ADD_NS_CONSTANT(XPointerNs, "XPOINTER");
    PYXMLSEC_ADD_NS_CONSTANT(NsExcC14N, "EXC_C14N");
    PYXMLSEC_ADD_NS_CONSTANT(NsExcC14NWithComments, "EXC_C14N_WITH_COMMENT");

    PYXMLSEC_CLOSE_NAMESPACE(nsCls);

#undef PYXMLSEC_ADD_NS_CONSTANT


#define PYXMLSEC_ADD_ENC_CONSTANT(name, lname) \
    tmp = PyUnicode_FromString((const char*)(JOIN(xmlSec, name))); \
    PYXMLSEC_ADD_CONSTANT(encryptionTypeCls, name, lname);

    // encryption type
    PYXMLSEC_DECLARE_NAMESPACE(encryptionTypeCls, "EncryptionType");

    PYXMLSEC_ADD_ENC_CONSTANT(TypeEncContent, "CONTENT");
    PYXMLSEC_ADD_ENC_CONSTANT(TypeEncElement, "ELEMENT");

    PYXMLSEC_CLOSE_NAMESPACE(encryptionTypeCls);

#undef PYXMLSEC_ADD_ENC_CONSTANT


#define PYXMLSEC_ADD_NODE_CONSTANT(name, lname) \
    tmp = PyUnicode_FromString((const char*)(JOIN(xmlSec, name))); \
    PYXMLSEC_ADD_CONSTANT(nodeCls, name, lname);

    // node
    PYXMLSEC_DECLARE_NAMESPACE(nodeCls, "Node");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeSignature, "SIGNATURE");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeSignedInfo, "SIGNED_INFO");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeCanonicalizationMethod, "CANONICALIZATION_METHOD");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeSignatureMethod, "SIGNATURE_METHOD");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeSignatureValue, "SIGNATURE_VALUE");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeSignatureProperties, "SIGNATURE_PROPERTIES");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeDigestMethod, "DIGEST_METHOD");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeDigestValue, "DIGEST_VALUE");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeObject, "OBJECT");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeManifest, "MANIFEST");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeEncryptedData, "ENCRYPTED_DATA");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeEncryptedKey, "ENCRYPTED_KEY");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeEncryptionMethod, "ENCRYPTION_METHOD");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeEncryptionProperty, "ENCRYPTION_PROPERTY");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeEncryptionProperties, "ENCRYPTION_PROPERTIES");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeCipherData, "CIPHER_DATA");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeCipherValue, "CIPHER_VALUE");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeCipherReference, "CIPHER_REFERENCE");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeDataReference, "DATA_REFERENCE");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeKeyReference, "KEY_REFERENCE");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeReference, "REFERENCE");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeReferenceList, "REFERENCE_LIST");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeKeyInfo, "KEY_INFO");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeKeyName, "KEY_NAME");
    PYXMLSEC_ADD_NODE_CONSTANT(NodeKeyValue, "KEY_VALUE");

    PYXMLSEC_ADD_NODE_CONSTANT(NodeX509Data, "X509_DATA");

    PYXMLSEC_CLOSE_NAMESPACE(nodeCls);
#undef PYXMLSEC_ADD_NODE_CONSTANT


#define PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(name, lname) \
    tmp = PyLong_FromUnsignedLong((unsigned long)(JOIN(xmlSec, name))); \
    PYXMLSEC_ADD_CONSTANT(keyFormatCls, name, lname);

    // key format
    PYXMLSEC_DECLARE_NAMESPACE(keyFormatCls, "KeyFormat");

    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatUnknown, "UNKNOWN");
    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatBinary, "BINARY");
    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatPem, "PEM");
    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatDer, "DER");
    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatPkcs8Pem, "PKCS8_PEM");
    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatPkcs8Der, "PKCS8_DER");;
    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatPkcs12, "PKCS12_PEM");
    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatCertPem, "CERT_PEM");
    PYXMLSEC_ADD_KEY_FORMAT_CONSTANT(KeyDataFormatCertDer, "CERT_DER");

    PYXMLSEC_CLOSE_NAMESPACE(keyFormatCls);
#undef PYXMLSEC_ADD_KEY_FORMAT_CONSTANT


#define PYXMLSEC_ADD_KEY_TYPE_CONSTANT(name, lname) \
    tmp = PyLong_FromUnsignedLong((unsigned long)(JOIN(xmlSec, name))); \
    PYXMLSEC_ADD_CONSTANT(keyDataTypeCls, name, lname);

    // key data type
    PYXMLSEC_DECLARE_NAMESPACE(keyDataTypeCls, "KeyDataType");

    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypeUnknown, "UNKNOWN");
    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypeNone, "NONE");
    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypePublic, "PUBLIC");
    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypePrivate, "PRIVATE");
    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypeSymmetric, "SYMMETRIC");
    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypeSession, "SESSION");
    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypePermanent, "PERMANENT");
    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypeTrusted, "TRUSTED");
    PYXMLSEC_ADD_KEY_TYPE_CONSTANT(KeyDataTypeAny, "ANY");

    PYXMLSEC_CLOSE_NAMESPACE(keyDataTypeCls);
#undef PYXMLSEC_ADD_KEY_TYPE_CONSTANT


#define PYXMLSEC_ADD_KEYDATA_CONSTANT(name, lname)  \
    tmp = PyXmlSec_KeyDataNew(xmlSec ## name ## Id); \
    PYXMLSEC_ADD_CONSTANT(keyDataCls, name, lname);

    // keydata
    PYXMLSEC_DECLARE_NAMESPACE(keyDataCls, "KeyData");

    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataName, "NAME")
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataValue, "VALUE")
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataRetrievalMethod, "RETRIEVALMETHOD")
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataEncryptedKey, "ENCRYPTEDKEY")
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataAes, "AES")
#ifndef XMLSEC_NO_DES
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataDes, "DES")
#endif
#ifndef XMLSEC_NO_DSA
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataDsa, "DSA")
#endif
#if XMLSEC_VERSION_HEX > 0x10212 && XMLSEC_VERSION_HEX < 0x10303
    // from version 1.2.19 to version 1.3.2 (inclusive)
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataEcdsa, "ECDSA")
#elif XMLSEC_VERSION_HEX >= 0x10303
    // from version 1.3.3 (inclusive)
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataEc, "ECDSA")
#endif
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataHmac, "HMAC")
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataRsa, "RSA")
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataX509, "X509")
    PYXMLSEC_ADD_KEYDATA_CONSTANT(KeyDataRawX509Cert, "RAWX509CERT")

    PYXMLSEC_CLOSE_NAMESPACE(keyDataCls);
#undef PYXMLSEC_ADD_KEYDATA_CONSTANT


#define PYXMLSEC_ADD_TRANSFORM_CONSTANT(name, lname)  \
    tmp = PyXmlSec_TransformNew(xmlSec ## name ## Id); \
    PYXMLSEC_ADD_CONSTANT(transformCls, name, lname);

    // transforms
    PYXMLSEC_DECLARE_NAMESPACE(transformCls, "Transform");

    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformInclC14N, "C14N");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformInclC14NWithComments, "C14N_COMMENTS");

    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformInclC14N11, "C14N11");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformInclC14N11WithComments, "C14N11_COMMENTS");

    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformExclC14N, "EXCL_C14N");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformExclC14NWithComments, "EXCL_C14N_COMMENTS");

    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformEnveloped, "ENVELOPED");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformXPath, "XPATH");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformXPath2, "XPATH2");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformXPointer, "XPOINTER");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRemoveXmlTagsC14N, "REMOVE_XML_TAGS_C14N");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformVisa3DHack, "VISA3D_HACK");

    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformAes128Cbc, "AES128");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformAes192Cbc, "AES192");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformAes256Cbc, "AES256");

    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformKWAes128, "KW_AES128");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformKWAes192, "KW_AES192");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformKWAes256, "KW_AES256");

#ifndef XMLSEC_NO_DES
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformDes3Cbc, "DES3");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformKWDes3, "KW_DES3");
#endif
#ifndef XMLSEC_NO_DSA
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformDsaSha1, "DSA_SHA1");
#endif
#ifndef XMLSEC_NO_XSLT
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformXslt, "XSLT");
#endif

#if XMLSEC_VERSION_HEX > 0x10212
    // from version 1.2.19
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformEcdsaSha1, "ECDSA_SHA1");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformEcdsaSha224, "ECDSA_SHA224");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformEcdsaSha256, "ECDSA_SHA256");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformEcdsaSha384, "ECDSA_SHA384");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformEcdsaSha512, "ECDSA_SHA512");
#endif

#ifndef XMLSEC_NO_MD5
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformHmacMd5, "HMAC_MD5");
#endif

#ifndef XMLSEC_NO_RIPEMD160
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformHmacRipemd160, "HMAC_RIPEMD160");
#endif
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformHmacSha1, "HMAC_SHA1");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformHmacSha224, "HMAC_SHA224");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformHmacSha256, "HMAC_SHA256");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformHmacSha384, "HMAC_SHA384");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformHmacSha512, "HMAC_SHA512");

#ifndef XMLSEC_NO_MD5
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaMd5, "RSA_MD5");
#endif

#ifndef XMLSEC_NO_RIPEMD160
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaRipemd160, "RSA_RIPEMD160");
#endif
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaSha1, "RSA_SHA1");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaSha224, "RSA_SHA224");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaSha256, "RSA_SHA256");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaSha384, "RSA_SHA384");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaSha512, "RSA_SHA512");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaPkcs1, "RSA_PKCS1");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRsaOaep, "RSA_OAEP");

#ifndef XMLSEC_NO_MD5
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformMd5, "MD5");
#endif

#ifndef XMLSEC_NO_RIPEMD160
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformRipemd160, "RIPEMD160");
#endif

    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformSha1, "SHA1");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformSha224, "SHA224");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformSha256, "SHA256");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformSha384, "SHA384");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformSha512, "SHA512");

#if XMLSEC_VERSION_HEX > 0x1021B
    // from version 1.2.28
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformAes128Gcm, "AES128_GCM");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformAes192Gcm, "AES192_GCM");
    PYXMLSEC_ADD_TRANSFORM_CONSTANT(TransformAes256Gcm, "AES256_GCM");
#endif

    PYXMLSEC_CLOSE_NAMESPACE(transformCls);

#undef PYXMLSEC_ADD_TRANSFORM_CONSTANT
#undef PYXMLSEC_ADD_CONSTANT
#undef PYXMLSEC_DECLARE_NAMESPACE

    if (PyModule_AddObject(package, "constants", constants) < 0) goto ON_FAIL;

    return 0;
ON_FAIL:
    Py_XDECREF(tmp);
    Py_XDECREF(nsCls);
    Py_XDECREF(nodeCls);
    Py_XDECREF(transformCls);
    Py_XDECREF(encryptionTypeCls);
    Py_XDECREF(keyFormatCls);
    Py_XDECREF(keyDataCls);
    Py_XDECREF(keyDataTypeCls);
    Py_DECREF(constants);

    return -1;
}
