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
#include "lxml.h"

#include <xmlsec/templates.h>

#define PYXMLSEC_TEMPLATES_DOC "Xml Templates processing"

static char PyXmlSec_TemplateCreate__doc__[] = \
    "create(node, c14n_method, sign_method, id = None, ns = None) -> lxml.etree._Element\n"
    "Creates new :xml:`<dsig:Signature/>` node with the mandatory :xml:`<dsig:SignedInfo/>`, :xml:`<dsig:CanonicalizationMethod/>`, "
    ":xml:`<dsig:SignatureMethod/>` and :xml:`<dsig:SignatureValue/>` children and sub-children.\n\n"
    ":param node: the signature node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param c14n_method: the signature canonicalization method\n"
    ":type c14n_method: :class:`__Transform`\n"
    ":param sign_method: the signature method\n"
    ":type sign_method: :class:`__Transform`\n"
    ":param id: the node id (optional)\n"
    ":type id: :class:`str` or :data:`None`\n"
    ":param ns: the namespace prefix for the signature element (e.g. ``\"dsig\"``) (optional)\n"
    ":type ns: :class:`str` or :data:`None`\n"
    ":return: the pointer to newly created :xml:`<dsig:Signature/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateCreate(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "c14n_method", "sign_method", "id", "ns", "name", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    PyXmlSec_Transform* c14n = NULL;
    PyXmlSec_Transform* sign = NULL;
    const char* id = NULL;
    const char* ns = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template create - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&O!O!|zzz:create", kwlist,
        PyXmlSec_LxmlElementConverter, &node, PyXmlSec_TransformType, &c14n, PyXmlSec_TransformType, &sign, &id, &ns, &id))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplSignatureCreateNsPref(node->_doc->_c_doc, c14n->id, sign->id, XSTR(id), XSTR(ns));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot create template.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template create - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template create - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddReference__doc__[] = \
    "add_reference(node, digest_method, id = None, uri = None, type = None) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:Reference/>` node with given ``\"URI\"`` (``uri``), ``\"Id\"`` (``id``) and ``\"Type\"`` (``type``) attributes and "
    "the required children :xml:`<dsig:DigestMethod/>` and :xml:`<dsig:DigestValue/>` to the :xml:`<dsig:SignedInfo/>` child of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:Signature/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param digest_method: the reference digest method\n"
    ":type digest_method: :class:`__Transform`\n"
    ":param id: the node id (optional)\n"
    ":type id: :class:`str` or :data:`None`\n"
    ":param uri: the reference node URI (optional)\n"
    ":type uri: :class:`str` or :data:`None`\n"
    ":param type: the reference node type (optional)\n"
    ":type type: :class:`str` or :data:`None`\n"
    ":return: the pointer to newly created :xml:`<dsig:Reference/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddReference(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "digest_method", "id", "uri", "type", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    PyXmlSec_Transform* digest = NULL;
    const char* id = NULL;
    const char* uri = NULL;
    const char* type = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template add_reference - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&O!|zzz:add_reference", kwlist,
        PyXmlSec_LxmlElementConverter, &node, PyXmlSec_TransformType, &digest, &id, &uri, &type))
    {
        goto ON_FAIL;
    }
    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplSignatureAddReference(node->_c_node, digest->id, XSTR(id), XSTR(uri), XSTR(type));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add reference.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template add_reference - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template add_reference - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddTransform__doc__[] = \
    "add_transform(node) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:Transform/>` node to the :xml:`<dsig:Reference/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:Reference/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param transform: the transform method id\n"
    ":type transform: :class:`__Transform`\n"
    ":return: the pointer to newly created :xml:`<dsig:Transform/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddTransform(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "transform", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    PyXmlSec_Transform* transform = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template add_transform - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&O!:add_transform", kwlist,
        PyXmlSec_LxmlElementConverter, &node, PyXmlSec_TransformType, &transform))
    {
        goto ON_FAIL;
    }
    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplReferenceAddTransform(node->_c_node, transform->id);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add transform.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template add_transform - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template add_transform - fail");
    return NULL;
}

static char PyXmlSec_TemplateEnsureKeyInfo__doc__[] = \
    "ensure_key_info(node, id = None) -> lxml.etree._Element\n"
    "Adds (if necessary) :xml:`<dsig:KeyInfo/>` node to the :xml:`<dsig:Signature/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:Signature/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param id: the node id (optional)\n"
    ":type id: :class:`str` or :data:`None`\n"
    ":return: the pointer to newly created :xml:`<dsig:KeyInfo/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateEnsureKeyInfo(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "id", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    const char* id = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template ensure_key_info - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&|z:ensure_key_info", kwlist, PyXmlSec_LxmlElementConverter, &node, &id))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplSignatureEnsureKeyInfo(node->_c_node, XSTR(id));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot ensure key info.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template ensure_key_info - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template ensure_key_info - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddKeyName__doc__[] = \
    "add_key_name(node, name = None) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:KeyName/>` node to the :xml:`<dsig:KeyInfo/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:KeyInfo/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param name: the key name (optional)\n"
    ":type name: :class:`str` or :data:`None`\n"
    ":return: the pointer to the newly created :xml:`<dsig:KeyName/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddKeyName(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "name", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    const char* name = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template add_key_name - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&|z:add_key_name", kwlist, PyXmlSec_LxmlElementConverter, &node, &name))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplKeyInfoAddKeyName(node->_c_node, XSTR(name));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add key name.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template add_key_name - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template add_key_name - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddKeyValue__doc__[] = \
    "add_key_value(node) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:KeyValue/>` node to the :xml:`<dsig:KeyInfo/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:KeyInfo/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to the newly created :xml:`<dsig:KeyValue/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddKeyValue(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template add_key_value - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:add_key_value", kwlist, PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplKeyInfoAddKeyValue(node->_c_node);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add key value.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template add_key_name - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template add_key_name - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddX509Data__doc__[] = \
    "add_x509_data(node) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:X509Data/>` node to the :xml:`<dsig:KeyInfo/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:KeyInfo/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to the newly created :xml:`<dsig:X509Data/>` node\n"
    ":rtype: :class:`lxml.etree._Element`\n";
static PyObject* PyXmlSec_TemplateAddX509Data(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template add_x509_data - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:add_x509_data", kwlist, PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplKeyInfoAddX509Data(node->_c_node);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add x509 data.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template add_x509_data - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template add_x509_data - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddX509DataAddIssuerSerial__doc__[] = \
    "x509_data_add_issuer_serial(node) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:X509IssuerSerial/>` node to the given :xml:`<dsig:X509Data/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:X509Data/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to the newly created :xml:`<dsig:X509IssuerSerial/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddX509DataAddIssuerSerial(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template x509_data_add_issuer_serial - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:x509_data_add_issuer_serial", kwlist,
        PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }
    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplX509DataAddIssuerSerial(node->_c_node);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add x509 issuer serial.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template x509_data_add_issuer_serial - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template x509_data_add_issuer_serial - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddX509DataIssuerSerialAddIssuerName__doc__[] = \
    "x509_issuer_serial_add_issuer_name(node, name = None) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:X509IssuerName/>` node to the :xml:`<dsig:X509IssuerSerial/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:X509IssuerSerial/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param name: the issuer name (optional)\n"
    ":type name: :class:`str` or :data:`None`\n"
    ":return: the pointer to the newly created :xml:`<dsig:X509IssuerName/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddX509DataIssuerSerialAddIssuerName(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "name", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    const char* name = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template x509_issuer_serial_add_issuer_name - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&|z:x509_issuer_serial_add_issuer_name", kwlist,
        PyXmlSec_LxmlElementConverter, &node, &name))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplX509IssuerSerialAddIssuerName(node->_c_node, XSTR(name));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add x509 issuer serial name.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template x509_issuer_serial_add_issuer_name - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template x509_issuer_serial_add_issuer_name - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddX509DataIssuerSerialAddIssuerSerialNumber__doc__[] = \
    "x509_issuer_serial_add_serial_number(node, serial = None) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:X509SerialNumber/>` node to the :xml:`<dsig:X509IssuerSerial/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:X509IssuerSerial/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param serial: the serial number (optional)\n"
    ":type serial: :class:`str` or :data:`None`\n"
    ":return: the pointer to the newly created :xml:`<dsig:X509SerialNumber/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddX509DataIssuerSerialAddIssuerSerialNumber(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "serial", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    const char* serial = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template x509_issuer_serial_add_serial_number - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&|z:x509_issuer_serial_add_serial_number", kwlist,
        PyXmlSec_LxmlElementConverter, &node, &serial))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplX509IssuerSerialAddSerialNumber(node->_c_node, XSTR(serial));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add x509 issuer serial number.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template x509_issuer_serial_add_serial_number - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template x509_issuer_serial_add_serial_number - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddX509DataAddSubjectName__doc__[] = \
    "x509_data_add_subject_name(node) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:X509SubjectName/>` node to the given :xml:`<dsig:X509Data/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:X509Data/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to the newly created :xml:`<dsig:X509SubjectName/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddX509DataAddSubjectName(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template x509_data_add_subject_name - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:x509_data_add_subject_name", kwlist,
        PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplX509DataAddSubjectName(node->_c_node);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add x509 subject name.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template x509_data_add_subject_name - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template x509_data_add_subject_name - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddX509DataAddSKI__doc__[] = \
    "x509_data_add_ski(node) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:X509SKI/>` node to the given :xml:`<dsig:X509Data/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:X509Data/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to the newly created :xml:`<dsig:X509SKI/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddX509DataAddSKI(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template x509_data_add_ski - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:x509_data_add_ski", kwlist,
        PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplX509DataAddSKI(node->_c_node);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add x509 SKI.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template x509_data_add_ski - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template x509_data_add_ski - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddX509DataAddCertificate__doc__[] = \
    "x509_data_add_certificate(node) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:X509Certificate/>` node to the given :xml:`<dsig:X509Data/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:X509Data/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to the newly created :xml:`<dsig:X509Certificate/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddX509DataAddCertificate(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template x509_data_add_certificate - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:x509_data_add_certificate", kwlist,
        PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplX509DataAddCertificate(node->_c_node);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add x509 certificate.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template x509_data_add_certificate - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template x509_data_add_certificate - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddX509DataAddCRL__doc__[] = \
    "x509_data_add_crl(node) -> lxml.etree._Element\n"
    "Adds :xml:`<dsig:X509CRL/>` node to the given :xml:`<dsig:X509Data/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:X509Data/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to the newly created :xml:`<dsig:X509CRL/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddX509DataAddCRL(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template x509_data_add_crl - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:x509_data_add_crl", kwlist,
        PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplX509DataAddCRL(node->_c_node);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add x509 CRL.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template x509_data_add_crl - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template x509_data_add_crl - fail");
    return NULL;
}

static char PyXmlSec_TemplateAddEncryptedKey__doc__[] = \
    "add_encrypted_key(node, method, id = None, type = None, recipient = None) -> lxml.etree._Element\n"
    "Adds :xml:`<enc:EncryptedKey/>` node with given attributes to the :xml:`<dsig:KeyInfo/>` node of *node*.\n\n"
    ":param node: the pointer to :xml:`<dsig:KeyInfo/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param method: the encryption method\n"
    ":type method: :class:`__Transform`\n"
    ":param id: the ``\"Id\"`` attribute (optional)\n"
    ":type id: :class:`str` or :data:`None`\n"
    ":param type: the ``\"Type\"`` attribute (optional)\n"
    ":type type: :class:`str` or :data:`None`\n"
    ":param recipient: the ``\"Recipient\"`` attribute (optional)\n"
    ":type recipient: :class:`str` or :data:`None`\n"
    ":return: the pointer to the newly created :xml:`<enc:EncryptedKey/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateAddEncryptedKey(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "method", "id", "type", "recipient", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    PyXmlSec_Transform* method = NULL;
    const char* id = NULL;
    const char* type = NULL;
    const char* recipient = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template add_encrypted_key - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&O!|zzz:add_encrypted_key", kwlist,
        PyXmlSec_LxmlElementConverter, &node, PyXmlSec_TransformType, &method, &id, &type, &recipient))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplKeyInfoAddEncryptedKey(node->_c_node, method->id, XSTR(id), XSTR(type), XSTR(recipient));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot add encrypted key.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template add_encrypted_key - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template add_encrypted_key - fail");
    return NULL;
}

static char PyXmlSec_TemplateCreateEncryptedData__doc__[] = \
    "encrypted_data_create(node, method, id = None, type = None, mime_type = None, encoding = None, ns = None) -> lxml.etree._Element\n"
    "Creates new :xml:`<{ns}:EncryptedData />` node for encryption template.\n\n"
    ":param node: the pointer to signature node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param method: the encryption method\n"
    ":type method: :class:`__Transform`\n"
    ":param id: the ``\"Id\"`` attribute (optional)\n"
    ":type id: :class:`str` or :data:`None`\n"
    ":param type: the ``\"Type\"`` attribute (optional)\n"
    ":type type: :class:`str` or :data:`None`\n"
    ":param mime_type: the ``\"Recipient\"`` attribute (optional)\n"
    ":type mime_type: :class:`str` or :data:`None`\n"
    ":param encoding: the ``\"MimeType\"`` attribute (optional)\n"
    ":type encoding: :class:`str` or :data:`None`\n"
    ":param ns: the namespace prefix (optional)\n"
    ":type ns: :class:`str` or :data:`None`\n"
    ":return: the pointer newly created :xml:`<enc:EncryptedData/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateCreateEncryptedData(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "method", "id", "type", "mime_type", "encoding", "ns", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    PyXmlSec_Transform* method = NULL;
    const char* id = NULL;
    const char* type = NULL;
    const char* mime_type = NULL;
    const char* encoding = NULL;
    const char* ns = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template encrypted_data_create - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&O!|zzzzz:encrypted_data_create", kwlist,
        PyXmlSec_LxmlElementConverter, &node, PyXmlSec_TransformType, &method, &id, &type, &mime_type, &encoding, &ns))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplEncDataCreate(node->_doc->_c_doc, method->id, XSTR(id), XSTR(type), XSTR(mime_type), XSTR(encoding));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot create encrypted data.");
        goto ON_FAIL;
    }
    if (ns != NULL) {
        res->ns->prefix = xmlStrdup(XSTR(ns));
    }

    PYXMLSEC_DEBUG("template encrypted_data_create - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template encrypted_data_create - fail");
    return NULL;
}

static char PyXmlSec_TemplateEncryptedDataEnsureKeyInfo__doc__[] = \
    "encrypted_data_ensure_key_info(node, id = None, ns = None) -> lxml.etree._Element\n"
    "Adds :xml:`<{ns}:KeyInfo/>` to the :xml:`<enc:EncryptedData/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<enc:EncryptedData/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param id: the ``\"Id\"`` attribute (optional)\n"
    ":type id: :class:`str` or :data:`None`\n"
    ":param ns: the namespace prefix (optional)\n"
    ":type ns: :class:`str` or :data:`None`\n"
    ":return: the pointer to newly created :xml:`<dsig:KeyInfo/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateEncryptedDataEnsureKeyInfo(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "id", "ns", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    const char* id = NULL;
    const char* ns = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template encrypted_data_ensure_key_info - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&|zz:encrypted_data_ensure_key_info", kwlist,
        PyXmlSec_LxmlElementConverter, &node, &id, &ns))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplEncDataEnsureKeyInfo(node->_c_node, XSTR(id));
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot ensure key info for encrypted data.");
        goto ON_FAIL;
    }
    if (ns != NULL) {
        res->ns->prefix = xmlStrdup(XSTR(ns));
    }

    PYXMLSEC_DEBUG("template encrypted_data_ensure_key_info - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template encrypted_data_ensure_key_info - fail");
    return NULL;
}

static char PyXmlSec_TemplateEncryptedDataEnsureCipherValue__doc__[] = \
    "encrypted_data_ensure_cipher_value(node) -> lxml.etree._Element\n"
    "Adds :xml:`<CipherValue/>` to the :xml:`<enc:EncryptedData/>` node of ``node``.\n\n"
    ":param node: the pointer to :xml:`<enc:EncryptedData/>` node\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":return: the pointer to newly created :xml:`<enc:CipherValue/>` node\n"
    ":rtype: :class:`lxml.etree._Element`";
static PyObject* PyXmlSec_TemplateEncryptedDataEnsureCipherValue(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    xmlNodePtr res;

    PYXMLSEC_DEBUG("template encrypted_data_ensure_cipher_value - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&:encrypted_data_ensure_cipher_value", kwlist,
        PyXmlSec_LxmlElementConverter, &node))
    {
        goto ON_FAIL;
    }

    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplEncDataEnsureCipherValue(node->_c_node);
    Py_END_ALLOW_THREADS;
    if (res == NULL) {
        PyXmlSec_SetLastError("cannot ensure cipher value for encrypted data.");
        goto ON_FAIL;
    }

    PYXMLSEC_DEBUG("template encrypted_data_ensure_cipher_value - ok");
    return (PyObject*)PyXmlSec_elementFactory(node->_doc, res);

ON_FAIL:
    PYXMLSEC_DEBUG("template encrypted_data_ensure_cipher_value - fail");
    return NULL;
}

static char PyXmlSec_TemplateTransformAddC14NInclNamespaces__doc__[] = \
    "transform_add_c14n_inclusive_namespaces(node, prefixes = None) -> None\n"
    "Adds 'inclusive' namespaces to the ExcC14N transform node ``node``.\n\n"
    ":param node: the pointer to :xml:`<dsig:Transform/>` node.\n"
    ":type node: :class:`lxml.etree._Element`\n"
    ":param prefixes: the list of namespace prefixes, where ``'default'`` indicates the default namespace (optional).\n"
    ":type prefixes: :class:`str` or :class:`list` of strings";
static PyObject* PyXmlSec_TemplateTransformAddC14NInclNamespaces(PyObject* self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = { "node", "prefixes", NULL};

    PyXmlSec_LxmlElementPtr node = NULL;
    PyObject* prefixes = NULL;
    PyObject* sep;
    int res;
    const char* c_prefixes;

    // transform_add_c14n_inclusive_namespaces
    PYXMLSEC_DEBUG("template encrypted_data_ensure_cipher_value - start");
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&O:transform_add_c14n_inclusive_namespaces", kwlist,
        PyXmlSec_LxmlElementConverter, &node, &prefixes))
    {
        prefixes = NULL;
        goto ON_FAIL;
    }
    if (PyList_Check(prefixes) || PyTuple_Check(prefixes)) {
        sep = PyUnicode_FromString(" ");
        prefixes = PyObject_CallMethod(sep, "join", "O", prefixes);
        Py_DECREF(sep);
    } else if (PyUnicode_Check(prefixes)) {
        Py_INCREF(prefixes);
    } else {
        PyErr_SetString(PyExc_TypeError, "expected instance of str or list of str");
        prefixes = NULL;
    }

    if (prefixes == NULL) {
        goto ON_FAIL;
    }


    c_prefixes = PyUnicode_AsUTF8(prefixes);
    Py_BEGIN_ALLOW_THREADS;
    res = xmlSecTmplTransformAddC14NInclNamespaces(node->_c_node, XSTR(c_prefixes));
    Py_END_ALLOW_THREADS;
    if (res != 0) {
        PyXmlSec_SetLastError("cannot add 'inclusive' namespaces to the ExcC14N transform node");
        goto ON_FAIL;
    }

    Py_DECREF(prefixes);
    PYXMLSEC_DEBUG("transform_add_c14n_inclusive_namespaces - ok");
    Py_RETURN_NONE;

ON_FAIL:
    PYXMLSEC_DEBUG("transform_add_c14n_inclusive_namespaces - fail");
    Py_XDECREF(prefixes);
    return NULL;
}

static PyMethodDef PyXmlSec_TemplateMethods[] = {
    {
        "create",
        (PyCFunction)PyXmlSec_TemplateCreate,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateCreate__doc__
    },
    {
        "add_reference",
        (PyCFunction)PyXmlSec_TemplateAddReference,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddReference__doc__
    },
    {
        "add_transform",
        (PyCFunction)PyXmlSec_TemplateAddTransform,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddTransform__doc__
    },
    {
        "ensure_key_info",
        (PyCFunction)PyXmlSec_TemplateEnsureKeyInfo,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateEnsureKeyInfo__doc__
    },
    {
        "add_key_name",
        (PyCFunction)PyXmlSec_TemplateAddKeyName,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddKeyName__doc__
    },
    {
        "add_key_value",
        (PyCFunction)PyXmlSec_TemplateAddKeyValue,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddKeyValue__doc__
    },
    {
        "add_x509_data",
        (PyCFunction)PyXmlSec_TemplateAddX509Data,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddX509Data__doc__
    },
    {
        "x509_data_add_issuer_serial",
        (PyCFunction)PyXmlSec_TemplateAddX509DataAddIssuerSerial,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddX509DataAddIssuerSerial__doc__
    },
    {
        "x509_issuer_serial_add_issuer_name",
        (PyCFunction)PyXmlSec_TemplateAddX509DataIssuerSerialAddIssuerName,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddX509DataIssuerSerialAddIssuerName__doc__
    },
    {
        "x509_issuer_serial_add_serial_number",
        (PyCFunction)PyXmlSec_TemplateAddX509DataIssuerSerialAddIssuerSerialNumber,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddX509DataIssuerSerialAddIssuerSerialNumber__doc__
    },
    {
        "x509_data_add_subject_name",
        (PyCFunction)PyXmlSec_TemplateAddX509DataAddSubjectName,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddX509DataAddSubjectName__doc__
    },
    {
        "x509_data_add_ski",
        (PyCFunction)PyXmlSec_TemplateAddX509DataAddSKI,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddX509DataAddSKI__doc__
    },
    {
        "x509_data_add_certificate",
        (PyCFunction)PyXmlSec_TemplateAddX509DataAddCertificate,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddX509DataAddCertificate__doc__
    },
    {
        "x509_data_add_crl",
        (PyCFunction)PyXmlSec_TemplateAddX509DataAddCRL,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddX509DataAddCRL__doc__
    },
    {
        "add_encrypted_key",
        (PyCFunction)PyXmlSec_TemplateAddEncryptedKey,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateAddEncryptedKey__doc__
    },
    {
        "encrypted_data_create",
        (PyCFunction)PyXmlSec_TemplateCreateEncryptedData,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateCreateEncryptedData__doc__
    },
    {
        "encrypted_data_ensure_key_info",
        (PyCFunction)PyXmlSec_TemplateEncryptedDataEnsureKeyInfo,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateEncryptedDataEnsureKeyInfo__doc__
    },
    {
        "encrypted_data_ensure_cipher_value",
        (PyCFunction)PyXmlSec_TemplateEncryptedDataEnsureCipherValue,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateEncryptedDataEnsureCipherValue__doc__
    },
    {
        "transform_add_c14n_inclusive_namespaces",
        (PyCFunction)PyXmlSec_TemplateTransformAddC14NInclNamespaces,
        METH_VARARGS|METH_KEYWORDS,
        PyXmlSec_TemplateTransformAddC14NInclNamespaces__doc__,
    },
    {NULL, NULL} /* sentinel */
};

static PyModuleDef PyXmlSec_TemplateModule =
{
    PyModuleDef_HEAD_INIT,
    STRINGIFY(MODULE_NAME) ".template",
    PYXMLSEC_TEMPLATES_DOC,
    -1,
    PyXmlSec_TemplateMethods, /* m_methods */
    NULL,                     /* m_slots */
    NULL,                     /* m_traverse */
    NULL,                     /* m_clear */
    NULL,                     /* m_free */
};

int PyXmlSec_TemplateModule_Init(PyObject* package) {
    PyObject* template = PyModule_Create(&PyXmlSec_TemplateModule);

    if (!template) goto ON_FAIL;
    PYXMLSEC_DEBUGF("%p", template);

    if (PyModule_AddObject(package, "template", template) < 0) goto ON_FAIL;

    return 0;
ON_FAIL:
    Py_XDECREF(template);
    return -1;
}
