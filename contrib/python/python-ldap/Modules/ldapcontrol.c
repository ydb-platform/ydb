/* See https://www.python-ldap.org/ for details. */

#include "common.h"
#include "LDAPObject.h"
#include "ldapcontrol.h"
#include "berval.h"
#include "constants.h"

/* Prints to stdout the contents of an array of LDAPControl objects */

/* XXX: This is a debugging tool, and the printf generates some warnings
 * about pointer types. I left it here in case something breaks and we
 * need to inspect an LDAPControl structure.

static void
LDAPControl_DumpList( LDAPControl** lcs ) {
    LDAPControl** lcp;
    LDAPControl* lc;
        for ( lcp = lcs; *lcp; lcp++ ) {
        lc = *lcp;
        printf("OID: %s\nCriticality: %d\nBER length: %d\nBER value: %x\n",
            lc->ldctl_oid, lc->ldctl_iscritical, lc->ldctl_value.bv_len,
            lc->ldctl_value.bv_val);
    }
} */

/* Free a single LDAPControl object created by Tuple_to_LDAPControl */

static void
LDAPControl_DEL(LDAPControl *lc)
{
    if (lc == NULL)
        return;

    if (lc->ldctl_oid)
        PyMem_DEL(lc->ldctl_oid);
    PyMem_DEL(lc);
}

/* Free an array of LDAPControl objects created by LDAPControls_from_object */

void
LDAPControl_List_DEL(LDAPControl **lcs)
{
    LDAPControl **lcp;

    if (lcs == NULL)
        return;

    for (lcp = lcs; *lcp; lcp++)
        LDAPControl_DEL(*lcp);

    PyMem_DEL(lcs);
}

/* Takes a tuple of the form:
 * (OID: string, Criticality: int/boolean, Value: string/None)
 * and converts it into an LDAPControl structure.
 *
 * The Value string should represent an ASN.1 encoded structure.
 */

static LDAPControl *
Tuple_to_LDAPControl(PyObject *tup)
{
    char *oid;
    char iscritical;
    struct berval berbytes;
    PyObject *bytes;
    LDAPControl *lc = NULL;
    Py_ssize_t len;

    if (!PyTuple_Check(tup)) {
        LDAPerror_TypeError("Tuple_to_LDAPControl(): expected a tuple", tup);
        return NULL;
    }

    if (!PyArg_ParseTuple
        (tup, "sbO:Tuple_to_LDAPControl", &oid, &iscritical, &bytes))
        return NULL;

    lc = PyMem_NEW(LDAPControl, 1);

    if (lc == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    lc->ldctl_iscritical = iscritical;

    len = strlen(oid);
    lc->ldctl_oid = PyMem_NEW(char, len + 1);

    if (lc->ldctl_oid == NULL) {
        PyErr_NoMemory();
        LDAPControl_DEL(lc);
        return NULL;
    }
    memcpy(lc->ldctl_oid, oid, len + 1);

    /* The berval can either be None or a String */
    if (PyNone_Check(bytes)) {
        berbytes.bv_len = 0;
        berbytes.bv_val = NULL;
    }
    else if (PyBytes_Check(bytes)) {
        berbytes.bv_len = PyBytes_Size(bytes);
        berbytes.bv_val = PyBytes_AsString(bytes);
    }
    else {
        LDAPerror_TypeError("Tuple_to_LDAPControl(): expected bytes", bytes);
        LDAPControl_DEL(lc);
        return NULL;
    }

    lc->ldctl_value = berbytes;

    return lc;
}

/* Convert a list of tuples (of a format acceptable to the Tuple_to_LDAPControl
 * function) into an array of LDAPControl objects. */

int
LDAPControls_from_object(PyObject *list, LDAPControl ***controls_ret)
{
    Py_ssize_t len, i;
    LDAPControl **ldcs;
    LDAPControl *ldc;
    PyObject *item;

    if (!PySequence_Check(list)) {
        LDAPerror_TypeError("LDAPControls_from_object(): expected a list",
                            list);
        return 0;
    }

    len = PySequence_Length(list);
    ldcs = PyMem_NEW(LDAPControl *, len + 1);

    if (ldcs == NULL) {
        PyErr_NoMemory();
        return 0;
    }

    for (i = 0; i < len; i++) {
        item = PySequence_GetItem(list, i);
        if (item == NULL) {
            PyMem_DEL(ldcs);
            return 0;
        }

        ldc = Tuple_to_LDAPControl(item);
        if (ldc == NULL) {
            Py_DECREF(item);
            PyMem_DEL(ldcs);
            return 0;
        }

        ldcs[i] = ldc;
        Py_DECREF(item);
    }

    ldcs[len] = NULL;
    *controls_ret = ldcs;
    return 1;
}

PyObject *
LDAPControls_to_List(LDAPControl **ldcs)
{
    PyObject *res = 0, *pyctrl;
    LDAPControl **tmp = ldcs;
    Py_ssize_t num_ctrls = 0, i;

    if (tmp)
        while (*tmp++)
            num_ctrls++;

    if ((res = PyList_New(num_ctrls)) == NULL) {
        return NULL;
    }

    for (i = 0; i < num_ctrls; i++) {
        pyctrl = Py_BuildValue("sbO&",
                               ldcs[i]->ldctl_oid,
                               ldcs[i]->ldctl_iscritical,
                               LDAPberval_to_object, &ldcs[i]->ldctl_value);
        if (pyctrl == NULL) {
            Py_DECREF(res);
            return NULL;
        }
        PyList_SET_ITEM(res, i, pyctrl);
    }
    return res;
}

/* --------------- en-/decoders ------------- */

/* Matched Values, aka, Values Return Filter */
static PyObject *
encode_rfc3876(PyObject *self, PyObject *args)
{
    PyObject *res = 0;
    int err;
    BerElement *vrber = 0;
    char *vrFilter;
    struct berval *ctrl_val;

    if (!PyArg_ParseTuple
        (args, "s:encode_valuesreturnfilter_control", &vrFilter)) {
        goto endlbl;
    }

    if (!(vrber = ber_alloc_t(LBER_USE_DER))) {
        LDAPerr(LDAP_NO_MEMORY);
        goto endlbl;
    }

    err = ldap_put_vrFilter(vrber, vrFilter);
    if (err == -1) {
        LDAPerr(LDAP_FILTER_ERROR);
        goto endlbl;
    }

    err = ber_flatten(vrber, &ctrl_val);
    if (err == -1) {
        LDAPerr(LDAP_NO_MEMORY);
        goto endlbl;
    }

    res = LDAPberval_to_object(ctrl_val);
    ber_bvfree(ctrl_val);

  endlbl:
    if (vrber)
        ber_free(vrber, 1);

    return res;
}

static PyObject *
encode_rfc2696(PyObject *self, PyObject *args)
{
    PyObject *res = 0;
    BerElement *ber = 0;
    struct berval cookie, *ctrl_val;
    Py_ssize_t cookie_len;
    int size = 0;               /* ber_int_t is int */
    ber_tag_t tag;

    if (!PyArg_ParseTuple(args, "is#:encode_page_control", &size,
                          &cookie.bv_val, &cookie_len)) {
        goto endlbl;
    }
    cookie.bv_len = (ber_len_t) cookie_len;

    if (!(ber = ber_alloc_t(LBER_USE_DER))) {
        LDAPerr(LDAP_NO_MEMORY);
        goto endlbl;
    }

    tag = ber_printf(ber, "{i", size);
    if (tag == LBER_ERROR) {
        LDAPerr(LDAP_ENCODING_ERROR);
        goto endlbl;
    }

    if (!cookie.bv_len)
        tag = ber_printf(ber, "o", "", 0);
    else
        tag = ber_printf(ber, "O", &cookie);
    if (tag == LBER_ERROR) {
        LDAPerr(LDAP_ENCODING_ERROR);
        goto endlbl;
    }

    tag = ber_printf(ber, /*{ */ "N}");
    if (tag == LBER_ERROR) {
        LDAPerr(LDAP_ENCODING_ERROR);
        goto endlbl;
    }

    if (-1 == ber_flatten(ber, &ctrl_val)) {
        LDAPerr(LDAP_NO_MEMORY);
        goto endlbl;
    }

    res = LDAPberval_to_object(ctrl_val);
    ber_bvfree(ctrl_val);

  endlbl:
    if (ber)
        ber_free(ber, 1);
    return res;
}

static PyObject *
decode_rfc2696(PyObject *self, PyObject *args)
{
    PyObject *res = 0;
    BerElement *ber = 0;
    struct berval ldctl_value;
    ber_tag_t tag;
    struct berval *cookiep;
    int count = 0;              /* ber_int_t is int */
    Py_ssize_t ldctl_value_len;

    if (!PyArg_ParseTuple(args, "s#:decode_page_control",
                          &ldctl_value.bv_val, &ldctl_value_len)) {
        goto endlbl;
    }
    ldctl_value.bv_len = (ber_len_t) ldctl_value_len;

    if (!(ber = ber_init(&ldctl_value))) {
        LDAPerr(LDAP_NO_MEMORY);
        goto endlbl;
    }

    tag = ber_scanf(ber, "{iO", &count, &cookiep);
    if (tag == LBER_ERROR) {
        LDAPerr(LDAP_DECODING_ERROR);
        goto endlbl;
    }

    res = Py_BuildValue("(iO&)", count, LDAPberval_to_object, cookiep);
    ber_bvfree(cookiep);

  endlbl:
    if (ber)
        ber_free(ber, 1);
    return res;
}

static PyObject *
encode_assertion_control(PyObject *self, PyObject *args)
{
    int err;
    PyObject *res = 0;
    char *assertion_filterstr;
    struct berval ctrl_val;
    LDAP *ld = NULL;
    PyThreadState *save;

    if (!PyArg_ParseTuple(args, "s:encode_assertion_control",
                          &assertion_filterstr)) {
        goto endlbl;
    }

    /* XXX: ldap_create() is a nasty and slow hack. It's creating a full blown
     * LDAP object just to encode assertion controls.
     */
    save = PyEval_SaveThread();
    err = ldap_create(&ld);
    PyEval_RestoreThread(save);
    if (err != LDAP_SUCCESS)
        return LDAPerror(ld);

    err = ldap_create_assertion_control_value(ld, assertion_filterstr,
                                              &ctrl_val);

    if (err != LDAP_SUCCESS) {
        LDAPerror(ld);
        save = PyEval_SaveThread();
        ldap_unbind_ext(ld, NULL, NULL);
        PyEval_RestoreThread(save);
        return NULL;
    }
    save = PyEval_SaveThread();
    ldap_unbind_ext(ld, NULL, NULL);
    PyEval_RestoreThread(save);
    res = LDAPberval_to_object(&ctrl_val);

    if (ctrl_val.bv_val != NULL) {
        ber_memfree(ctrl_val.bv_val);
    }
  endlbl:

    return res;
}

static PyMethodDef methods[] = {
    {"encode_page_control", encode_rfc2696, METH_VARARGS},
    {"decode_page_control", decode_rfc2696, METH_VARARGS},
    {"encode_valuesreturnfilter_control", encode_rfc3876, METH_VARARGS},
    {"encode_assertion_control", encode_assertion_control, METH_VARARGS},
    {NULL, NULL}
};

void
LDAPinit_control(PyObject *d)
{
    LDAPadd_methods(d, methods);
}
