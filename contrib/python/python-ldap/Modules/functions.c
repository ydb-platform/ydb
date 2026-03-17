/* See https://www.python-ldap.org/ for details. */

#include "common.h"
#include "functions.h"
#include "LDAPObject.h"
#include "berval.h"
#include "constants.h"
#include "options.h"

/* ldap_initialize */

static PyObject *
l_ldap_initialize(PyObject *unused, PyObject *args)
{
    char *uri;
    LDAP *ld = NULL;
    int ret;
    PyThreadState *save;

    if (!PyArg_ParseTuple(args, "s:initialize", &uri))
        return NULL;

    save = PyEval_SaveThread();
    ret = ldap_initialize(&ld, uri);
    PyEval_RestoreThread(save);

    if (ret != LDAP_SUCCESS)
        return LDAPerror(ld);

    return (PyObject *)newLDAPObject(ld);
}

#ifdef HAVE_LDAP_INIT_FD
/* initialize_fd(fileno, url) */

static PyObject *
l_ldap_initialize_fd(PyObject *unused, PyObject *args)
{
    char *url;
    LDAP *ld = NULL;
    int ret;
    int fd;
    int proto = -1;
    LDAPURLDesc *lud = NULL;

    PyThreadState *save;

    if (!PyArg_ParseTuple(args, "is:initialize_fd", &fd, &url))
        return NULL;

    /* Get LDAP protocol from scheme */
    ret = ldap_url_parse(url, &lud);
    if (ret != LDAP_SUCCESS)
        return LDAPerr(ret);

    if (strcmp(lud->lud_scheme, "ldap") == 0) {
        proto = LDAP_PROTO_TCP;
    }
    else if (strcmp(lud->lud_scheme, "ldaps") == 0) {
        proto = LDAP_PROTO_TCP;
    }
    else if (strcmp(lud->lud_scheme, "ldapi") == 0) {
        proto = LDAP_PROTO_IPC;
    }
#ifdef LDAP_CONNECTIONLESS
    else if (strcmp(lud->lud_scheme, "cldap") == 0) {
        proto = LDAP_PROTO_UDP;
    }
#endif
    else {
        ldap_free_urldesc(lud);
        PyErr_SetString(PyExc_ValueError, "unsupported URL scheme");
        return NULL;
    }
    ldap_free_urldesc(lud);

    save = PyEval_SaveThread();
    ret = ldap_init_fd((ber_socket_t) fd, proto, url, &ld);
    PyEval_RestoreThread(save);

    if (ret != LDAP_SUCCESS)
        return LDAPerror(ld);

    return (PyObject *)newLDAPObject(ld);
}
#endif

/* ldap_str2dn */

static PyObject *
l_ldap_str2dn(PyObject *unused, PyObject *args)
{
    struct berval str;
    LDAPDN dn;
    int flags = 0;
    PyObject *result = NULL, *tmp;
    int res, i, j;
    Py_ssize_t str_len;

    /*
     * From a DN string such as "a=b,c=d;e=f", build
     * a list-equivalent of AVA structures; namely:
     * ((('a','b',1),('c','d',1)),(('e','f',1),))
     * The integers are a bit combination of the AVA_* flags
     */
    if (!PyArg_ParseTuple(args, "z#|i:str2dn", &str.bv_val, &str_len, &flags))
        return NULL;
    str.bv_len = (ber_len_t) str_len;

    res = ldap_bv2dn(&str, &dn, flags);
    if (res != LDAP_SUCCESS)
        return LDAPerr(res);

    tmp = PyList_New(0);
    if (!tmp)
        goto failed;

    for (i = 0; dn[i]; i++) {
        LDAPRDN rdn;
        PyObject *rdnlist;

        rdn = dn[i];
        rdnlist = PyList_New(0);
        if (!rdnlist)
            goto failed;
        if (PyList_Append(tmp, rdnlist) == -1) {
            Py_DECREF(rdnlist);
            goto failed;
        }

        for (j = 0; rdn[j]; j++) {
            LDAPAVA *ava = rdn[j];
            PyObject *tuple;

            tuple = Py_BuildValue("(O&O&i)",
                                  LDAPberval_to_unicode_object, &ava->la_attr,
                                  LDAPberval_to_unicode_object, &ava->la_value,
                                  ava->la_flags & ~(LDAP_AVA_FREE_ATTR |
                                                    LDAP_AVA_FREE_VALUE));
            if (!tuple) {
                Py_DECREF(rdnlist);
                goto failed;
            }

            if (PyList_Append(rdnlist, tuple) == -1) {
                Py_DECREF(tuple);
                goto failed;
            }
            Py_DECREF(tuple);
        }
        Py_DECREF(rdnlist);
    }

    result = tmp;
    tmp = NULL;

  failed:
    Py_XDECREF(tmp);
    ldap_dnfree(dn);
    return result;
}

/* ldap_set_option (global options) */

static PyObject *
l_ldap_set_option(PyObject *self, PyObject *args)
{
    PyObject *value;
    int option;

    if (!PyArg_ParseTuple(args, "iO:set_option", &option, &value))
        return NULL;
    if (!LDAP_set_option(NULL, option, value))
        return NULL;
    Py_INCREF(Py_None);
    return Py_None;
}

/* ldap_get_option (global options) */

static PyObject *
l_ldap_get_option(PyObject *self, PyObject *args)
{
    int option;

    if (!PyArg_ParseTuple(args, "i:get_option", &option))
        return NULL;
    return LDAP_get_option(NULL, option);
}

/* methods */

static PyMethodDef methods[] = {
    {"initialize", (PyCFunction)l_ldap_initialize, METH_VARARGS},
#ifdef HAVE_LDAP_INIT_FD
    {"initialize_fd", (PyCFunction)l_ldap_initialize_fd, METH_VARARGS},
#endif
    {"str2dn", (PyCFunction)l_ldap_str2dn, METH_VARARGS},
    {"set_option", (PyCFunction)l_ldap_set_option, METH_VARARGS},
    {"get_option", (PyCFunction)l_ldap_get_option, METH_VARARGS},
    {NULL, NULL}
};

/* initialisation */

void
LDAPinit_functions(PyObject *d)
{
    LDAPadd_methods(d, methods);
}
