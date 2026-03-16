/* See https://www.python-ldap.org/ for details. */

#include "common.h"
#include "berval.h"

/*
 * Copies out the data from a berval, and returns it as a new Python object,
 * Returns None if the berval pointer is NULL.
 *
 * Note that this function is not the exact inverse of LDAPberval_from_object
 * with regards to the NULL/None conversion.
 *
 * Returns a new Python object on success, or NULL on failure.
 */
PyObject *
LDAPberval_to_object(const struct berval *bv)
{
    PyObject *ret = NULL;

    if (!bv || !bv->bv_val) {
        ret = Py_None;
        Py_INCREF(ret);
    }
    else {
        ret = PyBytes_FromStringAndSize(bv->bv_val, bv->bv_len);
    }

    return ret;
}

/*
 * Same as LDAPberval_to_object, but returns a Unicode PyObject.
 * Use when the value is known to be text (for instance a distinguishedName).
 *
 * Returns a new Python object on success, or NULL on failure.
 */
PyObject *
LDAPberval_to_unicode_object(const struct berval *bv)
{
    PyObject *ret = NULL;

    if (!bv) {
        ret = Py_None;
        Py_INCREF(ret);
    }
    else {
        ret = PyUnicode_FromStringAndSize(bv->bv_val, bv->bv_len);
    }

    return ret;
}
