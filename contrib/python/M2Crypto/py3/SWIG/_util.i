/* Copyright (c) 1999-2002 Ng Pheng Siong. All rights reserved.
 * Copyright (c) 2009-2010 Heikki Toivonen. All rights reserved.
*/
/* $Id$ */

%{
#include <openssl/x509v3.h>
%}

%warnfilter(454) _util_err;
%inline %{
static PyObject *_util_err;

void util_init(PyObject *util_err) {
    Py_INCREF(util_err);
    _util_err = util_err;
}

PyObject *util_hex_to_string(PyObject *blob) {
    PyObject *obj;
    const void *buf;
    char *ret;
    Py_ssize_t len;

    if (m2_PyObject_AsReadBuffer(blob, &buf, &len) == -1)
        return NULL;

    ret = hex_to_string((unsigned char *)buf, len);
    if (!ret) {
        m2_PyErr_Msg(_util_err);
        return NULL;
    }

    obj = PyBytes_FromString(ret);

    OPENSSL_free(ret);
    return obj;
}

PyObject *util_string_to_hex(PyObject *blob) {
    PyObject *obj;
    const void *buf;
    unsigned char *ret;
    Py_ssize_t len0;
    long len;

    if (m2_PyObject_AsReadBuffer(blob, &buf, &len0) == -1)
        return NULL;

    len = len0;
    ret = string_to_hex((char *)buf, &len);
    if (ret == NULL) {
        m2_PyErr_Msg(_util_err);
        return NULL;
    }
    obj = PyBytes_FromStringAndSize((char*)ret, len);
    OPENSSL_free(ret);
    return obj;
}
%}
