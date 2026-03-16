/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/* Copyright (c) 1999 Ng Pheng Siong. All rights reserved. */
/* $Id$ */

%include <openssl/opensslconf.h>

#if defined(OPENSSL_NO_RC4)
#undef OPENSSL_NO_RC4
%constant OPENSSL_NO_RC4 = 1;
#else
%constant OPENSSL_NO_RC4 = 0;

%{
#include <openssl/rc4.h>
%}

%apply Pointer NONNULL { RC4_KEY * };

%inline %{
RC4_KEY *rc4_new(void) {
    RC4_KEY *key;

    if (!(key = (RC4_KEY *)PyMem_Malloc(sizeof(RC4_KEY))))
        PyErr_SetString(PyExc_MemoryError, "rc4_new");
    return key;
}

void rc4_free(RC4_KEY *key) {
    PyMem_Free((void *)key);
}

PyObject *rc4_set_key(RC4_KEY *key, PyObject *value) {
    const void *vbuf = NULL;
    int vlen = 0;

    if (m2_PyObject_AsReadBufferInt(value, &vbuf, &vlen) == -1)
        return NULL;

    RC4_set_key(key, vlen, vbuf);
    Py_RETURN_NONE;
}

PyObject *rc4_update(RC4_KEY *key, PyObject *in) {
    PyObject *ret;
    const void *buf;
    Py_ssize_t len;
    void *out;

    if (m2_PyObject_AsReadBuffer(in, &buf, &len) == -1)
        return NULL;

    if (!(out = PyMem_Malloc(len))) {
        PyErr_SetString(PyExc_MemoryError, "expected a string object");
        return NULL;
    }
    RC4(key, len, buf, out);

    ret = PyBytes_FromStringAndSize(out, len);

    PyMem_Free(out);
    return ret;
}

int rc4_type_check(RC4_KEY *key) {
    return 1;
}
%}

#endif
