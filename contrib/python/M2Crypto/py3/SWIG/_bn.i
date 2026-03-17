/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/* Copyright (c) 2005-2006 Open Source Applications Foundation. All rights reserved. */

/* We are converting between the Python arbitrarily long integer and
 * the BIGNUM arbitrarily long integer by converting to and from
 * a string representation of the number (in hexadecimal).
 * Direct manipulation would be a possibility, but would require
 * tighter integration with the Python and OpenSSL internals.
 */


%{
#include <openssl/bn.h>
%}


%inline %{
PyObject *bn_rand(int bits, int top, int bottom)
{
    BIGNUM* rnd;
    PyObject *ret;
    char *randhex;

    rnd = BN_new();
    if (rnd == NULL) {
        m2_PyErr_Msg(PyExc_Exception);
        return NULL;
    }

    if (!BN_rand(rnd, bits, top, bottom)) {
        /*Custom errors?*/
        m2_PyErr_Msg(PyExc_Exception);
        BN_free(rnd);
        return NULL;
    }

    randhex = BN_bn2hex(rnd);
    if (!randhex) {
        /*Custom errors?*/
        m2_PyErr_Msg(PyExc_Exception);
        BN_free(rnd);
        return NULL;
    }
    BN_free(rnd);

    ret = PyLong_FromString(randhex, NULL, 16);
    OPENSSL_free(randhex);
    return ret;
}


PyObject *bn_rand_range(PyObject *range)
{
    BIGNUM* rnd;
    BIGNUM *rng = NULL;
    PyObject *ret, *tuple;
    PyObject *format, *rangePyString;
    char *randhex; /* PyLong_FromString is unhappy with const */
    const char *rangehex;

    /* Wow, it's a lot of work to convert into a hex string in C! */
    format = PyUnicode_FromString("%x");

    if (!format) {
        PyErr_SetString(PyExc_RuntimeError, "Cannot create Python string '%x'");
        return NULL;
    }
    tuple = PyTuple_New(1);
    if (!tuple) {
        Py_DECREF(format);
        PyErr_SetString(PyExc_RuntimeError, "PyTuple_New() fails");
        return NULL;
    }
    Py_INCREF(range);
    PyTuple_SET_ITEM(tuple, 0, range);

    rangePyString = PyUnicode_Format(format, tuple);

    if (!rangePyString) {
        PyErr_SetString(PyExc_Exception, "String Format failed");
        Py_DECREF(format);
        Py_DECREF(tuple);
        return NULL;
    }
    Py_DECREF(format);
    Py_DECREF(tuple);

    rangehex = (const char*)PyUnicode_AsUTF8(rangePyString);

    if (!BN_hex2bn(&rng, rangehex)) {
        /*Custom errors?*/
        m2_PyErr_Msg(PyExc_Exception);
        Py_DECREF(rangePyString);
        return NULL;
    }

    Py_DECREF(rangePyString);

    if (!(rnd = BN_new())) {
        PyErr_SetString(PyExc_MemoryError, "bn_rand_range");
        return NULL;
    }

    if (!BN_rand_range(rnd, rng)) {
        /*Custom errors?*/
        m2_PyErr_Msg(PyExc_Exception);
        BN_free(rnd);
        BN_free(rng);
        return NULL;
    }

    BN_free(rng);

    randhex = BN_bn2hex(rnd);
    if (!randhex) {
        /*Custom errors?*/
        m2_PyErr_Msg(PyExc_Exception);
        BN_free(rnd);
        return NULL;
    }
    BN_free(rnd);

    ret = PyLong_FromString(randhex, NULL, 16);
    OPENSSL_free(randhex);
    return ret;
}

%}
