/* pgcasts_basic.c - basic typecasting functions to python types
 *
 * Copyright (C) 2001-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020-2021 The Psycopg Team
 *
 * This file is part of psycopg.
 *
 * psycopg2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link this program with the OpenSSL library (or with
 * modified versions of OpenSSL that use the same license as OpenSSL),
 * and distribute linked combinations including the two.
 *
 * You must obey the GNU Lesser General Public License in all respects for
 * all of the code used other than OpenSSL.
 *
 * psycopg2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 */

/** INTEGER - cast normal integers (4 bytes) to python int **/

#define typecast_INTEGER_cast typecast_LONGINTEGER_cast

/** LONGINTEGER - cast long integers (8 bytes) to python long **/

static PyObject *
typecast_LONGINTEGER_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    char buffer[24];

    if (s == NULL) { Py_RETURN_NONE; }
    if (s[len] != '\0') {
        strncpy(buffer, s, (size_t) len); buffer[len] = '\0';
        s = buffer;
    }
    return PyLong_FromString((char *)s, NULL, 0);
}

/** FLOAT - cast floating point numbers to python float **/

static PyObject *
typecast_FLOAT_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    PyObject *str = NULL, *flo = NULL;

    if (s == NULL) { Py_RETURN_NONE; }
    if (!(str = Text_FromUTF8AndSize(s, len))) { return NULL; }
    flo = PyFloat_FromString(str);
    Py_DECREF(str);
    return flo;
}


/** BYTES - cast strings of any type to python bytes **/

static PyObject *
typecast_BYTES_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    if (s == NULL) { Py_RETURN_NONE; }
    return Bytes_FromStringAndSize(s, len);
}


/** UNICODE - cast strings of any type to a python unicode object **/

static PyObject *
typecast_UNICODE_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    connectionObject *conn;

    if (s == NULL) { Py_RETURN_NONE; }

    conn = ((cursorObject*)curs)->conn;
    return conn_decode(conn, s, len);
}


/** STRING - cast strings of any type to python string **/

#define typecast_STRING_cast typecast_UNICODE_cast


/** BOOLEAN - cast boolean value into right python object **/

static PyObject *
typecast_BOOLEAN_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    PyObject *res = NULL;

    if (s == NULL) { Py_RETURN_NONE; }

    switch (s[0]) {
        case 't':
        case 'T':
            res = Py_True;
            break;

        case 'f':
        case 'F':
            res = Py_False;
            break;

        default:
            PyErr_Format(InterfaceError, "can't parse boolean: '%s'", s);
            break;
    }

    Py_XINCREF(res);
    return res;
}

/** DECIMAL - cast any kind of number into a Python Decimal object **/

static PyObject *
typecast_DECIMAL_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    PyObject *res = NULL;
    PyObject *decimalType;
    char *buffer;

    if (s == NULL) { Py_RETURN_NONE; }

    if ((buffer = PyMem_Malloc(len+1)) == NULL)
        return PyErr_NoMemory();
    strncpy(buffer, s, (size_t) len); buffer[len] = '\0';
    decimalType = psyco_get_decimal_type();
    /* Fall back on float if decimal is not available */
    if (decimalType != NULL) {
        res = PyObject_CallFunction(decimalType, "s", buffer);
        Py_DECREF(decimalType);
    }
    else {
        PyErr_Clear();
        res = PyObject_CallFunction((PyObject*)&PyFloat_Type, "s", buffer);
    }
    PyMem_Free(buffer);

    return res;
}

/* some needed aliases */
#define typecast_NUMBER_cast   typecast_FLOAT_cast
#define typecast_ROWID_cast    typecast_INTEGER_cast
