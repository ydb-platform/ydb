/* bytes_format.c - bytes-oriented version of PyString_Format
 *
 * Copyright (C) 2010-2019 Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

/* This implementation is based on the PyString_Format function available in
 * Python 2.7.1. The function is altered to be used with both Python 2 strings
 * and Python 3 bytes and is stripped of the support of formats different than
 * 's'. Original license follows.
 *
 * PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
 * --------------------------------------------
 *
 * 1. This LICENSE AGREEMENT is between the Python Software Foundation
 * ("PSF"), and the Individual or Organization ("Licensee") accessing and
 * otherwise using this software ("Python") in source or binary form and
 * its associated documentation.
 *
 * 2. Subject to the terms and conditions of this License Agreement, PSF hereby
 * grants Licensee a nonexclusive, royalty-free, world-wide license to reproduce,
 * analyze, test, perform and/or display publicly, prepare derivative works,
 * distribute, and otherwise use Python alone or in any derivative version,
 * provided, however, that PSF's License Agreement and PSF's notice of copyright,
 * i.e., "Copyright (c) 2001-2019, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010
 * Python Software Foundation; All Rights Reserved" are retained in Python alone or
 * in any derivative version prepared by Licensee.
 *
 * 3. In the event Licensee prepares a derivative work that is based on
 * or incorporates Python or any part thereof, and wants to make
 * the derivative work available to others as provided herein, then
 * Licensee hereby agrees to include in any such work a brief summary of
 * the changes made to Python.
 *
 * 4. PSF is making Python available to Licensee on an "AS IS"
 * basis.  PSF MAKES NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR
 * IMPLIED.  BY WAY OF EXAMPLE, BUT NOT LIMITATION, PSF MAKES NO AND
 * DISCLAIMS ANY REPRESENTATION OR WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR ANY PARTICULAR PURPOSE OR THAT THE USE OF PYTHON WILL NOT
 * INFRINGE ANY THIRD PARTY RIGHTS.
 *
 * 5. PSF SHALL NOT BE LIABLE TO LICENSEE OR ANY OTHER USERS OF PYTHON
 * FOR ANY INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES OR LOSS AS
 * A RESULT OF MODIFYING, DISTRIBUTING, OR OTHERWISE USING PYTHON,
 * OR ANY DERIVATIVE THEREOF, EVEN IF ADVISED OF THE POSSIBILITY THEREOF.
 *
 * 6. This License Agreement will automatically terminate upon a material
 * breach of its terms and conditions.
 *
 * 7. Nothing in this License Agreement shall be deemed to create any
 * relationship of agency, partnership, or joint venture between PSF and
 * Licensee.  This License Agreement does not grant permission to use PSF
 * trademarks or trade name in a trademark sense to endorse or promote
 * products or services of Licensee, or any third party.
 *
 * 8. By copying, installing or otherwise using Python, Licensee
 * agrees to be bound by the terms and conditions of this License
 * Agreement.
 */

#define PSYCOPG_MODULE
#include "psycopg/psycopg.h"
#include "pyport.h"

/* Helpers for formatstring */

BORROWED Py_LOCAL_INLINE(PyObject *)
getnextarg(PyObject *args, Py_ssize_t arglen, Py_ssize_t *p_argidx)
{
    Py_ssize_t argidx = *p_argidx;
    if (argidx < arglen) {
        (*p_argidx)++;
        if (arglen < 0)
            return args;
        else
            return PyTuple_GetItem(args, argidx);
    }
    PyErr_SetString(PyExc_TypeError,
                    "not enough arguments for format string");
    return NULL;
}

/* wrapper around _Bytes_Resize offering normal Python call semantics */

STEALS(1)
Py_LOCAL_INLINE(PyObject *)
resize_bytes(PyObject *b, Py_ssize_t newsize) {
    if (0 == _Bytes_Resize(&b, newsize)) {
        return b;
    }
    else {
        return NULL;
    }
}

/* fmt%(v1,v2,...) is roughly equivalent to sprintf(fmt, v1, v2, ...) */

PyObject *
Bytes_Format(PyObject *format, PyObject *args)
{
    char *fmt, *res;
    Py_ssize_t arglen, argidx;
    Py_ssize_t reslen, rescnt, fmtcnt;
    int args_owned = 0;
    PyObject *result;
    PyObject *dict = NULL;
    if (format == NULL || !Bytes_Check(format) || args == NULL) {
        PyErr_SetString(PyExc_SystemError, "bad argument to internal function");
        return NULL;
    }
    fmt = Bytes_AS_STRING(format);
    fmtcnt = Bytes_GET_SIZE(format);
    reslen = rescnt = fmtcnt + 100;
    result = Bytes_FromStringAndSize((char *)NULL, reslen);
    if (result == NULL)
        return NULL;
    res = Bytes_AS_STRING(result);
    if (PyTuple_Check(args)) {
        arglen = PyTuple_GET_SIZE(args);
        argidx = 0;
    }
    else {
        arglen = -1;
        argidx = -2;
    }
    if (Py_TYPE(args)->tp_as_mapping && !PyTuple_Check(args) &&
        !PyObject_TypeCheck(args, &Bytes_Type))
        dict = args;
    while (--fmtcnt >= 0) {
        if (*fmt != '%') {
            if (--rescnt < 0) {
                rescnt = fmtcnt + 100;
                reslen += rescnt;
                if (!(result = resize_bytes(result, reslen))) {
                    return NULL;
                }
                res = Bytes_AS_STRING(result) + reslen - rescnt;
                --rescnt;
            }
            *res++ = *fmt++;
        }
        else {
            /* Got a format specifier */
            Py_ssize_t width = -1;
            int c = '\0';
            PyObject *v = NULL;
            PyObject *temp = NULL;
            char *pbuf;
            Py_ssize_t len;
            fmt++;
            if (*fmt == '(') {
                char *keystart;
                Py_ssize_t keylen;
                PyObject *key;
                int pcount = 1;

                if (dict == NULL) {
                    PyErr_SetString(PyExc_TypeError,
                             "format requires a mapping");
                    goto error;
                }
                ++fmt;
                --fmtcnt;
                keystart = fmt;
                /* Skip over balanced parentheses */
                while (pcount > 0 && --fmtcnt >= 0) {
                    if (*fmt == ')')
                        --pcount;
                    else if (*fmt == '(')
                        ++pcount;
                    fmt++;
                }
                keylen = fmt - keystart - 1;
                if (fmtcnt < 0 || pcount > 0) {
                    PyErr_SetString(PyExc_ValueError,
                               "incomplete format key");
                    goto error;
                }
                key = Text_FromUTF8AndSize(keystart, keylen);
                if (key == NULL)
                    goto error;
                if (args_owned) {
                    Py_DECREF(args);
                    args_owned = 0;
                }
                args = PyObject_GetItem(dict, key);
                Py_DECREF(key);
                if (args == NULL) {
                    goto error;
                }
                args_owned = 1;
                arglen = -1;
                argidx = -2;
            }
            while (--fmtcnt >= 0) {
                c = *fmt++;
                break;
            }
            if (fmtcnt < 0) {
                PyErr_SetString(PyExc_ValueError,
                                "incomplete format");
                goto error;
            }
            switch (c) {
            case '%':
                pbuf = "%";
                len = 1;
                break;
            case 's':
                /* only bytes! */
                if (!(v = getnextarg(args, arglen, &argidx)))
                    goto error;
                if (!Bytes_CheckExact(v)) {
                    PyErr_Format(PyExc_ValueError,
                                    "only bytes values expected, got %s",
                                    Py_TYPE(v)->tp_name);
                    goto error;
                }
                temp = v;
                Py_INCREF(v);
                pbuf = Bytes_AS_STRING(temp);
                len = Bytes_GET_SIZE(temp);
                break;
            default:
                PyErr_Format(PyExc_ValueError,
                  "unsupported format character '%c' (0x%x) "
                  "at index " FORMAT_CODE_PY_SSIZE_T,
                  c, c,
                  (Py_ssize_t)(fmt - 1 - Bytes_AS_STRING(format)));
                goto error;
            }
            if (width < len)
                width = len;
            if (rescnt < width) {
                reslen -= rescnt;
                rescnt = width + fmtcnt + 100;
                reslen += rescnt;
                if (reslen < 0) {
                    Py_DECREF(result);
                    Py_XDECREF(temp);
                    if (args_owned)
                        Py_DECREF(args);
                    return PyErr_NoMemory();
                }
                if (!(result = resize_bytes(result, reslen))) {
                    Py_XDECREF(temp);
                    if (args_owned)
                        Py_DECREF(args);
                    return NULL;
                }
                res = Bytes_AS_STRING(result)
                    + reslen - rescnt;
            }
            Py_MEMCPY(res, pbuf, len);
            res += len;
            rescnt -= len;
            while (--width >= len) {
                --rescnt;
                *res++ = ' ';
            }
            if (dict && (argidx < arglen) && c != '%') {
                PyErr_SetString(PyExc_TypeError,
                           "not all arguments converted during string formatting");
                Py_XDECREF(temp);
                goto error;
            }
            Py_XDECREF(temp);
        } /* '%' */
    } /* until end */
    if (argidx < arglen && !dict) {
        PyErr_SetString(PyExc_TypeError,
                        "not all arguments converted during string formatting");
        goto error;
    }
    if (args_owned) {
        Py_DECREF(args);
    }
    if (!(result = resize_bytes(result, reslen - rescnt))) {
        return NULL;
    }
    return result;

 error:
    Py_DECREF(result);
    if (args_owned) {
        Py_DECREF(args);
    }
    return NULL;
}
