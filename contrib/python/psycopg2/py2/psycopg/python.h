/* python.h - python version compatibility stuff
 *
 * Copyright (C) 2003-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020 The Psycopg Team
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

#ifndef PSYCOPG_PYTHON_H
#define PSYCOPG_PYTHON_H 1

#define PY_2 (PY_MAJOR_VERSION == 2)
#define PY_3 (PY_MAJOR_VERSION == 3)

#if PY_2 && PY_VERSION_HEX < 0x02070000
#error "psycopg requires Python 2.7"
#endif

#if PY_3 && PY_VERSION_HEX < 0x03040000
#error "psycopg requires Python 3.4"
#endif

#include <structmember.h>
#if PY_2
#include <stringobject.h>
#endif

/* hash() return size changed around version 3.2a4 on 64bit platforms.  Before
 *   this, the return size was always a long, regardless of arch.  ~3.2
 *   introduced the Py_hash_t & Py_uhash_t typedefs with the resulting sizes
 *   based upon arch. */
#if PY_VERSION_HEX < 0x030200A4
typedef long Py_hash_t;
typedef unsigned long Py_uhash_t;
#endif

/* Since Py_TYPE() is changed to the inline static function,
 * Py_TYPE(obj) = new_type must be replaced with Py_SET_TYPE(obj, new_type)
 * https://docs.python.org/3.10/whatsnew/3.10.html#id2
 */
#if PY_VERSION_HEX < 0x030900A4
  #define Py_SET_TYPE(obj, type) ((Py_TYPE(obj) = (type)), (void)0)
#endif

/* FORMAT_CODE_PY_SSIZE_T is for Py_ssize_t: */
#define FORMAT_CODE_PY_SSIZE_T "%" PY_FORMAT_SIZE_T "d"

/* FORMAT_CODE_SIZE_T is for plain size_t, not for Py_ssize_t: */
#ifdef _MSC_VER
  /* For MSVC: */
  #define FORMAT_CODE_SIZE_T "%Iu"
#else
  /* C99 standard format code: */
  #define FORMAT_CODE_SIZE_T "%zu"
#endif

#if PY_2

#define Text_Type PyString_Type
#define Text_Check(s) PyString_Check(s)
#define Text_Format(f,a) PyString_Format(f,a)
#define Text_FromUTF8(s) PyString_FromString(s)
#define Text_FromUTF8AndSize(s,n) PyString_FromStringAndSize(s,n)

#define Bytes_Type PyString_Type
#define Bytes_Check PyString_Check
#define Bytes_CheckExact PyString_CheckExact
#define Bytes_AS_STRING PyString_AS_STRING
#define Bytes_GET_SIZE PyString_GET_SIZE
#define Bytes_Size PyString_Size
#define Bytes_AsString PyString_AsString
#define Bytes_AsStringAndSize PyString_AsStringAndSize
#define Bytes_FromString PyString_FromString
#define Bytes_FromStringAndSize PyString_FromStringAndSize
#define Bytes_FromFormat PyString_FromFormat
#define Bytes_ConcatAndDel PyString_ConcatAndDel
#define _Bytes_Resize _PyString_Resize

#define PyDateTime_DELTA_GET_DAYS(o)         (((PyDateTime_Delta*)o)->days)
#define PyDateTime_DELTA_GET_SECONDS(o)      (((PyDateTime_Delta*)o)->seconds)
#define PyDateTime_DELTA_GET_MICROSECONDS(o) (((PyDateTime_Delta*)o)->microseconds)

#define INIT_MODULE(m) init ## m

/* fix #961, but don't change all types to longs. Sure someone will complain. */
#define PyLong_FromOid(x) (((x) & 0x80000000) ? \
    PyLong_FromUnsignedLong((unsigned long)(x)) : \
    PyInt_FromLong((x)))

#endif  /* PY_2 */

#if PY_3

#define Text_Type PyUnicode_Type
#define Text_Check(s) PyUnicode_Check(s)
#define Text_Format(f,a) PyUnicode_Format(f,a)
#define Text_FromUTF8(s) PyUnicode_FromString(s)
#define Text_FromUTF8AndSize(s,n) PyUnicode_FromStringAndSize(s,n)

#define PyInt_Type             PyLong_Type
#define PyInt_Check            PyLong_Check
#define PyInt_AsLong           PyLong_AsLong
#define PyInt_FromLong         PyLong_FromLong
#define PyInt_FromString       PyLong_FromString
#define PyInt_FromSsize_t      PyLong_FromSsize_t
#define PyExc_StandardError    PyExc_Exception
#define PyString_FromFormat    PyUnicode_FromFormat
#define Py_TPFLAGS_HAVE_ITER   0L
#define Py_TPFLAGS_HAVE_RICHCOMPARE 0L
#define Py_TPFLAGS_HAVE_WEAKREFS 0L

#ifndef PyNumber_Int
#define PyNumber_Int           PyNumber_Long
#endif

#define Bytes_Type PyBytes_Type
#define Bytes_Check PyBytes_Check
#define Bytes_CheckExact PyBytes_CheckExact
#define Bytes_AS_STRING PyBytes_AS_STRING
#define Bytes_GET_SIZE PyBytes_GET_SIZE
#define Bytes_Size PyBytes_Size
#define Bytes_AsString PyBytes_AsString
#define Bytes_AsStringAndSize PyBytes_AsStringAndSize
#define Bytes_FromString PyBytes_FromString
#define Bytes_FromStringAndSize PyBytes_FromStringAndSize
#define Bytes_FromFormat PyBytes_FromFormat
#define Bytes_ConcatAndDel PyBytes_ConcatAndDel
#define _Bytes_Resize _PyBytes_Resize

#define INIT_MODULE(m) PyInit_ ## m

#define PyLong_FromOid(x) (PyLong_FromUnsignedLong((unsigned long)(x)))

#endif  /* PY_3 */

/* expose Oid attributes in Python C objects */
#define T_OID T_UINT

#endif /* !defined(PSYCOPG_PYTHON_H) */
