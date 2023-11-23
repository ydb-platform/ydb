/* Copyright (c) 2015, Red Hat, Inc. and/or its affiliates
 * Licensed under the MIT license; see py3c.h
 */

#ifndef _PY3C_COMPAT_H_
#define _PY3C_COMPAT_H_
#include <Python.h>
#include <assert.h>

/* Mark a function as `static inline`.
 * Before C99, `inline` is not available, so use just `static` and silence
 * "unused definition" warnings on some compilers.
 */
#if __STDC_VERSION__ >= 199901L
#define _py3c_STATIC_INLINE_FUNCTION(d) static inline d
#elif defined(__GNUC__) || defined(__clang__)
#define _py3c_STATIC_INLINE_FUNCTION(d) static d __attribute__ ((unused)); static d
#else
#define _py3c_STATIC_INLINE_FUNCTION(d) static d
#endif

#if PY_MAJOR_VERSION >= 3

/***** Python 3 *****/

#define IS_PY3 1

/* Strings */

#define PyStr_Type PyUnicode_Type
#define PyStr_Check PyUnicode_Check
#define PyStr_CheckExact PyUnicode_CheckExact
#define PyStr_FromString PyUnicode_FromString
#define PyStr_FromStringAndSize PyUnicode_FromStringAndSize
#define PyStr_FromFormat PyUnicode_FromFormat
#define PyStr_FromFormatV PyUnicode_FromFormatV
#define PyStr_AsString PyUnicode_AsUTF8
#define PyStr_Concat PyUnicode_Concat
#define PyStr_Format PyUnicode_Format
#define PyStr_InternInPlace PyUnicode_InternInPlace
#define PyStr_InternFromString PyUnicode_InternFromString
#define PyStr_Decode PyUnicode_Decode

#define PyStr_AsUTF8String PyUnicode_AsUTF8String /* returns PyBytes */
#define PyStr_AsUTF8 PyUnicode_AsUTF8
#define PyStr_AsUTF8AndSize PyUnicode_AsUTF8AndSize

/* Ints */

#define PyInt_Type PyLong_Type
#define PyInt_Check PyLong_Check
#define PyInt_CheckExact PyLong_CheckExact
#define PyInt_FromString PyLong_FromString
#define PyInt_FromLong PyLong_FromLong
#define PyInt_FromSsize_t PyLong_FromSsize_t
#define PyInt_FromSize_t PyLong_FromSize_t
#define PyInt_AsLong PyLong_AsLong
#define PyInt_AS_LONG PyLong_AS_LONG
#define PyInt_AsUnsignedLongLongMask PyLong_AsUnsignedLongLongMask
#define PyInt_AsSsize_t PyLong_AsSsize_t

/* Module init */

#define MODULE_INIT_FUNC(name) \
    PyMODINIT_FUNC PyInit_ ## name(void); \
    PyMODINIT_FUNC PyInit_ ## name(void)

#else

/***** Python 2 *****/

#define IS_PY3 0

/* Strings */

#define PyStr_Type PyString_Type
#define PyStr_Check PyString_Check
#define PyStr_CheckExact PyString_CheckExact
#define PyStr_FromString PyString_FromString
#define PyStr_FromStringAndSize PyString_FromStringAndSize
#define PyStr_FromFormat PyString_FromFormat
#define PyStr_FromFormatV PyString_FromFormatV
#define PyStr_AsString PyString_AsString
#define PyStr_Format PyString_Format
#define PyStr_InternInPlace PyString_InternInPlace
#define PyStr_InternFromString PyString_InternFromString
#define PyStr_Decode PyString_Decode

_py3c_STATIC_INLINE_FUNCTION(PyObject *PyStr_Concat(PyObject *left, PyObject *right)) {
    PyObject *str = left;
    Py_INCREF(left);  /* reference to old left will be stolen */
    PyString_Concat(&str, right);
    if (str) {
        return str;
    } else {
        return NULL;
    }
}

#define PyStr_AsUTF8String(str) (Py_INCREF(str), (str))
#define PyStr_AsUTF8 PyString_AsString
#define PyStr_AsUTF8AndSize(pystr, sizeptr) \
    ((*sizeptr=PyString_Size(pystr)), PyString_AsString(pystr))

#define PyBytes_Type PyString_Type
#define PyBytes_Check PyString_Check
#define PyBytes_CheckExact PyString_CheckExact
#define PyBytes_FromString PyString_FromString
#define PyBytes_FromStringAndSize PyString_FromStringAndSize
#define PyBytes_FromFormat PyString_FromFormat
#define PyBytes_FromFormatV PyString_FromFormatV
#define PyBytes_Size PyString_Size
#define PyBytes_GET_SIZE PyString_GET_SIZE
#define PyBytes_AsString PyString_AsString
#define PyBytes_AS_STRING PyString_AS_STRING
#define PyBytes_AsStringAndSize PyString_AsStringAndSize
#define PyBytes_Concat PyString_Concat
#define PyBytes_ConcatAndDel PyString_ConcatAndDel
#define _PyBytes_Resize _PyString_Resize

/* Floats */

#define PyFloat_FromString(str) PyFloat_FromString(str, NULL)

/* Module init */

#define PyModuleDef_HEAD_INIT 0

typedef struct PyModuleDef {
    int m_base;
    const char* m_name;
    const char* m_doc;
    Py_ssize_t m_size;
    PyMethodDef *m_methods;
    void* m_slots;
    void* m_traverse;
    void* m_clear;
    void* m_free;
} PyModuleDef;

_py3c_STATIC_INLINE_FUNCTION(PyObject *PyModule_Create(PyModuleDef *def)) {
    assert(!def->m_slots);
    assert(!def->m_traverse);
    assert(!def->m_clear);
    assert(!def->m_free);
    return Py_InitModule3(def->m_name, def->m_methods, def->m_doc);
}

#define MODULE_INIT_FUNC(name) \
    static PyObject *PyInit_ ## name(void); \
    PyMODINIT_FUNC init ## name(void); \
    PyMODINIT_FUNC init ## name(void) { PyInit_ ## name(); } \
    static PyObject *PyInit_ ## name(void)


#endif

#endif
