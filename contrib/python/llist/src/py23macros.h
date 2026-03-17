/* Copyright (c) 2011-2018 Adam Jakubek, Rafał Gałczyński
 * Released under the MIT license (see attached LICENSE file).
 */

#ifndef MACROS_H
#define MACROS_H

#include <Python.h>


#if PY_MAJOR_VERSION >= 3

#define Py23String_FromString               PyUnicode_FromString

#define Py23String_Concat(left, right)                      \
    do {                                                    \
        PyObject* tmpConcatString;                          \
        tmpConcatString = PyUnicode_Concat(*left, right);   \
        Py_DECREF(*left);                                   \
        *left = tmpConcatString;                            \
    } while (0)

#define Py23String_ConcatAndDel(left, right)                \
    do {                                                    \
        PyObject* tmpConcatString;                          \
        tmpConcatString = PyUnicode_Concat(*left, right);   \
        Py_DECREF(*left);                                   \
        Py_DECREF(right);                                   \
        *left = tmpConcatString;                            \
    } while (0)

#define Py23Int_Check       PyLong_Check
#define Py23Int_AsSsize_t   PyLong_AsSsize_t

#else

#define Py23String_FromString       PyString_FromString
#define Py23String_Concat           PyString_Concat
#define Py23String_ConcatAndDel     PyString_ConcatAndDel

#define Py23Int_Check       PyInt_Check
#define Py23Int_AsSsize_t   PyInt_AsSsize_t

#endif /* PY_MAJOR_VERSION >= 3 */

#endif /* MACROS_H */
