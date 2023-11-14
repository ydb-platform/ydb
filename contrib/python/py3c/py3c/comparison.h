/* Copyright (c) 2015, Red Hat, Inc. and/or its affiliates
 * Licensed under the MIT license; see py3c.h
 */

#ifndef _PY3C_COMPARISON_H_
#define _PY3C_COMPARISON_H_
#include <Python.h>

/* Rich comparisons */

#ifndef Py_RETURN_NOTIMPLEMENTED
#define Py_RETURN_NOTIMPLEMENTED \
    return Py_INCREF(Py_NotImplemented), Py_NotImplemented
#endif

#ifndef Py_UNREACHABLE
#define Py_UNREACHABLE() abort()
#endif

#ifndef Py_RETURN_RICHCOMPARE
#define Py_RETURN_RICHCOMPARE(val1, val2, op)                               \
    do {                                                                    \
        switch (op) {                                                       \
        case Py_EQ: if ((val1) == (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_NE: if ((val1) != (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_LT: if ((val1) < (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;   \
        case Py_GT: if ((val1) > (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;   \
        case Py_LE: if ((val1) <= (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_GE: if ((val1) >= (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        default:                                                            \
            Py_UNREACHABLE();                                               \
        }                                                                   \
    } while (0)
#endif

#define PY3C_RICHCMP(val1, val2, op) \
    ((op) == Py_EQ) ? PyBool_FromLong((val1) == (val2)) : \
    ((op) == Py_NE) ? PyBool_FromLong((val1) != (val2)) : \
    ((op) == Py_LT) ? PyBool_FromLong((val1) < (val2)) : \
    ((op) == Py_GT) ? PyBool_FromLong((val1) > (val2)) : \
    ((op) == Py_LE) ? PyBool_FromLong((val1) <= (val2)) : \
    ((op) == Py_GE) ? PyBool_FromLong((val1) >= (val2)) : \
    (Py_INCREF(Py_NotImplemented), Py_NotImplemented)

#endif
