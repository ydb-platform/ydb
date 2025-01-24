/* Copyright (c) 2015, Red Hat, Inc. and/or its affiliates
 * Licensed under the MIT license; see py3c.h
 */

/*
 * WARNING: These flags are only to be used in class definitions.
 *
 * Before including this header file, check that you do not use
 * these flags with with PyType_HasFeature. Example command:
 *      grep -r PyType_HasFeature .
 *
 * In Python 3, *all objects* have the features corresponding to removed flags.
 */

#ifndef _PY3C_TPFLAGS_H_
#define _PY3C_TPFLAGS_H_
#include <Python.h>

#if PY_MAJOR_VERSION >= 3

#define Py_TPFLAGS_HAVE_GETCHARBUFFER  0
#define Py_TPFLAGS_HAVE_SEQUENCE_IN 0
#define Py_TPFLAGS_HAVE_INPLACEOPS 0
#define Py_TPFLAGS_CHECKTYPES 0
#define Py_TPFLAGS_HAVE_RICHCOMPARE 0
#define Py_TPFLAGS_HAVE_WEAKREFS 0
#define Py_TPFLAGS_HAVE_ITER 0
#define Py_TPFLAGS_HAVE_CLASS 0
/* Py_TPFLAGS_HEAPTYPE is still optional in py3 */
/* Py_TPFLAGS_BASETYPE is still optional in py3 */
/* Py_TPFLAGS_READY is still useful in py3 */
/* Py_TPFLAGS_READYING is still useful in py3 */
/* Py_TPFLAGS_HAVE_GC is still optional in py3 */
/* Py_TPFLAGS_HAVE_STACKLESS_EXTENSION is still optional in py3 */
#define Py_TPFLAGS_HAVE_INDEX 0
/* Py_TPFLAGS_HAVE_VERSION_TAG is still optional in py3 */
/* Py_TPFLAGS_VALID_VERSION_TAG is still optional in py3 */
/* Py_TPFLAGS_IS_ABSTRACT is still optional in py3 */
#define Py_TPFLAGS_HAVE_NEWBUFFER 0
/* Py_TPFLAGS_INT_SUBCLASS is still optional in py3 */
/* Py_TPFLAGS_LONG_SUBCLASS is still optional in py3 */
/* Py_TPFLAGS_LIST_SUBCLASS is still optional in py3 */
/* Py_TPFLAGS_TUPLE_SUBCLASS is still optional in py3 */
/* Py_TPFLAGS_STRING_SUBCLASS is still optional in py3 */
/* Py_TPFLAGS_UNICODE_SUBCLASS is still optional in py3 */
/* Py_TPFLAGS_DICT_SUBCLASS is still optional in py3 */
/* Py_TPFLAGS_BASE_EXC_SUBCLASS is still optional in py3 */
/* Py_TPFLAGS_TYPE_SUBCLASS is still optional in py3 */

/* py 3.4 adds Py_TPFLAGS_HAVE_FINALIZE */
#endif
#endif /* _PY3C_TPFLAGS_H_ */
