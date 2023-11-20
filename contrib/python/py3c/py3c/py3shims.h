/* Copyright (c) 2016, Red Hat, Inc. and/or its affiliates
 * Licensed under the MIT license; see py3c.h
 */

/*
 * Shims for new functionality from in Python 3.3+
 *
 * See https://docs.python.org/3/c-api/memory.html#raw-memory-interface
 */

#ifndef _PY3C_RAWMALLOC_H_
#define _PY3C_RAWMALLOC_H_
#include <Python.h>
#include <stdlib.h>


/* Py_UNUSED - added in Python 3.4, documneted in 3.7 */

#ifndef Py_UNUSED
#ifdef __GNUC__
#define Py_UNUSED(name) _unused_ ## name __attribute__((unused))
#else
#define Py_UNUSED(name) _unused_ ## name
#endif
#endif


/* PyMem_Raw{Malloc,Realloc,Free} - added in Python 3.4 */

#if PY_MAJOR_VERSION < 3 || (PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 4)
#define PyMem_RawMalloc(n) malloc((n) || 1)
#define PyMem_RawRealloc(p, n) realloc(p, (n) || 1)
#define PyMem_RawFree(p) free(p)
#endif /* version < 3.4 */


/* PyMem_RawCalloc - added in Python 3.5 */

#if PY_MAJOR_VERSION < 3 || (PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 5)
#define PyMem_RawCalloc(n, s) calloc((n) || 1, (s) || 1)
#endif /* version < 3.5 */


#endif /* _PY3C_RAWMALLOC_H_ */
