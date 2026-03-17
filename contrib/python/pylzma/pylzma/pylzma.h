/*
 * Python Bindings for LZMA
 *
 * Copyright (c) 2004-2015 by Joachim Bauch, mail@joachim-bauch.de
 * 7-Zip Copyright (C) 1999-2010 Igor Pavlov
 * LZMA SDK Copyright (C) 1999-2010 Igor Pavlov
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * 
 * $Id$
 *
 */

#ifndef ___PYLZMA__H___
#define ___PYLZMA__H___

#include <Python.h>

#include "7zTypes.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

#define BLOCK_SIZE (128*1024)

#define CHECK_NULL(a) if ((a) == NULL) { PyErr_NoMemory(); goto exit; }
#define DEC_AND_NULL(a) { Py_XDECREF(a); a = NULL; }
#define DELETE_AND_NULL(a) if (a != NULL) { delete a; a = NULL; }
#define FREE_AND_NULL(a) if (a != NULL) { free(a); a = NULL; }
#define CHECK_RANGE(x, a, b, msg) if ((x) < (a) || (x) > (b)) { PyErr_SetString(PyExc_ValueError, msg); goto exit; }

#if defined(WITH_THREAD)
/* For Python 2.3 and above, use the PyGILState_ calls */
#if (PY_VERSION_HEX >= 0x02030000)
#define PYLZMA_USE_GILSTATE
#endif

#if defined(PYLZMA_USE_GILSTATE)
// Python 2.3 and above
#define START_BLOCK_THREADS \
    PyGILState_STATE __savestate = PyGILState_Ensure();
#define END_BLOCK_THREADS \
    PyGILState_Release(__savestate);
#else
// Before Python 2.3
PyInterpreterState* _pylzma_interpreterState;
#define START_BLOCK_THREADS {                                   \
        PyThreadState* prevState;                               \
        PyThreadState* newState;                                \
        PyEval_AcquireLock();                                   \
        newState  = PyThreadState_New(_pylzma_interpreterState);\
        prevState = PyThreadState_Swap(newState);

#define END_BLOCK_THREADS                                       \
        newState = PyThreadState_Swap(prevState);               \
        PyThreadState_Clear(newState);                          \
        PyEval_ReleaseLock();                                   \
        PyThreadState_Delete(newState);                         \
        }
#endif
#else  // WITH_THREADS
#define START_BLOCK_THREADS
#define END_BLOCK_THREADS
#endif

#if (PY_VERSION_HEX < 0x02060000)
#define PyBytes_Check(ob)                       PyString_Check(ob)
#define PyBytes_AS_STRING(ob)                   PyString_AS_STRING(ob)
#define PyBytes_GET_SIZE(ob)                    PyString_GET_SIZE(ob)
#define PyBytes_Size(ob)                        PyString_Size(ob)
#define PyBytes_FromString(data)                PyString_FromString(data)
#define PyBytes_FromStringAndSize(data, size)   PyString_FromStringAndSize(data, size)
#define _PyBytes_Resize(data, newsize)          _PyString_Resize(data, newsize)
#endif

#if !defined(Py_TYPE)
#define Py_TYPE(ob)     ((ob)->ob_type)
#endif

#if !defined(PyVarObject_HEAD_INIT)
#define PyVarObject_HEAD_INIT(ob, size)    \
    PyObject_HEAD_INIT(NULL) \
    0,
#endif

#if defined(PY_SSIZE_T_CLEAN) && (PY_VERSION_HEX >= 0x02040000)
#define PARSE_LENGTH_TYPE   Py_ssize_t
#define PARSE_LENGTH_FORMAT "%zd"
#else
#define PARSE_LENGTH_TYPE   int
#define PARSE_LENGTH_FORMAT "%d"
#endif

extern ISzAlloc allocator;

#endif
