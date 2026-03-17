/* Licensed under the Apache License: http://www.apache.org/licenses/LICENSE-2.0 */
/* For details: https://github.com/coveragepy/coveragepy/blob/main/NOTICE.txt */

#ifndef _COVERAGE_UTIL_H
#define _COVERAGE_UTIL_H

#include <Python.h>
#include <stdatomic.h>

/* Compile-time debugging helpers */
#undef WHAT_LOG         /* Define to log the WHAT params in the trace function. */
#undef TRACE_LOG        /* Define to log our bookkeeping. */
#undef COLLECT_STATS    /* Collect counters: stats are printed when tracer is stopped. */
#undef DO_NOTHING       /* Define this to make the tracer do nothing. */

#if PY_VERSION_HEX >= 0x030B00A0
// 3.11 moved f_lasti into an internal structure. This is totally the wrong way
// to make this work, but it's all I've got until https://bugs.python.org/issue40421
// is resolved.
#if PY_VERSION_HEX < 0x030D0000
#include <internal/pycore_frame.h>
#endif

#if PY_VERSION_HEX >= 0x030B00A7
#define MyFrame_GetLasti(f)     (PyFrame_GetLasti(f))
#else
#define MyFrame_GetLasti(f)     ((f)->f_frame->f_lasti * 2)
#endif
#else
// The f_lasti field changed meaning in 3.10.0a7. It had been bytes, but
// now is instructions, so we need to adjust it to use it as a byte index.
#define MyFrame_GetLasti(f)     ((f)->f_lasti * 2)
#endif

#if PY_VERSION_HEX >= 0x030D0000
#define MyFrame_SetTrace(f, obj)    (PyObject_SetAttrString((PyObject*)(f), "f_trace", (PyObject*)(obj)))
#else
#define MyFrame_SetTrace(f, obj)    {Py_INCREF(obj); Py_XSETREF((f)->f_trace, (PyObject*)(obj));}
#endif

#if PY_VERSION_HEX >= 0x030B00B1
#define MyCode_GetCode(co)      (PyCode_GetCode(co))
#define MyCode_FreeCode(code)   Py_XDECREF(code)
#elif PY_VERSION_HEX >= 0x030B00A7
#define MyCode_GetCode(co)      (PyObject_GetAttrString((PyObject *)(co), "co_code"))
#define MyCode_FreeCode(code)   Py_XDECREF(code)
#else
#define MyCode_GetCode(co)      ((co)->co_code)
#define MyCode_FreeCode(code)
#endif

// Where does frame.f_lasti point when yielding from a generator?
// It used to point at the YIELD, in 3.13 it points at the RESUME,
// then it went back to the YIELD.
// https://github.com/python/cpython/issues/113728
#define ENV_LASTI_IS_YIELD ((PY_VERSION_HEX & 0xFFFF0000) != 0x030D0000)

/* The values returned to indicate ok or error. */
#define RET_OK      0
#define RET_ERROR   -1

/* Nicer booleans */
typedef int BOOL;
#define FALSE   0
#define TRUE    1

#if SIZEOF_LONG_LONG < 8
#error long long too small!
#endif
typedef unsigned long long uint64;

/* Only for extreme machete-mode debugging! */
#define CRASH       { printf("*** CRASH! ***\n"); *((int*)1) = 1; }

#endif /* _COVERAGE_UTIL_H */
