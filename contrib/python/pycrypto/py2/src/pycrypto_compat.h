/*
 *  pycrypto_compat.h: Compatibility with older versions of Python
 *
 * Written in 2008 by Dwayne C. Litzenberger <dlitz@dlitz.net>
 *
 * ===================================================================
 * The contents of this file are dedicated to the public domain.  To
 * the extent that dedication to the public domain is not available,
 * everyone is granted a worldwide, perpetual, royalty-free,
 * non-exclusive license to exercise all rights associated with the
 * contents of this file for any purpose whatsoever.
 * No rights are reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * ===================================================================
 */
#ifndef PYCRYPTO_COMPAT_H
#define PYCRYPTO_COMPAT_H
#include "Python.h"

/*
 * Python 3.x defines, for conditional compiles
 */
 
#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#else
#define PyBytes_GET_SIZE PyString_GET_SIZE
#define PyBytes_FromStringAndSize PyString_FromStringAndSize
#define PyBytes_AS_STRING PyString_AS_STRING
#define PyBytes_Check PyString_Check
#define PyBytes_Size PyString_Size
#define PyBytes_AsString PyString_AsString
#define PyBytesObject PyStringObject
#if PY_MINOR_VERSION <= 5 /* PyUnicode_FromString exists from Python 2.6 on up */
#define PyUnicode_FromString PyString_FromString
#endif
#endif

/*
 * Py_CLEAR for Python < 2.4
 * See http://docs.python.org/api/countingRefs.html
 */
#if PY_VERSION_HEX < 0x02040000 && !defined(Py_CLEAR)
#define Py_CLEAR(obj) \
    do {\
        PyObject *tmp = (PyObject *)(obj);\
        (obj) = NULL;\
        Py_XDECREF(tmp);\
    } while(0)
#endif

/*
 * Compatibility code for Python < 2.5 (see PEP 353)
 * PEP 353 has been placed into the public domain, so we can use this code
 * without restriction.
 */
#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

/* Compatibility code for Python < 2.3 */
#if PY_VERSION_HEX < 0x02030000
typedef void PyMODINIT_FUNC;
#endif

#endif /* PYCRYPTO_COMPAT_H */
/* vim:set ts=4 sw=4 sts=4 expandtab: */
