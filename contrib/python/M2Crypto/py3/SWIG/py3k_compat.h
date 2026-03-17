#ifndef PY3K_COMPAT_H
#define PY3K_COMPAT_H

#if PY_MAJOR_VERSION >= 3

FILE* PyFile_AsFile(PyObject *p);
PyObject* PyFile_Name(PyObject *p);

#else /* PY2K */

/* Concerning PyBytes* functions:
 *
 * Python 3’s str() type is equivalent to Python 2’s unicode(); the
 * C functions are called PyUnicode_* for both. The old 8-bit string
 * type has become bytes(), with C functions called PyBytes_*. Python
 * 2.6 and later provide a compatibility header, bytesobject.h, mapping
 * PyBytes names to PyString ones. For best compatibility with Python 3,
 * PyUnicode should be used for textual data and PyBytes for binary
 * data. It’s also important to remember that PyBytes and PyUnicode in
 * Python 3 are not interchangeable like PyString and PyUnicode are in
 * Python 2. The following example shows best practices with regards to
 * PyUnicode, PyString, and PyBytes.
 *
 * From https://docs.python.org/2.7/howto/cporting.html
 */

PyObject* PyLong_FromLong(long x);
const char* PyUnicode_AsUTF8(PyObject *unicode);

#endif /* PY_MAJOR_VERSION */

#endif /* PY3K_COMPAT_H */
