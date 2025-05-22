/* Copyright (c) 2015, Red Hat, Inc. and/or its affiliates
 * Licensed under the MIT license; see py3c.h
 */

#ifndef _PY3C_FILESHIM_H_
#define _PY3C_FILESHIM_H_
#include <Python.h>
#include <py3c/compat.h>

/*

For debugging purposes only.
Caveats:
 * Only works on file-like objects backed by an actual file
 * All C-level writes should be done before additional
   Python-level writes are allowed (e.g. by running Python code).
 * Though the function tries to flush, there is no guarantee that
   writes will be reordered due to different layers of buffering.

*/

static char FLUSH[] = "flush";
static char EMPTY_STRING[] = "";

_py3c_STATIC_INLINE_FUNCTION(FILE* py3c_PyFile_AsFileWithMode(PyObject *py_file, const char *mode)) {
    FILE *f;
    PyObject *ret;
    int fd;

    ret = PyObject_CallMethod(py_file, FLUSH, EMPTY_STRING);
    if (ret == NULL) {
        return NULL;
    }
    Py_DECREF(ret);

    fd = PyObject_AsFileDescriptor(py_file);
    if (fd == -1) {
        return NULL;
    }

        f = fdopen(fd, mode);
    if (f == NULL) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    return f;
}

#endif /* _PY3C_FILESHIM_H_ */
