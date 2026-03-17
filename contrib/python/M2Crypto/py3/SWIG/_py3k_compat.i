%{
#if PY_MAJOR_VERSION >= 3

FILE* PyFile_AsFile(PyObject *pyfile) {
    FILE* fp;
    int fd;
    const char *mode_str = NULL;
    PyObject *mode_obj;

    if ((fd = PyObject_AsFileDescriptor(pyfile)) == -1) {
        PyErr_SetString(PyExc_BlockingIOError,
                        "Cannot find file handler for the Python file!");
        return NULL;
    }

    if ((mode_obj = PyObject_GetAttrString(pyfile, "mode")) == NULL) {
        mode_str = "rb";
        PyErr_Clear();
    }
    else {
        /* convert to plain string
         * note that error checking is embedded in the function
         */
        mode_str = PyUnicode_AsUTF8AndSize(mode_obj, NULL);
    }

    if((fp = fdopen(fd, mode_str)) == NULL) {
         PyErr_SetFromErrno(PyExc_IOError);
    }

    Py_XDECREF(mode_obj);
    return fp;
}

#else /* PY2K */

#ifndef PyLong_FromLong
#define PyLong_FromLong(x) PyInt_FromLong(x)
#endif
#ifndef PyUnicode_AsUTF8
#define PyUnicode_AsUTF8(x) PyString_AsString(x)
#endif
#ifndef PyUnicode_FromString
#define PyUnicode_FromString(x) PyString_FromString(x)
#endif
#ifndef PyUnicode_Format
#define PyUnicode_Format(x, y) PyString_Format(x, y)
#endif

#endif /* PY_MAJOR_VERSION */
%}
