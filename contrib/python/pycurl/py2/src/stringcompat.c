#include "pycurl.h"

/*************************************************************************
// python utility functions
**************************************************************************/

PYCURL_INTERNAL int
PyText_AsStringAndSize(PyObject *obj, char **buffer, Py_ssize_t *length, PyObject **encoded_obj)
{
    if (PyByteStr_Check(obj)) {
        *encoded_obj = NULL;
        return PyByteStr_AsStringAndSize(obj, buffer, length);
    } else {
        int rv;
        assert(PyUnicode_Check(obj));
        *encoded_obj = PyUnicode_AsEncodedString(obj, "ascii", "strict");
        if (*encoded_obj == NULL) {
            return -1;
        }
        rv = PyByteStr_AsStringAndSize(*encoded_obj, buffer, length);
        if (rv != 0) {
            /* If we free the object, pointer must be reset to NULL */
            Py_CLEAR(*encoded_obj);
        }
        return rv;
    }
}


/* Like PyString_AsString(), but set an exception if the string contains
 * embedded NULs. Actually PyString_AsStringAndSize() already does that for
 * us if the `len' parameter is NULL - see Objects/stringobject.c.
 */

PYCURL_INTERNAL char *
PyText_AsString_NoNUL(PyObject *obj, PyObject **encoded_obj)
{
    char *s = NULL;
    Py_ssize_t r;
    r = PyText_AsStringAndSize(obj, &s, NULL, encoded_obj);
    if (r != 0)
        return NULL;    /* exception already set */
    assert(s != NULL);
    return s;
}


/* Returns true if the object is of a type that can be given to
 * curl_easy_setopt and such - either a byte string or a Unicode string
 * with ASCII code points only.
 */
#if PY_MAJOR_VERSION >= 3
PYCURL_INTERNAL int
PyText_Check(PyObject *o)
{
    return PyUnicode_Check(o) || PyBytes_Check(o);
}
#else
PYCURL_INTERNAL int
PyText_Check(PyObject *o)
{
    return PyUnicode_Check(o) || PyString_Check(o);
}
#endif

PYCURL_INTERNAL PyObject *
PyText_FromString_Ignore(const char *string)
{
    PyObject *v;

#if PY_MAJOR_VERSION >= 3
    PyObject *u;
    
    v = Py_BuildValue("y", string);
    if (v == NULL) {
        return NULL;
    }
    
    u = PyUnicode_FromEncodedObject(v, NULL, "replace");
    Py_DECREF(v);
    return u;
#else
    v = Py_BuildValue("s", string);
    return v;
#endif
}
