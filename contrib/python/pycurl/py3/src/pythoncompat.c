#include "pycurl.h"

#if PY_MAJOR_VERSION >= 3

PYCURL_INTERNAL PyObject *
my_getattro(PyObject *co, PyObject *name, PyObject *dict1, PyObject *dict2, PyMethodDef *m)
{
    PyObject *v = NULL;
    if( dict1 != NULL )
        v = PyDict_GetItem(dict1, name);
    if( v == NULL && dict2 != NULL )
        v = PyDict_GetItem(dict2, name);
    if( v != NULL )
    {
        Py_INCREF(v);
        return v;
    }
    PyErr_Format(PyExc_AttributeError, "trying to obtain a non-existing attribute: %U", name);
    return NULL;
}

PYCURL_INTERNAL int
my_setattro(PyObject **dict, PyObject *name, PyObject *v)
{
    if( *dict == NULL )
    {
        *dict = PyDict_New();
        if( *dict == NULL )
            return -1;
    }
    if (v != NULL)
        return PyDict_SetItem(*dict, name, v);
    else {
        int v = PyDict_DelItem(*dict, name);
        if (v != 0) {
            /* need to convert KeyError to AttributeError */
            if (PyErr_ExceptionMatches(PyExc_KeyError)) {
                PyErr_Format(PyExc_AttributeError, "trying to delete a non-existing attribute: %U", name);
            }
        }
        return v;
    }
}

#else /* PY_MAJOR_VERSION >= 3 */

PYCURL_INTERNAL int
my_setattr(PyObject **dict, char *name, PyObject *v)
{
    if (v == NULL) {
        int rv = -1;
        if (*dict != NULL)
            rv = PyDict_DelItemString(*dict, name);
        if (rv < 0)
            PyErr_Format(PyExc_AttributeError, "trying to delete a non-existing attribute: %s", name);
        return rv;
    }
    if (*dict == NULL) {
        *dict = PyDict_New();
        if (*dict == NULL)
            return -1;
    }
    return PyDict_SetItemString(*dict, name, v);
}

PYCURL_INTERNAL PyObject *
my_getattr(PyObject *co, char *name, PyObject *dict1, PyObject *dict2, PyMethodDef *m)
{
    PyObject *v = NULL;
    if (v == NULL && dict1 != NULL)
        v = PyDict_GetItemString(dict1, name);
    if (v == NULL && dict2 != NULL)
        v = PyDict_GetItemString(dict2, name);
    if (v != NULL) {
        Py_INCREF(v);
        return v;
    }
    return Py_FindMethod(m, co, name);
}

#endif /* PY_MAJOR_VERSION >= 3 */

PYCURL_INTERNAL int
PyListOrTuple_Check(PyObject *v)
{
    int result;
    
    if (PyList_Check(v)) {
        result = PYLISTORTUPLE_LIST;
    } else if (PyTuple_Check(v)) {
        result = PYLISTORTUPLE_TUPLE;
    } else {
        result = PYLISTORTUPLE_OTHER;
    }
    
    return result;
}

PYCURL_INTERNAL Py_ssize_t
PyListOrTuple_Size(PyObject *v, int which)
{
    switch (which) {
    case PYLISTORTUPLE_LIST:
        return PyList_Size(v);
    case PYLISTORTUPLE_TUPLE:
        return PyTuple_Size(v);
    default:
        assert(0);
        return 0;
    }
}

PYCURL_INTERNAL PyObject *
PyListOrTuple_GetItem(PyObject *v, Py_ssize_t i, int which)
{
    switch (which) {
    case PYLISTORTUPLE_LIST:
        return PyList_GetItem(v, i);
    case PYLISTORTUPLE_TUPLE:
        return PyTuple_GetItem(v, i);
    default:
        assert(0);
        return NULL;
    }
}

/* vi:ts=4:et:nowrap
 */
