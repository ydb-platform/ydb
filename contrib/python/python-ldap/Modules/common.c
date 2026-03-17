/* Miscellaneous common routines
 * See https://www.python-ldap.org/ for details. */

#include "common.h"

/* dynamically add the methods into the module dictionary d */

void
LDAPadd_methods(PyObject *d, PyMethodDef *methods)
{
    PyMethodDef *meth;

    for (meth = methods; meth->ml_meth; meth++) {
        PyObject *f = PyCFunction_New(meth, NULL);

        PyDict_SetItemString(d, meth->ml_name, f);
        Py_DECREF(f);
    }
}

/* Raise TypeError with custom message and object */
PyObject *
LDAPerror_TypeError(const char *msg, PyObject *obj)
{
    PyObject *args = Py_BuildValue("sO", msg, obj);

    if (args == NULL) {
        return NULL;
    }
    PyErr_SetObject(PyExc_TypeError, args);
    Py_DECREF(args);
    return NULL;
}
