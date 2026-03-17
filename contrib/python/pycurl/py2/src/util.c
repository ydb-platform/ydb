#include "pycurl.h"

static PyObject *
create_error_object(CurlObject *self, int code)
{
    PyObject *s, *v;
    
    s = PyText_FromString_Ignore(self->error);
    if (s == NULL) {
        return NULL;
    }
    v = Py_BuildValue("(iO)", code, s);
    if (v == NULL) {
        Py_DECREF(s);
        return NULL;
    }
    return v;
}

PYCURL_INTERNAL void
create_and_set_error_object(CurlObject *self, int code)
{
    PyObject *e;
    
    self->error[sizeof(self->error) - 1] = 0;
    e = create_error_object(self, code);
    if (e != NULL) {
        PyErr_SetObject(ErrorObject, e);
        Py_DECREF(e);
    }
}
