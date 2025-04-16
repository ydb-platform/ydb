#ifndef _MULTIDICT_ISTR_H
#define _MULTIDICT_ISTR_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    PyUnicodeObject str;
    PyObject * canonical;
} istrobject;

PyDoc_STRVAR(istr__doc__, "istr class implementation");

static PyTypeObject istr_type;

static inline void
istr_dealloc(istrobject *self)
{
    Py_XDECREF(self->canonical);
    PyUnicode_Type.tp_dealloc((PyObject*)self);
}

static inline PyObject *
istr_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyObject *x = NULL;
    static char *kwlist[] = {"object", "encoding", "errors", 0};
    PyObject *encoding = NULL;
    PyObject *errors = NULL;
    PyObject *canonical = NULL;
    PyObject * ret = NULL;
    if (kwds != NULL) {
        int cmp = PyDict_Pop(kwds, multidict_str_canonical, &canonical);
        if (cmp < 0) {
            return NULL;
        } else if (cmp > 0) {
            Py_INCREF(canonical);
        }
    }

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|OOO:str",
                                     kwlist, &x, &encoding, &errors)) {
        return NULL;
    }
    if (x != NULL && Py_TYPE(x) == &istr_type) {
        Py_INCREF(x);
        return x;
    }
    ret = PyUnicode_Type.tp_new(type, args, kwds);
    if (!ret) {
        goto fail;
    }

    if (canonical == NULL) {
        canonical = PyObject_CallMethodNoArgs(ret, multidict_str_lower);
        if (!canonical) {
            goto fail;
        }
    }
    if (!PyUnicode_CheckExact(canonical)) {
        PyObject *tmp = PyUnicode_FromObject(canonical);
        Py_CLEAR(canonical);
        if (tmp == NULL) {
            goto fail;
        }
        canonical = tmp;
    }
    ((istrobject*)ret)->canonical = canonical;
    return ret;
fail:
    Py_XDECREF(ret);
    return NULL;
}


static inline PyObject *
istr_reduce(PyObject *self)
{
    PyObject *str = NULL;
    PyObject *args = NULL;
    PyObject *result = NULL;

    str = PyUnicode_FromObject(self);
    if (str == NULL) {
        goto ret;
    }
    args = PyTuple_Pack(1, str);
    if (args == NULL) {
        goto ret;
    }
    result = PyTuple_Pack(2, Py_TYPE(self), args);
ret:
    Py_CLEAR(str);
    Py_CLEAR(args);
    return result;
}


static PyMethodDef istr_methods[] = {
    {"__reduce__", (PyCFunction)istr_reduce, METH_NOARGS, NULL},
    {NULL, NULL}   /* sentinel */
};

static PyTypeObject istr_type = {
    PyVarObject_HEAD_INIT(DEFERRED_ADDRESS(&PyType_Type), 0)
    "multidict._multidict.istr",
    sizeof(istrobject),
    .tp_dealloc = (destructor)istr_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT
              | Py_TPFLAGS_BASETYPE
              | Py_TPFLAGS_UNICODE_SUBCLASS,
    .tp_doc = istr__doc__,
    .tp_base = DEFERRED_ADDRESS(&PyUnicode_Type),
    .tp_methods = istr_methods,
    .tp_new = (newfunc)istr_new,
};


static inline PyObject *
IStr_New(PyObject *str, PyObject *canonical)
{
    PyObject *args = NULL;
    PyObject *kwds = NULL;
    PyObject *res = NULL;

    args = PyTuple_Pack(1, str);
    if (args == NULL) {
        goto ret;
    }

    if (canonical != NULL) {
        kwds = PyDict_New();
        if (kwds == NULL) {
            goto ret;
        }
        if (!PyUnicode_CheckExact(canonical)) {
            PyErr_SetString(PyExc_TypeError,
                            "'canonical' argument should be exactly str");
            goto ret;
        }
        if (PyDict_SetItem(kwds, multidict_str_canonical, canonical) < 0) {
            goto ret;
        }
    }

    res = istr_new(&istr_type, args, kwds);
ret:
    Py_CLEAR(args);
    Py_CLEAR(kwds);
    return res;
}

static inline int
istr_init(void)
{
    istr_type.tp_base = &PyUnicode_Type;
    if (PyType_Ready(&istr_type) < 0) {
        return -1;
    }
    return 0;
}

#ifdef __cplusplus
}
#endif
#endif
