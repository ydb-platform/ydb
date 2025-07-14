#ifndef _MULTIDICT_ISTR_H
#define _MULTIDICT_ISTR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "state.h"

typedef struct {
    PyUnicodeObject str;
    PyObject *canonical;
    mod_state *state;
} istrobject;

#define IStr_CheckExact(state, obj) Py_IS_TYPE(obj, state->IStrType)
#define IStr_Check(state, obj) \
    (IStr_CheckExact(state, obj) || PyObject_TypeCheck(obj, state->IStrType))

PyDoc_STRVAR(istr__doc__, "istr class implementation");

static inline void
istr_dealloc(istrobject *self)
{
    Py_XDECREF(self->canonical);
    PyUnicode_Type.tp_dealloc((PyObject *)self);
}

static inline PyObject *
istr_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyObject *mod = PyType_GetModuleByDef(type, &multidict_module);
    if (mod == NULL) {
        return NULL;
    }
    mod_state *state = get_mod_state(mod);

    PyObject *x = NULL;
    static char *kwlist[] = {"object", "encoding", "errors", 0};
    PyObject *encoding = NULL;
    PyObject *errors = NULL;
    PyObject *canonical = NULL;
    PyObject *ret = NULL;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "|OOO:str", kwlist, &x, &encoding, &errors)) {
        return NULL;
    }
    if (x != NULL && IStr_Check(state, x)) {
        Py_INCREF(x);
        return x;
    }
    ret = PyUnicode_Type.tp_new(type, args, kwds);
    if (!ret) {
        goto fail;
    }
    canonical = PyObject_CallMethodNoArgs(ret, state->str_lower);
    if (!canonical) {
        goto fail;
    }
    ((istrobject *)ret)->canonical = canonical;
    ((istrobject *)ret)->state = state;
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
    {NULL, NULL} /* sentinel */
};

static PyType_Slot istr_slots[] = {
    {Py_tp_dealloc, istr_dealloc},
    {Py_tp_doc, (void *)istr__doc__},
    {Py_tp_methods, istr_methods},
    {Py_tp_new, istr_new},
    {0, NULL},
};

static PyType_Spec istr_spec = {
    .name = "multidict._multidict.istr",
    .basicsize = sizeof(istrobject),
    .flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE
#if PY_VERSION_HEX >= 0x030a00f0
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_UNICODE_SUBCLASS),
    .slots = istr_slots,
};

static inline PyObject *
IStr_New(mod_state *state, PyObject *str, PyObject *canonical)
{
    PyObject *args = NULL;
    PyObject *res = NULL;
    args = PyTuple_Pack(1, str);
    if (args == NULL) {
        goto ret;
    }
    res = PyUnicode_Type.tp_new(state->IStrType, args, NULL);
    if (!res) {
        goto ret;
    }
    Py_INCREF(canonical);
    ((istrobject *)res)->canonical = canonical;
    ((istrobject *)res)->state = state;
ret:
    Py_CLEAR(args);
    return res;
}

static inline int
istr_init(PyObject *module, mod_state *state)
{
    PyObject *tpl = PyTuple_Pack(1, (PyObject *)&PyUnicode_Type);
    if (tpl == NULL) {
        return -1;
    }
    PyObject *tmp = PyType_FromModuleAndSpec(module, &istr_spec, tpl);
    Py_DECREF(tpl);
    if (tmp == NULL) {
        return -1;
    }
    state->IStrType = (PyTypeObject *)tmp;
    return 0;
}

#ifdef __cplusplus
}
#endif
#endif
