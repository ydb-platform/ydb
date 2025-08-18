#ifndef _MULTIDICT_ITER_H
#define _MULTIDICT_ITER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "dict.h"
#include "hashtable.h"
#include "state.h"

typedef struct multidict_iter {
    PyObject_HEAD
    MultiDictObject *md;  // MultiDict or CIMultiDict
    md_pos_t current;
} MultidictIter;

static inline void
_init_iter(MultidictIter *it, MultiDictObject *md)
{
    Py_INCREF(md);

    it->md = md;
    md_init_pos(md, &it->current);
}

static inline PyObject *
multidict_items_iter_new(MultiDictObject *md)
{
    MultidictIter *it =
        PyObject_GC_New(MultidictIter, md->state->ItemsIterType);
    if (it == NULL) {
        return NULL;
    }

    _init_iter(it, md);

    PyObject_GC_Track(it);
    return (PyObject *)it;
}

static inline PyObject *
multidict_keys_iter_new(MultiDictObject *md)
{
    MultidictIter *it =
        PyObject_GC_New(MultidictIter, md->state->KeysIterType);
    if (it == NULL) {
        return NULL;
    }

    _init_iter(it, md);

    PyObject_GC_Track(it);
    return (PyObject *)it;
}

static inline PyObject *
multidict_values_iter_new(MultiDictObject *md)
{
    MultidictIter *it =
        PyObject_GC_New(MultidictIter, md->state->ValuesIterType);
    if (it == NULL) {
        return NULL;
    }

    _init_iter(it, md);

    PyObject_GC_Track(it);
    return (PyObject *)it;
}

static inline PyObject *
multidict_items_iter_iternext(MultidictIter *self)
{
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *ret = NULL;

    int res = md_next(self->md, &self->current, NULL, &key, &value);
    if (res < 0) {
        return NULL;
    }
    if (res == 0) {
        Py_CLEAR(key);
        Py_CLEAR(value);
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    ret = PyTuple_Pack(2, key, value);
    Py_CLEAR(key);
    Py_CLEAR(value);
    if (ret == NULL) {
        return NULL;
    }

    return ret;
}

static inline PyObject *
multidict_values_iter_iternext(MultidictIter *self)
{
    PyObject *value = NULL;

    int res = md_next(self->md, &self->current, NULL, NULL, &value);
    if (res < 0) {
        return NULL;
    }
    if (res == 0) {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    return value;
}

static inline PyObject *
multidict_keys_iter_iternext(MultidictIter *self)
{
    PyObject *key = NULL;

    int res = md_next(self->md, &self->current, NULL, &key, NULL);
    if (res < 0) {
        return NULL;
    }
    if (res == 0) {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    return key;
}

static inline void
multidict_iter_dealloc(MultidictIter *self)
{
    PyObject_GC_UnTrack(self);
    Py_XDECREF(self->md);
    PyObject_GC_Del(self);
}

static inline int
multidict_iter_traverse(MultidictIter *self, visitproc visit, void *arg)
{
    Py_VISIT(self->md);
    return 0;
}

static inline int
multidict_iter_clear(MultidictIter *self)
{
    Py_CLEAR(self->md);
    return 0;
}

static inline PyObject *
multidict_iter_len(MultidictIter *self)
{
    return PyLong_FromLong(md_len(self->md));
}

PyDoc_STRVAR(length_hint_doc,
             "Private method returning an estimate of len(list(it)).");

static PyMethodDef multidict_iter_methods[] = {
    {"__length_hint__",
     (PyCFunction)(void (*)(void))multidict_iter_len,
     METH_NOARGS,
     length_hint_doc},
    {NULL, NULL} /* sentinel */
};

/***********************************************************************/

static PyType_Slot multidict_items_iter_slots[] = {
    {Py_tp_dealloc, multidict_iter_dealloc},
    {Py_tp_methods, multidict_iter_methods},
    {Py_tp_traverse, multidict_iter_traverse},
    {Py_tp_clear, multidict_iter_clear},
    {Py_tp_iter, PyObject_SelfIter},
    {Py_tp_iternext, multidict_items_iter_iternext},
    {0, NULL},
};

static PyType_Spec multidict_items_iter_spec = {
    .name = "multidict._multidict._itemsiter",
    .basicsize = sizeof(MultidictIter),
    .flags = (Py_TPFLAGS_DEFAULT
#if PY_VERSION_HEX >= 0x030a00f0
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_HAVE_GC),
    .slots = multidict_items_iter_slots,
};

static PyType_Slot multidict_values_iter_slots[] = {
    {Py_tp_dealloc, multidict_iter_dealloc},
    {Py_tp_methods, multidict_iter_methods},
    {Py_tp_traverse, multidict_iter_traverse},
    {Py_tp_clear, multidict_iter_clear},
    {Py_tp_iter, PyObject_SelfIter},
    {Py_tp_iternext, multidict_values_iter_iternext},
    {0, NULL},
};

static PyType_Spec multidict_values_iter_spec = {
    .name = "multidict._multidict._valuesiter",
    .basicsize = sizeof(MultidictIter),
    .flags = (Py_TPFLAGS_DEFAULT
#if PY_VERSION_HEX >= 0x030a00f0
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_HAVE_GC),
    .slots = multidict_values_iter_slots,
};

static PyType_Slot multidict_keys_iter_slots[] = {
    {Py_tp_dealloc, multidict_iter_dealloc},
    {Py_tp_methods, multidict_iter_methods},
    {Py_tp_traverse, multidict_iter_traverse},
    {Py_tp_clear, multidict_iter_clear},
    {Py_tp_iter, PyObject_SelfIter},
    {Py_tp_iternext, multidict_keys_iter_iternext},
    {0, NULL},
};

static PyType_Spec multidict_keys_iter_spec = {
    .name = "multidict._multidict._keysiter",
    .basicsize = sizeof(MultidictIter),
    .flags = (Py_TPFLAGS_DEFAULT
#if PY_VERSION_HEX >= 0x030a00f0
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_HAVE_GC),
    .slots = multidict_keys_iter_slots,
};

static inline int
multidict_iter_init(PyObject *module, mod_state *state)
{
    PyObject *tmp;
    tmp = PyType_FromModuleAndSpec(module, &multidict_items_iter_spec, NULL);
    if (tmp == NULL) {
        return -1;
    }
    state->ItemsIterType = (PyTypeObject *)tmp;

    tmp = PyType_FromModuleAndSpec(module, &multidict_values_iter_spec, NULL);
    if (tmp == NULL) {
        return -1;
    }
    state->ValuesIterType = (PyTypeObject *)tmp;

    tmp = PyType_FromModuleAndSpec(module, &multidict_keys_iter_spec, NULL);
    if (tmp == NULL) {
        return -1;
    }
    state->KeysIterType = (PyTypeObject *)tmp;

    return 0;
}

#ifdef __cplusplus
}
#endif
#endif
