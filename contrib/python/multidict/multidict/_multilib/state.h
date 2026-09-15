#ifndef _MULTIDICT_STATE_H
#define _MULTIDICT_STATE_H

#ifdef __cplusplus
extern "C" {
#endif

/* State of the _multidict module */
typedef struct {
    PyTypeObject *IStrType;

    PyTypeObject *MultiDictType;
    PyTypeObject *CIMultiDictType;
    PyTypeObject *MultiDictProxyType;
    PyTypeObject *CIMultiDictProxyType;

    PyTypeObject *KeysViewType;
    PyTypeObject *ItemsViewType;
    PyTypeObject *ValuesViewType;

    PyTypeObject *KeysIterType;
    PyTypeObject *ItemsIterType;
    PyTypeObject *ValuesIterType;

    PyObject *str_canonical;
    PyObject *str_lower;
    PyObject *str_name;

    uint64_t global_version;
} mod_state;

static inline mod_state *
get_mod_state(PyObject *mod)
{
    mod_state *state = (mod_state *)PyModule_GetState(mod);
    assert(state != NULL);
    return state;
}

static inline mod_state *
get_mod_state_by_cls(PyTypeObject *cls)
{
    mod_state *state = (mod_state *)PyType_GetModuleState(cls);
    assert(state != NULL);
    return state;
}

#if PY_VERSION_HEX < 0x030b0000
PyObject *
PyType_GetModuleByDef(PyTypeObject *tp, PyModuleDef *def)
{
    PyModuleDef *mod_def;
    if (!PyType_HasFeature(tp, Py_TPFLAGS_HEAPTYPE)) {
        goto err;
    }
    PyObject *mod = NULL;

    mod = PyType_GetModule(tp);
    if (mod == NULL) {
        PyErr_Clear();
    } else {
        mod_def = PyModule_GetDef(mod);
        if (mod_def == def) {
            return mod;
        }
    }

    PyObject *mro = tp->tp_mro;
    assert(mro != NULL);
    assert(PyTuple_Check(mro));
    assert(PyTuple_GET_SIZE(mro) >= 1);
    assert(PyTuple_GET_ITEM(mro, 0) == (PyObject *)tp);

    Py_ssize_t n = PyTuple_GET_SIZE(mro);
    for (Py_ssize_t i = 1; i < n; i++) {
        PyObject *super = PyTuple_GET_ITEM(mro, i);
        if (!PyType_HasFeature((PyTypeObject *)super, Py_TPFLAGS_HEAPTYPE)) {
            continue;
        }
        mod = PyType_GetModule((PyTypeObject *)super);
        if (mod == NULL) {
            PyErr_Clear();
        } else {
            mod_def = PyModule_GetDef(mod);
            if (mod_def == def) {
                return mod;
            }
        }
    }

err:
    PyErr_Format(
        PyExc_TypeError,
        "PyType_GetModuleByDef: No superclass of '%s' has the given module",
        tp->tp_name);
    return NULL;
}
#endif

static PyModuleDef multidict_module;

static inline int
get_mod_state_by_def_checked(PyObject *self, mod_state **ret)
{
    PyTypeObject *tp = Py_TYPE(self);
    PyObject *mod = PyType_GetModuleByDef(tp, &multidict_module);
    if (mod == NULL) {
        *ret = NULL;
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            return 0;
        }
        return -1;
    }
    *ret = get_mod_state(mod);
    return 1;
}

static inline mod_state *
get_mod_state_by_def(PyObject *self)
{
    PyTypeObject *tp = Py_TYPE(self);
    PyObject *mod = PyType_GetModuleByDef(tp, &multidict_module);
    assert(mod != NULL);
    return get_mod_state(mod);
}

static inline uint64_t
NEXT_VERSION(mod_state *state)
{
    return ++state->global_version;
}

#ifdef __cplusplus
}
#endif
#endif
