#include "Python.h"
#include "structmember.h"

#include "_multilib/pythoncapi_compat.h"

#include "_multilib/dict.h"
#include "_multilib/istr.h"
#include "_multilib/iter.h"
#include "_multilib/pair_list.h"
#include "_multilib/parser.h"
#include "_multilib/state.h"
#include "_multilib/views.h"


#define MultiDict_CheckExact(state, obj) Py_IS_TYPE(obj, state->MultiDictType)
#define MultiDict_Check(state, obj) \
    (MultiDict_CheckExact(state, obj) \
     || PyObject_TypeCheck(obj, state->MultiDictType))
#define CIMultiDict_CheckExact(state, obj) Py_IS_TYPE(obj, state->CIMultiDictType)
#define CIMultiDict_Check(state, obj) \
    (CIMultiDict_CheckExact(state, obj) \
     || PyObject_TypeCheck(obj, state->CIMultiDictType))
#define AnyMultiDict_Check(state, obj) \
    (MultiDict_CheckExact(state, obj) \
     || CIMultiDict_CheckExact(state, obj) \
     || PyObject_TypeCheck(obj, state->MultiDictType))
#define MultiDictProxy_CheckExact(state, obj) Py_IS_TYPE(obj, state->MultiDictProxyType)
#define MultiDictProxy_Check(state, obj) \
    (MultiDictProxy_CheckExact(state, obj) \
     || PyObject_TypeCheck(obj, state->MultiDictProxyType))
#define CIMultiDictProxy_CheckExact(state, obj) \
    Py_IS_TYPE(obj, state->CIMultiDictProxyType)
#define CIMultiDictProxy_Check(state, obj) \
    (CIMultiDictProxy_CheckExact(state, obj) \
     || PyObject_TypeCheck(obj, state->CIMultiDictProxyType))
#define AnyMultiDictProxy_Check(state, obj) \
    (MultiDictProxy_CheckExact(state, obj) \
     || CIMultiDictProxy_CheckExact(state, obj) \
     || PyObject_TypeCheck(obj, state->MultiDictProxyType))

/******************** Internal Methods ********************/

static inline PyObject *
_multidict_getone(MultiDictObject *self, PyObject *key, PyObject *_default)
{
    PyObject *val = NULL;

    if (pair_list_get_one(&self->pairs, key, &val) <0) {
        return NULL;
    }

    if (val == NULL) {
        if (_default != NULL) {
            Py_INCREF(_default);
            return _default;
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
            return NULL;
        }
    } else {
        return val;
    }
}


static inline int
_multidict_extend(MultiDictObject *self, PyObject *arg,
                     PyObject *kwds, const char *name, int do_add)
{
    mod_state *state = self->pairs.state;
    PyObject *used = NULL;
    PyObject *seq  = NULL;
    pair_list_t *list;

    if (!do_add) {
        used = PyDict_New();
        if (used == NULL) {
            goto fail;
        }
    }

    if (kwds && !PyArg_ValidateKeywordArguments(kwds)) {
        goto fail;
    }

    if (arg != NULL) {
        if (AnyMultiDict_Check(state, arg)) {
            list = &((MultiDictObject*)arg)->pairs;
            if (pair_list_update_from_pair_list(&self->pairs, used, list) < 0) {
                goto fail;
            }
        } else if (AnyMultiDictProxy_Check(state, arg)) {
            list = &((MultiDictProxyObject*)arg)->md->pairs;
            if (pair_list_update_from_pair_list(&self->pairs, used, list) < 0) {
                goto fail;
            }
        } else if (PyDict_CheckExact(arg)) {
            if (pair_list_update_from_dict(&self->pairs, used, arg) < 0) {
                goto fail;
            }
        } else {
            seq = PyMapping_Items(arg);
            if (seq == NULL) {
                PyErr_Clear();
                seq = Py_NewRef(arg);
            }

            if (pair_list_update_from_seq(&self->pairs, used, seq) < 0) {
                goto fail;
            }
        }
    }

    if (kwds != NULL) {
        if (pair_list_update_from_dict(&self->pairs, used, kwds) < 0) {
            goto fail;
        }
    }

    if (!do_add) {
        if (pair_list_post_update(&self->pairs, used) < 0) {
            goto fail;
        }
    }
    Py_CLEAR(seq);
    Py_CLEAR(used);
    return 0;
fail:
    Py_CLEAR(seq);
    Py_CLEAR(used);
    return -1;
}


static inline Py_ssize_t
_multidict_extend_parse_args(PyObject *args, PyObject *kwds,
                             const char *name, PyObject **parg)
{
    Py_ssize_t size = 0;
    Py_ssize_t s;
    if (args) {
        size = PyTuple_GET_SIZE(args);
        if (size > 1) {
            PyErr_Format(
                         PyExc_TypeError,
                         "%s takes from 1 to 2 positional arguments but %zd were given",
                         name, size + 1, NULL
                         );
            *parg = NULL;
            return -1;
        }
    }

    if (size == 1) {
        *parg = Py_NewRef(PyTuple_GET_ITEM(args, 0));
        s = PyObject_Length(*parg);
        if (s < 0) {
            // e.g. cannot calc size of generator object
            PyErr_Clear();
        } else {
            size += s;
        }
    } else {
        *parg = NULL;
    }

    if (kwds != NULL) {
        s = PyDict_Size(kwds);
        if (s < 0) {
            return -1;
        }
        size += s;
    }

    return size;
}

static inline PyObject *
multidict_copy(MultiDictObject *self)
{
    MultiDictObject *new_multidict = NULL;

    new_multidict = (MultiDictObject*)PyType_GenericNew(
        Py_TYPE(self), NULL, NULL);
    if (new_multidict == NULL) {
        goto fail;
    }

    if (Py_TYPE(self)->tp_init((PyObject*)new_multidict, NULL, NULL) < 0) {
        goto fail;
    }

    if (pair_list_update_from_pair_list(&new_multidict->pairs,
                                        NULL, &self->pairs) < 0) {
        goto fail;
    }
    return (PyObject*)new_multidict;
fail:
    Py_CLEAR(new_multidict);
    return NULL;
}

static inline PyObject *
_multidict_proxy_copy(MultiDictProxyObject *self, PyTypeObject *type)
{
    MultiDictObject *new_multidict = NULL;
    new_multidict = (MultiDictObject*)PyType_GenericNew(type, NULL, NULL);
    if (new_multidict == NULL) {
        goto fail;
    }
    if (type->tp_init((PyObject*)new_multidict, NULL, NULL) < 0) {
        goto fail;
    }
    if (pair_list_update_from_pair_list(&new_multidict->pairs,
                                        NULL, &self->md->pairs) < 0) {
        goto fail;
    }
    return (PyObject*)new_multidict;
fail:
    Py_CLEAR(new_multidict);
    return NULL;
}


/******************** Base Methods ********************/

static inline PyObject *
multidict_getall(MultiDictObject *self, PyObject *const *args,
                 Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *list     = NULL,
             *key      = NULL,
             *_default = NULL;

    if (parse2("getall", args, nargs, kwnames, 1,
                "key", &key, "default", &_default) < 0) {
        return NULL;
    }
    if (pair_list_get_all(&self->pairs, key, &list) <0) {
        return NULL;
    }

    if (list == NULL) {
        if (_default != NULL) {
            Py_INCREF(_default);
            return _default;
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
            return NULL;
        }
    } else {
        return list;
    }
}

static inline PyObject *
multidict_getone(MultiDictObject *self, PyObject *const *args,
                 Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key      = NULL,
             *_default = NULL;

    if (parse2("getone", args, nargs, kwnames, 1,
                "key", &key, "default", &_default) < 0) {
        return NULL;
    }
    return _multidict_getone(self, key, _default);
}

static inline PyObject *
multidict_get(MultiDictObject *self, PyObject *const *args,
              Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key      = NULL,
             *_default = NULL,
             *ret;

    if (parse2("get", args, nargs, kwnames, 1,
                "key", &key, "default", &_default) < 0) {
        return NULL;
    }
    if (_default == NULL) {
        // fixme, _default is potentially dangerous borrowed ref here
        _default = Py_None;
    }
    ret = _multidict_getone(self, key, _default);
    return ret;
}

static inline PyObject *
multidict_keys(MultiDictObject *self)
{
    return multidict_keysview_new(self);
}

static inline PyObject *
multidict_items(MultiDictObject *self)
{
    return multidict_itemsview_new(self);
}

static inline PyObject *
multidict_values(MultiDictObject *self)
{
    return multidict_valuesview_new(self);
}

static inline PyObject *
multidict_reduce(MultiDictObject *self)
{
    PyObject *items      = NULL,
             *items_list = NULL,
             *args       = NULL,
             *result     = NULL;

    items = multidict_itemsview_new(self);
    if (items == NULL) {
        goto ret;
    }

    items_list = PySequence_List(items);
    if (items_list == NULL) {
        goto ret;
    }

    args = PyTuple_Pack(1, items_list);
    if (args == NULL) {
        goto ret;
    }

    result = PyTuple_Pack(2, Py_TYPE(self), args);
ret:
    Py_XDECREF(args);
    Py_XDECREF(items_list);
    Py_XDECREF(items);

    return result;
}

static inline PyObject *
multidict_repr(MultiDictObject *self)
{
    int tmp = Py_ReprEnter((PyObject *)self);
    if (tmp < 0) {
        return NULL;
    }
    if (tmp > 0) {
        return PyUnicode_FromString("...");
    }
    PyObject *name = PyObject_GetAttrString((PyObject *)Py_TYPE(self), "__name__");
    if (name == NULL) {
        Py_ReprLeave((PyObject *)self);
        return NULL;
    }
    PyObject *ret = pair_list_repr(&self->pairs, name, true, true);
    Py_ReprLeave((PyObject *)self);
    Py_CLEAR(name);
    return ret;
}

static inline Py_ssize_t
multidict_mp_len(MultiDictObject *self)
{
    return pair_list_len(&self->pairs);
}

static inline PyObject *
multidict_mp_subscript(MultiDictObject *self, PyObject *key)
{
    return _multidict_getone(self, key, NULL);
}

static inline int
multidict_mp_as_subscript(MultiDictObject *self, PyObject *key, PyObject *val)
{
    if (val == NULL) {
        return pair_list_del(&self->pairs, key);
    } else {
        return pair_list_replace(&self->pairs, key, val);
    }
}

static inline int
multidict_sq_contains(MultiDictObject *self, PyObject *key)
{
    return pair_list_contains(&self->pairs, key, NULL);
}

static inline PyObject *
multidict_tp_iter(MultiDictObject *self)
{
    return multidict_keys_iter_new(self);
}

static inline PyObject *
multidict_tp_richcompare(PyObject *self, PyObject *other, int op)
{
    int cmp;

    if (op != Py_EQ && op != Py_NE) {
        Py_RETURN_NOTIMPLEMENTED;
    }

    if (self == other) {
        cmp = 1;
        if (op == Py_NE) {
            cmp = !cmp;
        }
        return PyBool_FromLong(cmp);
    }

    mod_state *state = ((MultiDictObject*)self)->pairs.state;
    if (AnyMultiDict_Check(state, other)) {
        cmp = pair_list_eq(
            &((MultiDictObject*)self)->pairs,
            &((MultiDictObject*)other)->pairs
        );
    } else if (AnyMultiDictProxy_Check(state, other)) {
        cmp = pair_list_eq(
            &((MultiDictObject*)self)->pairs,
            &((MultiDictProxyObject*)other)->md->pairs
        );
    } else {
        bool fits = false;
        fits = PyDict_Check(other);
        if (!fits) {
            PyObject *keys = PyMapping_Keys(other);
            if (keys != NULL) {
                fits = true;
            } else {
                // reset AttributeError exception
                PyErr_Clear();
            }
            Py_CLEAR(keys);
        }
        if (fits) {
            cmp = pair_list_eq_to_mapping(&((MultiDictObject*)self)->pairs,
                                          other);
        } else {
            cmp = 0; // e.g., multidict is not equal to a list
        }
    }
    if (cmp < 0) {
        return NULL;
    }
    if (op == Py_NE) {
        cmp = !cmp;
    }
    return PyBool_FromLong(cmp);
}

static inline void
multidict_tp_dealloc(MultiDictObject *self)
{
    PyObject_GC_UnTrack(self);
    Py_TRASHCAN_BEGIN(self, multidict_tp_dealloc)
    PyObject_ClearWeakRefs((PyObject *)self);
    pair_list_dealloc(&self->pairs);
    Py_TYPE(self)->tp_free((PyObject *)self);
    Py_TRASHCAN_END // there should be no code after this
}

static inline int
multidict_tp_traverse(MultiDictObject *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self));
    return pair_list_traverse(&self->pairs, visit, arg);
}

static inline int
multidict_tp_clear(MultiDictObject *self)
{
    return pair_list_clear(&self->pairs);
}

PyDoc_STRVAR(multidict_getall_doc,
"Return a list of all values matching the key.");

PyDoc_STRVAR(multidict_getone_doc,
"Get first value matching the key.");

PyDoc_STRVAR(multidict_get_doc,
"Get first value matching the key.\n\nThe method is alias for .getone().");

PyDoc_STRVAR(multidict_keys_doc,
"Return a new view of the dictionary's keys.");

PyDoc_STRVAR(multidict_items_doc,
"Return a new view of the dictionary's items *(key, value) pairs).");

PyDoc_STRVAR(multidict_values_doc,
"Return a new view of the dictionary's values.");

/******************** MultiDict ********************/

static inline int
multidict_tp_init(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    mod_state *state = get_mod_state_by_def((PyObject *)self);
    PyObject *arg = NULL;
    Py_ssize_t size = _multidict_extend_parse_args(args, kwds, "MultiDict", &arg);
    if (size < 0) {
        goto fail;
    }
    if (pair_list_init(&self->pairs, state, size) < 0) {
        goto fail;
    }
    if (_multidict_extend(self, arg, kwds, "MultiDict", 1) < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    return 0;
fail:
    Py_CLEAR(arg);
    return -1;
}

static inline PyObject *
multidict_add(MultiDictObject *self, PyObject *const *args,
              Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key = NULL,
             *val = NULL;

    if (parse2("add", args, nargs, kwnames, 2,
                "key", &key, "value", &val) < 0) {
        return NULL;
    }
    if (pair_list_add(&self->pairs, key, val) < 0) {
        return NULL;
    }

    Py_RETURN_NONE;
}

static inline PyObject *
multidict_extend(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL;
    Py_ssize_t size = _multidict_extend_parse_args(args, kwds, "extend", &arg);
    if (size < 0) {
        goto fail;
    }
    pair_list_grow(&self->pairs, size);
    if (_multidict_extend(self, arg, kwds, "extend", 1) < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    Py_RETURN_NONE;
fail:
    Py_CLEAR(arg);
    return NULL;
}

static inline PyObject *
multidict_clear(MultiDictObject *self)
{
    if (pair_list_clear(&self->pairs) < 0) {
        return NULL;
    }

    Py_RETURN_NONE;
}

static inline PyObject *
multidict_setdefault(MultiDictObject *self, PyObject *const *args,
                     Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key      = NULL,
             *_default = NULL;

    if (parse2("setdefault", args, nargs, kwnames, 1,
                "key", &key, "default", &_default) < 0) {
        return NULL;
    }
    return pair_list_set_default(&self->pairs, key, _default);
}

static inline PyObject *
multidict_popone(MultiDictObject *self, PyObject *const *args,
                 Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key      = NULL,
             *_default = NULL,
             *ret_val  = NULL;

    if (parse2("popone", args, nargs, kwnames, 1,
                "key", &key, "default", &_default) < 0) {
        return NULL;
    }
    if (pair_list_pop_one(&self->pairs, key, &ret_val) < 0) {
        return NULL;
    }

    if (ret_val == NULL) {
        if (_default != NULL) {
            Py_INCREF(_default);
            return _default;
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
            return NULL;
        }
    } else {
        return ret_val;
    }
}

static inline PyObject *
multidict_pop(
    MultiDictObject *self,
    PyObject *const *args,
    Py_ssize_t nargs,
    PyObject *kwnames
)
{
    PyObject *key      = NULL,
             *_default = NULL,
             *ret_val  = NULL;

    if (parse2("pop", args, nargs, kwnames, 1,
                "key", &key, "default", &_default) < 0) {
        return NULL;
    }
    if (pair_list_pop_one(&self->pairs, key, &ret_val) < 0) {
        return NULL;
    }

    if (ret_val == NULL) {
        if (_default != NULL) {
            Py_INCREF(_default);
            return _default;
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
            return NULL;
        }
    } else {
        return ret_val;
    }
}

static inline PyObject *
multidict_popall(MultiDictObject *self, PyObject *const *args,
                 Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key      = NULL,
             *_default = NULL,
             *ret_val  = NULL;

    if (parse2("popall", args, nargs, kwnames, 1,
                "key", &key, "default", &_default) < 0) {
        return NULL;
    }
    if (pair_list_pop_all(&self->pairs, key, &ret_val) < 0) {
        return NULL;
    }

    if (ret_val == NULL) {
        if (_default != NULL) {
            Py_INCREF(_default);
            return _default;
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
            return NULL;
        }
    } else {
        return ret_val;
    }
}

static inline PyObject *
multidict_popitem(MultiDictObject *self)
{
    return pair_list_pop_item(&self->pairs);
}

static inline PyObject *
multidict_update(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL;
    if (_multidict_extend_parse_args(args, kwds, "update", &arg) < 0) {
        goto fail;
    }
    if (_multidict_extend(self, arg, kwds, "update", 0) < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    Py_RETURN_NONE;
fail:
    Py_CLEAR(arg);
    return NULL;
}

PyDoc_STRVAR(multidict_add_doc,
"Add the key and value, not overwriting any previous value.");

PyDoc_STRVAR(multidict_copy_doc,
"Return a copy of itself.");

PyDoc_STRVAR(multdicit_method_extend_doc,
"Extend current MultiDict with more values.\n\
This method must be used instead of update.");

PyDoc_STRVAR(multidict_clear_doc,
"Remove all items from MultiDict");

PyDoc_STRVAR(multidict_setdefault_doc,
"Return value for key, set value to default if key is not present.");

PyDoc_STRVAR(multidict_popone_doc,
"Remove the last occurrence of key and return the corresponding value.\n\n\
If key is not found, default is returned if given, otherwise KeyError is \
raised.\n");

PyDoc_STRVAR(multidict_pop_doc,
"Remove the last occurrence of key and return the corresponding value.\n\n\
If key is not found, default is returned if given, otherwise KeyError is \
raised.\n");

PyDoc_STRVAR(multidict_popall_doc,
"Remove all occurrences of key and return the list of corresponding values.\n\n\
If key is not found, default is returned if given, otherwise KeyError is \
raised.\n");

PyDoc_STRVAR(multidict_popitem_doc,
"Remove and return an arbitrary (key, value) pair.");

PyDoc_STRVAR(multidict_update_doc,
"Update the dictionary from *other*, overwriting existing keys.");

PyDoc_STRVAR(sizeof__doc__,
"D.__sizeof__() -> size of D in memory, in bytes");

static inline PyObject *
_multidict_sizeof(MultiDictObject *self)
{
    Py_ssize_t size = sizeof(MultiDictObject);
    if (self->pairs.pairs != self->pairs.buffer) {
        size += (Py_ssize_t)sizeof(pair_t) * self->pairs.capacity;
    }
    return PyLong_FromSsize_t(size);
}


static PyMethodDef multidict_methods[] = {
    {
        "getall",
        (PyCFunction)multidict_getall,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_getall_doc
    },
    {
        "getone",
        (PyCFunction)multidict_getone,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_getone_doc
    },
    {
        "get",
        (PyCFunction)multidict_get,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_get_doc
    },
    {
        "keys",
        (PyCFunction)multidict_keys,
        METH_NOARGS,
        multidict_keys_doc
    },
    {
        "items",
        (PyCFunction)multidict_items,
        METH_NOARGS,
        multidict_items_doc
    },
    {
        "values",
        (PyCFunction)multidict_values,
        METH_NOARGS,
        multidict_values_doc
    },
    {
        "add",
        (PyCFunction)multidict_add,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_add_doc
    },
    {
        "copy",
        (PyCFunction)multidict_copy,
        METH_NOARGS,
        multidict_copy_doc
    },
    {
        "extend",
        (PyCFunction)multidict_extend,
        METH_VARARGS | METH_KEYWORDS,
        multdicit_method_extend_doc
    },
    {
        "clear",
        (PyCFunction)multidict_clear,
        METH_NOARGS,
        multidict_clear_doc
    },
    {
        "setdefault",
        (PyCFunction)multidict_setdefault,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_setdefault_doc
    },
    {
        "popone",
        (PyCFunction)multidict_popone,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_popone_doc
    },
    {
        "pop",
        (PyCFunction)multidict_pop,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_pop_doc
    },
    {
        "popall",
        (PyCFunction)multidict_popall,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_popall_doc
    },
    {
        "popitem",
        (PyCFunction)multidict_popitem,
        METH_NOARGS,
        multidict_popitem_doc
    },
    {
        "update",
        (PyCFunction)multidict_update,
        METH_VARARGS | METH_KEYWORDS,
        multidict_update_doc
    },
    {
        "__reduce__",
        (PyCFunction)multidict_reduce,
        METH_NOARGS,
        NULL,
    },
    {
        "__class_getitem__",
        (PyCFunction)Py_GenericAlias,
        METH_O | METH_CLASS,
        NULL
    },
    {
        "__sizeof__",
        (PyCFunction)_multidict_sizeof,
        METH_NOARGS,
        sizeof__doc__,
    },
    {
        NULL,
        NULL
    }   /* sentinel */
};


PyDoc_STRVAR(MultDict_doc,
"Dictionary with the support for duplicate keys.");

#ifndef MANAGED_WEAKREFS
static PyMemberDef multidict_members[] = {
    {"__weaklistoffset__", Py_T_PYSSIZET,
     offsetof(MultiDictObject, weaklist), Py_READONLY},
    {NULL}  /* Sentinel */
};
#endif

static PyType_Slot multidict_slots[] = {
    {Py_tp_dealloc, multidict_tp_dealloc},
    {Py_tp_repr, multidict_repr},
    {Py_tp_doc, (void *)MultDict_doc},

    {Py_sq_contains, multidict_sq_contains},
    {Py_mp_length, multidict_mp_len},
    {Py_mp_subscript, multidict_mp_subscript},
    {Py_mp_ass_subscript, multidict_mp_as_subscript},

    {Py_tp_traverse, multidict_tp_traverse},
    {Py_tp_clear, multidict_tp_clear},
    {Py_tp_richcompare, multidict_tp_richcompare},
    {Py_tp_iter, multidict_tp_iter},
    {Py_tp_methods, multidict_methods},
    {Py_tp_init, multidict_tp_init},
    {Py_tp_alloc, PyType_GenericAlloc},
    {Py_tp_new, PyType_GenericNew},
    {Py_tp_free, PyObject_GC_Del},

#ifndef MANAGED_WEAKREFS
    {Py_tp_members, multidict_members},
#endif
    {0, NULL},
};

static PyType_Spec multidict_spec = {
    .name = "multidict._multidict.MultiDict",
    .basicsize = sizeof(MultiDictObject),
    .flags = (Py_TPFLAGS_DEFAULT  | Py_TPFLAGS_BASETYPE
#if PY_VERSION_HEX >= 0x030a00f0
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
#ifdef MANAGED_WEAKREFS
              | Py_TPFLAGS_MANAGED_WEAKREF
#endif
              | Py_TPFLAGS_HAVE_GC),
    .slots = multidict_slots,
};


/******************** CIMultiDict ********************/

static inline int
cimultidict_tp_init(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    mod_state *state = get_mod_state_by_def((PyObject *)self);
    PyObject *arg = NULL;
    Py_ssize_t size = _multidict_extend_parse_args(args, kwds, "CIMultiDict", &arg);
    if (size < 0) {
        goto fail;
    }

    if (ci_pair_list_init(&self->pairs, state, size) < 0) {
        goto fail;
    }

    if (_multidict_extend(self, arg, kwds, "CIMultiDict", 1) < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    return 0;
fail:
    Py_CLEAR(arg);
    return -1;
}


PyDoc_STRVAR(CIMultDict_doc,
"Dictionary with the support for duplicate case-insensitive keys.");

static PyType_Slot cimultidict_slots[] = {
    {Py_tp_doc, (void *)CIMultDict_doc},
    {Py_tp_init, cimultidict_tp_init},
    {0, NULL},
};

static PyType_Spec cimultidict_spec = {
    .name = "multidict._multidict.CIMultiDict",
    .basicsize = sizeof(MultiDictObject),
    .flags = (Py_TPFLAGS_DEFAULT
#if PY_VERSION_HEX >= 0x030a00f0
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_BASETYPE),
    .slots = cimultidict_slots,
};

/******************** MultiDictProxy ********************/

static inline int
multidict_proxy_tp_init(MultiDictProxyObject *self, PyObject *args,
                        PyObject *kwds)
{
    mod_state *state = get_mod_state_by_def((PyObject *)self);
    PyObject        *arg = NULL;
    MultiDictObject *md  = NULL;

    if (!PyArg_UnpackTuple(args, "multidict._multidict.MultiDictProxy",
                           0, 1, &arg))
    {
        return -1;
    }
    if (arg == NULL) {
        PyErr_Format(
            PyExc_TypeError,
            "__init__() missing 1 required positional argument: 'arg'"
        );
        return -1;
    }
    if (!AnyMultiDictProxy_Check(state, arg) &&
        !AnyMultiDict_Check(state, arg))
    {
        PyErr_Format(
            PyExc_TypeError,
            "ctor requires MultiDict or MultiDictProxy instance, "
            "not <class '%s'>",
            Py_TYPE(arg)->tp_name
        );
        return -1;
    }

    if (AnyMultiDictProxy_Check(state, arg)) {
        md = ((MultiDictProxyObject*)arg)->md;
    } else {
        md = (MultiDictObject*)arg;
    }
    Py_INCREF(md);
    self->md = md;

    return 0;
}

static inline PyObject *
multidict_proxy_getall(MultiDictProxyObject *self, PyObject *const *args,
                       Py_ssize_t nargs, PyObject *kwnames)
{
    return multidict_getall(self->md, args, nargs, kwnames);
}

static inline PyObject *
multidict_proxy_getone(MultiDictProxyObject *self, PyObject *const *args,
                       Py_ssize_t nargs, PyObject *kwnames)
{
    return multidict_getone(self->md, args, nargs, kwnames);
}

static inline PyObject *
multidict_proxy_get(MultiDictProxyObject *self, PyObject *const *args,
                       Py_ssize_t nargs, PyObject *kwnames)
{
    return multidict_get(self->md, args, nargs, kwnames);
}

static inline PyObject *
multidict_proxy_keys(MultiDictProxyObject *self)
{
    return multidict_keys(self->md);
}

static inline PyObject *
multidict_proxy_items(MultiDictProxyObject *self)
{
    return multidict_items(self->md);
}

static inline PyObject *
multidict_proxy_values(MultiDictProxyObject *self)
{
    return multidict_values(self->md);
}

static inline PyObject *
multidict_proxy_copy(MultiDictProxyObject *self)
{
    return _multidict_proxy_copy(self, self->md->pairs.state->MultiDictType);
}

static inline PyObject *
multidict_proxy_reduce(MultiDictProxyObject *self)
{
    PyErr_Format(
        PyExc_TypeError,
        "can't pickle %s objects", Py_TYPE(self)->tp_name
    );

    return NULL;
}

static inline Py_ssize_t
multidict_proxy_mp_len(MultiDictProxyObject *self)
{
    return multidict_mp_len(self->md);
}

static inline PyObject *
multidict_proxy_mp_subscript(MultiDictProxyObject *self, PyObject *key)
{
    return multidict_mp_subscript(self->md, key);
}

static inline int
multidict_proxy_sq_contains(MultiDictProxyObject *self, PyObject *key)
{
    return multidict_sq_contains(self->md, key);
}

static inline PyObject *
multidict_proxy_tp_iter(MultiDictProxyObject *self)
{
    return multidict_tp_iter(self->md);
}

static inline PyObject *
multidict_proxy_tp_richcompare(MultiDictProxyObject *self, PyObject *other,
                               int op)
{
    return multidict_tp_richcompare((PyObject*)self->md, other, op);
}

static inline void
multidict_proxy_tp_dealloc(MultiDictProxyObject *self)
{
    PyObject_GC_UnTrack(self);
    PyObject_ClearWeakRefs((PyObject *)self);
    Py_XDECREF(self->md);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static inline int
multidict_proxy_tp_traverse(MultiDictProxyObject *self, visitproc visit,
                            void *arg)
{
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->md);
    return 0;
}

static inline int
multidict_proxy_tp_clear(MultiDictProxyObject *self)
{
    Py_CLEAR(self->md);
    return 0;
}

static inline PyObject *
multidict_proxy_repr(MultiDictProxyObject *self)
{
    PyObject *name = PyObject_GetAttrString((PyObject*)Py_TYPE(self), "__name__");
    if (name == NULL)
        return NULL;
    PyObject *ret = pair_list_repr(&self->md->pairs, name, true, true);
    Py_CLEAR(name);
    return ret;
}


static PyMethodDef multidict_proxy_methods[] = {
    {
        "getall",
        (PyCFunction)multidict_proxy_getall,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_getall_doc
    },
    {
        "getone",
        (PyCFunction)multidict_proxy_getone,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_getone_doc
    },
    {
        "get",
        (PyCFunction)multidict_proxy_get,
        METH_FASTCALL | METH_KEYWORDS,
        multidict_get_doc
    },
    {
        "keys",
        (PyCFunction)multidict_proxy_keys,
        METH_NOARGS,
        multidict_keys_doc
    },
    {
        "items",
        (PyCFunction)multidict_proxy_items,
        METH_NOARGS,
        multidict_items_doc
    },
    {
        "values",
        (PyCFunction)multidict_proxy_values,
        METH_NOARGS,
        multidict_values_doc
    },
    {
        "copy",
        (PyCFunction)multidict_proxy_copy,
        METH_NOARGS,
        multidict_copy_doc
    },
    {
        "__reduce__",
        (PyCFunction)multidict_proxy_reduce,
        METH_NOARGS,
        NULL
    },
    {
        "__class_getitem__",
        (PyCFunction)Py_GenericAlias,
        METH_O | METH_CLASS,
        NULL
    },
    {
        NULL,
        NULL
    }   /* sentinel */
};


PyDoc_STRVAR(MultDictProxy_doc,
"Read-only proxy for MultiDict instance.");

#ifndef MANAGED_WEAKREFS
static PyMemberDef multidict_proxy_members[] = {
    {"__weaklistoffset__", Py_T_PYSSIZET,
     offsetof(MultiDictProxyObject, weaklist), Py_READONLY},
    {NULL}  /* Sentinel */
};
#endif

static PyType_Slot multidict_proxy_slots[] = {
    {Py_tp_dealloc, multidict_proxy_tp_dealloc},
    {Py_tp_repr, multidict_proxy_repr},
    {Py_tp_doc, (void *)MultDictProxy_doc},

    {Py_sq_contains, multidict_proxy_sq_contains},
    {Py_mp_length, multidict_proxy_mp_len},
    {Py_mp_subscript, multidict_proxy_mp_subscript},

    {Py_tp_traverse, multidict_proxy_tp_traverse},
    {Py_tp_clear, multidict_proxy_tp_clear},
    {Py_tp_richcompare, multidict_proxy_tp_richcompare},
    {Py_tp_iter, multidict_proxy_tp_iter},
    {Py_tp_methods, multidict_proxy_methods},
    {Py_tp_init, multidict_proxy_tp_init},
    {Py_tp_alloc, PyType_GenericAlloc},
    {Py_tp_new, PyType_GenericNew},
    {Py_tp_free, PyObject_GC_Del},

#ifndef MANAGED_WEAKREFS
    {Py_tp_members, multidict_proxy_members},
#endif
    {0, NULL},
};

static PyType_Spec multidict_proxy_spec = {
    .name = "multidict._multidict.MultiDictProxy",
    .basicsize = sizeof(MultiDictProxyObject),
    .flags = (Py_TPFLAGS_DEFAULT  | Py_TPFLAGS_BASETYPE
#if PY_VERSION_HEX >= 0x030a00f0
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
#ifdef MANAGED_WEAKREFS
              | Py_TPFLAGS_MANAGED_WEAKREF
#endif
              | Py_TPFLAGS_HAVE_GC),
    .slots = multidict_proxy_slots,
};

/******************** CIMultiDictProxy ********************/

static inline int
cimultidict_proxy_tp_init(MultiDictProxyObject *self, PyObject *args,
                          PyObject *kwds)
{
    mod_state *state = get_mod_state_by_def((PyObject *)self);
    PyObject        *arg = NULL;
    MultiDictObject *md  = NULL;

    if (!PyArg_UnpackTuple(args, "multidict._multidict.CIMultiDictProxy",
                           1, 1, &arg))
    {
        return -1;
    }
    if (arg == NULL) {
        PyErr_Format(
            PyExc_TypeError,
            "__init__() missing 1 required positional argument: 'arg'"
        );
        return -1;
    }
    if (!CIMultiDictProxy_Check(state, arg)
        && !CIMultiDict_Check(state, arg)) {
        PyErr_Format(
            PyExc_TypeError,
            "ctor requires CIMultiDict or CIMultiDictProxy instance, "
            "not <class '%s'>",
            Py_TYPE(arg)->tp_name
        );
        return -1;
    }

    if (CIMultiDictProxy_Check(state, arg)) {
        md = ((MultiDictProxyObject*)arg)->md;
    } else {
        md = (MultiDictObject*)arg;
    }
    Py_INCREF(md);
    self->md = md;

    return 0;
}

static inline PyObject *
cimultidict_proxy_copy(MultiDictProxyObject *self)
{
    return _multidict_proxy_copy(self, self->md->pairs.state->CIMultiDictType);
}


PyDoc_STRVAR(CIMultDictProxy_doc,
"Read-only proxy for CIMultiDict instance.");

PyDoc_STRVAR(cimultidict_proxy_copy_doc,
"Return copy of itself");

static PyMethodDef cimultidict_proxy_methods[] = {
    {
        "copy",
        (PyCFunction)cimultidict_proxy_copy,
        METH_NOARGS,
        cimultidict_proxy_copy_doc
    },
    {
        NULL,
        NULL
    }   /* sentinel */
};

static PyType_Slot cimultidict_proxy_slots[] = {
    {Py_tp_doc, (void *)CIMultDictProxy_doc},
    {Py_tp_methods, cimultidict_proxy_methods},
    {Py_tp_init, cimultidict_proxy_tp_init},
    {0, NULL},
};

static PyType_Spec cimultidict_proxy_spec = {
    .name = "multidict._multidict.CIMultiDictProxy",
    .basicsize = sizeof(MultiDictProxyObject),
    .flags = (Py_TPFLAGS_DEFAULT
#if PY_VERSION_HEX >= 0x030a00f0
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_BASETYPE),
    .slots = cimultidict_proxy_slots,
};

/******************** Other functions ********************/

static inline PyObject *
getversion(PyObject *self, PyObject *md)
{
    mod_state *state = get_mod_state(self);
    pair_list_t *pairs = NULL;
    if (AnyMultiDict_Check(state, md)) {
        pairs = &((MultiDictObject*)md)->pairs;
    } else if (AnyMultiDictProxy_Check(state, md)) {
        pairs = &((MultiDictProxyObject*)md)->md->pairs;
    } else {
        PyErr_Format(PyExc_TypeError, "unexpected type");
        return NULL;
    }
    return PyLong_FromUnsignedLong(pair_list_version(pairs));
}

/******************** Module ********************/

static int
module_traverse(PyObject *mod, visitproc visit, void *arg)
{
    mod_state *state = get_mod_state(mod);

    Py_VISIT(state->IStrType);

    Py_VISIT(state->MultiDictType);
    Py_VISIT(state->CIMultiDictType);
    Py_VISIT(state->MultiDictProxyType);
    Py_VISIT(state->CIMultiDictProxyType);

    Py_VISIT(state->KeysViewType);
    Py_VISIT(state->ItemsViewType);
    Py_VISIT(state->ValuesViewType);

    Py_VISIT(state->KeysIterType);
    Py_VISIT(state->ItemsIterType);
    Py_VISIT(state->ValuesIterType);

    Py_VISIT(state->str_lower);
    Py_VISIT(state->str_canonical);

    return 0;
}

static int
module_clear(PyObject *mod)
{
    mod_state *state = get_mod_state(mod);

    Py_CLEAR(state->IStrType);

    Py_CLEAR(state->MultiDictType);
    Py_CLEAR(state->CIMultiDictType);
    Py_CLEAR(state->MultiDictProxyType);
    Py_CLEAR(state->CIMultiDictProxyType);

    Py_CLEAR(state->KeysViewType);
    Py_CLEAR(state->ItemsViewType);
    Py_CLEAR(state->ValuesViewType);

    Py_CLEAR(state->KeysIterType);
    Py_CLEAR(state->ItemsIterType);
    Py_CLEAR(state->ValuesIterType);

    Py_CLEAR(state->str_lower);
    Py_CLEAR(state->str_canonical);

    return 0;
}

static inline void
module_free(void *mod)
{
    (void)module_clear((PyObject *)mod);
}

static PyMethodDef module_methods[] = {
    {"getversion", (PyCFunction)getversion, METH_O},
    {NULL, NULL}   /* sentinel */
};


static int
module_exec(PyObject *mod)
{
    mod_state *state = get_mod_state(mod);
    PyObject *tmp;
    PyObject *tpl = NULL;

    state->str_lower = PyUnicode_InternFromString("lower");
    if (state->str_lower == NULL) {
        goto fail;
    }
    state->str_canonical = PyUnicode_InternFromString("_canonical");
    if (state->str_canonical == NULL) {
        goto fail;
    }

    if (multidict_views_init(mod, state) < 0) {
        goto fail;
    }

    if (multidict_iter_init(mod, state) < 0) {
        goto fail;
    }

    if (istr_init(mod, state) < 0) {
        goto fail;
    }

    tmp = PyType_FromModuleAndSpec(mod, &multidict_spec, NULL);
    if (tmp == NULL) {
        goto fail;
    }
    state->MultiDictType = (PyTypeObject *)tmp;

    tpl = PyTuple_Pack(1, (PyObject *)state->MultiDictType);
    if (tpl == NULL) {
        goto fail;
    }
    tmp = PyType_FromModuleAndSpec(mod, &cimultidict_spec, tpl);
    if (tmp == NULL) {
        goto fail;
    }
    state->CIMultiDictType = (PyTypeObject *)tmp;
    Py_CLEAR(tpl);

    tmp = PyType_FromModuleAndSpec(mod, &multidict_proxy_spec, NULL);
    if (tmp == NULL) {
        goto fail;
    }
    state->MultiDictProxyType = (PyTypeObject *)tmp;

    tpl = PyTuple_Pack(1, (PyObject *)state->MultiDictProxyType);
    if (tpl == NULL) {
        goto fail;
    }
    tmp = PyType_FromModuleAndSpec(mod, &cimultidict_proxy_spec, tpl);
    if (tmp == NULL) {
        goto fail;
    }
    state->CIMultiDictProxyType = (PyTypeObject *)tmp;
    Py_CLEAR(tpl);

    if (PyModule_AddType(mod, state->IStrType) < 0) {
        goto fail;
    }
    if (PyModule_AddType(mod, state->MultiDictType) < 0) {
        goto fail;
    }
    if (PyModule_AddType(mod, state->CIMultiDictType) < 0) {
        goto fail;
    }
    if (PyModule_AddType(mod, state->MultiDictProxyType) < 0) {
        goto fail;
    }
    if (PyModule_AddType(mod, state->CIMultiDictProxyType) < 0) {
        goto fail;
    }
    if (PyModule_AddType(mod, state->ItemsViewType) < 0) {
        goto fail;
    }
    if (PyModule_AddType(mod, state->KeysViewType) < 0) {
        goto fail;
    }
    if (PyModule_AddType(mod, state->ValuesViewType) < 0) {
        goto fail;
    }

    return 0;
fail:
    Py_CLEAR(tpl);
    return -1;
}


static struct PyModuleDef_Slot module_slots[] = {
    {Py_mod_exec, module_exec},
#if PY_VERSION_HEX >= 0x030c00f0
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
#endif
#if PY_VERSION_HEX >= 0x030d00f0
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL},
};


static PyModuleDef multidict_module = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "_multidict",
    .m_size = sizeof(mod_state),
    .m_methods = module_methods,
    .m_slots = module_slots,
    .m_traverse = module_traverse,
    .m_clear = module_clear,
    .m_free = (freefunc)module_free,
};

PyMODINIT_FUNC
PyInit__multidict(void)
{
    return PyModuleDef_Init(&multidict_module);
}
