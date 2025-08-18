#include <Python.h>
#include <structmember.h>

#include "_multilib/dict.h"
#include "_multilib/hashtable.h"
#include "_multilib/istr.h"
#include "_multilib/iter.h"
#include "_multilib/parser.h"
#include "_multilib/pythoncapi_compat.h"
#include "_multilib/state.h"
#include "_multilib/views.h"

#define MultiDict_CheckExact(state, obj) Py_IS_TYPE(obj, state->MultiDictType)
#define MultiDict_Check(state, obj)      \
    (MultiDict_CheckExact(state, obj) || \
     PyObject_TypeCheck(obj, state->MultiDictType))
#define CIMultiDict_CheckExact(state, obj) \
    Py_IS_TYPE(obj, state->CIMultiDictType)
#define CIMultiDict_Check(state, obj)      \
    (CIMultiDict_CheckExact(state, obj) || \
     PyObject_TypeCheck(obj, state->CIMultiDictType))
#define AnyMultiDict_Check(state, obj)     \
    (MultiDict_CheckExact(state, obj) ||   \
     CIMultiDict_CheckExact(state, obj) || \
     PyObject_TypeCheck(obj, state->MultiDictType))
#define MultiDictProxy_CheckExact(state, obj) \
    Py_IS_TYPE(obj, state->MultiDictProxyType)
#define MultiDictProxy_Check(state, obj)      \
    (MultiDictProxy_CheckExact(state, obj) || \
     PyObject_TypeCheck(obj, state->MultiDictProxyType))
#define CIMultiDictProxy_CheckExact(state, obj) \
    Py_IS_TYPE(obj, state->CIMultiDictProxyType)
#define CIMultiDictProxy_Check(state, obj)      \
    (CIMultiDictProxy_CheckExact(state, obj) || \
     PyObject_TypeCheck(obj, state->CIMultiDictProxyType))
#define AnyMultiDictProxy_Check(state, obj)     \
    (MultiDictProxy_CheckExact(state, obj) ||   \
     CIMultiDictProxy_CheckExact(state, obj) || \
     PyObject_TypeCheck(obj, state->MultiDictProxyType))

/******************** Internal Methods ********************/

static inline PyObject *
_multidict_getone(MultiDictObject *self, PyObject *key, PyObject *_default)
{
    PyObject *val = NULL;

    if (md_get_one(self, key, &val) < 0) {
        return NULL;
    }

    ASSERT_CONSISTENT(self, false);

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
_multidict_extend(MultiDictObject *self, PyObject *arg, PyObject *kwds,
                  const char *name, UpdateOp op)
{
    mod_state *state = self->state;
    PyObject *seq = NULL;

    if (kwds && !PyArg_ValidateKeywordArguments(kwds)) {
        goto fail;
    }

    if (arg != NULL) {
        if (AnyMultiDict_Check(state, arg)) {
            MultiDictObject *other = (MultiDictObject *)arg;
            if (md_update_from_ht(self, other, op) < 0) {
                goto fail;
            }
        } else if (AnyMultiDictProxy_Check(state, arg)) {
            MultiDictObject *other = ((MultiDictProxyObject *)arg)->md;
            if (md_update_from_ht(self, other, op) < 0) {
                goto fail;
            }
        } else if (PyDict_CheckExact(arg)) {
            if (md_update_from_dict(self, arg, op) < 0) {
                goto fail;
            }
        } else if (PyList_CheckExact(arg)) {
            if (md_update_from_seq(self, arg, op) < 0) {
                goto fail;
            }
        } else if (PyTuple_CheckExact(arg)) {
            if (md_update_from_seq(self, arg, op) < 0) {
                goto fail;
            }
        } else {
            seq = PyMapping_Items(arg);
            if (seq == NULL) {
                PyErr_Clear();
                seq = Py_NewRef(arg);
            }

            if (md_update_from_seq(self, seq, op) < 0) {
                goto fail;
            }
        }
    }

    if (kwds != NULL) {
        if (md_update_from_dict(self, kwds, op) < 0) {
            goto fail;
        }
    }

    if (op != Extend) {  // Update or Merge
        md_post_update(self);
    }

    ASSERT_CONSISTENT(self, false);
    Py_CLEAR(seq);
    return 0;
fail:
    if (op != Extend) {  // Update or Merge
        // Cleanup soft-deleted items
        md_post_update(self);
    }
    ASSERT_CONSISTENT(self, false);
    Py_CLEAR(seq);
    return -1;
}

static inline Py_ssize_t
_multidict_extend_parse_args(mod_state *state, PyObject *args, PyObject *kwds,
                             const char *name, PyObject **parg)
{
    Py_ssize_t size = 0;
    Py_ssize_t s = 0;
    if (args) {
        s = PyTuple_GET_SIZE(args);
        if (s > 1) {
            PyErr_Format(
                PyExc_TypeError,
                "%s takes from 1 to 2 positional arguments but %zd were given",
                name,
                s + 1,
                NULL);
            *parg = NULL;
            return -1;
        }
    }

    if (s == 1) {
        *parg = Py_NewRef(PyTuple_GET_ITEM(args, 0));
        if (PyTuple_CheckExact(*parg)) {
            size += PyTuple_GET_SIZE(*parg);
        } else if (PyList_CheckExact(*parg)) {
            size += PyList_GET_SIZE(*parg);
        } else if (PyDict_CheckExact(*parg)) {
            size += PyDict_GET_SIZE(*parg);
        } else if (MultiDict_CheckExact(state, *parg) ||
                   CIMultiDict_CheckExact(state, *parg)) {
            MultiDictObject *md = (MultiDictObject *)*parg;
            size += md_len(md);
        } else if (MultiDictProxy_CheckExact(state, *parg) ||
                   CIMultiDictProxy_CheckExact(state, *parg)) {
            MultiDictObject *md = ((MultiDictProxyObject *)*parg)->md;
            size += md_len(md);
        } else {
            s = PyObject_LengthHint(*parg, 0);
            if (s < 0) {
                // e.g. cannot calc size of generator object
                PyErr_Clear();
            } else {
                size += s;
            }
        }
    } else {
        *parg = NULL;
    }

    if (kwds != NULL) {
        assert((PyDict_CheckExact(kwds)));
        s = PyDict_GET_SIZE(kwds);
        if (s < 0) {
            return -1;
        }
        size += s;
    }

    return size;
}

static inline int
_multidict_clone_fast(mod_state *state, MultiDictObject *self, bool is_ci,
                      PyObject *arg, PyObject *kwds)
{
    int ret = 0;
    if (arg != NULL && kwds == NULL) {
        MultiDictObject *other = NULL;
        if (AnyMultiDict_Check(state, arg)) {
            other = (MultiDictObject *)arg;
        } else if (AnyMultiDictProxy_Check(state, arg)) {
            other = ((MultiDictProxyObject *)arg)->md;
        }
        if (other != NULL && other->is_ci == is_ci) {
            if (md_clone_from_ht(self, other) < 0) {
                ret = -1;
                goto done;
            }
            ret = 1;
            goto done;
        }
    }
done:
    return ret;
}

static inline PyObject *
multidict_copy(MultiDictObject *self)
{
    PyObject *ret = PyType_GenericNew(Py_TYPE(self), NULL, NULL);
    if (ret == NULL) {
        goto fail;
    }

    MultiDictObject *new_md = (MultiDictObject *)ret;
    if (md_clone_from_ht(new_md, self) < 0) {
        goto fail;
    }
    ASSERT_CONSISTENT(new_md, false);
    return ret;
fail:
    Py_XDECREF(ret);
    return NULL;
}

static inline PyObject *
_multidict_proxy_copy(MultiDictProxyObject *self, PyTypeObject *type)
{
    return multidict_copy(self->md);
}

/******************** Base Methods ********************/

static inline PyObject *
multidict_getall(MultiDictObject *self, PyObject *const *args,
                 Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *list = NULL, *key = NULL, *_default = NULL;

    if (parse2("getall",
               args,
               nargs,
               kwnames,
               1,
               "key",
               &key,
               "default",
               &_default) < 0) {
        return NULL;
    }
    if (md_get_all(self, key, &list) < 0) {
        return NULL;
    }

    ASSERT_CONSISTENT(self, false);

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
    PyObject *key = NULL, *_default = NULL;

    if (parse2("getone",
               args,
               nargs,
               kwnames,
               1,
               "key",
               &key,
               "default",
               &_default) < 0) {
        return NULL;
    }
    return _multidict_getone(self, key, _default);
}

static inline PyObject *
multidict_get(MultiDictObject *self, PyObject *const *args, Py_ssize_t nargs,
              PyObject *kwnames)
{
    PyObject *key = NULL;
    PyObject *_default = NULL;
    bool decref_default = false;

    if (parse2("get",
               args,
               nargs,
               kwnames,
               1,
               "key",
               &key,
               "default",
               &_default) < 0) {
        return NULL;
    }
    if (_default == NULL) {
        _default = Py_GetConstant(Py_CONSTANT_NONE);
        if (_default == NULL) {
            return NULL;
        }
        decref_default = true;
    }
    ASSERT_CONSISTENT(self, false);
    PyObject *ret = _multidict_getone(self, key, _default);
    if (decref_default) {
        Py_CLEAR(_default);
    }
    return ret;
}

static PyObject *
multidict_keys(MultiDictObject *self)
{
    return multidict_keysview_new(self);
}

static PyObject *
multidict_items(MultiDictObject *self)
{
    return multidict_itemsview_new(self);
}

static PyObject *
multidict_values(MultiDictObject *self)
{
    return multidict_valuesview_new(self);
}

static PyObject *
multidict_reduce(MultiDictObject *self)
{
    PyObject *items = NULL, *items_list = NULL, *args = NULL, *result = NULL;

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

static PyObject *
multidict_repr(MultiDictObject *self)
{
    int tmp = Py_ReprEnter((PyObject *)self);
    if (tmp < 0) {
        return NULL;
    }
    if (tmp > 0) {
        return PyUnicode_FromString("...");
    }
    PyObject *name =
        PyObject_GetAttr((PyObject *)Py_TYPE(self), self->state->str_name);
    if (name == NULL) {
        Py_ReprLeave((PyObject *)self);
        return NULL;
    }
    PyObject *ret = md_repr(self, name, true, true);
    Py_ReprLeave((PyObject *)self);
    Py_CLEAR(name);
    return ret;
}

static Py_ssize_t
multidict_mp_len(MultiDictObject *self)
{
    return md_len(self);
}

static PyObject *
multidict_mp_subscript(MultiDictObject *self, PyObject *key)
{
    return _multidict_getone(self, key, NULL);
}

static int
multidict_mp_as_subscript(MultiDictObject *self, PyObject *key, PyObject *val)
{
    if (val == NULL) {
        return md_del(self, key);
    } else {
        return md_replace(self, key, val);
    }
}

static int
multidict_sq_contains(MultiDictObject *self, PyObject *key)
{
    return md_contains(self, key, NULL);
}

static PyObject *
multidict_tp_iter(MultiDictObject *self)
{
    return multidict_keys_iter_new(self);
}

static PyObject *
multidict_tp_richcompare(MultiDictObject *self, PyObject *other, int op)
{
    int cmp;

    if (op != Py_EQ && op != Py_NE) {
        Py_RETURN_NOTIMPLEMENTED;
    }

    if ((PyObject *)self == other) {
        cmp = 1;
        if (op == Py_NE) {
            cmp = !cmp;
        }
        return PyBool_FromLong(cmp);
    }

    mod_state *state = self->state;
    if (AnyMultiDict_Check(state, other)) {
        cmp = md_eq(self, (MultiDictObject *)other);
    } else if (AnyMultiDictProxy_Check(state, other)) {
        cmp = md_eq(self, ((MultiDictProxyObject *)other)->md);
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
            cmp = md_eq_to_mapping(self, other);
        } else {
            cmp = 0;  // e.g., multidict is not equal to a list
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

static void
multidict_tp_dealloc(MultiDictObject *self)
{
    PyObject_GC_UnTrack(self);
    Py_TRASHCAN_BEGIN(self, multidict_tp_dealloc)
        PyObject_ClearWeakRefs((PyObject *)self);
    md_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
    Py_TRASHCAN_END  // there should be no code after this
}

static int
multidict_tp_traverse(MultiDictObject *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self));
    return md_traverse(self, visit, arg);
}

static int
multidict_tp_clear(MultiDictObject *self)
{
    return md_clear(self);
}

PyDoc_STRVAR(multidict_getall_doc,
             "Return a list of all values matching the key.");

PyDoc_STRVAR(multidict_getone_doc, "Get first value matching the key.");

PyDoc_STRVAR(
    multidict_get_doc,
    "Get first value matching the key.\n\nThe method is alias for .getone().");

PyDoc_STRVAR(multidict_keys_doc,
             "Return a new view of the dictionary's keys.");

PyDoc_STRVAR(
    multidict_items_doc,
    "Return a new view of the dictionary's items *(key, value) pairs).");

PyDoc_STRVAR(multidict_values_doc,
             "Return a new view of the dictionary's values.");

/******************** MultiDict ********************/

static int
multidict_tp_init(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    mod_state *state = get_mod_state_by_def((PyObject *)self);
    PyObject *arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(state, args, kwds, "MultiDict", &arg);
    if (size < 0) {
        goto fail;
    }
    int tmp = _multidict_clone_fast(state, self, false, args, kwds);
    if (tmp < 0) {
        goto fail;
    } else if (tmp == 1) {
        goto done;
    }
    if (md_init(self, state, false, size) < 0) {
        goto fail;
    }
    if (_multidict_extend(self, arg, kwds, "MultiDict", Extend) < 0) {
        goto fail;
    }
done:
    Py_CLEAR(arg);
    ASSERT_CONSISTENT(self, false);
    return 0;
fail:
    Py_CLEAR(arg);
    return -1;
}

static PyObject *
multidict_add(MultiDictObject *self, PyObject *const *args, Py_ssize_t nargs,
              PyObject *kwnames)
{
    PyObject *key = NULL, *val = NULL;

    if (parse2("add", args, nargs, kwnames, 2, "key", &key, "value", &val) <
        0) {
        return NULL;
    }
    if (md_add(self, key, val) < 0) {
        return NULL;
    }
    ASSERT_CONSISTENT(self, false);
    Py_RETURN_NONE;
}

static PyObject *
multidict_extend(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(self->state, args, kwds, "extend", &arg);
    if (size < 0) {
        goto fail;
    }
    if (md_reserve(self, size) < 0) {
        goto fail;
    }
    if (_multidict_extend(self, arg, kwds, "extend", Extend) < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    ASSERT_CONSISTENT(self, false);
    Py_RETURN_NONE;
fail:
    Py_CLEAR(arg);
    return NULL;
}

static PyObject *
multidict_clear(MultiDictObject *self)
{
    if (md_clear(self) < 0) {
        return NULL;
    }

    ASSERT_CONSISTENT(self, false);
    Py_RETURN_NONE;
}

static PyObject *
multidict_setdefault(MultiDictObject *self, PyObject *const *args,
                     Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key = NULL;
    PyObject *_default = NULL;
    bool decref_default = false;
    PyObject *ret = NULL;

    if (parse2("setdefault",
               args,
               nargs,
               kwnames,
               1,
               "key",
               &key,
               "default",
               &_default) < 0) {
        return NULL;
    }
    if (_default == NULL) {
        _default = Py_GetConstant(Py_CONSTANT_NONE);
        if (_default == NULL) {
            return NULL;
        }
        decref_default = true;
    }
    ASSERT_CONSISTENT(self, false);
    if (md_set_default(self, key, _default, &ret) < 0) {
        return NULL;
    }
    if (decref_default) {
        Py_CLEAR(_default);
    }
    return ret;
}

static PyObject *
multidict_popone(MultiDictObject *self, PyObject *const *args,
                 Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key = NULL, *_default = NULL, *ret_val = NULL;

    if (parse2("popone",
               args,
               nargs,
               kwnames,
               1,
               "key",
               &key,
               "default",
               &_default) < 0) {
        return NULL;
    }
    if (md_pop_one(self, key, &ret_val) < 0) {
        return NULL;
    }

    ASSERT_CONSISTENT(self, false);
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

static PyObject *
multidict_pop(MultiDictObject *self, PyObject *const *args, Py_ssize_t nargs,
              PyObject *kwnames)
{
    PyObject *key = NULL, *_default = NULL, *ret_val = NULL;

    if (parse2("pop",
               args,
               nargs,
               kwnames,
               1,
               "key",
               &key,
               "default",
               &_default) < 0) {
        return NULL;
    }
    if (md_pop_one(self, key, &ret_val) < 0) {
        return NULL;
    }

    ASSERT_CONSISTENT(self, false);
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

static PyObject *
multidict_popall(MultiDictObject *self, PyObject *const *args,
                 Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *key = NULL, *_default = NULL, *ret_val = NULL;

    if (parse2("popall",
               args,
               nargs,
               kwnames,
               1,
               "key",
               &key,
               "default",
               &_default) < 0) {
        return NULL;
    }
    if (md_pop_all(self, key, &ret_val) < 0) {
        return NULL;
    }

    ASSERT_CONSISTENT(self, false);
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

static PyObject *
multidict_popitem(MultiDictObject *self)
{
    return md_pop_item(self);
}

static PyObject *
multidict_update(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(self->state, args, kwds, "update", &arg);
    if (size < 0) {
        goto fail;
    }
    if (md_reserve(self, size) < 0) {
        goto fail;
    }
    if (_multidict_extend(self, arg, kwds, "update", Update) < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    ASSERT_CONSISTENT(self, false);
    Py_RETURN_NONE;
fail:
    Py_CLEAR(arg);
    return NULL;
}

static PyObject *
multidict_merge(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(self->state, args, kwds, "merge", &arg);
    if (size < 0) {
        goto fail;
    }
    if (md_reserve(self, size) < 0) {
        goto fail;
    }
    if (_multidict_extend(self, arg, kwds, "merge", Merge) < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    ASSERT_CONSISTENT(self, false);
    Py_RETURN_NONE;
fail:
    Py_CLEAR(arg);
    return NULL;
}

PyDoc_STRVAR(multidict_add_doc,
             "Add the key and value, not overwriting any previous value.");

PyDoc_STRVAR(multidict_copy_doc, "Return a copy of itself.");

PyDoc_STRVAR(multdicit_method_extend_doc,
             "Extend current MultiDict with more values.\n\
This method must be used instead of update.");

PyDoc_STRVAR(multidict_clear_doc, "Remove all items from MultiDict");

PyDoc_STRVAR(
    multidict_setdefault_doc,
    "Return value for key, set value to default if key is not present.");

PyDoc_STRVAR(
    multidict_popone_doc,
    "Remove the last occurrence of key and return the corresponding value.\n\n\
If key is not found, default is returned if given, otherwise KeyError is \
raised.\n");

PyDoc_STRVAR(
    multidict_pop_doc,
    "Remove the last occurrence of key and return the corresponding value.\n\n\
If key is not found, default is returned if given, otherwise KeyError is \
raised.\n");

PyDoc_STRVAR(
    multidict_popall_doc,
    "Remove all occurrences of key and return the list of corresponding values.\n\n\
If key is not found, default is returned if given, otherwise KeyError is \
raised.\n");

PyDoc_STRVAR(multidict_popitem_doc,
             "Remove and return an arbitrary (key, value) pair.");

PyDoc_STRVAR(multidict_update_doc,
             "Update the dictionary, overwriting existing keys.");

PyDoc_STRVAR(multidict_merge_doc,
             "Merge into the dictionary, adding non-existing keys.");

PyDoc_STRVAR(sizeof__doc__, "D.__sizeof__() -> size of D in memory, in bytes");

static PyObject *
multidict_sizeof(MultiDictObject *self)
{
    Py_ssize_t size = sizeof(MultiDictObject);
    if (self->keys != &empty_htkeys) size += htkeys_sizeof(self->keys);
    return PyLong_FromSsize_t(size);
}

static PyMethodDef multidict_methods[] = {
    {"getall",
     (PyCFunction)multidict_getall,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_getall_doc},
    {"getone",
     (PyCFunction)multidict_getone,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_getone_doc},
    {"get",
     (PyCFunction)multidict_get,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_get_doc},
    {"keys", (PyCFunction)multidict_keys, METH_NOARGS, multidict_keys_doc},
    {"items", (PyCFunction)multidict_items, METH_NOARGS, multidict_items_doc},
    {"values",
     (PyCFunction)multidict_values,
     METH_NOARGS,
     multidict_values_doc},
    {"add",
     (PyCFunction)multidict_add,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_add_doc},
    {"copy", (PyCFunction)multidict_copy, METH_NOARGS, multidict_copy_doc},
    {"extend",
     (PyCFunction)multidict_extend,
     METH_VARARGS | METH_KEYWORDS,
     multdicit_method_extend_doc},
    {"clear", (PyCFunction)multidict_clear, METH_NOARGS, multidict_clear_doc},
    {"setdefault",
     (PyCFunction)multidict_setdefault,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_setdefault_doc},
    {"popone",
     (PyCFunction)multidict_popone,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_popone_doc},
    {"pop",
     (PyCFunction)multidict_pop,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_pop_doc},
    {"popall",
     (PyCFunction)multidict_popall,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_popall_doc},
    {"popitem",
     (PyCFunction)multidict_popitem,
     METH_NOARGS,
     multidict_popitem_doc},
    {"update",
     (PyCFunction)multidict_update,
     METH_VARARGS | METH_KEYWORDS,
     multidict_update_doc},
    {"merge",
     (PyCFunction)multidict_merge,
     METH_VARARGS | METH_KEYWORDS,
     multidict_merge_doc},
    {
        "__reduce__",
        (PyCFunction)multidict_reduce,
        METH_NOARGS,
        NULL,
    },
    {"__class_getitem__",
     (PyCFunction)Py_GenericAlias,
     METH_O | METH_CLASS,
     NULL},
    {
        "__sizeof__",
        (PyCFunction)multidict_sizeof,
        METH_NOARGS,
        sizeof__doc__,
    },
    {NULL, NULL} /* sentinel */
};

PyDoc_STRVAR(MultDict_doc, "Dictionary with the support for duplicate keys.");

#ifndef MANAGED_WEAKREFS
static PyMemberDef multidict_members[] = {
    {"__weaklistoffset__",
     Py_T_PYSSIZET,
     offsetof(MultiDictObject, weaklist),
     Py_READONLY},
    {NULL} /* Sentinel */
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
    .flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE
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

static int
cimultidict_tp_init(MultiDictObject *self, PyObject *args, PyObject *kwds)
{
    mod_state *state = get_mod_state_by_def((PyObject *)self);
    PyObject *arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(state, args, kwds, "CIMultiDict", &arg);
    if (size < 0) {
        goto fail;
    }
    int tmp = _multidict_clone_fast(state, self, true, args, kwds);
    if (tmp < 0) {
        goto fail;
    } else if (tmp == 1) {
        goto done;
    }
    if (md_init(self, state, true, size) < 0) {
        goto fail;
    }
    if (_multidict_extend(self, arg, kwds, "CIMultiDict", Extend) < 0) {
        goto fail;
    }
done:
    Py_CLEAR(arg);
    ASSERT_CONSISTENT(self, false);
    return 0;
fail:
    Py_CLEAR(arg);
    return -1;
}

PyDoc_STRVAR(
    CIMultDict_doc,
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

static int
multidict_proxy_tp_init(MultiDictProxyObject *self, PyObject *args,
                        PyObject *kwds)
{
    mod_state *state = get_mod_state_by_def((PyObject *)self);
    PyObject *arg = NULL;
    MultiDictObject *md = NULL;

    if (!PyArg_UnpackTuple(
            args, "multidict._multidict.MultiDictProxy", 0, 1, &arg)) {
        return -1;
    }
    if (arg == NULL) {
        PyErr_Format(
            PyExc_TypeError,
            "__init__() missing 1 required positional argument: 'arg'");
        return -1;
    }
    if (kwds != NULL) {
        PyErr_Format(PyExc_TypeError,
                     "__init__() doesn't accept keyword arguments");
        return -1;
    }
    if (!AnyMultiDictProxy_Check(state, arg) &&
        !AnyMultiDict_Check(state, arg)) {
        PyErr_Format(PyExc_TypeError,
                     "ctor requires MultiDict or MultiDictProxy instance, "
                     "not <class '%s'>",
                     Py_TYPE(arg)->tp_name);
        return -1;
    }

    if (AnyMultiDictProxy_Check(state, arg)) {
        md = ((MultiDictProxyObject *)arg)->md;
    } else {
        md = (MultiDictObject *)arg;
    }
    Py_INCREF(md);
    self->md = md;

    return 0;
}

static PyObject *
multidict_proxy_getall(MultiDictProxyObject *self, PyObject *const *args,
                       Py_ssize_t nargs, PyObject *kwnames)
{
    return multidict_getall(self->md, args, nargs, kwnames);
}

static PyObject *
multidict_proxy_getone(MultiDictProxyObject *self, PyObject *const *args,
                       Py_ssize_t nargs, PyObject *kwnames)
{
    return multidict_getone(self->md, args, nargs, kwnames);
}

static PyObject *
multidict_proxy_get(MultiDictProxyObject *self, PyObject *const *args,
                    Py_ssize_t nargs, PyObject *kwnames)
{
    return multidict_get(self->md, args, nargs, kwnames);
}

static PyObject *
multidict_proxy_keys(MultiDictProxyObject *self)
{
    return multidict_keysview_new(self->md);
}

static PyObject *
multidict_proxy_items(MultiDictProxyObject *self)
{
    return multidict_itemsview_new(self->md);
}

static PyObject *
multidict_proxy_values(MultiDictProxyObject *self)
{
    return multidict_valuesview_new(self->md);
}

static PyObject *
multidict_proxy_copy(MultiDictProxyObject *self)
{
    return _multidict_proxy_copy(self, self->md->state->MultiDictType);
}

static PyObject *
multidict_proxy_reduce(MultiDictProxyObject *self)
{
    PyErr_Format(
        PyExc_TypeError, "can't pickle %s objects", Py_TYPE(self)->tp_name);

    return NULL;
}

static Py_ssize_t
multidict_proxy_mp_len(MultiDictProxyObject *self)
{
    return md_len(self->md);
}

static PyObject *
multidict_proxy_mp_subscript(MultiDictProxyObject *self, PyObject *key)
{
    return _multidict_getone(self->md, key, NULL);
}

static int
multidict_proxy_sq_contains(MultiDictProxyObject *self, PyObject *key)
{
    return md_contains(self->md, key, NULL);
}

static PyObject *
multidict_proxy_tp_iter(MultiDictProxyObject *self)
{
    return multidict_keys_iter_new(self->md);
}

static PyObject *
multidict_proxy_tp_richcompare(MultiDictProxyObject *self, PyObject *other,
                               int op)
{
    return multidict_tp_richcompare(self->md, other, op);
}

static void
multidict_proxy_tp_dealloc(MultiDictProxyObject *self)
{
    PyObject_GC_UnTrack(self);
    PyObject_ClearWeakRefs((PyObject *)self);
    Py_XDECREF(self->md);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int
multidict_proxy_tp_traverse(MultiDictProxyObject *self, visitproc visit,
                            void *arg)
{
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->md);
    return 0;
}

static int
multidict_proxy_tp_clear(MultiDictProxyObject *self)
{
    Py_CLEAR(self->md);
    return 0;
}

static PyObject *
multidict_proxy_repr(MultiDictProxyObject *self)
{
    PyObject *name =
        PyObject_GetAttr((PyObject *)Py_TYPE(self), self->md->state->str_name);
    if (name == NULL) return NULL;
    PyObject *ret = md_repr(self->md, name, true, true);
    Py_CLEAR(name);
    return ret;
}

static PyMethodDef multidict_proxy_methods[] = {
    {"getall",
     (PyCFunction)multidict_proxy_getall,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_getall_doc},
    {"getone",
     (PyCFunction)multidict_proxy_getone,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_getone_doc},
    {"get",
     (PyCFunction)multidict_proxy_get,
     METH_FASTCALL | METH_KEYWORDS,
     multidict_get_doc},
    {"keys",
     (PyCFunction)multidict_proxy_keys,
     METH_NOARGS,
     multidict_keys_doc},
    {"items",
     (PyCFunction)multidict_proxy_items,
     METH_NOARGS,
     multidict_items_doc},
    {"values",
     (PyCFunction)multidict_proxy_values,
     METH_NOARGS,
     multidict_values_doc},
    {"copy",
     (PyCFunction)multidict_proxy_copy,
     METH_NOARGS,
     multidict_copy_doc},
    {"__reduce__", (PyCFunction)multidict_proxy_reduce, METH_NOARGS, NULL},
    {"__class_getitem__",
     (PyCFunction)Py_GenericAlias,
     METH_O | METH_CLASS,
     NULL},
    {NULL, NULL} /* sentinel */
};

PyDoc_STRVAR(MultDictProxy_doc, "Read-only proxy for MultiDict instance.");

#ifndef MANAGED_WEAKREFS
static PyMemberDef multidict_proxy_members[] = {
    {"__weaklistoffset__",
     Py_T_PYSSIZET,
     offsetof(MultiDictProxyObject, weaklist),
     Py_READONLY},
    {NULL} /* Sentinel */
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
    .flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE
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

static int
cimultidict_proxy_tp_init(MultiDictProxyObject *self, PyObject *args,
                          PyObject *kwds)
{
    mod_state *state = get_mod_state_by_def((PyObject *)self);
    PyObject *arg = NULL;
    MultiDictObject *md = NULL;

    if (!PyArg_UnpackTuple(
            args, "multidict._multidict.CIMultiDictProxy", 1, 1, &arg)) {
        return -1;
    }
    if (arg == NULL) {
        PyErr_Format(
            PyExc_TypeError,
            "__init__() missing 1 required positional argument: 'arg'");
        return -1;
    }
    if (kwds != NULL) {
        PyErr_Format(PyExc_TypeError,
                     "__init__() doesn't accept keyword arguments");
        return -1;
    }
    if (!CIMultiDictProxy_Check(state, arg) &&
        !CIMultiDict_Check(state, arg)) {
        PyErr_Format(PyExc_TypeError,
                     "ctor requires CIMultiDict or CIMultiDictProxy instance, "
                     "not <class '%s'>",
                     Py_TYPE(arg)->tp_name);
        return -1;
    }

    if (CIMultiDictProxy_Check(state, arg)) {
        md = ((MultiDictProxyObject *)arg)->md;
    } else {
        md = (MultiDictObject *)arg;
    }
    Py_INCREF(md);
    self->md = md;

    return 0;
}

static PyObject *
cimultidict_proxy_copy(MultiDictProxyObject *self)
{
    return _multidict_proxy_copy(self, self->md->state->CIMultiDictType);
}

PyDoc_STRVAR(CIMultDictProxy_doc, "Read-only proxy for CIMultiDict instance.");

PyDoc_STRVAR(cimultidict_proxy_copy_doc, "Return copy of itself");

static PyMethodDef cimultidict_proxy_methods[] = {
    {"copy",
     (PyCFunction)cimultidict_proxy_copy,
     METH_NOARGS,
     cimultidict_proxy_copy_doc},
    {NULL, NULL} /* sentinel */
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

static PyObject *
getversion(PyObject *self, PyObject *arg)
{
    mod_state *state = get_mod_state(self);
    MultiDictObject *md;
    if (AnyMultiDict_Check(state, arg)) {
        md = (MultiDictObject *)arg;
    } else if (AnyMultiDictProxy_Check(state, arg)) {
        md = ((MultiDictProxyObject *)arg)->md;
    } else {
        PyErr_Format(PyExc_TypeError, "unexpected type");
        return NULL;
    }
    return PyLong_FromUnsignedLong(md_version(md));
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

    Py_VISIT(state->str_canonical);
    Py_VISIT(state->str_lower);
    Py_VISIT(state->str_name);

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

    Py_CLEAR(state->str_canonical);
    Py_CLEAR(state->str_lower);
    Py_CLEAR(state->str_name);

    return 0;
}

static void
module_free(void *mod)
{
    (void)module_clear((PyObject *)mod);
}

static PyMethodDef module_methods[] = {
    {"getversion", (PyCFunction)getversion, METH_O},
    {NULL, NULL} /* sentinel */
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
    state->str_name = PyUnicode_InternFromString("__name__");
    if (state->str_name == NULL) {
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
