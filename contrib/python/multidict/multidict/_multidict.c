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

static inline PyObject*
_multidict_getone(MultiDictObject* self, PyObject* key, PyObject* _default)
{
    PyObject* val = NULL;
    int tmp = md_get_one(self, key, &val);

    if (tmp < 0) {
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

static inline MultiDictObject*
_multidict_resolve_other(mod_state* state, PyObject* arg)
{
    if (arg == NULL) {
        return NULL;
    }
    if (AnyMultiDict_Check(state, arg)) {
        return (MultiDictObject*)arg;
    }
    if (AnyMultiDictProxy_Check(state, arg)) {
        return ((MultiDictProxyObject*)arg)->md;
    }
    return NULL;
}

static inline Py_ssize_t
_multidict_extend_parse_args(mod_state* state, PyObject* args, PyObject* kwds,
                             const char* name, PyObject** parg)
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
            MultiDictObject* md = (MultiDictObject*)*parg;
            size += md_len(md);
        } else if (MultiDictProxy_CheckExact(state, *parg) ||
                   CIMultiDictProxy_CheckExact(state, *parg)) {
            MultiDictObject* md = ((MultiDictProxyObject*)*parg)->md;
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
_multidict_clone_fast(mod_state* state, MultiDictObject* self, bool is_ci,
                      PyObject* arg, PyObject* kwds)
{
    int ret = 0;
    if (arg != NULL && kwds == NULL) {
        MultiDictObject* other = NULL;
        if (AnyMultiDict_Check(state, arg)) {
            other = (MultiDictObject*)arg;
        } else if (AnyMultiDictProxy_Check(state, arg)) {
            other = ((MultiDictProxyObject*)arg)->md;
        }
        if (other != NULL && other->is_ci == is_ci) {
            int clone_ret;
            if (other != self) {
                Py_BEGIN_CRITICAL_SECTION2(self, other);
                clone_ret = md_clone_from_ht(self, other);
                Py_END_CRITICAL_SECTION2();
            } else {
                Py_BEGIN_CRITICAL_SECTION(self);
                clone_ret = md_clone_from_ht(self, other);
                Py_END_CRITICAL_SECTION();
            }
            if (clone_ret < 0) {
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

static inline int
_multidict_vectorcall_impl(mod_state* state, MultiDictObject* self, bool is_ci,
                           PyObject* arg, PyObject* const* args,
                           Py_ssize_t nargs, PyObject* kwnames)
{
    int ret;
    Py_ssize_t nkwargs = kwnames == NULL ? 0 : PyTuple_GET_SIZE(kwnames);

    if (arg != NULL) {
        MultiDictObject* other = NULL;
        if (AnyMultiDict_Check(state, arg)) {
            other = (MultiDictObject*)arg;
        } else if (AnyMultiDictProxy_Check(state, arg)) {
            other = ((MultiDictProxyObject*)arg)->md;
        }
        if (other != NULL) {
            if (other->is_ci == is_ci) {
                Py_BEGIN_CRITICAL_SECTION(other);
                ret = md_clone_from_ht(self, other);
                ASSERT_CONSISTENT(self, false);
                Py_END_CRITICAL_SECTION();
            } else {
                Py_BEGIN_CRITICAL_SECTION(other);
                ret = md_init(self, is_ci, md_len(other) + nkwargs);
                if (ret == 0) {
                    ret = md_update_from_ht(self, other, Extend);
                    ASSERT_CONSISTENT(self, false);
                }
                Py_END_CRITICAL_SECTION();
            }
        } else if (PyDict_CheckExact(arg)) {
            Py_BEGIN_CRITICAL_SECTION(arg);
            ret = md_init(self, is_ci, PyDict_GET_SIZE(arg) + nkwargs);
            if (ret == 0) {
                ret = md_update_from_dict(self, arg, Extend);
                ASSERT_CONSISTENT(self, false);
            }
            Py_END_CRITICAL_SECTION();
        } else {
            Py_ssize_t extra;
            if (PyTuple_CheckExact(arg)) {
                extra = PyTuple_GET_SIZE(arg);
            } else if (PyList_CheckExact(arg)) {
                extra = PyList_GET_SIZE(arg);
            } else {
                extra = 0;
            }

            ret = md_init(self, is_ci, nkwargs + extra);
            if (ret == 0) {
                if (arg != NULL) {
                    ret = md_update_from_seq(self, arg, Extend);
                }
                ASSERT_CONSISTENT(self, false);
            }
        }
    } else {
        ret = md_init(self, is_ci, nkwargs);
    }

    if (ret == 0) {
        if (nkwargs > 0) {
            ret = md_update_from_kwnames(self, args, nargs, kwnames);
        }
    }
    return ret;
}

static PyObject*
_multidict_ctor_vectorcall(PyObject* type, PyObject* const* args,
                           size_t nargsf, PyObject* kwnames, bool is_ci)
{
    PyTypeObject* tp = (PyTypeObject*)type;
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    const char* name = is_ci ? "CIMultiDict" : "MultiDict";

    if (nargs > 1) {
        PyErr_Format(
            PyExc_TypeError,
            "%s takes from 1 to 2 positional arguments but %zd were given",
            name,
            nargs + 1);
        return NULL;
    }

    PyObject* mod = PyType_GetModuleByDef(tp, &multidict_module);
    if (mod == NULL) {
        return NULL;
    }
    mod_state* state = get_mod_state(mod);

    PyObject* arg = nargs == 1 ? args[0] : NULL;

    MultiDictObject* self = (MultiDictObject*)tp->tp_alloc(tp, 0);
    if (self == NULL) {
        return NULL;
    }
    self->state = state;

    int ret = _multidict_vectorcall_impl(
        state, self, is_ci, arg, args, nargs, kwnames);
    if (ret < 0) {
        Py_DECREF(self);
        return NULL;
    }
    return (PyObject*)self;
}

static PyObject*
multidict_vectorcall(PyObject* type, PyObject* const* args, size_t nargsf,
                     PyObject* kwnames)
{
    return _multidict_ctor_vectorcall(type, args, nargsf, kwnames, false);
}

static PyObject*
cimultidict_vectorcall(PyObject* type, PyObject* const* args, size_t nargsf,
                       PyObject* kwnames)
{
    return _multidict_ctor_vectorcall(type, args, nargsf, kwnames, true);
}

/* ---- MultiDictProxy/CIMultiDictProxy tp_vectorcall ---- */

static inline int
_multidict_proxy_ctor_do_init(mod_state* state, MultiDictProxyObject* self,
                              bool is_ci, PyObject* arg)
{
    bool ok = is_ci ? (CIMultiDictProxy_Check(state, arg) ||
                       CIMultiDict_Check(state, arg))
                    : (AnyMultiDictProxy_Check(state, arg) ||
                       AnyMultiDict_Check(state, arg));
    if (!ok) {
        PyErr_Format(PyExc_TypeError,
                     "ctor requires %s or %s instance, not <class '%s'>",
                     is_ci ? "CIMultiDict" : "MultiDict",
                     is_ci ? "CIMultiDictProxy" : "MultiDictProxy",
                     Py_TYPE(arg)->tp_name);
        return -1;
    }
    bool arg_is_proxy = is_ci ? CIMultiDictProxy_Check(state, arg)
                              : AnyMultiDictProxy_Check(state, arg);
    MultiDictObject* md = arg_is_proxy ? ((MultiDictProxyObject*)arg)->md
                                       : (MultiDictObject*)arg;
    self->md = (MultiDictObject*)Py_NewRef(md);
    return 0;
}

static PyObject*
_multidict_proxy_ctor_vectorcall(PyObject* type, PyObject* const* args,
                                 size_t nargsf, PyObject* kwnames, bool is_ci)
{
    PyTypeObject* tp = (PyTypeObject*)type;
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    const char* clsname = is_ci ? "multidict._multidict.CIMultiDictProxy"
                                : "multidict._multidict.MultiDictProxy";

    if (kwnames != NULL && PyTuple_GET_SIZE(kwnames) > 0) {
        PyErr_Format(
            PyExc_TypeError, "%s() doesn't accept keyword arguments", clsname);
        return NULL;
    }
    if (nargs == 0) {
        PyErr_Format(PyExc_TypeError,
                     "%s() missing 1 required positional argument: 'arg'",
                     clsname);
        return NULL;
    }
    if (nargs > 1) {
        PyErr_Format(PyExc_TypeError,
                     "%s() takes 1 positional argument but %zd were given",
                     clsname,
                     nargs);
        return NULL;
    }

    PyObject* mod = PyType_GetModuleByDef(tp, &multidict_module);
    if (mod == NULL) {
        return NULL;
    }
    mod_state* state = get_mod_state(mod);

    MultiDictProxyObject* self = (MultiDictProxyObject*)tp->tp_alloc(tp, 0);
    if (self == NULL) {
        return NULL;
    }
    if (_multidict_proxy_ctor_do_init(state, self, is_ci, args[0]) < 0) {
        Py_DECREF(self);
        return NULL;
    }
    return (PyObject*)self;
}

static PyObject*
multidict_proxy_vectorcall(PyObject* type, PyObject* const* args,
                           size_t nargsf, PyObject* kwnames)
{
    return _multidict_proxy_ctor_vectorcall(
        type, args, nargsf, kwnames, false);
}

static PyObject*
cimultidict_proxy_vectorcall(PyObject* type, PyObject* const* args,
                             size_t nargsf, PyObject* kwnames)
{
    return _multidict_proxy_ctor_vectorcall(type, args, nargsf, kwnames, true);
}

static inline PyObject*
multidict_copy(MultiDictObject* self)
{
    PyTypeObject* tp = Py_TYPE(self);
    PyObject* ret = NULL;

    ret = tp->tp_alloc(tp, 0);
    if (ret == NULL) {
        goto fail;
    }

    MultiDictObject* new_md = (MultiDictObject*)ret;
    new_md->state = self->state;
    int clone_ret;
    Py_BEGIN_CRITICAL_SECTION(self);
    clone_ret = md_clone_from_ht(new_md, self);
    Py_END_CRITICAL_SECTION();
    if (clone_ret < 0) {
        goto fail;
    }
    return ret;
fail:
    Py_XDECREF(ret);
    return NULL;
}

static inline PyObject*
_multidict_proxy_copy(MultiDictProxyObject* self, PyTypeObject* type)
{
    return multidict_copy(self->md);
}

PyDoc_STRVAR(multidict_to_dict_doc,
             "Return a dict with lists of all values for each key.");

static PyObject*
multidict_to_dict(MultiDictObject* self)
{
    PyObject* result = NULL;
    int tmp;
    Py_BEGIN_CRITICAL_SECTION(self);
    tmp = md_to_dict(self, &result);
    ASSERT_CONSISTENT(self, false);
    Py_END_CRITICAL_SECTION();
    if (tmp < 0) {
        return NULL;
    }
    return result;
}

/******************** Base Methods ********************/

static inline PyObject*
multidict_getall(MultiDictObject* self, PyObject* const* args,
                 Py_ssize_t nargs, PyObject* kwnames)
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
    int tmp;
    Py_BEGIN_CRITICAL_SECTION(self);
    tmp = md_get_all(self, key, &list);
    ASSERT_CONSISTENT(self, false);
    Py_END_CRITICAL_SECTION();
    if (tmp < 0) {
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

static inline PyObject*
multidict_getone(MultiDictObject* self, PyObject* const* args,
                 Py_ssize_t nargs, PyObject* kwnames)
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

static inline PyObject*
multidict_get(MultiDictObject* self, PyObject* const* args, Py_ssize_t nargs,
              PyObject* kwnames)
{
    PyObject* key = NULL;
    PyObject* _default = NULL;
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
    PyObject* ret = _multidict_getone(self, key, _default);
    if (decref_default) {
        Py_CLEAR(_default);
    }
    return ret;
}

static PyObject*
multidict_keys(MultiDictObject* self)
{
    return multidict_keysview_new(self);
}

static PyObject*
multidict_items(MultiDictObject* self)
{
    return multidict_itemsview_new(self);
}

static PyObject*
multidict_values(MultiDictObject* self)
{
    return multidict_valuesview_new(self);
}

static PyObject*
multidict_reduce(MultiDictObject* self)
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

static PyObject*
multidict_repr(MultiDictObject* self)
{
    PyObject* ret;
    Py_BEGIN_CRITICAL_SECTION(self);
    ret = md_repr(self, (PyObject*)self, true, true);
    Py_END_CRITICAL_SECTION();
    return ret;
}

static Py_ssize_t
multidict_mp_len(MultiDictObject* self)
{
    return md_len(self);
}

static PyObject*
multidict_mp_subscript(MultiDictObject* self, PyObject* key)
{
    return _multidict_getone(self, key, NULL);
}

static int
multidict_mp_as_subscript(MultiDictObject* self, PyObject* key, PyObject* val)
{
    int ret;
    Py_BEGIN_CRITICAL_SECTION(self);
    if (val == NULL) {
        ret = md_del(self, key);
    } else {
        ret = md_replace(self, key, val);
    }
    Py_END_CRITICAL_SECTION();
    return ret;
}

static int
multidict_sq_contains(MultiDictObject* self, PyObject* key)
{
    return md_contains(self, key, NULL);
}

static PyObject*
multidict_tp_iter(MultiDictObject* self)
{
    return multidict_keys_iter_new(self, 0);
}

static PyObject*
multidict_tp_richcompare(MultiDictObject* self, PyObject* other, int op)
{
    int cmp;

    if (op != Py_EQ && op != Py_NE) {
        Py_RETURN_NOTIMPLEMENTED;
    }

    if ((PyObject*)self == other) {
        cmp = 1;
        if (op == Py_NE) {
            cmp = !cmp;
        }
        return PyBool_FromLong(cmp);
    }

    mod_state* state = self->state;
    MultiDictObject* other_md = _multidict_resolve_other(state, other);
    if (other_md != NULL) {
        Py_BEGIN_CRITICAL_SECTION2(self, other_md);
        cmp = md_eq(self, other_md);
        Py_END_CRITICAL_SECTION2();
    } else {
        Py_BEGIN_CRITICAL_SECTION(self);
        bool fits = false;
        fits = PyDict_Check(other);
        if (!fits) {
            PyObject* keys = PyMapping_Keys(other);
            if (keys != NULL) {
                fits = true;
            } else if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
                // other is not a mapping (no keys()); treat as not equal
                PyErr_Clear();
            } else {
                // propagate MemoryError / KeyboardInterrupt / etc.
                cmp = -1;
                goto done;
            }
            Py_CLEAR(keys);
        }
        if (fits) {
            cmp = md_eq_to_mapping(self, other);
        } else {
            cmp = 0;  // e.g., multidict is not equal to a list
        }
    done:;
        Py_END_CRITICAL_SECTION();
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
multidict_tp_dealloc(MultiDictObject* self)
{
    PyTypeObject* tp = Py_TYPE(self);
    PyObject_GC_UnTrack(self);
    Py_TRASHCAN_BEGIN(self, multidict_tp_dealloc)
        PyObject_ClearWeakRefs((PyObject*)self);
    md_clear(self);
    tp->tp_free((PyObject*)self);
    Py_DECREF(tp);
    Py_TRASHCAN_END  // there should be no code after this
}

static int
multidict_tp_traverse(MultiDictObject* self, visitproc visit, void* arg)
{
    Py_VISIT(Py_TYPE(self));
    return md_traverse(self, visit, arg);
}

static int
multidict_tp_clear(MultiDictObject* self)
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
multidict_tp_init(MultiDictObject* self, PyObject* args, PyObject* kwds)
{
    mod_state* state = get_mod_state_by_def((PyObject*)self);
    PyObject* arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(state, args, kwds, "MultiDict", &arg);
    if (size < 0) {
        goto fail;
    }
    if (kwds && !PyArg_ValidateKeywordArguments(kwds)) {
        goto fail;
    }
    int tmp = _multidict_clone_fast(state, self, false, arg, kwds);
    if (tmp < 0) {
        goto fail;
    } else if (tmp == 1) {
        goto done;
    }
    MultiDictObject* other = _multidict_resolve_other(state, arg);
    bool arg_is_dict = arg != NULL && PyDict_CheckExact(arg);
    int ret;
    if (other != NULL && other != self) {
        Py_BEGIN_CRITICAL_SECTION2(self, other);
        ret = md_init(self, false, size);
        if (ret == 0) {
            ret = md_update_from_ht(self, other, Extend);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION2();
    } else if (arg_is_dict) {
        Py_BEGIN_CRITICAL_SECTION2(self, arg);
        ret = md_init(self, false, size);
        if (ret == 0) {
            ret = md_update_from_dict(self, arg, Extend);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION2();
    } else {
        Py_BEGIN_CRITICAL_SECTION(self);
        ret = md_init(self, false, size);
        if (ret == 0) {
            if (other != NULL) {
                ret = md_extend_self(self);
            } else if (arg != NULL) {
                ret = md_update_from_seq(self, arg, Extend);
            }
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION();
    }
    if (ret < 0) {
        goto fail;
    }
done:
    Py_CLEAR(arg);
    return 0;
fail:
    Py_CLEAR(arg);
    return -1;
}

static PyObject*
multidict_tp_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    /* Initialize the object to a valid empty container.
       Otherwise ``MultiDict.__new__(MultiDict)`` (or a subclass that
       skips ``super().__init__()``) without subsequent ``md.__init__()``
       call leaves the object in incorrect state,
       any its usage leads to segfault. */
    PyObject* mod = PyType_GetModuleByDef(type, &multidict_module);
    if (mod == NULL) {
        return NULL;
    }
    mod_state* state = get_mod_state(mod);
    MultiDictObject* self = (MultiDictObject*)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    self->state = state;
    if (md_init(self, false, 0) < 0) {
        Py_DECREF(self);
        return NULL;
    }
    return (PyObject*)self;
}

static PyObject*
multidict_add(MultiDictObject* self, PyObject* const* args, Py_ssize_t nargs,
              PyObject* kwnames)
{
    PyObject *key = NULL, *val = NULL;

    if (parse2("add", args, nargs, kwnames, 2, "key", &key, "value", &val) <
        0) {
        return NULL;
    }
    int tmp;
    Py_BEGIN_CRITICAL_SECTION(self);
    tmp = md_add(self, key, val);
    ASSERT_CONSISTENT(self, false);
    Py_END_CRITICAL_SECTION();
    if (tmp < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject*
multidict_extend(MultiDictObject* self, PyObject* args, PyObject* kwds)
{
    PyObject* arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(self->state, args, kwds, "extend", &arg);
    if (size < 0) {
        goto fail;
    }
    if (kwds && !PyArg_ValidateKeywordArguments(kwds)) {
        goto fail;
    }
    MultiDictObject* other = _multidict_resolve_other(self->state, arg);
    bool arg_is_dict = arg != NULL && PyDict_CheckExact(arg);
    int ret;
    if (other != NULL && other != self) {
        Py_BEGIN_CRITICAL_SECTION2(self, other);
        ret = md_reserve(self, size);
        if (ret == 0) {
            ret = md_update_from_ht(self, other, Extend);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION2();
    } else if (arg_is_dict) {
        Py_BEGIN_CRITICAL_SECTION2(self, arg);
        ret = md_reserve(self, size);
        if (ret == 0) {
            ret = md_update_from_dict(self, arg, Extend);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION2();
    } else {
        Py_BEGIN_CRITICAL_SECTION(self);
        ret = md_reserve(self, size);
        if (ret == 0) {
            if (other != NULL) {
                ret = md_extend_self(self);
            } else if (arg != NULL) {
                ret = md_update_from_seq(self, arg, Extend);
            }
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION();
    }
    if (ret < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    Py_RETURN_NONE;
fail:
    Py_CLEAR(arg);
    return NULL;
}

static PyObject*
multidict_clear(MultiDictObject* self)
{
    int ret;
    Py_BEGIN_CRITICAL_SECTION(self);
    ret = md_clear(self);
    Py_END_CRITICAL_SECTION();
    if (ret < 0) {
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject*
multidict_setdefault(MultiDictObject* self, PyObject* const* args,
                     Py_ssize_t nargs, PyObject* kwnames)
{
    PyObject* key = NULL;
    PyObject* _default = NULL;
    bool decref_none_default = false;
    PyObject* ret = NULL;

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
        decref_none_default = true;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    ASSERT_CONSISTENT(self, false);
    if (md_set_default(self, key, _default, &ret) < 0) {
        assert(ret == NULL);
    }
    Py_END_CRITICAL_SECTION();
    if (decref_none_default) {
        Py_CLEAR(_default);  // never raises exception
    }
    return ret;
}

static PyObject*
multidict_popone(MultiDictObject* self, PyObject* const* args,
                 Py_ssize_t nargs, PyObject* kwnames)
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
    int tmp;
    Py_BEGIN_CRITICAL_SECTION(self);
    tmp = md_pop_one(self, key, &ret_val);
    ASSERT_CONSISTENT(self, false);
    Py_END_CRITICAL_SECTION();
    if (tmp < 0) {
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

static PyObject*
multidict_pop(MultiDictObject* self, PyObject* const* args, Py_ssize_t nargs,
              PyObject* kwnames)
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
    int tmp;
    Py_BEGIN_CRITICAL_SECTION(self);
    tmp = md_pop_one(self, key, &ret_val);
    ASSERT_CONSISTENT(self, false);
    Py_END_CRITICAL_SECTION();
    if (tmp < 0) {
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

static PyObject*
multidict_popall(MultiDictObject* self, PyObject* const* args,
                 Py_ssize_t nargs, PyObject* kwnames)
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
    int tmp;
    Py_BEGIN_CRITICAL_SECTION(self);
    tmp = md_pop_all(self, key, &ret_val);
    ASSERT_CONSISTENT(self, false);
    Py_END_CRITICAL_SECTION();
    if (tmp < 0) {
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

static PyObject*
multidict_popitem(MultiDictObject* self)
{
    PyObject* ret;
    Py_BEGIN_CRITICAL_SECTION(self);
    ret = md_pop_item(self);
    Py_END_CRITICAL_SECTION();
    return ret;
}

static PyObject*
multidict_update(MultiDictObject* self, PyObject* args, PyObject* kwds)
{
    PyObject* arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(self->state, args, kwds, "update", &arg);
    if (size < 0) {
        goto fail;
    }
    if (kwds && !PyArg_ValidateKeywordArguments(kwds)) {
        goto fail;
    }
    MultiDictObject* other = _multidict_resolve_other(self->state, arg);
    bool arg_is_dict = arg != NULL && PyDict_CheckExact(arg);
    int ret;
    if (other != NULL && other != self) {
        Py_BEGIN_CRITICAL_SECTION2(self, other);
        ret = md_reserve(self, size);
        if (ret == 0) {
            ret = md_update_from_ht(self, other, Update);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Update);
            }
            ASSERT_CONSISTENT(self, true);
        }
        md_post_update(self);
        Py_END_CRITICAL_SECTION2();
    } else if (arg_is_dict) {
        Py_BEGIN_CRITICAL_SECTION2(self, arg);
        ret = md_reserve(self, size);
        if (ret == 0) {
            ret = md_update_from_dict(self, arg, Update);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Update);
            }
            ASSERT_CONSISTENT(self, true);
        }
        md_post_update(self);
        Py_END_CRITICAL_SECTION2();
    } else {
        Py_BEGIN_CRITICAL_SECTION(self);
        ret = md_reserve(self, size);
        if (ret == 0) {
            // self-referential update() is a no-op: entries already match
            if (other == NULL && arg != NULL) {
                ret = md_update_from_seq(self, arg, Update);
            }
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Update);
            }
            ASSERT_CONSISTENT(self, true);
        }
        md_post_update(self);
        Py_END_CRITICAL_SECTION();
    }
    if (ret < 0) {
        goto fail;
    }
    Py_CLEAR(arg);
    Py_RETURN_NONE;
fail:
    Py_CLEAR(arg);
    return NULL;
}

static PyObject*
multidict_merge(MultiDictObject* self, PyObject* args, PyObject* kwds)
{
    PyObject* arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(self->state, args, kwds, "merge", &arg);
    if (size < 0) {
        goto fail;
    }
    if (kwds && !PyArg_ValidateKeywordArguments(kwds)) {
        goto fail;
    }
    MultiDictObject* other = _multidict_resolve_other(self->state, arg);
    bool arg_is_dict = arg != NULL && PyDict_CheckExact(arg);
    int ret;
    if (other != NULL && other != self) {
        Py_BEGIN_CRITICAL_SECTION2(self, other);
        ret = md_reserve(self, size);
        if (ret == 0) {
            ret = md_update_from_ht(self, other, Merge);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Merge);
            }
            ASSERT_CONSISTENT(self, true);
        }
        md_post_update(self);
        Py_END_CRITICAL_SECTION2();
    } else if (arg_is_dict) {
        Py_BEGIN_CRITICAL_SECTION2(self, arg);
        ret = md_reserve(self, size);
        if (ret == 0) {
            ret = md_update_from_dict(self, arg, Merge);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Merge);
            }
            ASSERT_CONSISTENT(self, true);
        }
        md_post_update(self);
        Py_END_CRITICAL_SECTION2();
    } else {
        Py_BEGIN_CRITICAL_SECTION(self);
        ret = md_reserve(self, size);
        if (ret == 0) {
            // self-referential merge() is a no-op: entries already match
            if (other == NULL && arg != NULL) {
                ret = md_update_from_seq(self, arg, Merge);
            }
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Merge);
            }
            ASSERT_CONSISTENT(self, true);
        }
        md_post_update(self);
        Py_END_CRITICAL_SECTION();
    }
    if (ret < 0) {
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

static PyObject*
multidict_sizeof(MultiDictObject* self)
{
    Py_ssize_t size = sizeof(MultiDictObject);
    Py_BEGIN_CRITICAL_SECTION(self);
    htkeys_t* keys = self->keys;
    if (keys != &empty_htkeys) {
        size += htkeys_sizeof(keys);
        if (keys->resume_slots != NULL) {
            size += (Py_ssize_t)htkeys_resume_slots_bytes(keys->log2_size);
        }
    }
    Py_END_CRITICAL_SECTION();
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
    {"to_dict",
     (PyCFunction)multidict_to_dict,
     METH_NOARGS,
     multidict_to_dict_doc},
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
    {Py_tp_doc, (void*)MultDict_doc},

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
    {Py_tp_new, multidict_tp_new},
    {Py_tp_free, PyObject_GC_Del},

#if PY_VERSION_HEX >= 0x030e00f0
    {Py_tp_vectorcall, multidict_vectorcall},
#endif
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

static PyObject*
cimultidict_tp_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    MultiDictObject* self =
        (MultiDictObject*)multidict_tp_new(type, args, kwds);
    if (self == NULL) {
        return NULL;
    }
    self->is_ci = true;
    return (PyObject*)self;
}

static int
cimultidict_tp_init(MultiDictObject* self, PyObject* args, PyObject* kwds)
{
    mod_state* state = get_mod_state_by_def((PyObject*)self);
    PyObject* arg = NULL;
    Py_ssize_t size =
        _multidict_extend_parse_args(state, args, kwds, "CIMultiDict", &arg);
    if (size < 0) {
        goto fail;
    }
    if (kwds && !PyArg_ValidateKeywordArguments(kwds)) {
        goto fail;
    }
    int tmp = _multidict_clone_fast(state, self, true, arg, kwds);
    if (tmp < 0) {
        goto fail;
    } else if (tmp == 1) {
        goto done;
    }
    MultiDictObject* other = _multidict_resolve_other(state, arg);
    bool arg_is_dict = arg != NULL && PyDict_CheckExact(arg);
    int ret;
    if (other != NULL && other != self) {
        Py_BEGIN_CRITICAL_SECTION2(self, other);
        ret = md_init(self, true, size);
        if (ret == 0) {
            ret = md_update_from_ht(self, other, Extend);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION2();
    } else if (arg_is_dict) {
        Py_BEGIN_CRITICAL_SECTION2(self, arg);
        ret = md_init(self, true, size);
        if (ret == 0) {
            ret = md_update_from_dict(self, arg, Extend);
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION2();
    } else {
        Py_BEGIN_CRITICAL_SECTION(self);
        ret = md_init(self, true, size);
        if (ret == 0) {
            if (other != NULL) {
                ret = md_extend_self(self);
            } else if (arg != NULL) {
                ret = md_update_from_seq(self, arg, Extend);
            }
            if (ret == 0 && kwds != NULL) {
                ret = md_update_from_dict(self, kwds, Extend);
            }
            ASSERT_CONSISTENT(self, false);
        }
        Py_END_CRITICAL_SECTION();
    }
    if (ret < 0) {
        goto fail;
    }
done:
    Py_CLEAR(arg);
    return 0;
fail:
    Py_CLEAR(arg);
    return -1;
}

PyDoc_STRVAR(
    CIMultDict_doc,
    "Dictionary with the support for duplicate case-insensitive keys.");

static PyType_Slot cimultidict_slots[] = {
    {Py_tp_doc, (void*)CIMultDict_doc},
    {Py_tp_init, cimultidict_tp_init},
    {Py_tp_new, cimultidict_tp_new},
#if PY_VERSION_HEX >= 0x030e00f0
    {Py_tp_vectorcall, cimultidict_vectorcall},
#endif
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
multidict_proxy_tp_init(MultiDictProxyObject* self, PyObject* args,
                        PyObject* kwds)
{
    mod_state* state = get_mod_state_by_def((PyObject*)self);
    PyObject* arg = NULL;
    MultiDictObject* md = NULL;

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
        md = ((MultiDictProxyObject*)arg)->md;
    } else {
        md = (MultiDictObject*)arg;
    }
    Py_INCREF(md);
    Py_XSETREF(self->md, md);

    return 0;
}

static PyObject*
multidict_proxy_getall(MultiDictProxyObject* self, PyObject* const* args,
                       Py_ssize_t nargs, PyObject* kwnames)
{
    return multidict_getall(self->md, args, nargs, kwnames);
}

static PyObject*
multidict_proxy_getone(MultiDictProxyObject* self, PyObject* const* args,
                       Py_ssize_t nargs, PyObject* kwnames)
{
    return multidict_getone(self->md, args, nargs, kwnames);
}

static PyObject*
multidict_proxy_get(MultiDictProxyObject* self, PyObject* const* args,
                    Py_ssize_t nargs, PyObject* kwnames)
{
    return multidict_get(self->md, args, nargs, kwnames);
}

static PyObject*
multidict_proxy_keys(MultiDictProxyObject* self)
{
    return multidict_keysview_new(self->md);
}

static PyObject*
multidict_proxy_items(MultiDictProxyObject* self)
{
    return multidict_itemsview_new(self->md);
}

static PyObject*
multidict_proxy_values(MultiDictProxyObject* self)
{
    return multidict_valuesview_new(self->md);
}

static PyObject*
multidict_proxy_copy(MultiDictProxyObject* self)
{
    return _multidict_proxy_copy(self, self->md->state->MultiDictType);
}

static PyObject*
multidict_proxy_reduce(MultiDictProxyObject* self)
{
    PyErr_Format(
        PyExc_TypeError, "can't pickle %s objects", Py_TYPE(self)->tp_name);

    return NULL;
}

static PyObject*
multidict_proxy_to_dict(MultiDictProxyObject* self)
{
    return multidict_to_dict(self->md);
}

static Py_ssize_t
multidict_proxy_mp_len(MultiDictProxyObject* self)
{
    return md_len(self->md);
}

static PyObject*
multidict_proxy_mp_subscript(MultiDictProxyObject* self, PyObject* key)
{
    return _multidict_getone(self->md, key, NULL);
}

static int
multidict_proxy_sq_contains(MultiDictProxyObject* self, PyObject* key)
{
    return multidict_sq_contains(self->md, key);
}

static PyObject*
multidict_proxy_tp_iter(MultiDictProxyObject* self)
{
    return multidict_keys_iter_new(self->md, 0);
}

static PyObject*
multidict_proxy_tp_richcompare(MultiDictProxyObject* self, PyObject* other,
                               int op)
{
    return multidict_tp_richcompare(self->md, other, op);
}

static void
multidict_proxy_tp_dealloc(MultiDictProxyObject* self)
{
    PyTypeObject* tp = Py_TYPE(self);
    PyObject_GC_UnTrack(self);
    PyObject_ClearWeakRefs((PyObject*)self);
    Py_XDECREF(self->md);
    tp->tp_free((PyObject*)self);
    Py_DECREF(tp);
}

static int
multidict_proxy_tp_traverse(MultiDictProxyObject* self, visitproc visit,
                            void* arg)
{
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->md);
    return 0;
}

static int
multidict_proxy_tp_clear(MultiDictProxyObject* self)
{
    Py_CLEAR(self->md);
    return 0;
}

static PyObject*
multidict_proxy_repr(MultiDictProxyObject* self)
{
    PyObject* ret;
    Py_BEGIN_CRITICAL_SECTION(self->md);
    ret = md_repr(self->md, (PyObject*)self, true, true);
    Py_END_CRITICAL_SECTION();
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
    {"to_dict",
     (PyCFunction)multidict_proxy_to_dict,
     METH_NOARGS,
     multidict_to_dict_doc},
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
    {Py_tp_doc, (void*)MultDictProxy_doc},

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

#if PY_VERSION_HEX >= 0x030e00f0
    {Py_tp_vectorcall, multidict_proxy_vectorcall},
#endif
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
cimultidict_proxy_tp_init(MultiDictProxyObject* self, PyObject* args,
                          PyObject* kwds)
{
    mod_state* state = get_mod_state_by_def((PyObject*)self);
    PyObject* arg = NULL;
    MultiDictObject* md = NULL;

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
        md = ((MultiDictProxyObject*)arg)->md;
    } else {
        md = (MultiDictObject*)arg;
    }
    Py_INCREF(md);
    Py_XSETREF(self->md, md);

    return 0;
}

static PyObject*
cimultidict_proxy_copy(MultiDictProxyObject* self)
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
    {Py_tp_doc, (void*)CIMultDictProxy_doc},
    {Py_tp_methods, cimultidict_proxy_methods},
    {Py_tp_init, cimultidict_proxy_tp_init},
#if PY_VERSION_HEX >= 0x030e00f0
    {Py_tp_vectorcall, cimultidict_proxy_vectorcall},
#endif
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

static PyObject*
getversion(PyObject* self, PyObject* arg)
{
    mod_state* state = get_mod_state(self);
    MultiDictObject* md;
    if (AnyMultiDict_Check(state, arg)) {
        md = (MultiDictObject*)arg;
    } else if (AnyMultiDictProxy_Check(state, arg)) {
        md = ((MultiDictProxyObject*)arg)->md;
    } else {
        PyErr_Format(PyExc_TypeError, "unexpected type");
        return NULL;
    }
    return PyLong_FromUnsignedLong(md_version(md));
}

/******************** Module ********************/

static int
module_traverse(PyObject* mod, visitproc visit, void* arg)
{
    mod_state* state = get_mod_state(mod);

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
module_clear(PyObject* mod)
{
    mod_state* state = get_mod_state(mod);

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
module_free(void* mod)
{
    (void)module_clear((PyObject*)mod);
}

static PyMethodDef module_methods[] = {
    {"getversion", (PyCFunction)getversion, METH_O},
    {NULL, NULL} /* sentinel */
};

static int
module_exec(PyObject* mod)
{
    mod_state* state = get_mod_state(mod);
    PyObject* tmp;
    PyObject* tpl = NULL;

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
    state->MultiDictType = (PyTypeObject*)tmp;
#if PY_VERSION_HEX < 0x030e00f0
    /* 3.14+ sets this via the Py_tp_vectorcall slot instead: MultiDict(...)
       construction behaves like tp_new + tp_init, but reads its arguments
       directly off the vectorcall stack instead of requiring type_call()
       to first pack them into an args tuple and a kwargs dict. */
    state->MultiDictType->tp_vectorcall = multidict_vectorcall;
#endif

    tpl = PyTuple_Pack(1, (PyObject*)state->MultiDictType);
    if (tpl == NULL) {
        goto fail;
    }
    tmp = PyType_FromModuleAndSpec(mod, &cimultidict_spec, tpl);
    if (tmp == NULL) {
        goto fail;
    }
    state->CIMultiDictType = (PyTypeObject*)tmp;
#if PY_VERSION_HEX < 0x030e00f0
    state->CIMultiDictType->tp_vectorcall = cimultidict_vectorcall;
#endif
    Py_CLEAR(tpl);

    tmp = PyType_FromModuleAndSpec(mod, &multidict_proxy_spec, NULL);
    if (tmp == NULL) {
        goto fail;
    }
    state->MultiDictProxyType = (PyTypeObject*)tmp;
#if PY_VERSION_HEX < 0x030e00f0
    state->MultiDictProxyType->tp_vectorcall = multidict_proxy_vectorcall;
#endif

    tpl = PyTuple_Pack(1, (PyObject*)state->MultiDictProxyType);
    if (tpl == NULL) {
        goto fail;
    }
    tmp = PyType_FromModuleAndSpec(mod, &cimultidict_proxy_spec, tpl);
    if (tmp == NULL) {
        goto fail;
    }
    state->CIMultiDictProxyType = (PyTypeObject*)tmp;
#if PY_VERSION_HEX < 0x030e00f0
    state->CIMultiDictProxyType->tp_vectorcall = cimultidict_proxy_vectorcall;
#endif
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
