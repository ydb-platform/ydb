#ifndef _MULTIDICT_VIEWS_H
#define _MULTIDICT_VIEWS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "dict.h"
#include "hashtable.h"
#include "state.h"

typedef struct {
    PyObject_HEAD
    MultiDictObject *md;
} _Multidict_ViewObject;

#define Items_CheckExact(state, obj) Py_IS_TYPE(obj, state->ItemsViewType)
#define Keys_CheckExact(state, obj) Py_IS_TYPE(obj, state->KeysViewType)
#define Values_CheckExact(state, obj) Py_IS_TYPE(obj, state->ValuesViewType)

/********** Base **********/

static inline void
_init_view(_Multidict_ViewObject *self, MultiDictObject *md)
{
    Py_INCREF(md);
    self->md = md;
}

static inline void
multidict_view_dealloc(_Multidict_ViewObject *self)
{
    PyObject_GC_UnTrack(self);
    Py_XDECREF(self->md);
    PyObject_GC_Del(self);
}

static inline int
multidict_view_traverse(_Multidict_ViewObject *self, visitproc visit,
                        void *arg)
{
    Py_VISIT(self->md);
    return 0;
}

static inline int
multidict_view_clear(_Multidict_ViewObject *self)
{
    Py_CLEAR(self->md);
    return 0;
}

static inline Py_ssize_t
multidict_view_len(_Multidict_ViewObject *self)
{
    return md_len(self->md);
}

static inline PyObject *
multidict_view_richcompare(_Multidict_ViewObject *self, PyObject *other,
                           int op)
{
    int tmp;
    Py_ssize_t self_size = md_len(self->md);
    Py_ssize_t size = PyObject_Length(other);
    if (size < 0) {
        PyErr_Clear();
        Py_RETURN_NOTIMPLEMENTED;
    }
    PyObject *iter = NULL;
    PyObject *item = NULL;
    switch (op) {
        case Py_LT:
            if (self_size >= size) Py_RETURN_FALSE;
            return multidict_view_richcompare(self, other, Py_LE);
        case Py_LE:
            if (self_size > size) {
                Py_RETURN_FALSE;
            }
            iter = PyObject_GetIter((PyObject *)self);
            if (iter == NULL) {
                goto fail;
            }
            while ((item = PyIter_Next(iter))) {
                tmp = PySequence_Contains(other, item);
                if (tmp < 0) {
                    goto fail;
                }
                Py_CLEAR(item);
                if (tmp == 0) {
                    Py_CLEAR(iter);
                    Py_RETURN_FALSE;
                }
            }
            Py_CLEAR(iter);
            if (PyErr_Occurred()) {
                goto fail;
            }
            Py_RETURN_TRUE;
        case Py_EQ:
            if (self_size != size) Py_RETURN_FALSE;
            return multidict_view_richcompare(self, other, Py_LE);
        case Py_NE:
            item = multidict_view_richcompare(self, other, Py_EQ);
            if (item == NULL) {
                goto fail;
            }
            if (item == Py_True) {
                Py_DECREF(item);
                Py_RETURN_FALSE;
            } else {
                Py_DECREF(item);
                Py_RETURN_TRUE;
            }
        case Py_GT:
            if (self_size <= size) Py_RETURN_FALSE;
            return multidict_view_richcompare(self, other, Py_GE);
        case Py_GE:
            if (self_size < size) {
                Py_RETURN_FALSE;
            }
            iter = PyObject_GetIter(other);
            if (iter == NULL) {
                goto fail;
            }
            while ((item = PyIter_Next(iter))) {
                tmp = PySequence_Contains((PyObject *)self, item);
                if (tmp < 0) {
                    goto fail;
                }
                Py_CLEAR(item);
                if (tmp == 0) {
                    Py_CLEAR(iter);
                    Py_RETURN_FALSE;
                }
            }
            Py_CLEAR(iter);
            if (PyErr_Occurred()) {
                goto fail;
            }
            Py_RETURN_TRUE;
    }
fail:
    Py_CLEAR(item);
    Py_CLEAR(iter);
    return NULL;
}

/********** Items **********/

static inline PyObject *
multidict_itemsview_new(MultiDictObject *md)
{
    _Multidict_ViewObject *mv =
        PyObject_GC_New(_Multidict_ViewObject, md->state->ItemsViewType);
    if (mv == NULL) {
        return NULL;
    }

    _init_view(mv, md);

    PyObject_GC_Track(mv);
    return (PyObject *)mv;
}

static inline PyObject *
multidict_itemsview_iter(_Multidict_ViewObject *self)
{
    return multidict_items_iter_new(self->md);
}

static inline PyObject *
multidict_itemsview_repr(_Multidict_ViewObject *self)
{
    int tmp = Py_ReprEnter((PyObject *)self);
    if (tmp < 0) {
        return NULL;
    }
    if (tmp > 0) {
        return PyUnicode_FromString("...");
    }
    PyObject *name =
        PyObject_GetAttrString((PyObject *)Py_TYPE(self), "__name__");
    if (name == NULL) {
        Py_ReprLeave((PyObject *)self);
        return NULL;
    }
    PyObject *ret = md_repr(self->md, name, true, true);
    Py_ReprLeave((PyObject *)self);
    Py_CLEAR(name);
    return ret;
}

static inline int
_multidict_itemsview_parse_item(_Multidict_ViewObject *self, PyObject *arg,
                                PyObject **pidentity, PyObject **pkey,
                                PyObject **pvalue)
{
    assert(pidentity != NULL);
    if (!PyTuple_Check(arg)) {
        return 0;
    }

    Py_ssize_t size = PyTuple_Size(arg);
    if (size != 2) {
        return 0;
    }

    PyObject *key = Py_NewRef(PyTuple_GET_ITEM(arg, 0));

    if (pkey != NULL) {
        *pkey = Py_NewRef(key);
    }
    if (pvalue != NULL) {
        *pvalue = Py_NewRef(PyTuple_GET_ITEM(arg, 1));
    }

    *pidentity = md_calc_identity(self->md, key);
    Py_DECREF(key);
    if (*pidentity == NULL) {
        if (pkey != NULL) {
            Py_CLEAR(*pkey);
        }
        if (pvalue != NULL) {
            Py_CLEAR(*pvalue);
        }
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            return 0;
        } else {
            return -1;
        }
    }
    return 1;
}

static inline int
_set_add(PyObject *set, PyObject *key, PyObject *value)
{
    PyObject *tpl = PyTuple_Pack(2, key, value);
    if (tpl == NULL) {
        return -1;
    }
    int tmp = PySet_Add(set, tpl);
    Py_DECREF(tpl);
    return tmp;
}

static inline PyObject *
multidict_itemsview_and1(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *identity = NULL;
    PyObject *key = NULL;
    PyObject *key2 = NULL;
    PyObject *value = NULL;
    PyObject *value2 = NULL;
    PyObject *arg = NULL;
    PyObject *ret = NULL;
    md_finder_t finder = {0};

    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New(NULL);
    if (ret == NULL) {
        goto fail;
    }
    while ((arg = PyIter_Next(iter))) {
        int tmp = _multidict_itemsview_parse_item(
            self, arg, &identity, &key, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            Py_CLEAR(arg);
            continue;
        }

        if (md_init_finder(self->md, identity, &finder) < 0) {
            assert(PyErr_Occurred());
            goto fail;
        }

        while ((tmp = md_find_next(&finder, &key2, &value2)) > 0) {
            tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
            if (tmp < 0) {
                goto fail;
            }
            if (tmp > 0) {
                if (_set_add(ret, key2, value2) < 0) {
                    goto fail;
                }
            }
            Py_CLEAR(key2);
            Py_CLEAR(value2);
        }
        if (tmp < 0) {
            goto fail;
        }
        md_finder_cleanup(&finder);
        Py_CLEAR(arg);
        Py_CLEAR(identity);
        Py_CLEAR(key);
        Py_CLEAR(value);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    md_finder_cleanup(&finder);
    Py_CLEAR(arg);
    Py_CLEAR(identity);
    Py_CLEAR(key);
    Py_CLEAR(key2);
    Py_CLEAR(value);
    Py_CLEAR(value2);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_itemsview_and2(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *identity = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *value2 = NULL;
    PyObject *arg = NULL;
    PyObject *ret = NULL;
    md_finder_t finder = {0};

    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New(NULL);
    if (ret == NULL) {
        goto fail;
    }
    while ((arg = PyIter_Next(iter))) {
        int tmp = _multidict_itemsview_parse_item(
            self, arg, &identity, &key, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            Py_CLEAR(arg);
            continue;
        }

        if (md_init_finder(self->md, identity, &finder) < 0) {
            assert(PyErr_Occurred());
            goto fail;
        }

        while ((tmp = md_find_next(&finder, NULL, &value2)) > 0) {
            tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
            if (tmp < 0) {
                goto fail;
            }
            if (tmp > 0) {
                if (_set_add(ret, key, value2) < 0) {
                    goto fail;
                }
            }
            Py_CLEAR(value2);
        }
        if (tmp < 0) {
            goto fail;
        }
        md_finder_cleanup(&finder);
        Py_CLEAR(arg);
        Py_CLEAR(identity);
        Py_CLEAR(key);
        Py_CLEAR(value);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    md_finder_cleanup(&finder);
    Py_CLEAR(arg);
    Py_CLEAR(identity);
    Py_CLEAR(key);
    Py_CLEAR(value);
    Py_CLEAR(value2);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_itemsview_and(PyObject *lft, PyObject *rht)
{
    mod_state *state;
    int tmp = get_mod_state_by_def_checked(lft, &state);
    if (tmp < 0) {
        return NULL;
    } else if (tmp == 0) {
        tmp = get_mod_state_by_def_checked(rht, &state);
        if (tmp < 0) {
            return NULL;
        } else if (tmp == 0) {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }
    assert(state != NULL);
    if (Items_CheckExact(state, lft)) {
        return multidict_itemsview_and1((_Multidict_ViewObject *)lft, rht);
    } else if (Items_CheckExact(state, rht)) {
        return multidict_itemsview_and2((_Multidict_ViewObject *)rht, lft);
    }
    Py_RETURN_NOTIMPLEMENTED;
}

static inline PyObject *
multidict_itemsview_or1(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *identity = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *value2 = NULL;
    PyObject *arg = NULL;
    PyObject *ret = NULL;
    md_finder_t finder = {0};

    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New((PyObject *)self);
    if (ret == NULL) {
        goto fail;
    }
    while ((arg = PyIter_Next(iter))) {
        int tmp = _multidict_itemsview_parse_item(
            self, arg, &identity, &key, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            if (PySet_Add(ret, arg) < 0) {
                goto fail;
            }
            Py_CLEAR(arg);
            continue;
        }

        if (md_init_finder(self->md, identity, &finder) < 0) {
            assert(PyErr_Occurred());
            goto fail;
        }

        while ((tmp = md_find_next(&finder, NULL, &value2)) > 0) {
            tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
            if (tmp < 0) {
                goto fail;
            }
            if (tmp > 0) {
                Py_CLEAR(value2);
                break;
            }
            Py_CLEAR(value2);
        }
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            if (PySet_Add(ret, arg) < 0) {
                goto fail;
            }
        }
        md_finder_cleanup(&finder);
        Py_CLEAR(arg);
        Py_CLEAR(identity);
        Py_CLEAR(key);
        Py_CLEAR(value);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    md_finder_cleanup(&finder);
    Py_CLEAR(arg);
    Py_CLEAR(identity);
    Py_CLEAR(key);
    Py_CLEAR(value);
    Py_CLEAR(value2);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_itemsview_or2(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *identity = NULL;
    PyObject *iter = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *arg = NULL;
    PyObject *tmp_set = NULL;

    md_pos_t pos;

    PyObject *ret = PySet_New(other);
    if (ret == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    iter = PyObject_GetIter(other);
    if (iter == NULL) {
        goto fail;
    }
    tmp_set = PySet_New(NULL);
    if (tmp_set == NULL) {
        goto fail;
    }
    while ((arg = PyIter_Next(iter))) {
        int tmp = _multidict_itemsview_parse_item(
            self, arg, &identity, NULL, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp > 0) {
            if (_set_add(tmp_set, identity, value) < 0) {
                goto fail;
            }
        }
        Py_CLEAR(arg);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);

    md_init_pos(self->md, &pos);

    while (true) {
        int tmp = md_next(self->md, &pos, &identity, &key, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            break;
        } else {
            PyObject *tpl = PyTuple_Pack(2, identity, value);
            if (tpl == NULL) {
                goto fail;
            }
            tmp = PySet_Contains(tmp_set, tpl);
            if (tmp < 0) {
                goto fail;
            }
            if (tmp == 0) {
                if (_set_add(ret, key, value) < 0) {
                    goto fail;
                }
            }
            Py_CLEAR(identity);
            Py_CLEAR(key);
            Py_CLEAR(value);
        }
    }
    Py_CLEAR(tmp_set);
    return ret;
fail:
    Py_CLEAR(arg);
    Py_CLEAR(identity);
    Py_CLEAR(key);
    Py_CLEAR(value);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    Py_CLEAR(tmp_set);
    return NULL;
}

static inline PyObject *
multidict_itemsview_or(PyObject *lft, PyObject *rht)
{
    mod_state *state;
    int tmp = get_mod_state_by_def_checked(lft, &state);
    if (tmp < 0) {
        return NULL;
    } else if (tmp == 0) {
        tmp = get_mod_state_by_def_checked(rht, &state);
        if (tmp < 0) {
            return NULL;
        } else if (tmp == 0) {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }
    assert(state != NULL);
    if (Items_CheckExact(state, lft)) {
        return multidict_itemsview_or1((_Multidict_ViewObject *)lft, rht);
    } else if (Items_CheckExact(state, rht)) {
        return multidict_itemsview_or2((_Multidict_ViewObject *)rht, lft);
    }
    Py_RETURN_NOTIMPLEMENTED;
}

static inline PyObject *
multidict_itemsview_sub1(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *arg = NULL;
    PyObject *identity = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *ret = NULL;
    PyObject *tmp_set = NULL;

    md_pos_t pos;

    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New(NULL);
    if (ret == NULL) {
        goto fail;
    }
    tmp_set = PySet_New(NULL);
    if (tmp_set == NULL) {
        goto fail;
    }
    while ((arg = PyIter_Next(iter))) {
        int tmp = _multidict_itemsview_parse_item(
            self, arg, &identity, NULL, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp > 0) {
            if (_set_add(tmp_set, identity, value) < 0) {
                goto fail;
            }
        }
        Py_CLEAR(arg);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);

    md_init_pos(self->md, &pos);

    while (true) {
        int tmp = md_next(self->md, &pos, &identity, &key, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            break;
        } else {
            PyObject *tpl = PyTuple_Pack(2, identity, value);
            if (tpl == NULL) {
                goto fail;
            }
            tmp = PySet_Contains(tmp_set, tpl);
            if (tmp < 0) {
                goto fail;
            }
            if (tmp == 0) {
                if (_set_add(ret, key, value) < 0) {
                    goto fail;
                }
            }
            Py_CLEAR(identity);
            Py_CLEAR(key);
            Py_CLEAR(value);
        }
    }
    Py_CLEAR(tmp_set);
    return ret;
fail:
    Py_CLEAR(identity);
    Py_CLEAR(key);
    Py_CLEAR(value);
    Py_CLEAR(ret);
    Py_CLEAR(tmp_set);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_itemsview_sub2(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *arg = NULL;
    PyObject *identity = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *value2 = NULL;
    PyObject *ret = NULL;
    PyObject *iter = PyObject_GetIter(other);
    md_finder_t finder = {0};

    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New(NULL);
    if (ret == NULL) {
        goto fail;
    }
    while ((arg = PyIter_Next(iter))) {
        int tmp = _multidict_itemsview_parse_item(
            self, arg, &identity, NULL, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            if (PySet_Add(ret, arg) < 0) {
                goto fail;
            }
            Py_CLEAR(arg);
            continue;
        }

        if (md_init_finder(self->md, identity, &finder) < 0) {
            assert(PyErr_Occurred());
            goto fail;
        }

        while ((tmp = md_find_next(&finder, NULL, &value2)) > 0) {
            tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
            if (tmp < 0) {
                goto fail;
            }
            if (tmp > 0) {
                Py_CLEAR(value2);
                break;
            }
            Py_CLEAR(value2);
        }
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            if (PySet_Add(ret, arg) < 0) {
                goto fail;
            }
        }
        md_finder_cleanup(&finder);
        Py_CLEAR(arg);
        Py_CLEAR(identity);
        Py_CLEAR(key);
        Py_CLEAR(value);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    md_finder_cleanup(&finder);
    Py_CLEAR(arg);
    Py_CLEAR(identity);
    Py_CLEAR(key);
    Py_CLEAR(value);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_itemsview_sub(PyObject *lft, PyObject *rht)
{
    mod_state *state;
    int tmp = get_mod_state_by_def_checked(lft, &state);
    if (tmp < 0) {
        return NULL;
    } else if (tmp == 0) {
        tmp = get_mod_state_by_def_checked(rht, &state);
        if (tmp < 0) {
            return NULL;
        } else if (tmp == 0) {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }
    assert(state != NULL);
    if (Items_CheckExact(state, lft)) {
        return multidict_itemsview_sub1((_Multidict_ViewObject *)lft, rht);
    } else if (Items_CheckExact(state, rht)) {
        return multidict_itemsview_sub2((_Multidict_ViewObject *)rht, lft);
    }
    Py_RETURN_NOTIMPLEMENTED;
}

static inline PyObject *
multidict_itemsview_xor(_Multidict_ViewObject *self, PyObject *other)
{
    mod_state *state;
    int tmp = get_mod_state_by_def_checked((PyObject *)self, &state);
    if (tmp < 0) {
        return NULL;
    } else if (tmp == 0) {
        tmp = get_mod_state_by_def_checked(other, &state);
        if (tmp < 0) {
            return NULL;
        } else if (tmp == 0) {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }
    assert(state != NULL);
    if (!Items_CheckExact(state, self)) {
        if (Items_CheckExact(state, other)) {
            return multidict_itemsview_xor((_Multidict_ViewObject *)other,
                                           (PyObject *)self);
        } else {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }

    PyObject *ret = NULL;
    PyObject *tmp1 = NULL;
    PyObject *tmp2 = NULL;
    PyObject *rht = PySet_New(other);
    if (rht == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    tmp1 = PyNumber_Subtract((PyObject *)self, rht);
    if (tmp1 == NULL) {
        goto fail;
    }
    tmp2 = PyNumber_Subtract(rht, (PyObject *)self);
    if (tmp2 == NULL) {
        goto fail;
    }
    ret = PyNumber_InPlaceOr(tmp1, tmp2);
    if (ret == NULL) {
        goto fail;
    }
    Py_CLEAR(tmp1);
    Py_CLEAR(tmp2);
    Py_CLEAR(rht);
    return ret;
fail:
    Py_CLEAR(tmp1);
    Py_CLEAR(tmp2);
    Py_CLEAR(rht);
    Py_CLEAR(ret);
    return NULL;
}

static inline int
multidict_itemsview_contains(_Multidict_ViewObject *self, PyObject *obj)
{
    PyObject *identity = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *value2 = NULL;
    int tmp;
    int ret = 0;
    md_finder_t finder = {0};

    if (PyTuple_CheckExact(obj)) {
        if (PyTuple_GET_SIZE(obj) != 2) {
            return 0;
        }
        key = Py_NewRef(PyTuple_GET_ITEM(obj, 0));
        value = Py_NewRef(PyTuple_GET_ITEM(obj, 1));
    } else if (PyList_CheckExact(obj)) {
        if (PyList_GET_SIZE(obj) != 2) {
            return 0;
        }
        key = Py_NewRef(PyList_GET_ITEM(obj, 0));
        value = Py_NewRef(PyList_GET_ITEM(obj, 1));
    } else {
        tmp = PyObject_Length(obj);
        if (tmp < 0) {
            PyErr_Clear();
            return 0;
        }
        if (tmp != 2) {
            return 0;
        }
        key = PySequence_GetItem(obj, 0);
        if (key == NULL) {
            return -1;
        }
        value = PySequence_GetItem(obj, 1);
        if (value == NULL) {
            return -1;
        }
    }

    identity = md_calc_identity(self->md, key);
    if (identity == NULL) {
        PyErr_Clear();
        ret = 0;
        goto done;
    }

    if (md_init_finder(self->md, identity, &finder) < 0) {
        assert(PyErr_Occurred());
        ret = -1;
        goto done;
    }

    while ((tmp = md_find_next(&finder, NULL, &value2)) > 0) {
        tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
        Py_CLEAR(value2);
        if (tmp < 0) {
            ret = -1;
            goto done;
        }
        if (tmp > 0) {
            ret = 1;
            goto done;
        }
    }
    if (tmp < 0) {
        ret = -1;
        goto done;
    }

done:
    md_finder_cleanup(&finder);
    Py_CLEAR(identity);
    Py_CLEAR(key);
    Py_CLEAR(value);
    ASSERT_CONSISTENT(self->md, false);
    return ret;
}

static inline PyObject *
multidict_itemsview_isdisjoint(_Multidict_ViewObject *self, PyObject *other)
{
    md_finder_t finder = {0};
    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        return NULL;
    }
    PyObject *arg = NULL;
    PyObject *identity = NULL;
    PyObject *value = NULL;
    PyObject *value2 = NULL;

    while ((arg = PyIter_Next(iter))) {
        int tmp = _multidict_itemsview_parse_item(
            self, arg, &identity, NULL, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            Py_CLEAR(arg);
            continue;
        }

        if (md_init_finder(self->md, identity, &finder) < 0) {
            assert(PyErr_Occurred());
            goto fail;
        }

        while ((tmp = md_find_next(&finder, NULL, &value2)) > 0) {
            tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
            Py_CLEAR(value2);
            if (tmp < 0) {
                goto fail;
            }
            if (tmp > 0) {
                md_finder_cleanup(&finder);
                Py_CLEAR(iter);
                Py_CLEAR(arg);
                Py_CLEAR(identity);
                Py_CLEAR(value);
                ASSERT_CONSISTENT(self->md, false);
                Py_RETURN_FALSE;
            }
        }
        if (tmp < 0) {
            goto fail;
        }
        md_finder_cleanup(&finder);
        Py_CLEAR(arg);
        Py_CLEAR(identity);
        Py_CLEAR(value);
    }
    Py_CLEAR(iter);
    if (PyErr_Occurred()) {
        return NULL;
    }
    ASSERT_CONSISTENT(self->md, false);
    Py_RETURN_TRUE;
fail:
    md_finder_cleanup(&finder);
    Py_CLEAR(iter);
    Py_CLEAR(arg);
    Py_CLEAR(identity);
    Py_CLEAR(value);
    Py_CLEAR(value2);
    return NULL;
}

PyDoc_STRVAR(itemsview_isdisjoint_doc,
             "Return True if two sets have a null intersection.");

static PyMethodDef multidict_itemsview_methods[] = {
    {"isdisjoint",
     (PyCFunction)multidict_itemsview_isdisjoint,
     METH_O,
     itemsview_isdisjoint_doc},
    {NULL, NULL} /* sentinel */
};

static inline PyObject *
multidict_view_forbidden_new(PyTypeObject *type, PyObject *args,
                             PyObject *kwargs)
{
    PyErr_Format(PyExc_TypeError,
                 "cannot create '%s' instances directly",
                 type->tp_name);
    return NULL;
}

static PyType_Slot multidict_itemsview_slots[] = {
    {Py_tp_new, multidict_view_forbidden_new},
    {Py_tp_dealloc, multidict_view_dealloc},
    {Py_tp_repr, multidict_itemsview_repr},

    {Py_nb_subtract, multidict_itemsview_sub},
    {Py_nb_and, multidict_itemsview_and},
    {Py_nb_xor, multidict_itemsview_xor},
    {Py_nb_or, multidict_itemsview_or},
    {Py_sq_length, multidict_view_len},
    {Py_sq_contains, multidict_itemsview_contains},
    {Py_tp_getattro, PyObject_GenericGetAttr},
    {Py_tp_traverse, multidict_view_traverse},
    {Py_tp_clear, multidict_view_clear},
    {Py_tp_richcompare, multidict_view_richcompare},
    {Py_tp_iter, multidict_itemsview_iter},
    {Py_tp_methods, multidict_itemsview_methods},
    {0, NULL},
};

static PyType_Spec multidict_itemsview_spec = {
    .name = "multidict._multidict._ItemsView",
    .basicsize = sizeof(_Multidict_ViewObject),
    .flags = (Py_TPFLAGS_DEFAULT
#if PY_VERSION_HEX >= 0x030a0000
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_HAVE_GC),
    .slots = multidict_itemsview_slots,
};

/********** Keys **********/

static inline PyObject *
multidict_keysview_new(MultiDictObject *md)
{
    _Multidict_ViewObject *mv =
        PyObject_GC_New(_Multidict_ViewObject, md->state->KeysViewType);
    if (mv == NULL) {
        return NULL;
    }

    _init_view(mv, md);

    PyObject_GC_Track(mv);
    return (PyObject *)mv;
}

static inline PyObject *
multidict_keysview_iter(_Multidict_ViewObject *self)
{
    return multidict_keys_iter_new(self->md);
}

static inline PyObject *
multidict_keysview_repr(_Multidict_ViewObject *self)
{
    PyObject *name =
        PyObject_GetAttrString((PyObject *)Py_TYPE(self), "__name__");
    if (name == NULL) {
        return NULL;
    }
    PyObject *ret = md_repr(self->md, name, true, false);
    Py_CLEAR(name);
    return ret;
}

static inline PyObject *
multidict_keysview_and1(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *key = NULL;
    PyObject *key2 = NULL;
    PyObject *ret = NULL;
    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New(NULL);
    if (ret == NULL) {
        goto fail;
    }
    while ((key = PyIter_Next(iter))) {
        if (!PyUnicode_Check(key)) {
            Py_CLEAR(key);
            continue;
        }
        int tmp = md_contains(self->md, key, &key2);
        if (tmp < 0) {
            goto fail;
        }
        if (tmp > 0) {
            if (PySet_Add(ret, key2) < 0) {
                goto fail;
            }
        }
        Py_CLEAR(key);
        Py_CLEAR(key2);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    Py_CLEAR(key);
    Py_CLEAR(key2);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_keysview_and2(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *key = NULL;
    PyObject *ret = NULL;
    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New(NULL);
    if (ret == NULL) {
        goto fail;
    }
    while ((key = PyIter_Next(iter))) {
        if (!PyUnicode_Check(key)) {
            Py_CLEAR(key);
            continue;
        }
        int tmp = md_contains(self->md, key, NULL);
        if (tmp < 0) {
            goto fail;
        }
        if (tmp > 0) {
            if (PySet_Add(ret, key) < 0) {
                goto fail;
            }
        }
        Py_CLEAR(key);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    Py_CLEAR(key);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_keysview_and(PyObject *lft, PyObject *rht)
{
    mod_state *state;
    int tmp = get_mod_state_by_def_checked(lft, &state);
    if (tmp < 0) {
        return NULL;
    } else if (tmp == 0) {
        tmp = get_mod_state_by_def_checked(rht, &state);
        if (tmp < 0) {
            return NULL;
        } else if (tmp == 0) {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }
    assert(state != NULL);
    if (Keys_CheckExact(state, lft)) {
        return multidict_keysview_and1((_Multidict_ViewObject *)lft, rht);
    } else if (Keys_CheckExact(state, rht)) {
        return multidict_keysview_and2((_Multidict_ViewObject *)rht, lft);
    }
    Py_RETURN_NOTIMPLEMENTED;
}

static inline PyObject *
multidict_keysview_or1(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *key = NULL;
    PyObject *ret = NULL;
    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New((PyObject *)self);
    if (ret == NULL) {
        goto fail;
    }
    while ((key = PyIter_Next(iter))) {
        if (!PyUnicode_Check(key)) {
            if (PySet_Add(ret, key) < 0) {
                goto fail;
            }
            Py_CLEAR(key);
            continue;
        }
        int tmp = md_contains(self->md, key, NULL);
        if (tmp < 0) {
            goto fail;
        }
        if (tmp == 0) {
            if (PySet_Add(ret, key) < 0) {
                goto fail;
            }
        }
        Py_CLEAR(key);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    Py_CLEAR(key);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_keysview_or2(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *iter = NULL;
    PyObject *identity = NULL;
    PyObject *key = NULL;
    PyObject *tmp_set = NULL;
    PyObject *ret = PySet_New(other);
    if (ret == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    iter = PyObject_GetIter(ret);
    if (iter == NULL) {
        goto fail;
    }
    tmp_set = PySet_New(NULL);
    if (tmp_set == NULL) {
        goto fail;
    }
    while ((key = PyIter_Next(iter))) {
        if (!PyUnicode_Check(key)) {
            Py_CLEAR(key);
            continue;
        }
        identity = md_calc_identity(self->md, key);
        if (identity == NULL) {
            goto fail;
        }
        if (PySet_Add(tmp_set, identity) < 0) {
            goto fail;
        }
        Py_CLEAR(identity);
        Py_CLEAR(key);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);

    md_pos_t pos;
    md_init_pos(self->md, &pos);

    while (true) {
        int tmp = md_next(self->md, &pos, &identity, &key, NULL);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            break;
        }

        tmp = PySet_Contains(tmp_set, identity);
        if (tmp < 0) {
            goto fail;
        }
        if (tmp == 0) {
            if (PySet_Add(ret, key) < 0) {
                goto fail;
            }
        }
        Py_CLEAR(identity);
        Py_CLEAR(key);
    }
    Py_CLEAR(tmp_set);
    return ret;
fail:
    Py_CLEAR(identity);
    Py_CLEAR(key);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    Py_CLEAR(tmp_set);
    return NULL;
}

static inline PyObject *
multidict_keysview_or(PyObject *lft, PyObject *rht)
{
    mod_state *state;
    int tmp = get_mod_state_by_def_checked(lft, &state);
    if (tmp < 0) {
        return NULL;
    } else if (tmp == 0) {
        tmp = get_mod_state_by_def_checked(rht, &state);
        if (tmp < 0) {
            return NULL;
        } else if (tmp == 0) {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }
    assert(state != NULL);
    if (Keys_CheckExact(state, lft)) {
        return multidict_keysview_or1((_Multidict_ViewObject *)lft, rht);
    } else if (Keys_CheckExact(state, rht)) {
        return multidict_keysview_or2((_Multidict_ViewObject *)rht, lft);
    }
    Py_RETURN_NOTIMPLEMENTED;
}

static inline PyObject *
multidict_keysview_sub1(_Multidict_ViewObject *self, PyObject *other)
{
    int tmp;
    PyObject *key = NULL;
    PyObject *key2 = NULL;
    PyObject *ret = NULL;
    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New((PyObject *)self);
    if (ret == NULL) {
        goto fail;
    }
    while ((key = PyIter_Next(iter))) {
        if (!PyUnicode_Check(key)) {
            Py_CLEAR(key);
            continue;
        }
        tmp = md_contains(self->md, key, &key2);
        if (tmp < 0) {
            goto fail;
        }
        if (tmp > 0) {
            if (PySet_Discard(ret, key2) < 0) {
                goto fail;
            }
        }
        Py_CLEAR(key);
        Py_CLEAR(key2);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    Py_CLEAR(key);
    Py_CLEAR(key2);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_keysview_sub2(_Multidict_ViewObject *self, PyObject *other)
{
    int tmp;
    PyObject *key = NULL;
    PyObject *ret = NULL;
    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    ret = PySet_New(other);
    if (ret == NULL) {
        goto fail;
    }
    while ((key = PyIter_Next(iter))) {
        if (!PyUnicode_Check(key)) {
            Py_CLEAR(key);
            continue;
        }
        tmp = md_contains(self->md, key, NULL);
        if (tmp < 0) {
            goto fail;
        }
        if (tmp > 0) {
            if (PySet_Discard(ret, key) < 0) {
                goto fail;
            }
        }
        Py_CLEAR(key);
    }
    if (PyErr_Occurred()) {
        goto fail;
    }
    Py_CLEAR(iter);
    return ret;
fail:
    Py_CLEAR(key);
    Py_CLEAR(iter);
    Py_CLEAR(ret);
    return NULL;
}

static inline PyObject *
multidict_keysview_sub(PyObject *lft, PyObject *rht)
{
    mod_state *state;
    int tmp = get_mod_state_by_def_checked(lft, &state);
    if (tmp < 0) {
        return NULL;
    } else if (tmp == 0) {
        tmp = get_mod_state_by_def_checked(rht, &state);
        if (tmp < 0) {
            return NULL;
        } else if (tmp == 0) {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }
    assert(state != NULL);
    if (Keys_CheckExact(state, lft)) {
        return multidict_keysview_sub1((_Multidict_ViewObject *)lft, rht);
    } else if (Keys_CheckExact(state, rht)) {
        return multidict_keysview_sub2((_Multidict_ViewObject *)rht, lft);
    }
    Py_RETURN_NOTIMPLEMENTED;
}

static inline PyObject *
multidict_keysview_xor(_Multidict_ViewObject *self, PyObject *other)
{
    mod_state *state;
    int tmp = get_mod_state_by_def_checked((PyObject *)self, &state);
    if (tmp < 0) {
        return NULL;
    } else if (tmp == 0) {
        tmp = get_mod_state_by_def_checked(other, &state);
        if (tmp < 0) {
            return NULL;
        } else if (tmp == 0) {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }
    assert(state != NULL);
    if (!Keys_CheckExact(state, self)) {
        if (Keys_CheckExact(state, other)) {
            return multidict_keysview_xor((_Multidict_ViewObject *)other,
                                          (PyObject *)self);
        } else {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }

    PyObject *ret = NULL;
    PyObject *tmp1 = NULL;
    PyObject *tmp2 = NULL;
    PyObject *rht = PySet_New(other);
    if (rht == NULL) {
        if (PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
            Py_RETURN_NOTIMPLEMENTED;
        }
        goto fail;
    }
    tmp1 = PyNumber_Subtract((PyObject *)self, rht);
    if (tmp1 == NULL) {
        goto fail;
    }
    tmp2 = PyNumber_Subtract(rht, (PyObject *)self);
    if (tmp2 == NULL) {
        goto fail;
    }
    ret = PyNumber_InPlaceOr(tmp1, tmp2);
    if (ret == NULL) {
        goto fail;
    }
    Py_CLEAR(tmp1);
    Py_CLEAR(tmp2);
    Py_CLEAR(rht);
    return ret;
fail:
    Py_CLEAR(tmp1);
    Py_CLEAR(tmp2);
    Py_CLEAR(rht);
    Py_CLEAR(ret);
    return NULL;
}

static inline int
multidict_keysview_contains(_Multidict_ViewObject *self, PyObject *key)
{
    return md_contains(self->md, key, NULL);
}

static inline PyObject *
multidict_keysview_isdisjoint(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        return NULL;
    }
    PyObject *key = NULL;
    while ((key = PyIter_Next(iter))) {
        int tmp = md_contains(self->md, key, NULL);
        Py_CLEAR(key);
        if (tmp < 0) {
            Py_CLEAR(iter);
            return NULL;
        }
        if (tmp > 0) {
            Py_CLEAR(iter);
            Py_RETURN_FALSE;
        }
    }
    Py_CLEAR(iter);
    if (PyErr_Occurred()) {
        return NULL;
    }
    Py_RETURN_TRUE;
}

PyDoc_STRVAR(keysview_isdisjoint_doc,
             "Return True if two sets have a null intersection.");

static PyMethodDef multidict_keysview_methods[] = {
    {"isdisjoint",
     (PyCFunction)multidict_keysview_isdisjoint,
     METH_O,
     keysview_isdisjoint_doc},
    {NULL, NULL} /* sentinel */
};

static PyType_Slot multidict_keysview_slots[] = {
    {Py_tp_new, multidict_view_forbidden_new},
    {Py_tp_dealloc, multidict_view_dealloc},
    {Py_tp_repr, multidict_keysview_repr},

    {Py_nb_subtract, multidict_keysview_sub},
    {Py_nb_and, multidict_keysview_and},
    {Py_nb_xor, multidict_keysview_xor},
    {Py_nb_or, multidict_keysview_or},
    {Py_sq_length, multidict_view_len},
    {Py_sq_contains, multidict_keysview_contains},
    {Py_tp_getattro, PyObject_GenericGetAttr},
    {Py_tp_traverse, multidict_view_traverse},
    {Py_tp_clear, multidict_view_clear},
    {Py_tp_richcompare, multidict_view_richcompare},
    {Py_tp_iter, multidict_keysview_iter},
    {Py_tp_methods, multidict_keysview_methods},
    {0, NULL},
};

static PyType_Spec multidict_keysview_spec = {
    .name = "multidict._multidict._KeysView",
    .basicsize = sizeof(_Multidict_ViewObject),
    .flags = (Py_TPFLAGS_DEFAULT
#if PY_VERSION_HEX >= 0x030a0000
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_HAVE_GC),
    .slots = multidict_keysview_slots,
};

/********** Values **********/

static inline PyObject *
multidict_valuesview_new(MultiDictObject *md)
{
    _Multidict_ViewObject *mv =
        PyObject_GC_New(_Multidict_ViewObject, md->state->ValuesViewType);
    if (mv == NULL) {
        return NULL;
    }

    _init_view(mv, md);

    PyObject_GC_Track(mv);
    return (PyObject *)mv;
}

static inline PyObject *
multidict_valuesview_iter(_Multidict_ViewObject *self)
{
    return multidict_values_iter_new(self->md);
}

static inline PyObject *
multidict_valuesview_repr(_Multidict_ViewObject *self)
{
    int tmp = Py_ReprEnter((PyObject *)self);
    if (tmp < 0) {
        return NULL;
    }
    if (tmp > 0) {
        return PyUnicode_FromString("...");
    }
    PyObject *name =
        PyObject_GetAttrString((PyObject *)Py_TYPE(self), "__name__");
    if (name == NULL) {
        Py_ReprLeave((PyObject *)self);
        return NULL;
    }
    PyObject *ret = md_repr(self->md, name, false, true);
    Py_ReprLeave((PyObject *)self);
    Py_CLEAR(name);
    return ret;
}

static PyType_Slot multidict_valuesview_slots[] = {
    {Py_tp_new, multidict_view_forbidden_new},
    {Py_tp_dealloc, multidict_view_dealloc},
    {Py_tp_repr, multidict_valuesview_repr},

    {Py_sq_length, multidict_view_len},
    {Py_tp_getattro, PyObject_GenericGetAttr},
    {Py_tp_traverse, multidict_view_traverse},
    {Py_tp_clear, multidict_view_clear},
    {Py_tp_iter, multidict_valuesview_iter},
    {0, NULL},
};

static PyType_Spec multidict_valuesview_spec = {
    .name = "multidict._multidict._ValuesView",
    .basicsize = sizeof(_Multidict_ViewObject),
    .flags = (Py_TPFLAGS_DEFAULT
#if PY_VERSION_HEX >= 0x030a0000
              | Py_TPFLAGS_IMMUTABLETYPE
#endif
              | Py_TPFLAGS_HAVE_GC),
    .slots = multidict_valuesview_slots,
};

static inline int
multidict_views_init(PyObject *module, mod_state *state)
{
    PyObject *tmp;
    tmp = PyType_FromModuleAndSpec(module, &multidict_itemsview_spec, NULL);
    if (tmp == NULL) {
        return -1;
    }
    state->ItemsViewType = (PyTypeObject *)tmp;

    tmp = PyType_FromModuleAndSpec(module, &multidict_valuesview_spec, NULL);
    if (tmp == NULL) {
        return -1;
    }
    state->ValuesViewType = (PyTypeObject *)tmp;

    tmp = PyType_FromModuleAndSpec(module, &multidict_keysview_spec, NULL);
    if (tmp == NULL) {
        return -1;
    }
    state->KeysViewType = (PyTypeObject *)tmp;

    return 0;
}

#ifdef __cplusplus
}
#endif
#endif
