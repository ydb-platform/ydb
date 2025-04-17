#ifndef _MULTIDICT_VIEWS_H
#define _MULTIDICT_VIEWS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "dict.h"
#include "pair_list.h"
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
multidict_view_traverse(_Multidict_ViewObject *self, visitproc visit, void *arg)
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
    return pair_list_len(&self->md->pairs);
}

static inline PyObject *
multidict_view_richcompare(PyObject *self, PyObject *other, int op)
{
    int tmp;
    Py_ssize_t self_size = PyObject_Length(self);
    if (self_size < 0) {
        return NULL;
    }
    Py_ssize_t size = PyObject_Length(other);
    if (size < 0) {
        PyErr_Clear();
        Py_RETURN_NOTIMPLEMENTED;
    }
    PyObject *iter = NULL;
    PyObject *item = NULL;
    switch(op) {
        case Py_LT:
            if (self_size >= size)
                Py_RETURN_FALSE;
            return PyObject_RichCompare(self, other, Py_LE);
        case Py_LE:
            if (self_size > size) {
                Py_RETURN_FALSE;
            }
            iter = PyObject_GetIter(self);
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
            if (self_size != size)
                Py_RETURN_FALSE;
            return PyObject_RichCompare(self, other, Py_LE);
        case Py_NE:
            tmp = PyObject_RichCompareBool(self, other, Py_EQ);
            if (tmp < 0)
                goto fail;
            return PyBool_FromLong(!tmp);
        case Py_GT:
            if (self_size <= size)
                Py_RETURN_FALSE;
            return PyObject_RichCompare(self, other, Py_GE);
        case Py_GE:
            if (self_size < size) {
                Py_RETURN_FALSE;
            }
            iter = PyObject_GetIter(other);
            if (iter == NULL) {
                goto fail;
            }
            while ((item = PyIter_Next(iter))) {
                tmp = PySequence_Contains(self, item);
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
    _Multidict_ViewObject *mv = PyObject_GC_New(
        _Multidict_ViewObject, md->pairs.state->ItemsViewType);
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
    PyObject *name = PyObject_GetAttrString((PyObject*)Py_TYPE(self), "__name__");
    if (name == NULL) {
        Py_ReprLeave((PyObject *)self);
        return NULL;
    }
    PyObject *ret = pair_list_repr(&self->md->pairs, name, true, true);
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

    *pidentity = pair_list_calc_identity(&self->md->pairs, key);
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

static int
_set_add(PyObject *set, PyObject *key, PyObject * value)
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

    pair_list_pos_t pos;

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
        int tmp = _multidict_itemsview_parse_item(self, arg,
                                                  &identity, &key, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            Py_CLEAR(arg);
            continue;
        }

        pair_list_init_pos(&self->md->pairs, &pos);

        while (true) {
            tmp = pair_list_next_by_identity(&self->md->pairs, &pos,
                                             identity, &key2, &value2);
            if (tmp < 0) {
                goto fail;
            } else if (tmp == 0) {
                break;
            } else {
                tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
                if (tmp < 0) {
                    goto fail;
                }
                if (tmp > 0) {
                    if (_set_add(ret, key2, value2) < 0) {
                        goto fail;
                    }
                }
            }
            Py_CLEAR(key2);
            Py_CLEAR(value2);
        }
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

    pair_list_pos_t pos;

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
        int tmp = _multidict_itemsview_parse_item(self, arg,
                                                  &identity, &key, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            Py_CLEAR(arg);
            continue;
        }

        pair_list_init_pos(&self->md->pairs, &pos);

        while (true) {
            tmp = pair_list_next_by_identity(&self->md->pairs, &pos,
                                             identity, NULL, &value2);
            if (tmp < 0) {
                goto fail;
            } else if (tmp == 0) {
                break;
            } else {
                tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
                if (tmp < 0) {
                    goto fail;
                }
                if (tmp > 0) {
                    if (_set_add(ret, key, value2) < 0) {
                        goto fail;
                    }
                }
            }
            Py_CLEAR(value2);
        }
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
    mod_state * state;
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

    pair_list_pos_t pos;

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
        int tmp = _multidict_itemsview_parse_item(self, arg,
                                                  &identity, &key, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            if (PySet_Add(ret, arg) < 0) {
                goto fail;
            }
            Py_CLEAR(arg);
            continue;
        }

        pair_list_init_pos(&self->md->pairs, &pos);

        while (true) {
            tmp = pair_list_next_by_identity(&self->md->pairs, &pos,
                                             identity, NULL, &value2);
            if (tmp < 0) {
                goto fail;
            } else if (tmp == 0) {
                if (PySet_Add(ret, arg) < 0) {
                    goto fail;
                }
                break;
            } else {
                tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
                if (tmp < 0) {
                    goto fail;
                }
                if (tmp > 0) {
                    Py_CLEAR(value2);
                    break;
                }
            }
            Py_CLEAR(value2);
        }
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

    pair_list_pos_t pos;

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
        int tmp = _multidict_itemsview_parse_item(self, arg,
                                                  &identity, NULL, &value);
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

    pair_list_init_pos(&self->md->pairs, &pos);

    while (true) {
        int tmp = pair_list_next(&self->md->pairs, &pos,
                                 &identity, &key, &value);
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
    mod_state * state;
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

    pair_list_pos_t pos;

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
        int tmp = _multidict_itemsview_parse_item(self, arg,
                                                  &identity, NULL, &value);
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

    pair_list_init_pos(&self->md->pairs, &pos);

    while (true) {
        int tmp = pair_list_next(&self->md->pairs, &pos,
                                 &identity, &key, &value);
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

    pair_list_pos_t pos;

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
        int tmp = _multidict_itemsview_parse_item(self, arg,
                                                  &identity, NULL, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            if (PySet_Add(ret, arg) < 0) {
                goto fail;
            }
            Py_CLEAR(arg);
            continue;
        }

        pair_list_init_pos(&self->md->pairs, &pos);

        while (true) {
            tmp = pair_list_next_by_identity(&self->md->pairs, &pos,
                                             identity, NULL, &value2);
            if (tmp < 0) {
                goto fail;
            } else if (tmp == 0) {
                if (PySet_Add(ret, arg) < 0) {
                    goto fail;
                }
                break;
            } else {
                tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
                if (tmp < 0) {
                    goto fail;
                }
                if (tmp > 0) {
                    Py_CLEAR(value2);
                    break;
                }
            }
            Py_CLEAR(value2);
        }

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
    mod_state * state;
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
    mod_state * state;
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
    PyObject *akey  = NULL,
             *aval  = NULL,
             *bkey  = NULL,
             *bval  = NULL,
             *iter  = NULL,
             *item  = NULL;
    int ret1, ret2;

    if (!PyTuple_Check(obj) || PyTuple_GET_SIZE(obj) != 2) {
        return 0;
    }

    bkey = PyTuple_GET_ITEM(obj, 0);
    bval = PyTuple_GET_ITEM(obj, 1);

    iter = multidict_itemsview_iter(self);
    if (iter == NULL) {
        return 0;
    }

    while ((item = PyIter_Next(iter)) != NULL) {
        akey = PyTuple_GET_ITEM(item, 0);
        aval = PyTuple_GET_ITEM(item, 1);

        ret1 = PyObject_RichCompareBool(akey, bkey, Py_EQ);
        if (ret1 < 0) {
            Py_DECREF(iter);
            Py_DECREF(item);
            return -1;
        }
        ret2 = PyObject_RichCompareBool(aval, bval, Py_EQ);
        if (ret2 < 0) {
            Py_DECREF(iter);
            Py_DECREF(item);
            return -1;
        }
        if (ret1 > 0 && ret2 > 0)
        {
            Py_DECREF(iter);
            Py_DECREF(item);
            return 1;
        }

        Py_DECREF(item);
    }

    Py_DECREF(iter);

    if (PyErr_Occurred()) {
        return -1;
    }

    return 0;
}

static inline PyObject *
multidict_itemsview_isdisjoint(_Multidict_ViewObject *self, PyObject *other)
{
    PyObject *iter = PyObject_GetIter(other);
    if (iter == NULL) {
        return NULL;
    }
    PyObject *arg = NULL;
    PyObject *identity = NULL;
    PyObject *value = NULL;
    PyObject *value2 = NULL;

    pair_list_pos_t pos;

    while ((arg = PyIter_Next(iter))) {
        int tmp = _multidict_itemsview_parse_item(self, arg,
                                                  &identity, NULL, &value);
        if (tmp < 0) {
            goto fail;
        } else if (tmp == 0) {
            Py_CLEAR(arg);
            continue;
        }

        pair_list_init_pos(&self->md->pairs, &pos);

        while (true) {
            tmp = pair_list_next_by_identity(&self->md->pairs, &pos,
                                             identity, NULL, &value2);
            if (tmp < 0) {
                goto fail;
            } else if (tmp == 0) {
                Py_CLEAR(value2);
                break;
            } else {
                tmp = PyObject_RichCompareBool(value, value2, Py_EQ);
                Py_CLEAR(value2);
                if (tmp < 0) {
                    goto fail;
                }
                if (tmp > 0) {
                    Py_CLEAR(iter);
                    Py_CLEAR(arg);
                    Py_CLEAR(identity);
                    Py_CLEAR(value);
                    Py_RETURN_FALSE;
                }
            }
        }
        Py_CLEAR(arg);
        Py_CLEAR(identity);
        Py_CLEAR(value);
    }
    Py_CLEAR(iter);
    if (PyErr_Occurred()) {
        return NULL;
    }
    Py_RETURN_TRUE;
fail:
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
    {"isdisjoint", (PyCFunction)multidict_itemsview_isdisjoint,
     METH_O, itemsview_isdisjoint_doc},
    {NULL, NULL}   /* sentinel */
};

static PyType_Slot multidict_itemsview_slots[] = {
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
    _Multidict_ViewObject *mv = PyObject_GC_New(
        _Multidict_ViewObject, md->pairs.state->KeysViewType);
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
    PyObject *name = PyObject_GetAttrString((PyObject*)Py_TYPE(self), "__name__");
    if (name == NULL) {
        return NULL;
    }
    PyObject *ret = pair_list_repr(&self->md->pairs, name, true, false);
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
        int tmp = pair_list_contains(&self->md->pairs, key, &key2);
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
        int tmp = pair_list_contains(&self->md->pairs, key, NULL);
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
    mod_state * state;
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
        int tmp = pair_list_contains(&self->md->pairs, key, NULL);
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
        identity = pair_list_calc_identity(&self->md->pairs, key);
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

    pair_list_pos_t pos;
    pair_list_init_pos(&self->md->pairs, &pos);

    while (true) {
        int tmp = pair_list_next(&self->md->pairs, &pos, &identity, &key, NULL);
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
    mod_state * state;
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
        tmp = pair_list_contains(&self->md->pairs, key, &key2);
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
        tmp = pair_list_contains(&self->md->pairs, key, NULL);
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
    mod_state * state;
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
    mod_state * state;
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
    return pair_list_contains(&self->md->pairs, key, NULL);
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
        int tmp = pair_list_contains(&self->md->pairs, key, NULL);
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
    {"isdisjoint", (PyCFunction)multidict_keysview_isdisjoint,
     METH_O, keysview_isdisjoint_doc},
    {NULL, NULL}   /* sentinel */
};

static PyType_Slot multidict_keysview_slots[] = {
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
    _Multidict_ViewObject *mv = PyObject_GC_New(
        _Multidict_ViewObject, md->pairs.state->ValuesViewType);
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
    PyObject *name = PyObject_GetAttrString((PyObject*)Py_TYPE(self), "__name__");
    if (name == NULL) {
        Py_ReprLeave((PyObject *)self);
        return NULL;
    }
    PyObject *ret = pair_list_repr(&self->md->pairs, name, false, true);
    Py_ReprLeave((PyObject *)self);
    Py_CLEAR(name);
    return ret;
}

static PyType_Slot multidict_valuesview_slots[] = {
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
    PyObject * tmp;
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
