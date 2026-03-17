/*
 * Copyright 2010-2025 The pygit2 contributors
 *
 * This file is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * In addition to the permissions in the GNU General Public License,
 * the authors give you unlimited permission to link the compiled
 * version of this file into combinations with other programs,
 * and to distribute those combinations without any restriction
 * coming from the use of this file.  (The General Public License
 * restrictions do apply in other respects; for example, they cover
 * modification of the file, and distribution when not linked into
 * a combined executable.)
 *
 * This file is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file COPYING.  If not, write to
 * the Free Software Foundation, 51 Franklin Street, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "object.h"
#include "error.h"
#include "types.h"
#include "utils.h"
#include "oid.h"

PyTypeObject StashType;


PyDoc_STRVAR(Stash_commit_id__doc__, "The commit id of the stashed state.");

PyObject *
Stash_commit_id__get__(Stash *self)
{
    Py_INCREF(self->commit_id);
    return self->commit_id;
}


PyDoc_STRVAR(Stash_message__doc__, "Stash message.");

PyObject *
Stash_message__get__(Stash *self)
{
    return to_unicode(self->message, "utf-8", "strict");
}


PyDoc_STRVAR(Stash_raw_message__doc__, "Stash message (bytes).");

PyObject *
Stash_raw_message__get__(Stash *self)
{
    return PyBytes_FromString(self->message);
}


static void
Stash_dealloc(Stash *self)
{
    Py_CLEAR(self->commit_id);
    free(self->message);
    PyObject_Del(self);
}


static PyObject *
Stash_repr(Stash *self)
{
    return PyUnicode_FromFormat("<pygit2.Stash{%S}>", self->commit_id);
}


PyObject *
Stash_richcompare(PyObject *o1, PyObject *o2, int op)
{
    int eq = 0;
    Stash *s1, *s2;
    git_oid *oid1, *oid2;

    /* We only support comparing to another stash */
    if (!PyObject_TypeCheck(o2, &StashType)) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    s1 = (Stash *)o1;
    s2 = (Stash *)o2;

    oid1 = &((Oid *)s1->commit_id)->oid;
    oid2 = &((Oid *)s2->commit_id)->oid;

    eq = git_oid_equal(oid1, oid2) &&
        (0 == strcmp(s1->message, s2->message));

    switch (op) {
        case Py_EQ:
            if (eq) {
                Py_RETURN_TRUE;
            } else {
                Py_RETURN_FALSE;
            }
        case Py_NE:
            if (eq) {
                Py_RETURN_FALSE;
            } else {
                Py_RETURN_TRUE;
            }
        default:
            Py_INCREF(Py_NotImplemented);
            return Py_NotImplemented;
    }
}


PyGetSetDef Stash_getseters[] = {
    GETTER(Stash, commit_id),
    GETTER(Stash, message),
    GETTER(Stash, raw_message),
    {NULL}
};


PyDoc_STRVAR(Stash__doc__, "Stashed state.");

PyTypeObject StashType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Stash",                           /* tp_name           */
    sizeof(Stash),                             /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Stash_dealloc,                 /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    (reprfunc)Stash_repr,                      /* tp_repr           */
    0,                                         /* tp_as_number      */
    0,                                         /* tp_as_sequence    */
    0,                                         /* tp_as_mapping     */
    0,                                         /* tp_hash           */
    0,                                         /* tp_call           */
    0,                                         /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT,                        /* tp_flags          */
    Stash__doc__,                              /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    Stash_richcompare,                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    Stash_getseters,                           /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

