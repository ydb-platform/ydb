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
#include "error.h"
#include "object.h"
#include "types.h"
#include "utils.h"

extern PyTypeObject RevSpecType;

PyObject*
wrap_revspec(git_revspec *revspec, Repository *repo)
{
    RevSpec *py_revspec;

    py_revspec = PyObject_New(RevSpec, &RevSpecType);
    if (py_revspec) {
        py_revspec->flags = revspec->flags;

        if (revspec->from != NULL) {
            py_revspec->from = wrap_object(revspec->from, repo, NULL);
        } else {
            py_revspec->from = NULL;
        }

        if (revspec->to != NULL) {
            py_revspec->to = wrap_object(revspec->to, repo, NULL);
        } else {
            py_revspec->to = NULL;
        }
    }

    return (PyObject*) py_revspec;
}

PyDoc_STRVAR(RevSpec_from_object__doc__, "From revision");

PyObject *
RevSpec_from_object__get__(RevSpec *self)
{
    if (self->from == NULL)
        Py_RETURN_NONE;

    Py_INCREF(self->from);
    return self->from;
}

PyDoc_STRVAR(RevSpec_to_object__doc__, "To revision");

PyObject *
RevSpec_to_object__get__(RevSpec *self)
{
    if (self->to == NULL)
        Py_RETURN_NONE;

    Py_INCREF(self->to);
    return self->to;
}

PyDoc_STRVAR(RevSpec_flags__doc__,
    "A combination of enums.RevSpecFlag constants indicating the\n"
    "intended behavior of the spec passed to Repository.revparse()");

PyObject *
RevSpec_flags__get__(RevSpec *self)
{
    return PyLong_FromLong(self->flags);
}

static PyObject *
RevSpec_repr(RevSpec *self)
{
    return PyUnicode_FromFormat("<pygit2.RevSpec{from=%S,to=%S}>",
                                (self->from != NULL) ? self->from : Py_None,
                                (self->to != NULL) ? self->to : Py_None);
}

static void
RevSpec_dealloc(RevSpec *self)
{
    Py_XDECREF(self->from);
    Py_XDECREF(self->to);
    PyObject_Del(self);
}

PyGetSetDef RevSpec_getsetters[] = {
    GETTER(RevSpec, from_object),
    GETTER(RevSpec, to_object),
    GETTER(RevSpec, flags),
    {NULL}
};

PyDoc_STRVAR(RevSpec__doc__, "RevSpec object, output from Repository.revparse().");

PyTypeObject RevSpecType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.RevSpec",                         /* tp_name           */
    sizeof(RevSpec),                           /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)RevSpec_dealloc,               /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    (reprfunc)RevSpec_repr,                    /* tp_repr           */
    0,                                         /* tp_as_number      */
    0,                                         /* tp_as_sequence    */
    0,                                         /* tp_as_mapping     */
    0,                                         /* tp_hash           */
    0,                                         /* tp_call           */
    0,                                         /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,  /* tp_flags          */
    RevSpec__doc__,                            /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    RevSpec_getsetters,                        /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};
