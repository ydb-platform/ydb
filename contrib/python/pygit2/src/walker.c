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
#include "oid.h"
#include "tree.h"
#include "utils.h"
#include "walker.h"

extern PyTypeObject CommitType;

void
Walker_dealloc(Walker *self)
{
    Py_CLEAR(self->repo);
    git_revwalk_free(self->walk);
    PyObject_Del(self);
}


PyDoc_STRVAR(Walker_hide__doc__,
  "hide(oid: Oid)\n"
  "\n"
  "Mark a commit (and its ancestors) uninteresting for the output.");

PyObject *
Walker_hide(Walker *self, PyObject *py_hex)
{
    int err;
    git_oid oid;

    err = py_oid_to_git_oid_expand(self->repo->repo, py_hex, &oid);
    if (err < 0)
        return NULL;

    err = git_revwalk_hide(self->walk, &oid);
    if (err < 0)
        return Error_set(err);

    Py_RETURN_NONE;
}


PyDoc_STRVAR(Walker_push__doc__,
  "push(oid: Oid)\n"
  "\n"
  "Mark a commit to start traversal from.");

PyObject *
Walker_push(Walker *self, PyObject *py_hex)
{
    int err;
    git_oid oid;

    err = py_oid_to_git_oid_expand(self->repo->repo, py_hex, &oid);
    if (err < 0)
        return NULL;

    err = git_revwalk_push(self->walk, &oid);
    if (err < 0)
        return Error_set(err);

    Py_RETURN_NONE;
}


PyDoc_STRVAR(Walker_sort__doc__,
  "sort(mode: enums.SortMode)\n"
  "\n"
  "Change the sorting mode (this resets the walker).");

PyObject *
Walker_sort(Walker *self, PyObject *py_sort_mode)
{
    long sort_mode;

    sort_mode = PyLong_AsLong(py_sort_mode);
    if (sort_mode == -1 && PyErr_Occurred())
        return NULL;

    git_revwalk_sorting(self->walk, (unsigned int)sort_mode);

    Py_RETURN_NONE;
}


PyDoc_STRVAR(Walker_reset__doc__,
  "reset()\n"
  "\n"
  "Reset the walking machinery for reuse.");

PyObject *
Walker_reset(Walker *self)
{
    git_revwalk_reset(self->walk);
    Py_RETURN_NONE;
}

PyDoc_STRVAR(Walker_simplify_first_parent__doc__,
  "simplify_first_parent()\n"
  "\n"
  "Simplify the history by first-parent.");

PyObject *
Walker_simplify_first_parent(Walker *self)
{
    git_revwalk_simplify_first_parent(self->walk);
    Py_RETURN_NONE;
}

PyObject *
Walker_iter(Walker *self)
{
    Py_INCREF(self);
    return (PyObject*)self;
}

PyObject *
Walker_iternext(Walker *self)
{
    int err;
    git_commit *commit;
    git_oid oid;

    Py_BEGIN_ALLOW_THREADS
    err = git_revwalk_next(&oid, self->walk);
    Py_END_ALLOW_THREADS

    if (err < 0)
        return Error_set(err);

    err = git_commit_lookup(&commit, self->repo->repo, &oid);
    if (err < 0)
        return Error_set(err);

    return wrap_object((git_object*)commit, self->repo, NULL);
}

PyMethodDef Walker_methods[] = {
    METHOD(Walker, hide, METH_O),
    METHOD(Walker, push, METH_O),
    METHOD(Walker, reset, METH_NOARGS),
    METHOD(Walker, simplify_first_parent, METH_NOARGS),
    METHOD(Walker, sort, METH_O),
    {NULL}
};


PyDoc_STRVAR(Walker__doc__, "Revision walker.");

PyTypeObject WalkerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Walker",                          /* tp_name           */
    sizeof(Walker),                            /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Walker_dealloc,                /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    0,                                         /* tp_repr           */
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
    Walker__doc__,                             /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    (getiterfunc)Walker_iter,                  /* tp_iter           */
    (iternextfunc)Walker_iternext,             /* tp_iternext       */
    Walker_methods,                            /* tp_methods        */
    0,                                         /* tp_members        */
    0,                                         /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};
