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
#include <structmember.h>
#include "error.h"
#include "utils.h"
#include "types.h"
#include "worktree.h"


PyDoc_STRVAR(Worktree_name__doc__,
    "Gets name worktree\n");
PyObject *
Worktree_name__get__(Worktree *self)
{
    return to_unicode(git_worktree_name(self->worktree), NULL, NULL);
}

PyDoc_STRVAR(Worktree_path__doc__,
    "Gets path worktree\n");
PyObject *
Worktree_path__get__(Worktree *self)
{
    return to_unicode(git_worktree_path(self->worktree), NULL, NULL);
}

PyDoc_STRVAR(Worktree_is_prunable__doc__,
    "Is the worktree prunable with the given set of flags?\n");
PyObject *
Worktree_is_prunable__get__(Worktree *self, PyObject *args)
{
    if (git_worktree_is_prunable(self->worktree, 0) > 0)
        Py_RETURN_TRUE;

    Py_RETURN_FALSE;
}

PyDoc_STRVAR(Worktree_prune__doc__,
    "prune(force=False)\n"
    "\n"
    "Prune a worktree object.");
PyObject *
Worktree_prune(Worktree *self, PyObject *args)
{
    int err, force = 0;
    git_worktree_prune_options prune_opts;

    if (!PyArg_ParseTuple(args, "|i", &force))
        return NULL;

    git_worktree_prune_options_init(&prune_opts, GIT_WORKTREE_PRUNE_OPTIONS_VERSION);
    prune_opts.flags = force & (GIT_WORKTREE_PRUNE_VALID | GIT_WORKTREE_PRUNE_LOCKED);

    err = git_worktree_prune(self->worktree, &prune_opts);
    if (err < 0)
        return Error_set(err);

    Py_RETURN_NONE;
}

static void
Worktree_dealloc(Worktree *self)
{
    Py_CLEAR(self->repo);
    git_worktree_free(self->worktree);
    PyObject_Del(self);
}


PyMethodDef Worktree_methods[] = {
    METHOD(Worktree, prune, METH_VARARGS),
    {NULL}
};

PyGetSetDef Worktree_getseters[] = {
    GETTER(Worktree, path),
    GETTER(Worktree, name),
    GETTER(Worktree, is_prunable),
    {NULL}
};

PyDoc_STRVAR(Worktree__doc__, "Worktree object.");

PyTypeObject WorktreeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Worktree",                        /* tp_name           */
    sizeof(Worktree),                          /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Worktree_dealloc,              /* tp_dealloc        */
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
    Worktree__doc__,                           /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    Worktree_methods,                          /* tp_methods        */
    0,                                         /* tp_members        */
    Worktree_getseters,                        /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyObject *
wrap_worktree(Repository* repo, git_worktree* wt)
{
    Worktree* py_wt = NULL;

    py_wt = PyObject_New(Worktree, &WorktreeType);
    if (py_wt == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    py_wt->repo = repo;
    Py_INCREF(repo);
    py_wt->worktree = wt;

    return (PyObject*) py_wt;
}


