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

#include "types.h"
#include "branch.h"
#include "error.h"
#include "reference.h"
#include "utils.h"


extern PyObject *GitError;
extern PyTypeObject ReferenceType;


PyDoc_STRVAR(Branch_delete__doc__,
  "delete()\n"
  "\n"
  "Delete this branch. It will no longer be valid!");

PyObject *
Branch_delete(Branch *self, PyObject *args)
{
    int err;

    CHECK_REFERENCE(self);

    /* Delete the branch */
    err = git_branch_delete(self->reference);
    if (err < 0)
        return Error_set(err);

    git_reference_free(self->reference);
    self->reference = NULL; /* Invalidate the pointer */

    Py_RETURN_NONE;
}


PyDoc_STRVAR(Branch_is_head__doc__,
  "is_head() -> bool\n"
  "\n"
  "True if HEAD points at the branch, False otherwise.");

PyObject *
Branch_is_head(Branch *self)
{
    int err;

    CHECK_REFERENCE(self);

    err = git_branch_is_head(self->reference);
    if (err == 1)
        Py_RETURN_TRUE;
    else if (err == 0)
        Py_RETURN_FALSE;
    else
        return Error_set(err);
}

PyDoc_STRVAR(Branch_is_checked_out__doc__,
  "is_checked_out() -> bool\n"
  "\n"
  "True if branch is checked out by any repo connected to the current one, "
  " False otherwise.");

PyObject *
Branch_is_checked_out(Branch *self)
{
    int err;

    CHECK_REFERENCE(self);

    err = git_branch_is_checked_out(self->reference);
    if (err == 1)
        Py_RETURN_TRUE;
    else if (err == 0)
        Py_RETURN_FALSE;
    else
        return Error_set(err);
}


PyDoc_STRVAR(Branch_rename__doc__,
  "rename(name: str, force: bool = False)\n"
  "\n"
  "Move/rename an existing local branch reference. The new branch name will be "
  "checked for validity.\n"
  "Returns the new branch.");

PyObject *
Branch_rename(Branch *self, PyObject *args)
{
    int err, force = 0;
    git_reference *c_out;
    const char *c_name;

    CHECK_REFERENCE(self);

    if (!PyArg_ParseTuple(args, "s|i", &c_name, &force))
        return NULL;

    err = git_branch_move(&c_out, self->reference, c_name, force);
    if (err == GIT_OK)
        return wrap_branch(c_out, self->repo);
    else
        return Error_set(err);
}


PyDoc_STRVAR(Branch_branch_name__doc__,
  "The name of the local or remote branch.");

PyObject *
Branch_branch_name__get__(Branch *self)
{
    int err;
    const char *c_name;

    CHECK_REFERENCE(self);

    err = git_branch_name(&c_name, self->reference);
    if (err == GIT_OK)
        return to_unicode(c_name, NULL, NULL);
    else
        return Error_set(err);
}

PyDoc_STRVAR(Branch_raw_branch_name__doc__,
  "The name of the local or remote branch (bytes).");

PyObject *
Branch_raw_branch_name__get__(Branch *self)
{
    int err;
    const char *c_name;

    CHECK_REFERENCE(self);

    err = git_branch_name(&c_name, self->reference);
    if (err == GIT_OK)
        return PyBytes_FromString(c_name);
    else
        return Error_set(err);
}

PyDoc_STRVAR(Branch_remote_name__doc__,
    "Find the remote name of a remote-tracking branch.\n"
    "\n"
    "This will return the name of the remote whose fetch refspec is matching "
    "the given branch. E.g. given a branch 'refs/remotes/test/master', it will "
    "extract the 'test' part. If refspecs from multiple remotes match, the "
    "function will raise ValueError.");

PyObject *
Branch_remote_name__get__(Branch *self)
{
    int err;
    git_buf name = {NULL};
    const char *branch_name;
    PyObject *py_name;

    CHECK_REFERENCE(self);

    branch_name = git_reference_name(self->reference);
    err = git_branch_remote_name(&name, self->repo->repo, branch_name);
    if (err < GIT_OK)
        return Error_set(err);

    py_name = to_unicode_n(name.ptr, name.size, NULL, NULL);
    git_buf_dispose(&name);

    return py_name;
}


PyDoc_STRVAR(Branch_upstream__doc__,
  "The branch's upstream branch or None if this branch does not have an upstream set. "
  "Set to None to unset the upstream configuration.");

PyObject *
Branch_upstream__get__(Branch *self)
{
    int err;
    git_reference *c_reference;

    CHECK_REFERENCE(self);

    err = git_branch_upstream(&c_reference, self->reference);
    if (err == GIT_ENOTFOUND)
        Py_RETURN_NONE;
    else if (err < GIT_OK)
        return Error_set(err);

    return wrap_branch(c_reference, self->repo);
}

int Branch_upstream__set__(Branch *self, Reference *py_ref)
{
    int err;
    const char *branch_name = NULL;

    CHECK_REFERENCE_INT(self);

    if ((PyObject *)py_ref != Py_None) {
        if (!PyObject_TypeCheck(py_ref, (PyTypeObject *)&ReferenceType)) {
            PyErr_SetObject(PyExc_TypeError, (PyObject *)py_ref);
            return -1;
        }

        CHECK_REFERENCE_INT(py_ref);
        err = git_branch_name(&branch_name, py_ref->reference);
        if (err < GIT_OK) {
            Error_set(err);
            return -1;
        }
    }

    err = git_branch_set_upstream(self->reference, branch_name);
    if (err < GIT_OK) {
        Error_set(err);
        return -1;
    }

    return 0;
}


PyDoc_STRVAR(Branch_upstream_name__doc__,
  "The name of the reference set to be the upstream of this one");

PyObject *
Branch_upstream_name__get__(Branch *self)
{
    int err;
    git_buf name = {NULL};
    const char *branch_name;
    PyObject *py_name;

    CHECK_REFERENCE(self);

    branch_name = git_reference_name(self->reference);

    err = git_branch_upstream_name(&name, self->repo->repo, branch_name);
    if (err < GIT_OK)
        return Error_set(err);

    py_name = to_unicode_n(name.ptr, name.size, NULL, NULL);
    git_buf_dispose(&name);

    return py_name;
}


PyMethodDef Branch_methods[] = {
    METHOD(Branch, delete, METH_NOARGS),
    METHOD(Branch, is_head, METH_NOARGS),
    METHOD(Branch, is_checked_out, METH_NOARGS),
    METHOD(Branch, rename, METH_VARARGS),
    {NULL}
};

PyGetSetDef Branch_getseters[] = {
    GETTER(Branch, branch_name),
    GETTER(Branch, raw_branch_name),
    GETTER(Branch, remote_name),
    GETSET(Branch, upstream),
    GETTER(Branch, upstream_name),
    {NULL}
};

PyDoc_STRVAR(Branch__doc__, "Branch.");

PyTypeObject BranchType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Branch",                          /* tp_name           */
    sizeof(Branch),                            /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    0,                                         /* tp_dealloc        */
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
    Branch__doc__,                             /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    Branch_methods,                            /* tp_methods        */
    0,                                         /* tp_members        */
    Branch_getseters,                          /* tp_getset         */
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
wrap_branch(git_reference *c_reference, Repository *repo)
{
    Branch *py_branch=NULL;

    py_branch = PyObject_New(Branch, &BranchType);
    if (py_branch) {
        py_branch->reference = c_reference;
        if (repo) {
            py_branch->repo = repo;
            Py_INCREF(repo);
        }
    }

    return (PyObject *)py_branch;
}
