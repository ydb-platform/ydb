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
#include <string.h>
#include "error.h"
#include "utils.h"
#include "repository.h"
#include "object.h"
#include "oid.h"
#include "tree.h"
#include "diff.h"

extern PyTypeObject TreeType;
extern PyTypeObject DiffType;
extern PyTypeObject TreeIterType;
extern PyTypeObject IndexType;


PyObject *
treeentry_to_object(const git_tree_entry *entry, Repository *repo)
{
    if (repo == NULL) {
        PyErr_SetString(PyExc_ValueError, "expected repository");
        return NULL;
    }

    return wrap_object(NULL, repo, entry);
}

Py_ssize_t
Tree_len(Tree *self)
{
    if (Object__load((Object*)self) == NULL) { return -1; } // Lazy load
    return (Py_ssize_t)git_tree_entrycount(self->tree);
}

int
Tree_contains(Tree *self, PyObject *py_name)
{
    if (Object__load((Object*)self) == NULL) { return -1; } // Lazy load

    PyObject *tvalue;
    char *name = pgit_borrow_fsdefault(py_name, &tvalue);
    if (name == NULL)
        return -1;

    git_tree_entry *entry;
    int err = git_tree_entry_bypath(&entry, self->tree, name);
    Py_DECREF(tvalue);

    if (err == GIT_ENOTFOUND) {
        return 0;
    } else if (err < 0) {
        Error_set(err);
        return -1;
    }

    git_tree_entry_free(entry);

    return 1;
}

int
Tree_fix_index(const git_tree *tree, PyObject *py_index)
{
    long index;
    size_t len;
    long slen;

    index = PyLong_AsLong(py_index);
    if (PyErr_Occurred())
        return -1;

    len = git_tree_entrycount(tree);
    slen = (long)len;
    if (index >= slen) {
        PyErr_SetObject(PyExc_IndexError, py_index);
        return -1;
    }
    else if (index < -slen) {
        PyErr_SetObject(PyExc_IndexError, py_index);
        return -1;
    }

    /* This function is called via mp_subscript, which doesn't do negative
     * index rewriting, so we have to do it manually. */
    if (index < 0)
        index = len + index;
    return (int)index;
}

PyObject *
Tree_iter(Tree *self)
{
    TreeIter *iter;

    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    iter = PyObject_New(TreeIter, &TreeIterType);
    if (iter) {
        Py_INCREF(self);
        iter->owner = self;
        iter->i = 0;
    }
    return (PyObject*)iter;
}

PyObject*
tree_getentry_by_index(const git_tree *tree, Repository *repo, PyObject *py_index)
{
    int index;
    const git_tree_entry *entry_src;
    git_tree_entry *entry;

    index = Tree_fix_index(tree, py_index);
    if (PyErr_Occurred())
        return NULL;

    entry_src = git_tree_entry_byindex(tree, index);
    if (!entry_src) {
        PyErr_SetObject(PyExc_IndexError, py_index);
        return NULL;
    }

    if (git_tree_entry_dup(&entry, entry_src) < 0) {
        PyErr_SetNone(PyExc_MemoryError);
        return NULL;
    }

    return treeentry_to_object(entry, repo);
}

PyObject*
tree_getentry_by_path(const git_tree *tree, Repository *repo, PyObject *py_path)
{
    PyObject *tvalue;
    char *path = pgit_borrow_fsdefault(py_path, &tvalue);
    if (path == NULL) {
        PyErr_SetString(PyExc_TypeError, "Value must be a path string");
        return NULL;
    }

    git_tree_entry *entry;
    int err = git_tree_entry_bypath(&entry, tree, path);
    Py_DECREF(tvalue);

    if (err == GIT_ENOTFOUND) {
        PyErr_SetObject(PyExc_KeyError, py_path);
        return NULL;
    }

    if (err < 0)
        return Error_set(err);

    /* git_tree_entry_dup is already done in git_tree_entry_bypath */
    return treeentry_to_object(entry, repo);
}

PyObject*
Tree_subscript(Tree *self, PyObject *value)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    /* Case 1: integer */
    if (PyLong_Check(value))
        return tree_getentry_by_index(self->tree, self->repo, value);

    /* Case 2: byte or text string */
    return tree_getentry_by_path(self->tree, self->repo, value);
}

PyObject *
Tree_divide(Tree *self, PyObject *value)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load
    return tree_getentry_by_path(self->tree, self->repo, value);
}


PyDoc_STRVAR(Tree_diff_to_workdir__doc__,
  "diff_to_workdir(flags: enums.DiffOption = enums.DiffOption.NORMAL, context_lines: int = 3, interhunk_lines: int = 0) -> Diff\n"
  "\n"
  "Show the changes between the :py:class:`~pygit2.Tree` and the workdir.\n"
  "\n"
  "Parameters:\n"
  "\n"
  "flags\n"
  "    A combination of enums.DiffOption constants.\n"
  "\n"
  "context_lines\n"
  "    The number of unchanged lines that define the boundary of a hunk\n"
  "    (and to display before and after).\n"
  "\n"
  "interhunk_lines\n"
  "    The maximum number of unchanged lines between hunk boundaries before\n"
  "    the hunks will be merged into a one.\n");

PyObject *
Tree_diff_to_workdir(Tree *self, PyObject *args, PyObject *kwds)
{
    git_diff_options opts = GIT_DIFF_OPTIONS_INIT;
    git_diff *diff;
    int err;

    char *keywords[] = {"flags", "context_lines", "interhunk_lines", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|IHH", keywords, &opts.flags,
                                     &opts.context_lines, &opts.interhunk_lines))
        return NULL;

    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    err = git_diff_tree_to_workdir(&diff, self->repo->repo, self->tree, &opts);
    if (err < 0)
        return Error_set(err);

    return wrap_diff(diff, self->repo);
}


PyDoc_STRVAR(Tree_diff_to_index__doc__,
  "diff_to_index(index: Index, flags: enums.DiffOption = enums.DiffOption.NORMAL, context_lines: int = 3, interhunk_lines: int = 0) -> Diff\n"
  "\n"
  "Show the changes between the index and a given :py:class:`~pygit2.Tree`.\n"
  "\n"
  "Parameters:\n"
  "\n"
  "index : :py:class:`~pygit2.Index`\n"
  "    The index to diff.\n"
  "\n"
  "flags\n"
  "    A combination of enums.DiffOption constants.\n"
  "\n"
  "context_lines\n"
  "    The number of unchanged lines that define the boundary of a hunk\n"
  "    (and to display before and after).\n"
  "\n"
  "interhunk_lines\n"
  "    The maximum number of unchanged lines between hunk boundaries before\n"
  "    the hunks will be merged into a one.\n");

PyObject *
Tree_diff_to_index(Tree *self, PyObject *args, PyObject *kwds)
{
    git_diff_options opts = GIT_DIFF_OPTIONS_INIT;
    git_diff *diff;
    git_index *index;
    char *buffer;
    Py_ssize_t length;
    PyObject *py_idx;
    int err;

    if (!PyArg_ParseTuple(args, "O|IHH", &py_idx, &opts.flags,
                                        &opts.context_lines,
                                        &opts.interhunk_lines))
        return NULL;

    /* Check whether the first argument is an index.
     * FIXME Uses duck typing. This would be easy and correct if we had
     * _pygit2.Index. */
    PyObject *pygit2_index = PyObject_GetAttrString(py_idx, "_index");
    if (!pygit2_index) {
        PyErr_SetString(PyExc_TypeError, "argument must be an Index");
        return NULL;
    }
    Py_DECREF(pygit2_index);

    /* Get git_index from cffi's pointer */
    PyObject *py_idx_ptr = PyObject_GetAttrString(py_idx, "_pointer");
    if (!py_idx_ptr)
        return NULL;

    /* Here we need to do the opposite conversion from the _pointer getters */
    if (PyBytes_AsStringAndSize(py_idx_ptr, &buffer, &length))
        goto error;

    if (length != sizeof(git_index *)) {
        PyErr_SetString(PyExc_TypeError, "passed value is not a pointer");
        goto error;
    }

    index = *((git_index **) buffer); /* the "buffer" contains the pointer */

    /* Call git_diff_tree_to_index */
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    err = git_diff_tree_to_index(&diff, self->repo->repo, self->tree, index, &opts);
    Py_DECREF(py_idx_ptr);

    if (err < 0)
        return Error_set(err);

    return wrap_diff(diff, self->repo);

error:
    Py_DECREF(py_idx_ptr);
    return NULL;
}


PyDoc_STRVAR(Tree_diff_to_tree__doc__,
  "diff_to_tree([tree: Tree, flags: enums.DiffOption = enums.DiffOption.NORMAL, context_lines: int = 3, interhunk_lines: int = 0, swap: bool = False]) -> Diff\n"
  "\n"
  "Show the changes between two trees.\n"
  "\n"
  "Parameters:\n"
  "\n"
  "tree: :py:class:`~pygit2.Tree`\n"
  "    The tree to diff. If no tree is given the empty tree will be used\n"
  "    instead.\n"
  "\n"
  "flags\n"
  "    A combination of enums.DiffOption constants.\n"
  "\n"
  "context_lines\n"
  "    The number of unchanged lines that define the boundary of a hunk\n"
  "    (and to display before and after).\n"
  "\n"
  "interhunk_lines\n"
  "    The maximum number of unchanged lines between hunk boundaries before\n"
  "    the hunks will be merged into a one.\n"
  "\n"
  "swap\n"
  "    Instead of diffing a to b. Diff b to a.\n");

PyObject *
Tree_diff_to_tree(Tree *self, PyObject *args, PyObject *kwds)
{
    git_diff_options opts = GIT_DIFF_OPTIONS_INIT;
    git_diff *diff;
    git_tree *from, *to = NULL, *tmp;
    int err, swap = 0;
    char *keywords[] = {"obj", "flags", "context_lines", "interhunk_lines", "swap", NULL};

    Tree *other = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O!IHHi", keywords,
                                     &TreeType, &other, &opts.flags,
                                     &opts.context_lines,
                                     &opts.interhunk_lines, &swap))
        return NULL;

    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load
    from = self->tree;

    if (other) {
        if (Object__load((Object*)other) == NULL) { return NULL; } // Lazy load
        to = other->tree;
    }

    if (swap > 0) {
        tmp = from;
        from = to;
        to = tmp;
    }

    err = git_diff_tree_to_tree(&diff, self->repo->repo, from, to, &opts);
    if (err < 0)
        return Error_set(err);

    return wrap_diff(diff, self->repo);
}


PySequenceMethods Tree_as_sequence = {
    0,                          /* sq_length */
    0,                          /* sq_concat */
    0,                          /* sq_repeat */
    0,                          /* sq_item */
    0,                          /* sq_slice */
    0,                          /* sq_ass_item */
    0,                          /* sq_ass_slice */
    (objobjproc)Tree_contains,  /* sq_contains */
};

PyMappingMethods Tree_as_mapping = {
    (lenfunc)Tree_len,            /* mp_length */
    (binaryfunc)Tree_subscript,   /* mp_subscript */
    0,                            /* mp_ass_subscript */
};

PyMethodDef Tree_methods[] = {
    METHOD(Tree, diff_to_tree, METH_VARARGS | METH_KEYWORDS),
    METHOD(Tree, diff_to_workdir, METH_VARARGS | METH_KEYWORDS),
    METHOD(Tree, diff_to_index, METH_VARARGS | METH_KEYWORDS),
    {NULL}
};

/* Py2/3 compatible structure
 * see https://py3c.readthedocs.io/en/latest/ext-types.html#pynumbermethods
 */
PyNumberMethods Tree_as_number = {
    0,                          /* nb_add */
    0,                          /* nb_subtract */
    0,                          /* nb_multiply */
    0,                          /* nb_remainder */
    0,                          /* nb_divmod */
    0,                          /* nb_power */
    0,                          /* nb_negative */
    0,                          /* nb_positive */
    0,                          /* nb_absolute */
    0,                          /* nb_bool (Py2: nb_nonzero) */
    0,                          /* nb_invert */
    0,                          /* nb_lshift */
    0,                          /* nb_rshift */
    0,                          /* nb_and */
    0,                          /* nb_xor */
    0,                          /* nb_or */
    0,                          /* nb_int */
    0,                          /* nb_reserved (Py2: nb_long) */
    0,                          /* nb_float */
    0,                          /* nb_inplace_add */
    0,                          /* nb_inplace_subtract */
    0,                          /* nb_inplace_multiply */
    0,                          /* nb_inplace_remainder */
    0,                          /* nb_inplace_power */
    0,                          /* nb_inplace_lshift */
    0,                          /* nb_inplace_rshift */
    0,                          /* nb_inplace_and */
    0,                          /* nb_inplace_xor */
    0,                          /* nb_inplace_or */
    0,                          /* nb_floor_divide */
    (binaryfunc)Tree_divide,    /* nb_true_divide */
    0,                          /* nb_inplace_floor_divide */
    0,                          /* nb_inplace_true_divide */
    0,                          /* nb_index */
    0,                          /* nb_matrix_multiply */
    0,                          /* nb_inplace_matrix_multiply */
};

PyDoc_STRVAR(Tree__doc__, "Tree objects.");

PyTypeObject TreeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Tree",                            /* tp_name           */
    sizeof(Tree),                              /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    0,                                         /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    (reprfunc)Object_repr,                     /* tp_repr           */
    &Tree_as_number,                           /* tp_as_number      */
    &Tree_as_sequence,                         /* tp_as_sequence    */
    &Tree_as_mapping,                          /* tp_as_mapping     */
    0,                                         /* tp_hash           */
    0,                                         /* tp_call           */
    0,                                         /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,  /* tp_flags */
    Tree__doc__,                               /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    (getiterfunc)Tree_iter,                    /* tp_iter           */
    0,                                         /* tp_iternext       */
    Tree_methods,                              /* tp_methods        */
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


void
TreeIter_dealloc(TreeIter *self)
{
    Py_CLEAR(self->owner);
    PyObject_Del(self);
}

PyObject*
TreeIter_iternext(TreeIter *self)
{
    const git_tree_entry *entry_src;
    git_tree_entry *entry;

    entry_src = git_tree_entry_byindex(self->owner->tree, self->i);
    if (!entry_src)
        return NULL;

    self->i += 1;

    if (git_tree_entry_dup(&entry, entry_src) < 0) {
        PyErr_SetNone(PyExc_MemoryError);
        return NULL;
    }

    return treeentry_to_object(entry, self->owner->repo);
}


PyDoc_STRVAR(TreeIter__doc__, "Tree iterator.");

PyTypeObject TreeIterType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.TreeIter",                        /* tp_name           */
    sizeof(TreeIter),                          /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)TreeIter_dealloc ,             /* tp_dealloc        */
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
    TreeIter__doc__,                           /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    PyObject_SelfIter,                         /* tp_iter           */
    (iternextfunc)TreeIter_iternext,           /* tp_iternext       */
};
