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
#include "refdb.h"
#include "types.h"
#include "utils.h"
#include <git2/refdb.h>
#include <git2/sys/refdb_backend.h>

extern PyTypeObject RepositoryType;
extern PyTypeObject RefdbType;

void
Refdb_dealloc(Refdb *self)
{
    git_refdb_free(self->refdb);

    Py_TYPE(self)->tp_free((PyObject *) self);
}

PyDoc_STRVAR(Refdb_compress__doc__,
    "compress()\n"
    "\n"
     "Suggests that the given refdb compress or optimize its references.\n"
     "This mechanism is implementation specific.  For on-disk reference\n"
     "databases, for example, this may pack all loose references.");

PyObject *
Refdb_compress(Refdb *self)
{
    int err = git_refdb_compress(self->refdb);
    if (err != 0)
        return Error_set(err);
    Py_RETURN_NONE;
}

PyDoc_STRVAR(Refdb_set_backend__doc__,
        "set_backend(backend: RefdbBackend)\n"
        "\n"
        "Sets a custom RefdbBackend for this Refdb.");

PyObject *
Refdb_set_backend(Refdb *self, RefdbBackend *backend)
{
    int err;
    err = git_refdb_set_backend(self->refdb, backend->refdb_backend);
    if (err != 0)
        return Error_set(err);
    Py_RETURN_NONE;
}

PyDoc_STRVAR(Refdb_new__doc__, "Refdb.new(repo: Repository) -> Refdb\n"
        "Creates a new refdb with no backend.");

PyObject *
Refdb_new(PyObject *self, Repository *repo)
{
    if (!PyObject_IsInstance((PyObject *)repo, (PyObject *)&RepositoryType)) {
        PyErr_SetString(PyExc_TypeError,
                        "Refdb.new expects an object of type "
                        "pygit2.Repository");
        return NULL;
    }

    git_refdb *refdb;
    int err = git_refdb_new(&refdb, repo->repo);
    if (err) {
        Error_set(err);
        return NULL;
    }

    return wrap_refdb(refdb);
}

PyDoc_STRVAR(Refdb_open__doc__,
    "open(repo: Repository) -> Refdb\n"
    "\n"
     "Create a new reference database and automatically add\n"
     "the default backends, assuming the repository dir as the folder.");

PyObject *
Refdb_open(PyObject *self, Repository *repo)
{
    if (!PyObject_IsInstance((PyObject *)repo, (PyObject *)&RepositoryType)) {
        PyErr_SetString(PyExc_TypeError,
                        "Refdb.open expects an object of type "
                        "pygit2.Repository");
        return NULL;
    }

    git_refdb *refdb;
    int err = git_refdb_open(&refdb, repo->repo);
    if (err) {
        Error_set(err);
        return NULL;
    }

    return wrap_refdb(refdb);
}

PyMethodDef Refdb_methods[] = {
    METHOD(Refdb, compress, METH_NOARGS),
    METHOD(Refdb, set_backend, METH_O),
    {"new", (PyCFunction) Refdb_new,
      METH_O | METH_STATIC, Refdb_new__doc__},
    {"open", (PyCFunction) Refdb_open,
      METH_O | METH_STATIC, Refdb_open__doc__},
    {NULL}
};

PyDoc_STRVAR(Refdb__doc__, "Reference database.");

PyTypeObject RefdbType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Refdb",                           /* tp_name           */
    sizeof(Refdb),                             /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Refdb_dealloc,                 /* tp_dealloc        */
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
    Refdb__doc__,                              /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    Refdb_methods,                             /* tp_methods        */
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

PyObject *
wrap_refdb(git_refdb *c_refdb)
{
    Refdb *py_refdb = PyObject_New(Refdb, &RefdbType);

    if (py_refdb)
        py_refdb->refdb = c_refdb;

    return (PyObject *)py_refdb;
}
