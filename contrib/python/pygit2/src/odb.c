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
#include "odb_backend.h"
#include "oid.h"
#include "types.h"
#include "utils.h"
#include <git2/odb.h>

extern PyTypeObject OdbBackendType;

static git_otype
int_to_loose_object_type(int type_id)
{
    switch((git_otype)type_id) {
        case GIT_OBJECT_COMMIT:
        case GIT_OBJECT_TREE:
        case GIT_OBJECT_BLOB:
        case GIT_OBJECT_TAG:
            return (git_otype)type_id;
        default:
            return GIT_OBJECT_INVALID;
    }
}

int
Odb_init(Odb *self, PyObject *args, PyObject *kwds)
{
    if (kwds && PyDict_Size(kwds) > 0) {
        PyErr_SetString(PyExc_TypeError, "Odb takes no keyword arguments");
        return -1;
    }

    PyObject *py_path = NULL;
    if (!PyArg_ParseTuple(args, "|O", &py_path))
        return -1;

    int err;
    if (py_path) {
        PyObject *tvalue;
        char *path = pgit_borrow_fsdefault(py_path, &tvalue);
        if (path == NULL)
            return -1;
        err = git_odb_open(&self->odb, path);
        Py_DECREF(tvalue);
    }
    else {
        err = git_odb_new(&self->odb);
    }

    if (err) {
        Error_set(err);
        return -1;
    }

    return 0;
}

void
Odb_dealloc(Odb *self)
{
    git_odb_free(self->odb);

    Py_TYPE(self)->tp_free((PyObject *) self);
}

static int
Odb_build_as_iter(const git_oid *oid, void *accum)
{
    int err;
    PyObject *py_oid = git_oid_to_python(oid);
    if (py_oid == NULL)
        return GIT_EUSER;

    err = PyList_Append((PyObject*)accum, py_oid);
    Py_DECREF(py_oid);
    if (err < 0)
        return GIT_EUSER;

    return 0;
}

PyObject *
Odb_as_iter(Odb *self)
{
    int err;
    PyObject *accum = PyList_New(0);
    PyObject *ret = NULL;

    err = git_odb_foreach(self->odb, Odb_build_as_iter, (void*)accum);
    if (err == GIT_EUSER)
        goto exit;
    if (err < 0) {
        ret = Error_set(err);
        goto exit;
    }

    ret = PyObject_GetIter(accum);

exit:
    Py_DECREF(accum);
    return ret;
}


PyDoc_STRVAR(Odb_add_disk_alternate__doc__,
  "add_disk_alternate(path: str)\n"
  "\n"
  "Adds a path on disk as an alternate backend for objects.\n"
  "Alternate backends are checked for objects only *after* the main backends\n"
  "are checked. Writing is disabled on alternate backends.\n");

PyObject *
Odb_add_disk_alternate(Odb *self, PyObject *py_path)
{
    PyObject *tvalue;
    char *path = pgit_borrow_fsdefault(py_path, &tvalue);
    if (path == NULL)
        return NULL;

    int err = git_odb_add_disk_alternate(self->odb, path);
    Py_DECREF(tvalue);
    if (err)
        return Error_set(err);

    Py_RETURN_NONE;
}

git_odb_object *
Odb_read_raw(git_odb *odb, const git_oid *oid, size_t len)
{
    git_odb_object *obj;
    int err;

    err = git_odb_read_prefix(&obj, odb, oid, (unsigned int)len);
    if (err < 0 && err != GIT_EUSER) {
        Error_set_oid(err, oid, len);
        return NULL;
    }

    return obj;
}

PyDoc_STRVAR(Odb_read__doc__,
  "read(oid) -> type, data, size\n"
  "\n"
  "Read raw object data from the object db.");

PyObject *
Odb_read(Odb *self, PyObject *py_hex)
{
    git_oid oid;
    git_odb_object *obj;
    size_t len;
    PyObject* tuple;

    len = py_oid_to_git_oid(py_hex, &oid);
    if (len == 0)
        return NULL;

    obj = Odb_read_raw(self->odb, &oid, len);
    if (obj == NULL)
        return NULL;

    tuple = Py_BuildValue(
        "(ny#)",
        git_odb_object_type(obj),
        git_odb_object_data(obj),
        git_odb_object_size(obj));

    git_odb_object_free(obj);
    return tuple;
}

PyDoc_STRVAR(Odb_write__doc__,
    "write(type: int, data: bytes) -> Oid\n"
    "\n"
    "Write raw object data into the object db. First arg is the object\n"
    "type, the second one a buffer with data. Return the Oid of the created\n"
    "object.");

PyObject *
Odb_write(Odb *self, PyObject *args)
{
    int err;
    git_oid oid;
    git_odb_stream* stream;
    int type_id;
    const char* buffer;
    Py_ssize_t buflen;
    git_otype type;

    if (!PyArg_ParseTuple(args, "Is#", &type_id, &buffer, &buflen))
        return NULL;

    type = int_to_loose_object_type(type_id);
    if (type == GIT_OBJECT_INVALID)
        return PyErr_Format(PyExc_ValueError, "%d", type_id);

    err = git_odb_open_wstream(&stream, self->odb, buflen, type);
    if (err < 0)
        return Error_set(err);

    err = git_odb_stream_write(stream, buffer, buflen);
    if (err) {
        git_odb_stream_free(stream);
        return Error_set(err);
    }

    err = git_odb_stream_finalize_write(&oid, stream);
    git_odb_stream_free(stream);
    if (err)
        return Error_set(err);

    return git_oid_to_python(&oid);
}

PyDoc_STRVAR(Odb_exists__doc__,
    "exists(oid: Oid) -> bool\n"
    "\n"
    "Returns true if the given oid can be found in this odb.");

PyObject *
Odb_exists(Odb *self, PyObject *py_hex)
{
    git_oid oid;
    size_t len;
    int result;

    len = py_oid_to_git_oid(py_hex, &oid);
    if (len == 0)
        return NULL;

    result = git_odb_exists(self->odb, &oid);
    if (result < 0)
        return Error_set(result);
    else if (result == 0)
        Py_RETURN_FALSE;
    else
        Py_RETURN_TRUE;
}


PyDoc_STRVAR(Odb_add_backend__doc__,
    "add_backend(backend: OdbBackend, priority: int)\n"
    "\n"
    "Adds an OdbBackend to the list of backends for this object database.\n");

PyObject *
Odb_add_backend(Odb *self, PyObject *args)
{
    int err, priority;
    OdbBackend *backend;

    if (!PyArg_ParseTuple(args, "OI", &backend, &priority))
        return NULL;

    if (!PyObject_IsInstance((PyObject *)backend, (PyObject *)&OdbBackendType)) {
        PyErr_SetString(PyExc_TypeError, "add_backend expects an instance of pygit2.OdbBackend");
        return NULL;
    }

    err = git_odb_add_backend(self->odb, backend->odb_backend, priority);
    if (err != 0)
        return Error_set(err);

    Py_INCREF(backend);

    Py_RETURN_NONE;
}


PyMethodDef Odb_methods[] = {
    METHOD(Odb, add_disk_alternate, METH_O),
    METHOD(Odb, read, METH_O),
    METHOD(Odb, write, METH_VARARGS),
    METHOD(Odb, exists, METH_O),
    METHOD(Odb, add_backend, METH_VARARGS),
    {NULL}
};


PyDoc_STRVAR(Odb_backends__doc__,
    "Return an iterable of backends for this object database.");

PyObject *
Odb_backends__get__(Odb *self)
{
    int err;
    git_odb_backend *backend;
    PyObject *ret = NULL;
    PyObject *py_backend;

    PyObject *accum = PyList_New(0);
    if (accum == NULL)
        return NULL;

    size_t nbackends = git_odb_num_backends(self->odb);
    for (size_t i = 0; i < nbackends; ++i) {
        err = git_odb_get_backend(&backend, self->odb, i);
        if (err != 0) {
            ret = Error_set(err);
            goto exit;
        }

        // XXX This won't return the correct class for custom backends (add a
        // test and fix)
        py_backend = wrap_odb_backend(backend);
        if (py_backend == NULL)
            goto exit;

        err = PyList_Append(accum, py_backend);
        if (err != 0)
            goto exit;
    }

    ret = PyObject_GetIter(accum);

exit:
    Py_DECREF(accum);
    return ret;
}


PyGetSetDef Odb_getseters[] = {
    GETTER(Odb, backends),
    {NULL}
};


int
Odb_contains(Odb *self, PyObject *py_name)
{
    git_oid oid;
    size_t len;

    len = py_oid_to_git_oid(py_name, &oid);
    if (len == 0) {
        PyErr_SetString(PyExc_TypeError, "name must be an oid");
        return -1;
    }

    return git_odb_exists(self->odb, &oid);
}

PySequenceMethods Odb_as_sequence = {
    0,                          /* sq_length */
    0,                          /* sq_concat */
    0,                          /* sq_repeat */
    0,                          /* sq_item */
    0,                          /* sq_slice */
    0,                          /* sq_ass_item */
    0,                          /* sq_ass_slice */
    (objobjproc)Odb_contains,   /* sq_contains */
};

PyDoc_STRVAR(Odb__doc__, "Object database.");

PyTypeObject OdbType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Odb",                             /* tp_name           */
    sizeof(Odb),                               /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Odb_dealloc,                   /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    0,                                         /* tp_repr           */
    0,                                         /* tp_as_number      */
    &Odb_as_sequence,                          /* tp_as_sequence    */
    0,                                         /* tp_as_mapping     */
    0,                                         /* tp_hash           */
    0,                                         /* tp_call           */
    0,                                         /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT,                        /* tp_flags          */
    Odb__doc__,                                /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    (getiterfunc)Odb_as_iter,                  /* tp_iter           */
    0,                                         /* tp_iternext       */
    Odb_methods,                               /* tp_methods        */
    0,                                         /* tp_members        */
    Odb_getseters,                             /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)Odb_init,                        /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyObject *
wrap_odb(git_odb *c_odb)
{
    Odb *py_odb = PyObject_New(Odb, &OdbType);

    if (py_odb)
        py_odb->odb = c_odb;

    return (PyObject *)py_odb;
}
