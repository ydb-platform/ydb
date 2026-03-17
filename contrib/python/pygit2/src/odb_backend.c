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
 *
 *
 * TODO This still needs much work to make it usable, and maintanable!
 * - Create OdbBackendCustomType that inherits from OdbBackendType.
 *   OdbBackendType should not be subclassed, instead subclass
 *   OdbBackendCustomType to develop custom backends in Python.
 *   Implement this new type in src/odb_backend_custom.c
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "error.h"
#include "object.h"
#include "oid.h"
#include "types.h"
#include "utils.h"
#include <git2/odb_backend.h>
#include <git2/sys/alloc.h>
#include <git2/sys/odb_backend.h>

/*
 * pgit_odb_backend_t is a container for the state associated with a custom
 * implementation of git_odb_backend. The git_odb_backend field's function
 * pointers are assigned to the pgit_odb_backend_* functions, which handle
 * translating between the libgit2 ABI and the Python ABI.
 * It holds a pointer to the <OdbBackend> subclass, which must implement
 * the callbacks in Python.
 */
typedef struct {
    git_odb_backend backend;
    PyObject *py_backend;
} pgit_odb_backend;

static int
pgit_odb_backend_read(void **ptr, size_t *sz, git_object_t *type,
                      git_odb_backend *_be, const git_oid *oid)
{
    pgit_odb_backend *be = (pgit_odb_backend *)_be;

    PyObject *py_oid = git_oid_to_python(oid);
    if (py_oid == NULL)
        return GIT_EUSER;

    PyObject *result = PyObject_CallMethod(be->py_backend, "read_cb", "N", py_oid);
    if (result == NULL)
        return git_error_for_exc();

    const char *bytes;
    Py_ssize_t type_value;
    if (!PyArg_ParseTuple(result, "ny#", &type_value, &bytes, sz) || !bytes) {
        Py_DECREF(result);
        return GIT_EUSER;
    }
    *type = (git_object_t)type_value;

    *ptr = git_odb_backend_data_alloc(_be, *sz);
    if (!*ptr) {
        Py_DECREF(result);
        return GIT_EUSER;
    }

    memcpy(*ptr, bytes, *sz);
    Py_DECREF(result);
    return 0;
}

static int
pgit_odb_backend_read_prefix(git_oid *oid_out, void **ptr, size_t *sz, git_object_t *type,
                             git_odb_backend *_be, const git_oid *short_id, size_t len)
{
    // short_id to hex
    char short_id_hex[GIT_OID_HEXSZ];
    git_oid_nfmt(short_id_hex, len, short_id);

    // Call callback
    pgit_odb_backend *be = (pgit_odb_backend *)_be;
    PyObject *result = PyObject_CallMethod(be->py_backend, "read_prefix_cb", "s#", short_id_hex, len);
    if (result == NULL)
        return git_error_for_exc();

    // Parse output from callback
    PyObject *py_oid_out;
    Py_ssize_t type_value;
    const char *bytes;
    if (!PyArg_ParseTuple(result, "ny#O", &type_value, &bytes, sz, &py_oid_out) || !bytes) {
        Py_DECREF(result);
        return GIT_EUSER;
    }
    *type = (git_object_t)type_value;

    *ptr = git_odb_backend_data_alloc(_be, *sz);
    if (!*ptr) {
        Py_DECREF(result);
        return GIT_EUSER;
    }

    memcpy(*ptr, bytes, *sz);
    py_oid_to_git_oid(py_oid_out, oid_out);
    Py_DECREF(result);
    return 0;
}

static int
pgit_odb_backend_read_header(size_t *len, git_object_t *type,
                             git_odb_backend *_be, const git_oid *oid)
{
    pgit_odb_backend *be = (pgit_odb_backend *)_be;

    PyObject *py_oid = git_oid_to_python(oid);
    if (py_oid == NULL)
        return GIT_EUSER;

    PyObject *result = PyObject_CallMethod(be->py_backend, "read_header_cb", "N", py_oid);
    if (result == NULL)
        return git_error_for_exc();

    Py_ssize_t type_value;
    if (!PyArg_ParseTuple(result, "nn", &type_value, len)) {
        Py_DECREF(result);
        return GIT_EUSER;
    }
    *type = (git_object_t)type_value;

    Py_DECREF(result);
    return 0;
}

static int
pgit_odb_backend_write(git_odb_backend *_be, const git_oid *oid,
        const void *data, size_t sz, git_object_t typ)
{
    pgit_odb_backend *be = (pgit_odb_backend *)_be;

    PyObject *py_oid = git_oid_to_python(oid);
    if (py_oid == NULL)
        return GIT_EUSER;

    PyObject *result = PyObject_CallMethod(be->py_backend, "write_cb", "Ny#n", py_oid, data, sz, typ);
    if (result == NULL)
        return git_error_for_exc();

    Py_DECREF(result);
    return 0;
}

static int
pgit_odb_backend_exists(git_odb_backend *_be, const git_oid *oid)
{
    pgit_odb_backend *be = (pgit_odb_backend *)_be;

    PyObject *py_oid = git_oid_to_python(oid);
    if (py_oid == NULL)
        return GIT_EUSER;

    PyObject *result = PyObject_CallMethod(be->py_backend, "exists_cb", "N", py_oid);
    if (result == NULL)
        return git_error_for_exc();

    int r = PyObject_IsTrue(result);
    Py_DECREF(result);
    return r;
}

static int
pgit_odb_backend_exists_prefix(git_oid *out, git_odb_backend *_be,
                               const git_oid *short_id, size_t len)
{
    // short_id to hex
    char short_id_hex[GIT_OID_HEXSZ];
    git_oid_nfmt(short_id_hex, len, short_id);

    // Call callback
    pgit_odb_backend *be = (pgit_odb_backend *)_be;
    PyObject *py_oid = PyObject_CallMethod(be->py_backend, "exists_prefix_cb", "s#", short_id_hex, len);
    if (py_oid == NULL)
        return git_error_for_exc();

    py_oid_to_git_oid(py_oid, out);
    Py_DECREF(py_oid);
    return 0;
}

static int
pgit_odb_backend_refresh(git_odb_backend *_be)
{
    pgit_odb_backend *be = (pgit_odb_backend *)_be;
    PyObject_CallMethod(be->py_backend, "refresh_cb", NULL);
    return git_error_for_exc();
}

static int
pgit_odb_backend_foreach(git_odb_backend *_be,
        git_odb_foreach_cb cb, void *payload)
{
    PyObject *item;
    git_oid oid;
    pgit_odb_backend *be = (pgit_odb_backend *)_be;
    PyObject *iterator = PyObject_GetIter((PyObject *)be->py_backend);
    assert(iterator);

    while ((item = PyIter_Next(iterator))) {
        py_oid_to_git_oid(item, &oid);
        cb(&oid, payload);
        Py_DECREF(item);
    }

    return git_error_for_exc();
}

static void
pgit_odb_backend_free(git_odb_backend *backend)
{
    pgit_odb_backend *custom_backend = (pgit_odb_backend *)backend;
    Py_DECREF(custom_backend->py_backend);
}

int
OdbBackend_init(OdbBackend *self, PyObject *args, PyObject *kwds)
{
    // Check input arguments
    if (args && PyTuple_Size(args) > 0) {
        PyErr_SetString(PyExc_TypeError, "OdbBackend takes no arguments");
        return -1;
    }

    if (kwds && PyDict_Size(kwds) > 0) {
        PyErr_SetString(PyExc_TypeError, "OdbBackend takes no keyword arguments");
        return -1;
    }

    // Create the C backend
    pgit_odb_backend *custom_backend = calloc(1, sizeof(pgit_odb_backend));
    custom_backend->backend.version = GIT_ODB_BACKEND_VERSION;

    // Fill the member methods
    custom_backend->backend.free = pgit_odb_backend_free;
    custom_backend->backend.read = pgit_odb_backend_read;
    custom_backend->backend.read_prefix = pgit_odb_backend_read_prefix;
    custom_backend->backend.read_header = pgit_odb_backend_read_header;
    custom_backend->backend.write = pgit_odb_backend_write;
    custom_backend->backend.exists = pgit_odb_backend_exists;
    custom_backend->backend.exists_prefix = pgit_odb_backend_exists_prefix;
    custom_backend->backend.refresh = pgit_odb_backend_refresh;
//  custom_backend->backend.writepack = pgit_odb_backend_writepack;
//  custom_backend->backend.freshen = pgit_odb_backend_freshen;
//  custom_backend->backend.writestream = pgit_odb_backend_writestream;
//  custom_backend->backend.readstream = pgit_odb_backend_readstream;
    if (PyIter_Check((PyObject *)self))
        custom_backend->backend.foreach = pgit_odb_backend_foreach;

    // Cross reference (don't incref because it's something internal)
    custom_backend->py_backend = (PyObject *)self;
    self->odb_backend = (git_odb_backend *)custom_backend;

    return 0;
}

void
OdbBackend_dealloc(OdbBackend *self)
{
    if (self->odb_backend && self->odb_backend->read == pgit_odb_backend_read) {
        pgit_odb_backend *custom_backend = (pgit_odb_backend *)self->odb_backend;
        free(custom_backend);
    }

    Py_TYPE(self)->tp_free((PyObject *) self);
}

static int
OdbBackend_build_as_iter(const git_oid *oid, void *accum)
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
OdbBackend_as_iter(OdbBackend *self)
{
    PyObject *accum = PyList_New(0);
    PyObject *iter = NULL;

    int err = self->odb_backend->foreach(self->odb_backend, OdbBackend_build_as_iter, (void*)accum);
    if (err == GIT_EUSER)
        goto exit;

    if (err < 0) {
        Error_set(err);
        goto exit;
    }

    iter = PyObject_GetIter(accum);

exit:
    Py_DECREF(accum);
    return iter;
}

PyDoc_STRVAR(OdbBackend_read__doc__,
    "read(oid) -> (type, data)\n"
    "\n"
    "Read raw object data from this odb backend.\n");

PyObject *
OdbBackend_read(OdbBackend *self, PyObject *py_hex)
{
    int err;
    git_oid oid;
    git_object_t type;
    size_t len, size;
    void *data;

    if (self->odb_backend->read == NULL)
        Py_RETURN_NOTIMPLEMENTED;

    len = py_oid_to_git_oid(py_hex, &oid);
    if (len == 0)
        return NULL;

    err = self->odb_backend->read(&data, &size, &type, self->odb_backend, &oid);
    if (err != 0) {
        Error_set_oid(err, &oid, len);
        return NULL;
    }

    PyObject *tuple = Py_BuildValue("(ny#)", type, data, size);

    git_odb_backend_data_free(self->odb_backend, data);

    return tuple;
}

PyDoc_STRVAR(OdbBackend_read_prefix__doc__,
    "read_prefix(oid: Oid) -> tuple[int, bytes, Oid]\n"
    "\n"
    "Read raw object data from this odb backend based on an oid prefix.\n"
    "The returned tuple contains (type, data, oid).");

PyObject *
OdbBackend_read_prefix(OdbBackend *self, PyObject *py_hex)
{
    int err;
    git_oid oid, oid_out;
    git_object_t type;
    size_t len, size;
    void *data;

    if (self->odb_backend->read_prefix == NULL)
        Py_RETURN_NOTIMPLEMENTED;

    len = py_oid_to_git_oid(py_hex, &oid);
    if (len == 0)
        return NULL;

    err = self->odb_backend->read_prefix(&oid_out, &data, &size, &type, self->odb_backend, &oid, len);
    if (err != 0) {
        Error_set_oid(err, &oid, len);
        return NULL;
    }

    PyObject *py_oid_out = git_oid_to_python(&oid_out);
    if (py_oid_out == NULL)
        return Error_set_exc(PyExc_MemoryError);

    PyObject *tuple = Py_BuildValue("(ny#N)", type, data, size, py_oid_out);

    git_odb_backend_data_free(self->odb_backend, data);

    return tuple;
}

PyDoc_STRVAR(OdbBackend_read_header__doc__,
    "read_header(oid) -> (type, len)\n"
    "\n"
    "Read raw object header from this odb backend.");

PyObject *
OdbBackend_read_header(OdbBackend *self, PyObject *py_hex)
{
    int err;
    size_t len;
    git_object_t type;
    git_oid oid;

    if (self->odb_backend->read_header == NULL)
        Py_RETURN_NOTIMPLEMENTED;

    len = py_oid_to_git_oid(py_hex, &oid);
    if (len == 0)
        return NULL;

    err = self->odb_backend->read_header(&len, &type, self->odb_backend, &oid);
    if (err != 0) {
        Error_set_oid(err, &oid, len);
        return NULL;
    }

    return Py_BuildValue("(ni)", type, len);
}

PyDoc_STRVAR(OdbBackend_exists__doc__,
    "exists(oid: Oid) -> bool\n"
    "\n"
    "Returns true if the given oid can be found in this odb.");

PyObject *
OdbBackend_exists(OdbBackend *self, PyObject *py_hex)
{
    int result;
    size_t len;
    git_oid oid;

    if (self->odb_backend->exists == NULL)
        Py_RETURN_NOTIMPLEMENTED;

    len = py_oid_to_git_oid(py_hex, &oid);
    if (len == 0)
        return NULL;

    result = self->odb_backend->exists(self->odb_backend, &oid);
    if (result < 0)
        return Error_set(result);
    else if (result == 0)
        Py_RETURN_FALSE;
    else
        Py_RETURN_TRUE;
}

PyDoc_STRVAR(OdbBackend_exists_prefix__doc__,
    "exists_prefix(partial_id: Oid) -> Oid\n"
    "\n"
    "Given a partial oid, returns the full oid. Raises KeyError if not found,\n"
    "or ValueError if ambiguous.");

PyObject *
OdbBackend_exists_prefix(OdbBackend *self, PyObject *py_hex)
{
    int result;
    size_t len;
    git_oid oid;

    if (self->odb_backend->exists_prefix == NULL)
        Py_RETURN_NOTIMPLEMENTED;

    len = py_oid_to_git_oid(py_hex, &oid);
    if (len == 0)
        return NULL;

    git_oid out;
    result = self->odb_backend->exists_prefix(&out, self->odb_backend, &oid, len);

    if (result < 0)
        return Error_set(result);

    return git_oid_to_python(&out);
}

PyDoc_STRVAR(OdbBackend_refresh__doc__,
    "refresh()\n"
    "\n"
    "If the backend supports a refreshing mechanism, this function will invoke\n"
    "it. However, the backend implementation should try to stay up-to-date as\n"
    "much as possible by itself as libgit2 will not automatically invoke this\n"
    "function. For instance, a potential strategy for the backend\n"
    "implementation to utilize this could be internally calling the refresh\n"
    "function on failed lookups.");

PyObject *
OdbBackend_refresh(OdbBackend *self)
{
    if (self->odb_backend->refresh == NULL)
        Py_RETURN_NOTIMPLEMENTED;

    self->odb_backend->refresh(self->odb_backend);
    Py_RETURN_NONE;
}

/*
 * TODO:
 * - write
 * - writepack
 * - writestream
 * - readstream
 * - freshen
 */
PyMethodDef OdbBackend_methods[] = {
    METHOD(OdbBackend, read, METH_O),
    METHOD(OdbBackend, read_prefix, METH_O),
    METHOD(OdbBackend, read_header, METH_O),
    METHOD(OdbBackend, exists, METH_O),
    METHOD(OdbBackend, exists_prefix, METH_O),
    METHOD(OdbBackend, refresh, METH_NOARGS),
    {NULL}
};

PyDoc_STRVAR(OdbBackend__doc__, "Object database backend.");

PyTypeObject OdbBackendType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.OdbBackend",                      /* tp_name           */
    sizeof(OdbBackend),                        /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)OdbBackend_dealloc,            /* tp_dealloc        */
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
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,  /* tp_flags          */
    OdbBackend__doc__,                         /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    (getiterfunc)OdbBackend_as_iter,           /* tp_iter           */
    0,                                         /* tp_iternext       */
    OdbBackend_methods,                        /* tp_methods        */
    0,                                         /* tp_members        */
    0,                                         /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)OdbBackend_init,                 /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyObject *
wrap_odb_backend(git_odb_backend *odb_backend)
{
    OdbBackend *py_backend = PyObject_New(OdbBackend, &OdbBackendType);
    if (py_backend)
        py_backend->odb_backend = odb_backend;

    return (PyObject *)py_backend;
}

PyDoc_STRVAR(OdbBackendPack__doc__, "Object database backend for packfiles.");

int
OdbBackendPack_init(OdbBackendPack *self, PyObject *args, PyObject *kwds)
{
    if (kwds && PyDict_Size(kwds) > 0) {
        PyErr_SetString(PyExc_TypeError, "OdbBackendPack takes no keyword arguments");
        return -1;
    }

    PyObject *py_path;
    if (!PyArg_ParseTuple(args, "O", &py_path))
        return -1;

    PyObject *tvalue;
    char *path = pgit_borrow_fsdefault(py_path, &tvalue);
    if (path == NULL)
        return -1;

    int err = git_odb_backend_pack(&self->super.odb_backend, path);
    Py_DECREF(tvalue);
    if (err) {
        Error_set(err);
        return -1;
    }

    return 0;
}

PyTypeObject OdbBackendPackType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.OdbBackendPack",                  /* tp_name           */
    sizeof(OdbBackendPack),                    /* tp_basicsize      */
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
    OdbBackendPack__doc__,                     /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    0,                                         /* tp_getset         */
    &OdbBackendType,                           /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)OdbBackendPack_init,             /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyDoc_STRVAR(OdbBackendLoose__doc__,
        "OdbBackendLoose(objects_dir, compression_level,"
        " do_fsync, dir_mode=0, file_mode=0)\n"
        "\n"
        "Object database backend for loose objects.\n"
        "\n"
        "Parameters:\n"
        "\n"
        "objects_dir\n"
        "    path to top-level object dir on disk\n"
        "\n"
        "compression_level\n"
        "    zlib compression level to use\n"
        "\n"
        "do_fsync\n"
        "    true to fsync() after writing\n"
        "\n"
        "dir_mode\n"
        "    mode for new directories, or 0 for default\n"
        "\n"
        "file_mode\n"
        "    mode for new files, or 0 for default");

int
OdbBackendLoose_init(OdbBackendLoose *self, PyObject *args, PyObject *kwds)
{
    if (kwds && PyDict_Size(kwds) > 0) {
        PyErr_SetString(PyExc_TypeError, "OdbBackendLoose takes no keyword arguments");
        return -1;
    }

    PyObject *py_path;
    int compression_level, do_fsync;
    unsigned int dir_mode = 0, file_mode = 0;
    if (!PyArg_ParseTuple(args, "Oip|II", &py_path, &compression_level,
                          &do_fsync, &dir_mode, &file_mode))
        return -1;

    PyObject *tvalue;
    char *path = pgit_borrow_fsdefault(py_path, &tvalue);
    if (path == NULL)
        return -1;

    int err = git_odb_backend_loose(&self->super.odb_backend, path, compression_level,
                                    do_fsync, dir_mode, file_mode);
    Py_DECREF(tvalue);
    if (err) {
        Error_set(err);
        return -1;
    }

    return 0;
}

PyTypeObject OdbBackendLooseType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.OdbBackendLoose",                 /* tp_name           */
    sizeof(OdbBackendLoose),                   /* tp_basicsize      */
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
    OdbBackendLoose__doc__,                    /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    0,                                         /* tp_getset         */
    &OdbBackendType,                           /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)OdbBackendLoose_init,            /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};
