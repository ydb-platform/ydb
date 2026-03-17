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
#include "types.h"
#include "oid.h"
#include "reference.h"
#include "signature.h"
#include "utils.h"
#include "wildmatch.h"
#include <git2/refdb.h>
#include <git2/sys/refdb_backend.h>

extern PyTypeObject ReferenceType;
extern PyTypeObject RepositoryType;
extern PyTypeObject SignatureType;

struct pygit2_refdb_backend
{
    git_refdb_backend backend;
    PyObject *RefdbBackend;
    PyObject *exists,
             *lookup,
             *iterator,
             *write,
             *rename,
             *delete,
             *compress,
             *has_log,
             *ensure_log,
             *reflog_read,
             *reflog_write,
             *reflog_rename,
             *reflog_delete,
             *lock,
             *unlock;
};

struct pygit2_refdb_iterator {
    struct git_reference_iterator base;
    PyObject *iterator;
    char *glob;
};

static Reference *
iterator_get_next(struct pygit2_refdb_iterator *iter)
{
    Reference *ref;
    while ((ref = (Reference *)PyIter_Next(iter->iterator)) != NULL) {
        if (!iter->glob) {
            return ref;
        }
        const char *name = git_reference_name(ref->reference);
        if (wildmatch(iter->glob, name, 0) != WM_NOMATCH) {
            return ref;
        }
    }
    return NULL;
}

static int
pygit2_refdb_iterator_next(git_reference **out, git_reference_iterator *_iter)
{
    struct pygit2_refdb_iterator *iter = (struct pygit2_refdb_iterator *)_iter;
    Reference *ref = iterator_get_next(iter);
    if (ref == NULL) {
        *out = NULL;
        return GIT_ITEROVER;
    }
    if (!PyObject_IsInstance((PyObject *)ref, (PyObject *)&ReferenceType)) {
        PyErr_SetString(PyExc_TypeError,
                        "RefdbBackend iterator must yield References");
        return GIT_EUSER;
    }
    *out = ref->reference;
    return 0;
}

static int
pygit2_refdb_iterator_next_name(const char **ref_name, git_reference_iterator *_iter)
{
    struct pygit2_refdb_iterator *iter = (struct pygit2_refdb_iterator *)_iter;
    Reference *ref = iterator_get_next(iter);
    if (ref == NULL) {
        *ref_name = NULL;
        return GIT_ITEROVER;
    }
    if (!PyObject_IsInstance((PyObject *)ref, (PyObject *)&ReferenceType)) {
        PyErr_SetString(PyExc_TypeError,
                        "RefdbBackend iterator must yield References");
        return GIT_EUSER;
    }
    *ref_name = git_reference_name(ref->reference);
    return 0;
}

static void
pygit2_refdb_iterator_free(git_reference_iterator *_iter)
{
    struct pygit2_refdb_iterator *iter = (struct pygit2_refdb_iterator *)_iter;
    Py_DECREF(iter->iterator);
    free(iter->glob);
}

static int
pygit2_refdb_backend_iterator(git_reference_iterator **iter,
    struct git_refdb_backend *_be,
    const char *glob)
{
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;
    PyObject *iterator = PyObject_GetIter((PyObject *)be->RefdbBackend);
    assert(iterator);

    struct pygit2_refdb_iterator *pyiter =
        calloc(1, sizeof(struct pygit2_refdb_iterator));
    *iter = (git_reference_iterator *)pyiter;
    pyiter->iterator = iterator;
    pyiter->base.next = pygit2_refdb_iterator_next;
    pyiter->base.next_name = pygit2_refdb_iterator_next_name;
    pyiter->base.free = pygit2_refdb_iterator_free;
    pyiter->glob = strdup(glob);
    return 0;
}

static int
pygit2_refdb_backend_exists(int *exists,
        git_refdb_backend *_be, const char *ref_name)
{
    int err;
    PyObject *args, *result;
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;

    if ((args = Py_BuildValue("(s)", ref_name)) == NULL)
        return GIT_EUSER;
    result = PyObject_CallObject(be->exists, args);
    Py_DECREF(args);

    if ((err = git_error_for_exc()) != 0)
        goto out;

    *exists = PyObject_IsTrue(result);

out:
    Py_DECREF(result);
    return 0;
}

static int
pygit2_refdb_backend_lookup(git_reference **out,
        git_refdb_backend *_be, const char *ref_name)
{
    int err;
    PyObject *args;
    Reference *result;
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;

    if ((args = Py_BuildValue("(s)", ref_name)) == NULL)
        return GIT_EUSER;
    result = (Reference *)PyObject_CallObject(be->lookup, args);
    Py_DECREF(args);

    if ((err = git_error_for_exc()) != 0)
        goto out;

    if (!PyObject_IsInstance((PyObject *)result, (PyObject *)&ReferenceType)) {
        PyErr_SetString(PyExc_TypeError, "Expected object of type pygit2.Reference");
        err = GIT_EUSER;
        goto out;
    }

    *out = result->reference;
out:
    return err;
}

static int
pygit2_refdb_backend_write(git_refdb_backend *_be,
        const git_reference *_ref, int force,
        const git_signature *_who, const char *message,
        const git_oid *_old, const char *old_target)
{
    int err;
    PyObject *args = NULL, *ref = NULL, *who = NULL, *old = NULL;
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;

    // XXX: Drops const
    if ((ref = wrap_reference((git_reference *)_ref, NULL)) == NULL)
        goto euser;
    if ((who = build_signature(NULL, _who, "utf-8")) == NULL)
        goto euser;
    if ((old = git_oid_to_python(_old)) == NULL)
        goto euser;
    if ((args = Py_BuildValue("(NNNsNs)", ref,
            force ? Py_True : Py_False,
            who, message, old, old_target)) == NULL)
        goto euser;

    PyObject_CallObject(be->write, args);
    err = git_error_for_exc();
out:
    Py_DECREF(ref);
    Py_DECREF(who);
    Py_DECREF(old);
    Py_DECREF(args);
    return err;
euser:
    err = GIT_EUSER;
    goto out;
}

static int
pygit2_refdb_backend_rename(git_reference **out, git_refdb_backend *_be,
        const char *old_name, const char *new_name, int force,
        const git_signature *_who, const char *message)
{
    int err;
    PyObject *args, *who;
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;

    if ((who = build_signature(NULL, _who, "utf-8")) != NULL)
        return GIT_EUSER;
    if ((args = Py_BuildValue("(ssNNs)", old_name, new_name,
            force ? Py_True : Py_False, who, message)) == NULL) {
        Py_DECREF(who);
        return GIT_EUSER;
    }
    Reference *ref = (Reference *)PyObject_CallObject(be->rename, args);
    Py_DECREF(who);
    Py_DECREF(args);

    if ((err = git_error_for_exc()) != 0)
        return err;

    if (!PyObject_IsInstance((PyObject *)ref, (PyObject *)&ReferenceType)) {
        PyErr_SetString(PyExc_TypeError, "Expected object of type pygit2.Reference");
        return GIT_EUSER;
    }

    git_reference_dup(out, ref->reference);
    Py_DECREF(ref);
    return 0;
}

static int
pygit2_refdb_backend_del(git_refdb_backend *_be,
        const char *ref_name, const git_oid *_old, const char *old_target)
{
    PyObject *args, *old;
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;
    old = git_oid_to_python(_old);

    if ((args = Py_BuildValue("(sOs)", ref_name, old, old_target)) == NULL) {
        Py_DECREF(old);
        return GIT_EUSER;
    }
    PyObject_CallObject(be->rename, args);
    Py_DECREF(old);
    Py_DECREF(args);
    return git_error_for_exc();
}

static int
pygit2_refdb_backend_compress(git_refdb_backend *_be)
{
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;
    PyObject_CallObject(be->rename, NULL);
    return git_error_for_exc();
}

static int
pygit2_refdb_backend_has_log(git_refdb_backend *_be, const char *refname)
{
    int err;
    PyObject *args, *result;
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;

    if ((args = Py_BuildValue("(s)", refname)) == NULL) {
        return GIT_EUSER;
    }
    result = PyObject_CallObject(be->has_log, args);
    Py_DECREF(args);

    if ((err = git_error_for_exc()) != 0) {
        return err;
    }

    if (PyObject_IsTrue(result)) {
        Py_DECREF(result);
        return 1;
    }

    Py_DECREF(result);
    return 0;
}

static int
pygit2_refdb_backend_ensure_log(git_refdb_backend *_be, const char *refname)
{
    int err;
    PyObject *args, *result;
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;

    if ((args = Py_BuildValue("(s)", refname)) == NULL) {
        return GIT_EUSER;
    }
    result = PyObject_CallObject(be->ensure_log, args);
    Py_DECREF(args);

    if ((err = git_error_for_exc()) != 0) {
        return err;
    }

    if (PyObject_IsTrue(result)) {
        Py_DECREF(result);
        return 1;
    }

    Py_DECREF(result);
    return 0;
}

static int
pygit2_refdb_backend_reflog_read(git_reflog **out,
        git_refdb_backend *backend, const char *name)
{
    /* TODO: Implement first-class pygit2 reflog support
     * These stubs are here because libgit2 requires refdb_backend to implement
     * them. libgit2 doesn't actually use them as of 0.99; it assumes the refdb
     * backend will update the reflogs itself. */
    return GIT_EUSER;
}

static int
pygit2_refdb_backend_reflog_write(git_refdb_backend *backend, git_reflog *reflog)
{
    /* TODO: Implement first-class pygit2 reflog support */
    return GIT_EUSER;
}

static int
pygit2_refdb_backend_reflog_rename(git_refdb_backend *_backend,
        const char *old_name, const char *new_name)
{
    /* TODO: Implement first-class pygit2 reflog support */
    return GIT_EUSER;
}

static int
pygit2_refdb_backend_reflog_delete(git_refdb_backend *backend, const char *name)
{
    /* TODO: Implement first-class pygit2 reflog support */
    return GIT_EUSER;
}

static void
pygit2_refdb_backend_free(git_refdb_backend *_be)
{
    struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)_be;
    Py_DECREF(be->RefdbBackend);
}

int
RefdbBackend_init(RefdbBackend *self, PyObject *args, PyObject *kwds)
{
    if (args && PyTuple_Size(args) > 0) {
        PyErr_SetString(PyExc_TypeError,
                        "RefdbBackend takes no arguments");
        return -1;
    }

    if (kwds && PyDict_Size(kwds) > 0) {
        PyErr_SetString(PyExc_TypeError,
                        "RefdbBackend takes no keyword arguments");
        return -1;
    }

    struct pygit2_refdb_backend *be = calloc(1, sizeof(struct pygit2_refdb_backend));
    git_refdb_init_backend(&be->backend, GIT_REFDB_BACKEND_VERSION);
    be->RefdbBackend = (PyObject *)self;

    if (PyIter_Check((PyObject *)self)) {
        be->backend.iterator = pygit2_refdb_backend_iterator;
    }

    if (PyObject_HasAttrString((PyObject *)self, "exists")) {
        be->exists = PyObject_GetAttrString((PyObject *)self, "exists");
        be->backend.exists = pygit2_refdb_backend_exists;
    }

    if (PyObject_HasAttrString((PyObject *)self, "lookup")) {
        be->lookup = PyObject_GetAttrString((PyObject *)self, "lookup");
        be->backend.lookup = pygit2_refdb_backend_lookup;
    }

    if (PyObject_HasAttrString((PyObject *)self, "write")) {
        be->write = PyObject_GetAttrString((PyObject *)self, "write");
        be->backend.write = pygit2_refdb_backend_write;
    }

    if (PyObject_HasAttrString((PyObject *)self, "rename")) {
        be->rename = PyObject_GetAttrString((PyObject *)self, "rename");
        be->backend.rename = pygit2_refdb_backend_rename;
    }

    if (PyObject_HasAttrString((PyObject *)self, "delete")) {
        be->delete = PyObject_GetAttrString((PyObject *)self, "delete");
        be->backend.del = pygit2_refdb_backend_del;
    }

    if (PyObject_HasAttrString((PyObject *)self, "compress")) {
        be->compress = PyObject_GetAttrString((PyObject *)self, "compress");
        be->backend.compress = pygit2_refdb_backend_compress;
    }

    if (PyObject_HasAttrString((PyObject *)self, "has_log")) {
        be->has_log = PyObject_GetAttrString((PyObject *)self, "has_log");
        be->backend.has_log = pygit2_refdb_backend_has_log;
    }

    if (PyObject_HasAttrString((PyObject *)self, "ensure_log")) {
        be->ensure_log = PyObject_GetAttrString((PyObject *)self, "ensure_log");
        be->backend.ensure_log = pygit2_refdb_backend_ensure_log;
    }

    /* TODO: First-class reflog support */
    be->backend.reflog_read = pygit2_refdb_backend_reflog_read;
    be->backend.reflog_write = pygit2_refdb_backend_reflog_write;
    be->backend.reflog_rename = pygit2_refdb_backend_reflog_rename;
    be->backend.reflog_delete = pygit2_refdb_backend_reflog_delete;

    /* TODO: transactions
    if (PyObject_HasAttrString((PyObject *)self, "lock")) {
        be->lock = PyObject_GetAttrString((PyObject *)self, "lock");
        be->backend.lock = pygit2_refdb_backend_lock;
    }

    if (PyObject_HasAttrString((PyObject *)self, "unlock")) {
        be->unlock = PyObject_GetAttrString((PyObject *)self, "unlock");
        be->backend.unlock = pygit2_refdb_backend_unlock;
    }
    */

    Py_INCREF((PyObject *)self);
    be->backend.free = pygit2_refdb_backend_free;

    self->refdb_backend = (git_refdb_backend *)be;
    return 0;
}

void
RefdbBackend_dealloc(RefdbBackend *self)
{
    if (self->refdb_backend && self->refdb_backend->free == pygit2_refdb_backend_free) {
        struct pygit2_refdb_backend *be = (struct pygit2_refdb_backend *)self->refdb_backend;
        Py_CLEAR(be->exists);
        Py_CLEAR(be->lookup);
        Py_CLEAR(be->iterator);
        Py_CLEAR(be->write);
        Py_CLEAR(be->rename);
        Py_CLEAR(be->delete);
        Py_CLEAR(be->compress);
        Py_CLEAR(be->has_log);
        Py_CLEAR(be->ensure_log);
        Py_CLEAR(be->reflog_read);
        Py_CLEAR(be->reflog_write);
        Py_CLEAR(be->reflog_rename);
        Py_CLEAR(be->reflog_delete);
        Py_CLEAR(be->lock);
        Py_CLEAR(be->unlock);
        free(be);
    }
    Py_TYPE(self)->tp_free((PyObject *) self);
}

PyDoc_STRVAR(RefdbBackend_exists__doc__,
    "exists(refname: str) -> bool\n"
    "\n"
    "Returns True if a ref by this name exists, or False otherwise.");

PyObject *
RefdbBackend_exists(RefdbBackend *self, PyObject *py_str)
{
    int err, exists;
    const char *ref_name;
    if (self->refdb_backend->exists == NULL) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    if (!PyUnicode_Check(py_str)) {
        PyErr_SetString(PyExc_TypeError,
                "RefdbBackend.exists takes a string argument");
        return NULL;
    }
    ref_name = PyUnicode_AsUTF8(py_str);

    err = self->refdb_backend->exists(&exists, self->refdb_backend, ref_name);
    if (err != 0)
        return Error_set(err);

    if (exists)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}

PyDoc_STRVAR(RefdbBackend_lookup__doc__,
    "lookup(refname: str) -> Reference\n"
    "\n"
    "Looks up a reference and returns it, or None if not found.");

PyObject *
RefdbBackend_lookup(RefdbBackend *self, PyObject *py_str)
{
    int err;
    git_reference *ref;
    const char *ref_name;
    if (self->refdb_backend->lookup == NULL) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    if (!PyUnicode_Check(py_str)) {
        PyErr_SetString(PyExc_TypeError,
                "RefdbBackend.lookup takes a string argument");
        return NULL;
    }
    ref_name = PyUnicode_AsUTF8(py_str);

    err = self->refdb_backend->lookup(&ref, self->refdb_backend, ref_name);

    if (err == GIT_ENOTFOUND) {
        Py_RETURN_NONE;
    } else if (err != 0) {
        return Error_set(err);
    }

    return wrap_reference(ref, NULL);
}

PyDoc_STRVAR(RefdbBackend_write__doc__,
    "write(ref: Reference, force: bool, who: Signature, message: str, old: Oid, old_target: str)\n"
    "\n"
    "Writes a new reference to the reference database.");
// TODO: Better docs? libgit2 is scant on documentation for this, too.

PyObject *
RefdbBackend_write(RefdbBackend *self, PyObject *args)
{
    int err;
    Reference *ref;
    int force;
    Signature *who;
    const git_signature *sig = NULL;
    char *message, *old_target;
    PyObject *py_old;
    git_oid _old, *old = NULL;
    if (self->refdb_backend->write == NULL) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    if (!PyArg_ParseTuple(args, "O!pOzOz", &ReferenceType, &ref,
                &force, &who, &message, &py_old, &old_target))
        return NULL;

    if ((PyObject *)py_old != Py_None) {
        py_oid_to_git_oid(py_old, &_old);
        old = &_old;
    }

    if ((PyObject *)who != Py_None) {
        if (!PyObject_IsInstance((PyObject *)who, (PyObject *)&SignatureType)) {
            PyErr_SetString(PyExc_TypeError,
                            "Signature must be type pygit2.Signature");
            return NULL;
        }
        sig = who->signature;
    }

    err = self->refdb_backend->write(self->refdb_backend,
            ref->reference, force, sig, message, old, old_target);
    if (err != 0)
        return Error_set(err);

    Py_RETURN_NONE;
}

PyDoc_STRVAR(RefdbBackend_rename__doc__,
    "rename(old_name: str, new_name: str, force: bool, who: Signature, message: str) -> Reference\n"
    "\n"
    "Renames a reference.");

PyObject *
RefdbBackend_rename(RefdbBackend *self, PyObject *args)
{
    int err;
    int force;
    Signature *who;
    char *old_name, *new_name, *message;
    git_reference *out;

    if (self->refdb_backend->rename == NULL) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    if (!PyArg_ParseTuple(args, "sspO!s", &old_name, &new_name,
                &force, &SignatureType, &who, &message))
        return NULL;

    err = self->refdb_backend->rename(&out, self->refdb_backend,
            old_name, new_name, force, who->signature, message);
    if (err != 0)
        return Error_set(err);

    return (PyObject *)wrap_reference(out, NULL);
}

PyDoc_STRVAR(RefdbBackend_delete__doc__,
    "delete(ref_name: str, old_id: Oid, old_target: str)\n"
    "\n"
    "Deletes a reference.");

PyObject *
RefdbBackend_delete(RefdbBackend *self, PyObject *args)
{
    int err;
    PyObject *py_old_id;
    git_oid old_id;
    char *ref_name, *old_target;

    if (self->refdb_backend->del == NULL) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    if (!PyArg_ParseTuple(args, "sOz", &ref_name, &py_old_id, &old_target))
        return NULL;

    if (py_old_id != Py_None) {
        py_oid_to_git_oid(py_old_id, &old_id);
        err = self->refdb_backend->del(self->refdb_backend,
                ref_name, &old_id, old_target);
    } else {
        err = self->refdb_backend->del(self->refdb_backend,
                ref_name, NULL, old_target);
    }

    if (err != 0) {
        return Error_set(err);
    }

    Py_RETURN_NONE;
}

PyDoc_STRVAR(RefdbBackend_compress__doc__,
    "compress()\n"
    "\n"
    "Suggests that the implementation compress or optimize its references.\n"
    "This behavior is implementation-specific.");

PyObject *
RefdbBackend_compress(RefdbBackend *self)
{
    int err;
    if (self->refdb_backend->compress == NULL) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    err = self->refdb_backend->compress(self->refdb_backend);
    if (err != 0)
        return Error_set(err);

    Py_RETURN_NONE;
}

PyDoc_STRVAR(RefdbBackend_has_log__doc__,
    "has_log(ref_name: str) -> bool\n"
    "\n"
    "Returns True if a ref log is available for this reference.\n"
    "It may be empty even if it exists.");

PyObject *
RefdbBackend_has_log(RefdbBackend *self, PyObject *_ref_name)
{
    int err;
    const char *ref_name;
    if (self->refdb_backend->has_log == NULL) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    if (!PyUnicode_Check(_ref_name)) {
        PyErr_SetString(PyExc_TypeError,
                "RefdbBackend.has_log takes a string argument");
        return NULL;
    }
    ref_name = PyUnicode_AsUTF8(_ref_name);

    err = self->refdb_backend->has_log(self->refdb_backend, ref_name);
    if (err < 0) {
        return Error_set(err);
    }

    if (err == 1) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

PyDoc_STRVAR(RefdbBackend_ensure_log__doc__,
    "ensure_log(ref_name: str) -> bool\n"
    "\n"
    "Ensure that a particular reference will have a reflog which will be\n"
    "appended to on writes.");

PyObject *
RefdbBackend_ensure_log(RefdbBackend *self, PyObject *_ref_name)
{
    int err;
    const char *ref_name;
    if (self->refdb_backend->ensure_log == NULL) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    if (!PyUnicode_Check(_ref_name)) {
        PyErr_SetString(PyExc_TypeError,
                "RefdbBackend.ensure_log takes a string argument");
        return NULL;
    }
    ref_name = PyUnicode_AsUTF8(_ref_name);

    err = self->refdb_backend->ensure_log(self->refdb_backend, ref_name);
    if (err < 0) {
        return Error_set(err);
    }

    if (err == 0) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

PyMethodDef RefdbBackend_methods[] = {
    METHOD(RefdbBackend, exists, METH_O),
    METHOD(RefdbBackend, lookup, METH_O),
    METHOD(RefdbBackend, write, METH_VARARGS),
    METHOD(RefdbBackend, rename, METH_VARARGS),
    METHOD(RefdbBackend, delete, METH_VARARGS),
    METHOD(RefdbBackend, compress, METH_NOARGS),
    METHOD(RefdbBackend, has_log, METH_O),
    METHOD(RefdbBackend, ensure_log, METH_O),
    {NULL}
};

PyDoc_STRVAR(RefdbBackend__doc__, "Reference database backend.");

PyTypeObject RefdbBackendType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.RefdbBackend",                    /* tp_name           */
    sizeof(RefdbBackend),                      /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)RefdbBackend_dealloc,          /* tp_dealloc        */
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
    RefdbBackend__doc__,                       /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0 /* TODO: Wrap git_reference_iterator */, /* tp_iter           */
    0,                                         /* tp_iternext       */
    RefdbBackend_methods,                      /* tp_methods        */
    0,                                         /* tp_members        */
    0,                                         /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)RefdbBackend_init,               /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyObject *
wrap_refdb_backend(git_refdb_backend *c_refdb_backend)
{
    RefdbBackend *pygit2_refdb_backend = PyObject_New(RefdbBackend, &RefdbBackendType);

    if (pygit2_refdb_backend)
        pygit2_refdb_backend->refdb_backend = c_refdb_backend;

    return (PyObject *)pygit2_refdb_backend;
}

PyDoc_STRVAR(RefdbFsBackend__doc__,
        "RefdbFsBackend(repo: Repository)\n"
        "\n"
        "Reference database filesystem backend. The path to the repository\n"
        "is used as the basis of the reference database.");

int
RefdbFsBackend_init(RefdbFsBackend *self, PyObject *args, PyObject *kwds)
{
    if (kwds && PyDict_Size(kwds) > 0) {
        PyErr_SetString(PyExc_TypeError, "RefdbFsBackend takes no keyword arguments");
        return -1;
    }

    Repository *repo = NULL;
    if (!PyArg_ParseTuple(args, "O!", &RepositoryType, &repo))
        return -1;

    int err = git_refdb_backend_fs(&self->super.refdb_backend, repo->repo);
    if (err) {
        Error_set(err);
        return -1;
    }

    return 0;
}

PyTypeObject RefdbFsBackendType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.RefdbFsBackend",                  /* tp_name           */
    sizeof(RefdbFsBackend),                    /* tp_basicsize      */
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
    RefdbFsBackend__doc__,                     /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    0,                                         /* tp_getset         */
    &RefdbBackendType,                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)RefdbFsBackend_init,             /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};
