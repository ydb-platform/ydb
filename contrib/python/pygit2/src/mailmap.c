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
#include "mailmap.h"
#include "signature.h"

extern PyTypeObject SignatureType;
extern PyTypeObject RepositoryType;

int
Mailmap_init(Mailmap *self, PyObject *args, PyObject *kwargs)
{
    char *keywords[] = {NULL};
    git_mailmap *mm;
    int error;

    /* Our init method does not handle parameters */
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "", keywords))
        return -1;

    error = git_mailmap_new(&mm);
    if (error < 0) {
        Error_set(error);
        return -1;
    }

    self->mailmap = mm;
    return 0;
}

PyDoc_STRVAR(Mailmap_from_repository__doc__,
    "from_repository(repository: Repository) -> Mailmap\n"
    "\n"
    "Create a new mailmap instance from a repository, loading mailmap files based on the repository's configuration.\n"
    "\n"
    "Mailmaps are loaded in the following order:\n"
    " 1. '.mailmap' in the root of the repository's working directory, if present.\n"
    " 2. The blob object identified by the 'mailmap.blob' config entry, if set.\n"
    "    [NOTE: 'mailmap.blob' defaults to 'HEAD:.mailmap' in bare repositories]\n"
    " 3. The path in the 'mailmap.file' config entry, if set.");
PyObject *
Mailmap_from_repository(Mailmap *dummy, PyObject *args)
{
    Repository *repo = NULL;
    git_mailmap *mm = NULL;
    int error;

    if (!PyArg_ParseTuple(args, "O!", &RepositoryType, &repo))
        return NULL;

    error = git_mailmap_from_repository(&mm, repo->repo);
    if (error < 0)
        return Error_set(error);

    return wrap_mailmap(mm);
}

PyDoc_STRVAR(Mailmap_from_buffer__doc__,
    "from_buffer(buffer: str) -> Mailmap\n"
    "\n"
    "Parse a passed-in buffer and construct a mailmap object.");
PyObject *
Mailmap_from_buffer(Mailmap *dummy, PyObject *args)
{
    char *buffer = NULL;
    Py_ssize_t size = 0;
    git_mailmap *mm = NULL;
    int error;

    if (!PyArg_ParseTuple(args, "s#", &buffer, &size))
        return NULL;

    error = git_mailmap_from_buffer(&mm, buffer, size);
    if (error < 0)
        return Error_set(error);

    return wrap_mailmap(mm);
}

PyDoc_STRVAR(Mailmap_add_entry__doc__,
    "add_entry(real_name: str = None, real_email: str = None, replace_name: str = None, replace_email: str)\n"
    "\n"
    "Add a new entry to the mailmap, overriding existing entries.");
PyObject *
Mailmap_add_entry(Mailmap *self, PyObject *args, PyObject *kwargs)
{
    char *keywords[] = {"real_name", "real_email", "replace_name", "replace_email", NULL};
    char *real_name = NULL, *real_email = NULL;
    char *replace_name = NULL, *replace_email = NULL;
    int error;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|zzzs", keywords,
                                     &real_name, &real_email,
                                     &replace_name, &replace_email))
        return NULL;

    /* replace_email cannot be null */
    if (!replace_email) {
        PyErr_BadArgument();
        return NULL;
    }

    error = git_mailmap_add_entry(self->mailmap, real_name, real_email,
                                  replace_name, replace_email);
    if (error < 0)
        return Error_set(error);

    Py_RETURN_NONE;
}

PyDoc_STRVAR(Mailmap_resolve__doc__,
    "resolve(name: str, email: str) -> tuple[str, str]\n"
    "\n"
    "Resolve name & email to a real name and email.");
PyObject *
Mailmap_resolve(Mailmap *self, PyObject *args)
{
    const char *name = NULL, *email = NULL;
    const char *real_name = NULL, *real_email = NULL;
    int error;

    if (!PyArg_ParseTuple(args, "ss", &name, &email))
        return NULL;

    error = git_mailmap_resolve(&real_name, &real_email, self->mailmap, name, email);
    if (error < 0)
        return Error_set(error);

    return Py_BuildValue("ss", real_name, real_email);
}

PyDoc_STRVAR(Mailmap_resolve_signature__doc__,
    "resolve_signature(sig: Signature) -> Signature\n"
    "\n"
    "Resolve signature to real name and email.");
PyObject *
Mailmap_resolve_signature(Mailmap *self, PyObject *args)
{
    Signature *sig = NULL;
    git_signature *resolved = NULL;
    int error;

    if (!PyArg_ParseTuple(args, "O!", &SignatureType, &sig))
        return NULL;

    error = git_mailmap_resolve_signature(&resolved, self->mailmap, sig->signature);
    if (error < 0)
        return Error_set(error);

    return build_signature(sig->obj, resolved, sig->encoding);
}

static void
Mailmap_dealloc(Mailmap *self)
{
    git_mailmap_free(self->mailmap);
    PyObject_Del(self);
}


PyMethodDef Mailmap_methods[] = {
    METHOD(Mailmap, add_entry, METH_VARARGS | METH_KEYWORDS),
    METHOD(Mailmap, resolve, METH_VARARGS),
    METHOD(Mailmap, resolve_signature, METH_VARARGS),
    METHOD(Mailmap, from_repository, METH_VARARGS | METH_STATIC),
    METHOD(Mailmap, from_buffer, METH_VARARGS | METH_STATIC),
    {NULL}
};


PyDoc_STRVAR(Mailmap__doc__, "Mailmap object.");

PyTypeObject MailmapType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Mailmap",                         /* tp_name           */
    sizeof(Mailmap),                           /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Mailmap_dealloc,               /* tp_dealloc        */
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
    Mailmap__doc__,                            /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    Mailmap_methods,                           /* tp_methods        */
    0,                                         /* tp_members        */
    0,                                         /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)Mailmap_init,                    /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyObject *
wrap_mailmap(git_mailmap* mm)
{
    Mailmap* py_mm = NULL;

    py_mm = PyObject_New(Mailmap, &MailmapType);
    if (py_mm == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    py_mm->mailmap = mm;

    return (PyObject*) py_mm;
}
