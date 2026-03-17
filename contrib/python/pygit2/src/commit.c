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
#include "utils.h"
#include "signature.h"
#include "object.h"
#include "oid.h"

extern PyTypeObject TreeType;
extern PyObject *GitError;


PyDoc_STRVAR(Commit_message_encoding__doc__, "Message encoding.");

PyObject *
Commit_message_encoding__get__(Commit *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const char *encoding = git_commit_message_encoding(self->commit);
    if (encoding == NULL)
        Py_RETURN_NONE;

    return to_encoding(encoding);
}


PyDoc_STRVAR(Commit_message__doc__, "The commit message, a text string.");

PyObject *
Commit_message__get__(Commit *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const char *message = git_commit_message(self->commit);
    const char *encoding = git_commit_message_encoding(self->commit);
    return to_unicode(message, encoding, NULL);
}

PyDoc_STRVAR(Commit_gpg_signature__doc__, "A tuple with the GPG signature and the signed payload.");

PyObject *
Commit_gpg_signature__get__(Commit *self)
{
    git_buf gpg_signature = { NULL }, signed_data = { NULL };
    PyObject *py_gpg_signature, *py_signed_data;

    const git_oid *oid = Object__id((Object*)self);
    int err = git_commit_extract_signature(
        &gpg_signature, &signed_data, self->repo->repo, (git_oid*) oid, NULL
    );

    if (err != GIT_OK){
        git_buf_dispose(&gpg_signature);
        git_buf_dispose(&signed_data);

        if (err == GIT_ENOTFOUND){
            return Py_BuildValue("OO", Py_None, Py_None);
        }

        return Error_set(err);
    }

    py_gpg_signature = PyBytes_FromString(gpg_signature.ptr);
    py_signed_data = PyBytes_FromString(signed_data.ptr);
    git_buf_dispose(&gpg_signature);
    git_buf_dispose(&signed_data);

    return Py_BuildValue("NN", py_gpg_signature, py_signed_data);
}


PyDoc_STRVAR(Commit_message_trailers__doc__,
    "Returns commit message trailers (e.g., Bug: 1234) as a dictionary."
);

PyObject *
Commit_message_trailers__get__(Commit *self)
{
    git_message_trailer_array gmt_arr;
    int i, trailer_count, err;
    PyObject *dict;
    PyObject *py_val;
    const char *message = git_commit_message(self->commit);
    const char *encoding = git_commit_message_encoding(self->commit);

    err = git_message_trailers(&gmt_arr, message);
    if (err < 0)
        return Error_set(err);

    dict = PyDict_New();
    if (dict == NULL)
        goto error;

    trailer_count = gmt_arr.count;
    for (i=0; i < trailer_count; i++) {
        py_val = to_unicode(gmt_arr.trailers[i].value, encoding, NULL);
        err = PyDict_SetItemString(dict, gmt_arr.trailers[i].key, py_val);
        Py_DECREF(py_val);
        if (err < 0)
            goto error;

    }

    git_message_trailer_array_free(&gmt_arr);
    return dict;

error:
    git_message_trailer_array_free(&gmt_arr);
    Py_CLEAR(dict);
    return NULL;
}

PyDoc_STRVAR(Commit_raw_message__doc__, "Message (bytes).");

PyObject *
Commit_raw_message__get__(Commit *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load
    return PyBytes_FromString(git_commit_message(self->commit));
}


PyDoc_STRVAR(Commit_commit_time__doc__, "Commit time.");

PyObject *
Commit_commit_time__get__(Commit *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load
    return PyLong_FromLongLong(git_commit_time(self->commit));
}


PyDoc_STRVAR(Commit_commit_time_offset__doc__, "Commit time offset.");

PyObject *
Commit_commit_time_offset__get__(Commit *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load
    return PyLong_FromLong(git_commit_time_offset(self->commit));
}


PyDoc_STRVAR(Commit_committer__doc__, "The committer of the commit.");

PyObject *
Commit_committer__get__(Commit *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const git_signature *signature = git_commit_committer(self->commit);
    const char *encoding = git_commit_message_encoding(self->commit);

    return build_signature((Object*)self, signature, encoding);
}


PyDoc_STRVAR(Commit_author__doc__, "The author of the commit.");

PyObject *
Commit_author__get__(Commit *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    const git_signature *signature = git_commit_author(self->commit);
    const char *encoding = git_commit_message_encoding(self->commit);

    return build_signature((Object*)self, signature, encoding);
}

PyDoc_STRVAR(Commit_tree__doc__, "The tree object attached to the commit.");

PyObject *
Commit_tree__get__(Commit *self)
{
    git_tree *tree;

    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    int err = git_commit_tree(&tree, self->commit);
    if (err == GIT_ENOTFOUND) {
        char tree_id[GIT_OID_HEXSZ + 1] = { 0 };
        git_oid_fmt(tree_id, git_commit_tree_id(self->commit));
        return PyErr_Format(GitError, "Unable to read tree %s", tree_id);
    }

    if (err < 0)
        return Error_set(err);

    return wrap_object((git_object*)tree, self->repo, NULL);
}

PyDoc_STRVAR(Commit_tree_id__doc__, "The id of the tree attached to the commit.");

PyObject *
Commit_tree_id__get__(Commit *self)
{
    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load
    return git_oid_to_python(git_commit_tree_id(self->commit));
}

PyDoc_STRVAR(Commit_parents__doc__, "The list of parent commits.");

PyObject *
Commit_parents__get__(Commit *self)
{
    Repository *py_repo;
    unsigned int i, parent_count;
    const git_oid *parent_oid;
    git_commit *parent;
    int err;
    PyObject *py_parent;
    PyObject *list;

    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    parent_count = git_commit_parentcount(self->commit);
    list = PyList_New(parent_count);
    if (!list)
        return NULL;

    py_repo = self->repo;
    for (i=0; i < parent_count; i++) {
        parent_oid = git_commit_parent_id(self->commit, i);
        if (parent_oid == NULL) {
            Py_DECREF(list);
            Error_set(GIT_ENOTFOUND);
            return NULL;
        }

        err = git_commit_lookup(&parent, py_repo->repo, parent_oid);
        if (err < 0) {
            Py_DECREF(list);
            return Error_set_oid(err, parent_oid, GIT_OID_HEXSZ);
        }

        py_parent = wrap_object((git_object*)parent, py_repo, NULL);
        if (py_parent == NULL) {
            Py_DECREF(list);
            return NULL;
        }

        PyList_SET_ITEM(list, i, py_parent);
    }

    return list;
}

PyDoc_STRVAR(Commit_parent_ids__doc__, "The list of parent commits' ids.");

PyObject *
Commit_parent_ids__get__(Commit *self)
{
    unsigned int i, parent_count;
    const git_oid *id;
    PyObject *list;

    if (Object__load((Object*)self) == NULL) { return NULL; } // Lazy load

    parent_count = git_commit_parentcount(self->commit);
    list = PyList_New(parent_count);
    if (!list)
        return NULL;

    for (i=0; i < parent_count; i++) {
        id = git_commit_parent_id(self->commit, i);
        PyList_SET_ITEM(list, i, git_oid_to_python(id));
    }

    return list;
}

PyGetSetDef Commit_getseters[] = {
    GETTER(Commit, message_encoding),
    GETTER(Commit, message),
    GETTER(Commit, raw_message),
    GETTER(Commit, commit_time),
    GETTER(Commit, commit_time_offset),
    GETTER(Commit, committer),
    GETTER(Commit, author),
    GETTER(Commit, gpg_signature),
    GETTER(Commit, tree),
    GETTER(Commit, tree_id),
    GETTER(Commit, parents),
    GETTER(Commit, parent_ids),
    GETTER(Commit, message_trailers),
    {NULL}
};


PyDoc_STRVAR(Commit__doc__, "Commit objects.");

PyTypeObject CommitType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Commit",                          /* tp_name           */
    sizeof(Commit),                            /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    0,                                         /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    (reprfunc)Object_repr,                     /* tp_repr           */
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
    Commit__doc__,                             /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    Commit_getseters,                          /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};
