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
#include "oid.h"
#include "note.h"

extern PyTypeObject SignatureType;

PyDoc_STRVAR(Note_remove__doc__,
    "remove(author: Signature, committer: Signature, ref: str = \"refs/notes/commits\")\n"
    "\n"
    "Removes a note for an annotated object");

PyObject *
Note_remove(Note *self, PyObject* args)
{
    char *ref = "refs/notes/commits";
    int err = GIT_ERROR;
    Signature *py_author, *py_committer;
    Oid *id;

    if (!PyArg_ParseTuple(args, "O!O!|s",
                          &SignatureType, &py_author,
                          &SignatureType, &py_committer,
                          &ref))
        return NULL;

    id = (Oid *) self->annotated_id;
    err = git_note_remove(self->repo->repo, ref, py_author->signature,
        py_committer->signature, &id->oid);
    if (err < 0)
        return Error_set(err);

    Py_RETURN_NONE;
}


PyDoc_STRVAR(Note_message__doc__,
  "Gets message of the note\n");

PyObject *
Note_message__get__(Note *self)
{
    int err;

    // Lazy load
    if (self->note == NULL) {
        err = git_note_read(&self->note,
                            self->repo->repo,
                            self->ref,
                            &((Oid *)self->annotated_id)->oid);
        if (err < 0)
            return Error_set(err);
    }

    return to_unicode(git_note_message(self->note), NULL, NULL);
}


static void
Note_dealloc(Note *self)
{
    Py_CLEAR(self->repo);
    Py_CLEAR(self->annotated_id);
    Py_CLEAR(self->id);
    if (self->note != NULL)
        git_note_free(self->note);
    PyObject_Del(self);
}


PyMethodDef Note_methods[] = {
    METHOD(Note, remove, METH_VARARGS),
    {NULL}
};

PyMemberDef Note_members[] = {
    MEMBER(Note, id, T_OBJECT, "id of the note object."),
    MEMBER(Note, annotated_id, T_OBJECT, "id of the annotated object."),
    {NULL}
};

PyGetSetDef Note_getseters[] = {
    GETTER(Note, message),
    {NULL}
};

PyDoc_STRVAR(Note__doc__, "Note object.");

PyTypeObject NoteType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Note",                            /* tp_name           */
    sizeof(Note),                              /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Note_dealloc,                  /* tp_dealloc        */
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
    Note__doc__,                               /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    Note_methods,                              /* tp_methods        */
    Note_members,                              /* tp_members        */
    Note_getseters,                            /* tp_getset         */
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
NoteIter_iternext(NoteIter *self)
{
    int err;
    git_oid note_id, annotated_id;

    err = git_note_next(&note_id, &annotated_id, self->iter);
    if (err < 0)
        return Error_set(err);

    return (PyObject*) wrap_note(self->repo, &note_id, &annotated_id, self->ref);
}

void
NoteIter_dealloc(NoteIter *self)
{
    Py_CLEAR(self->repo);
    git_note_iterator_free(self->iter);
    PyObject_Del(self);
}


PyDoc_STRVAR(NoteIter__doc__, "Note iterator object.");

PyTypeObject NoteIterType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.NoteIter",                        /* tp_name           */
    sizeof(NoteIter),                          /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)NoteIter_dealloc,              /* tp_dealloc        */
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
    NoteIter__doc__,                           /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    PyObject_SelfIter,                         /* tp_iter           */
    (iternextfunc) NoteIter_iternext,          /* tp_iternext       */
};


PyObject *
wrap_note(Repository* repo, git_oid* note_id, git_oid* annotated_id, const char* ref)
{
    Note* py_note = NULL;
    int err = GIT_ERROR;

    py_note = PyObject_New(Note, &NoteType);
    if (py_note == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    Py_INCREF(repo);
    py_note->repo = repo;
    py_note->ref = ref;
    py_note->annotated_id = git_oid_to_python(annotated_id);
    py_note->id = NULL;
    py_note->note = NULL;

    /* If the note has been provided, defer the git_note_read() call */
    if (note_id != NULL) {
        py_note->id = git_oid_to_python(note_id);
    } else {
        err = git_note_read(&py_note->note, repo->repo, ref, annotated_id);
        if (err < 0) {
            Py_DECREF(py_note);
            return Error_set(err);
        }
        py_note->id = git_oid_to_python(git_note_id(py_note->note));
    }

    return (PyObject*) py_note;
}


