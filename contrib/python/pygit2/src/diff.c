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
#include "diff.h"
#include "error.h"
#include "oid.h"
#include "patch.h"
#include "types.h"
#include "utils.h"

extern PyObject *GitError;

extern PyTypeObject TreeType;
extern PyTypeObject IndexType;
extern PyTypeObject DiffType;
extern PyTypeObject DiffDeltaType;
extern PyTypeObject DiffFileType;
extern PyTypeObject DiffHunkType;
extern PyTypeObject DiffLineType;
extern PyTypeObject DiffStatsType;
extern PyTypeObject RepositoryType;

extern PyObject *DeltaStatusEnum;
extern PyObject *DiffFlagEnum;
extern PyObject *FileModeEnum;

PyObject *
wrap_diff(git_diff *diff, Repository *repo)
{
    Diff *py_diff;

    py_diff = PyObject_New(Diff, &DiffType);
    if (py_diff) {
        Py_XINCREF(repo);
        py_diff->repo = repo;
        py_diff->diff = diff;
    }

    return (PyObject*) py_diff;
}

PyObject *
wrap_diff_file(const git_diff_file *file)
{
    DiffFile *py_file;

    if (!file)
        Py_RETURN_NONE;

    py_file = PyObject_New(DiffFile, &DiffFileType);
    if (py_file) {
        py_file->id = git_oid_to_python(&file->id);
        if (file->path) {
            py_file->path = strdup(file->path);
            py_file->raw_path = PyBytes_FromString(file->path);
        } else {
            py_file->path = NULL;
            py_file->raw_path = NULL;
        }
        py_file->size = file->size;
        py_file->flags = file->flags;
        py_file->mode = file->mode;
    }

    return (PyObject *) py_file;
}

PyObject *
wrap_diff_delta(const git_diff_delta *delta)
{
    DiffDelta *py_delta;

    if (!delta)
        Py_RETURN_NONE;

    py_delta = PyObject_New(DiffDelta, &DiffDeltaType);
    if (py_delta) {
        py_delta->status = delta->status;
        py_delta->flags = delta->flags;
        py_delta->similarity = delta->similarity;
        py_delta->nfiles = delta->nfiles;
        py_delta->old_file = wrap_diff_file(&delta->old_file);
        py_delta->new_file = wrap_diff_file(&delta->new_file);
    }

    return (PyObject *) py_delta;
}

PyObject *
wrap_diff_hunk(Patch *patch, size_t idx)
{
    DiffHunk *py_hunk;
    const git_diff_hunk *hunk;
    size_t lines_in_hunk;
    int err;

    err = git_patch_get_hunk(&hunk, &lines_in_hunk, patch->patch, idx);
    if (err < 0)
        return Error_set(err);

    py_hunk = PyObject_New(DiffHunk, &DiffHunkType);
    if (py_hunk) {
        Py_INCREF(patch);
        py_hunk->patch = patch;
        py_hunk->hunk = hunk;
        py_hunk->idx = idx;
        py_hunk->n_lines = lines_in_hunk;
    }

    return (PyObject *) py_hunk;
}

PyObject *
wrap_diff_stats(git_diff *diff)
{
    git_diff_stats *stats;
    DiffStats *py_stats;
    int err;

    err = git_diff_get_stats(&stats, diff);
    if (err < 0)
        return Error_set(err);

    py_stats = PyObject_New(DiffStats, &DiffStatsType);
    if (!py_stats) {
        git_diff_stats_free(stats);
        return NULL;
    }

    py_stats->stats = stats;

    return (PyObject *) py_stats;
}

PyObject *
wrap_diff_line(const git_diff_line *line, DiffHunk *hunk)
{
    DiffLine *py_line;

    py_line = PyObject_New(DiffLine, &DiffLineType);
    if (py_line) {
        Py_INCREF(hunk);
        py_line->hunk = hunk;
        py_line->line = line;
    }

    return (PyObject *) py_line;
}

static void
DiffFile_dealloc(DiffFile *self)
{
    Py_CLEAR(self->id);
    Py_CLEAR(self->raw_path);
    free(self->path);
    PyObject_Del(self);
}

PyDoc_STRVAR(DiffFile_from_c__doc__, "Method exposed for _checkout_notify_cb to hook into");

/* Expose wrap_diff_file to python so we can call it in callbacks.py. */
PyObject *
DiffFile_from_c(DiffFile *dummy, PyObject *py_diff_file_ptr)
{
    const git_diff_file *diff_file;
    char *buffer;
    Py_ssize_t length;

    /* Here we need to do the opposite conversion from the _pointer getters */
    if (PyBytes_AsStringAndSize(py_diff_file_ptr, &buffer, &length))
        return NULL;

    if (length != sizeof(git_diff_file *)) {
        PyErr_SetString(PyExc_TypeError, "passed value is not a pointer");
        return NULL;
    }

    /* the "buffer" contains the pointer */
    diff_file = *((const git_diff_file **) buffer);

    return wrap_diff_file(diff_file);
}

PyDoc_STRVAR(DiffFile_flags__doc__,
    "A combination of enums.DiffFlag constants."
);

PyObject *
DiffFile_flags__get__(DiffFile *self)
{
    return pygit2_enum(DiffFlagEnum, self->flags);
}

PyDoc_STRVAR(DiffFile_mode__doc__,
    "Mode of the entry (an enums.FileMode constant)."
);

PyObject *
DiffFile_mode__get__(DiffFile *self)
{
    return pygit2_enum(FileModeEnum, self->mode);
}

PyMemberDef DiffFile_members[] = {
    MEMBER(DiffFile, id, T_OBJECT, "Oid of the item."),
    MEMBER(DiffFile, path, T_STRING, "Path to the entry."),
    MEMBER(DiffFile, raw_path, T_OBJECT, "Path to the entry (bytes)."),
    MEMBER(DiffFile, size, T_LONG, "Size of the entry."),
    {NULL}
};

PyMethodDef DiffFile_methods[] = {
    METHOD(DiffFile, from_c, METH_STATIC | METH_O),
    {NULL},
};

PyGetSetDef DiffFile_getsetters[] = {
    GETTER(DiffFile, flags),
    GETTER(DiffFile, mode),
    {NULL},
};

PyDoc_STRVAR(DiffFile__doc__, "DiffFile object.");

PyTypeObject DiffFileType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.DiffFile",                        /* tp_name           */
    sizeof(DiffFile),                          /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)DiffFile_dealloc,              /* tp_dealloc        */
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
    DiffFile__doc__,                           /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    DiffFile_methods,                          /* tp_methods        */
    DiffFile_members,                          /* tp_members        */
    DiffFile_getsetters,                       /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};


PyDoc_STRVAR(DiffDelta_status_char__doc__,
  "status_char() -> str\n"
  "\n"
  "Return the single character abbreviation for a delta status code."
);

PyObject *
DiffDelta_status_char(DiffDelta *self)
{
    char status = git_diff_status_char(self->status);
    return Py_BuildValue("C", status);
}

PyDoc_STRVAR(DiffDelta_is_binary__doc__,
    "True if binary data, False if text, None if not (yet) known."
);

PyObject *
DiffDelta_is_binary__get__(DiffDelta *self)
{
    if (self->flags & GIT_DIFF_FLAG_BINARY)
        Py_RETURN_TRUE;

    if (self->flags & GIT_DIFF_FLAG_NOT_BINARY)
        Py_RETURN_FALSE;

    // This means the file has not been loaded, so we don't know whether it's
    // binary or text
    Py_RETURN_NONE;
}

PyDoc_STRVAR(DiffDelta_status__doc__,
    "An enums.DeltaStatus constant."
);

PyObject *
DiffDelta_status__get__(DiffDelta *self)
{
    return pygit2_enum(DeltaStatusEnum, self->status);
}

PyDoc_STRVAR(DiffDelta_flags__doc__,
    "A combination of enums.DiffFlag constants."
);

PyObject *
DiffDelta_flags__get__(DiffDelta *self)
{
    return pygit2_enum(DiffFlagEnum, self->flags);
}

static void
DiffDelta_dealloc(DiffDelta *self)
{
    Py_CLEAR(self->old_file);
    Py_CLEAR(self->new_file);
    PyObject_Del(self);
}

static PyMethodDef DiffDelta_methods[] = {
    METHOD(DiffDelta, status_char, METH_NOARGS),
    {NULL}
};

PyMemberDef DiffDelta_members[] = {
    MEMBER(DiffDelta, similarity, T_USHORT, "For renamed and copied."),
    MEMBER(DiffDelta, nfiles, T_USHORT, "Number of files in the delta."),
    MEMBER(DiffDelta, old_file, T_OBJECT, "\"from\" side of the diff."),
    MEMBER(DiffDelta, new_file, T_OBJECT, "\"to\" side of the diff."),
    {NULL}
};

PyGetSetDef DiffDelta_getsetters[] = {
    GETTER(DiffDelta, is_binary),
    GETTER(DiffDelta, status),
    GETTER(DiffDelta, flags),
    {NULL}
};

PyDoc_STRVAR(DiffDelta__doc__, "DiffDelta object.");

PyTypeObject DiffDeltaType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.DiffDelta",                       /* tp_name           */
    sizeof(DiffDelta),                         /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)DiffDelta_dealloc,             /* tp_dealloc        */
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
    DiffDelta__doc__,                          /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    DiffDelta_methods,                         /* tp_methods        */
    DiffDelta_members,                         /* tp_members        */
    DiffDelta_getsetters,                      /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

static void
DiffLine_dealloc(DiffLine *self)
{
    Py_CLEAR(self->hunk);
    PyObject_Del(self);
}

PyDoc_STRVAR(DiffLine_origin__doc__, "Type of the diff line");
PyObject *
DiffLine_origin__get__(DiffLine *self)
{
    return PyUnicode_FromStringAndSize(&(self->line->origin), 1);
}

PyDoc_STRVAR(DiffLine_old_lineno__doc__, "Line number in old file or -1 for added line");
PyObject *
DiffLine_old_lineno__get__(DiffLine *self)
{
    return PyLong_FromLong(self->line->old_lineno);
}

PyDoc_STRVAR(DiffLine_new_lineno__doc__, "Line number in new file or -1 for deleted line");
PyObject *
DiffLine_new_lineno__get__(DiffLine *self)
{
    return PyLong_FromLong(self->line->new_lineno);
}

PyDoc_STRVAR(DiffLine_num_lines__doc__, "Number of newline characters in content");
PyObject *
DiffLine_num_lines__get__(DiffLine *self)
{
    return PyLong_FromLong(self->line->num_lines);
}

PyDoc_STRVAR(DiffLine_content_offset__doc__, "Offset in the original file to the content");
PyObject *
DiffLine_content_offset__get__(DiffLine *self)
{
    return PyLong_FromLongLong(self->line->content_offset);
}

PyDoc_STRVAR(DiffLine_content__doc__, "Content of the diff line");
PyObject *
DiffLine_content__get__(DiffLine *self)
{
    return to_unicode_n(self->line->content, self->line->content_len, NULL, NULL);
}

PyDoc_STRVAR(DiffLine_raw_content__doc__, "Content of the diff line (byte string)");
PyObject *
DiffLine_raw_content__get__(DiffLine *self)
{
    return PyBytes_FromStringAndSize(self->line->content, self->line->content_len);
}

PyGetSetDef DiffLine_getsetters[] = {
    GETTER(DiffLine, origin),
    GETTER(DiffLine, old_lineno),
    GETTER(DiffLine, new_lineno),
    GETTER(DiffLine, num_lines),
    GETTER(DiffLine, content_offset),
    GETTER(DiffLine, content),
    GETTER(DiffLine, raw_content),
    {NULL}
};

PyDoc_STRVAR(DiffLine__doc__, "DiffLine object.");

PyTypeObject DiffLineType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.DiffLine",                        /* tp_name           */
    sizeof(DiffLine),                          /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)DiffLine_dealloc,              /* tp_dealloc        */
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
    DiffLine__doc__,                           /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    DiffLine_getsetters,                       /* tp_getset         */
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
diff_get_patch_byindex(git_diff *diff, size_t idx)
{
    git_patch *patch = NULL;
    int err;

    err = git_patch_from_diff(&patch, diff, idx);
    if (err < 0)
        return Error_set(err);

    /* libgit2 may decide not to create a patch if the file is
       "unchanged or binary", but this isn't an error case */
    if (patch == NULL)
        Py_RETURN_NONE;

    return (PyObject*) wrap_patch(patch, NULL, NULL);
}

PyObject *
DiffIter_iternext(DiffIter *self)
{
    if (self->i < self->n)
        return diff_get_patch_byindex(self->diff->diff, self->i++);

    PyErr_SetNone(PyExc_StopIteration);
    return NULL;
}

void
DiffIter_dealloc(DiffIter *self)
{
    Py_CLEAR(self->diff);
    PyObject_Del(self);
}


PyDoc_STRVAR(DiffIter__doc__, "Diff iterator object.");

PyTypeObject DiffIterType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.DiffIter",                        /* tp_name           */
    sizeof(DiffIter),                          /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)DiffIter_dealloc,              /* tp_dealloc        */
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
    DiffIter__doc__,                           /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    PyObject_SelfIter,                         /* tp_iter           */
    (iternextfunc) DiffIter_iternext,          /* tp_iternext       */
};

PyObject *
diff_get_delta_byindex(git_diff *diff, size_t idx)
{
    const git_diff_delta *delta = git_diff_get_delta(diff, idx);
    if (delta == NULL) {
        PyErr_SetObject(PyExc_IndexError, PyLong_FromSize_t(idx));
        return NULL;
    }

    return (PyObject*) wrap_diff_delta(delta);
}

PyObject *
DeltasIter_iternext(DeltasIter *self)
{
    if (self->i < self->n)
        return diff_get_delta_byindex(self->diff->diff, self->i++);

    PyErr_SetNone(PyExc_StopIteration);
    return NULL;
}

void
DeltasIter_dealloc(DeltasIter *self)
{
    Py_CLEAR(self->diff);
    PyObject_Del(self);
}

PyDoc_STRVAR(DeltasIter__doc__, "Deltas iterator object.");

PyTypeObject DeltasIterType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.DeltasIter",                      /* tp_name           */
    sizeof(DeltasIter),                        /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)DeltasIter_dealloc,            /* tp_dealloc        */
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
    DeltasIter__doc__,                         /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    PyObject_SelfIter,                         /* tp_iter           */
    (iternextfunc) DeltasIter_iternext,        /* tp_iternext       */
};


Py_ssize_t
Diff_len(Diff *self)
{
    assert(self->diff);
    return (Py_ssize_t)git_diff_num_deltas(self->diff);
}

PyDoc_STRVAR(Diff_patchid__doc__,
    "Corresponding patchid.");

PyObject *
Diff_patchid__get__(Diff *self)
{
    git_oid oid;
    int err;

    err = git_diff_patchid(&oid, self->diff, NULL);
    if (err < 0)
        return Error_set(err);
    return git_oid_to_python(&oid);
}


PyDoc_STRVAR(Diff_deltas__doc__, "Iterate over the diff deltas.");

PyObject *
Diff_deltas__get__(Diff *self)
{
    DeltasIter *iter;

    iter = PyObject_New(DeltasIter, &DeltasIterType);
    if (iter != NULL) {
        Py_INCREF(self);
        iter->diff = self;
        iter->i = 0;
        iter->n = git_diff_num_deltas(self->diff);
    }
    return (PyObject*)iter;
}

PyDoc_STRVAR(Diff_patch__doc__,
    "Patch diff string. Can be None in some cases, such as empty commits.");

PyObject *
Diff_patch__get__(Diff *self)
{
    git_buf buf = {NULL};

    int err = git_diff_to_buf(&buf, self->diff, GIT_DIFF_FORMAT_PATCH);
    if (err < 0)
        return Error_set(err);

    PyObject *py_patch = to_unicode_n(buf.ptr, buf.size, NULL, NULL);

    git_buf_dispose(&buf);
    return py_patch;
}


static void
DiffHunk_dealloc(DiffHunk *self)
{
    Py_CLEAR(self->patch);
    PyObject_Del(self);
}

PyDoc_STRVAR(DiffHunk_old_start__doc__, "Old start.");

PyObject *
DiffHunk_old_start__get__(DiffHunk *self)
{
  return PyLong_FromLong(self->hunk->old_start);
}

PyDoc_STRVAR(DiffHunk_old_lines__doc__, "Old lines.");

PyObject *
DiffHunk_old_lines__get__(DiffHunk *self)
{
  return PyLong_FromLong(self->hunk->old_lines);
}

PyDoc_STRVAR(DiffHunk_new_start__doc__, "New start.");

PyObject *
DiffHunk_new_start__get__(DiffHunk *self)
{
  return PyLong_FromLong(self->hunk->new_start);
}

PyDoc_STRVAR(DiffHunk_new_lines__doc__, "New lines.");

PyObject *
DiffHunk_new_lines__get__(DiffHunk *self)
{
  return PyLong_FromLong(self->hunk->new_lines);
}

PyDoc_STRVAR(DiffHunk_header__doc__, "Header.");

PyObject *
DiffHunk_header__get__(DiffHunk *self)
{
    return to_unicode_n((const char *) &self->hunk->header,
                        self->hunk->header_len, NULL, NULL);
}

PyDoc_STRVAR(DiffHunk_lines__doc__, "Lines.");

PyObject *
DiffHunk_lines__get__(DiffHunk *self)
{
    PyObject *py_lines;
    PyObject *py_line;
    const git_diff_line *line;
    size_t i;
    int err;

    // TODO Replace by an iterator
    py_lines = PyList_New(self->n_lines);
    for (i = 0; i < self->n_lines; ++i) {
        err = git_patch_get_line_in_hunk(&line, self->patch->patch, self->idx, i);
        if (err < 0)
            return Error_set(err);

        py_line = wrap_diff_line(line, self);
        if (py_line == NULL)
            return NULL;

        PyList_SetItem(py_lines, i, py_line);
   }
   return py_lines;
}


PyGetSetDef DiffHunk_getsetters[] = {
    GETTER(DiffHunk, old_start),
    GETTER(DiffHunk, old_lines),
    GETTER(DiffHunk, new_start),
    GETTER(DiffHunk, new_lines),
    GETTER(DiffHunk, header),
    GETTER(DiffHunk, lines),
    {NULL}
};

PyDoc_STRVAR(DiffHunk__doc__, "DiffHunk object.");

PyTypeObject DiffHunkType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.DiffHunk",                        /* tp_name           */
    sizeof(DiffHunk),                          /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)DiffHunk_dealloc,              /* tp_dealloc        */
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
    DiffHunk__doc__,                           /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    DiffHunk_getsetters,                       /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyDoc_STRVAR(DiffStats_insertions__doc__, "Total number of insertions");

PyObject *
DiffStats_insertions__get__(DiffStats *self)
{
    return PyLong_FromSize_t(git_diff_stats_insertions(self->stats));
}

PyDoc_STRVAR(DiffStats_deletions__doc__, "Total number of deletions");

PyObject *
DiffStats_deletions__get__(DiffStats *self)
{
    return PyLong_FromSize_t(git_diff_stats_deletions(self->stats));
}

PyDoc_STRVAR(DiffStats_files_changed__doc__, "Total number of files changed");

PyObject *
DiffStats_files_changed__get__(DiffStats *self)
{
    return PyLong_FromSize_t(git_diff_stats_files_changed(self->stats));
}

PyDoc_STRVAR(DiffStats_format__doc__,
    "format(format: enums.DiffStatsFormat, width: int) -> str\n"
    "\n"
    "Format the stats as a string.\n"
    "\n"
    "Returns: str.\n"
    "\n"
    "Parameters:\n"
    "\n"
    "format\n"
    "    The format to use. A combination of DiffStatsFormat constants.\n"
    "\n"
    "width\n"
    "    The width of the output. The output will be scaled to fit.");

PyObject *
DiffStats_format(DiffStats *self, PyObject *args, PyObject *kwds)
{
    int err, format;
    git_buf buf = { 0 };
    Py_ssize_t width;
    PyObject *str;
    char *keywords[] = {"format", "width", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "in", keywords, &format, &width))
        return NULL;

    if (width <= 0) {
        PyErr_SetString(PyExc_ValueError, "width must be positive");
        return NULL;
    }

    err = git_diff_stats_to_buf(&buf, self->stats, format, width);
    if (err < 0)
        return Error_set(err);

    str = to_unicode_n(buf.ptr, buf.size, NULL, NULL);
    git_buf_dispose(&buf);

    return str;
}

static void
DiffStats_dealloc(DiffStats *self)
{
    git_diff_stats_free(self->stats);
    PyObject_Del(self);
}

PyMethodDef DiffStats_methods[] = {
    METHOD(DiffStats, format, METH_VARARGS | METH_KEYWORDS),
    {NULL}
};

PyGetSetDef DiffStats_getsetters[] = {
    GETTER(DiffStats, insertions),
    GETTER(DiffStats, deletions),
    GETTER(DiffStats, files_changed),
    {NULL}
};

PyDoc_STRVAR(DiffStats__doc__, "DiffStats object.");

PyTypeObject DiffStatsType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.DiffStats",                       /* tp_name           */
    sizeof(DiffStats),                         /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)DiffStats_dealloc,             /* tp_dealloc        */
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
    DiffStats__doc__,                          /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    DiffStats_methods,                         /* tp_methods        */
    0,                                         /* tp_members        */
    DiffStats_getsetters,                      /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyDoc_STRVAR(Diff_from_c__doc__, "Method exposed for Index to hook into");

PyObject *
Diff_from_c(Diff *dummy, PyObject *args)
{
    PyObject *py_diff, *py_repository;
    git_diff *diff;
    char *buffer;
    Py_ssize_t length;

    if (!PyArg_ParseTuple(args, "OO!", &py_diff, &RepositoryType, &py_repository))
        return NULL;

    /* Here we need to do the opposite conversion from the _pointer getters */
    if (PyBytes_AsStringAndSize(py_diff, &buffer, &length))
        return NULL;

    if (length != sizeof(git_diff *)) {
        PyErr_SetString(PyExc_TypeError, "passed value is not a pointer");
        return NULL;
    }

    /* the "buffer" contains the pointer */
    diff = *((git_diff **) buffer);

    return wrap_diff(diff, (Repository *) py_repository);
}

PyDoc_STRVAR(Diff_merge__doc__,
  "merge(diff: Diff)\n"
  "\n"
  "Merge one diff into another.");

PyObject *
Diff_merge(Diff *self, PyObject *args)
{
    Diff *py_diff;
    int err;

    if (!PyArg_ParseTuple(args, "O!", &DiffType, &py_diff))
        return NULL;

    err = git_diff_merge(self->diff, py_diff->diff);
    if (err < 0)
        return Error_set(err);

    Py_RETURN_NONE;
}


PyDoc_STRVAR(Diff_find_similar__doc__,
  "find_similar(flags: enums.DiffFind = enums.DiffFind.FIND_BY_CONFIG, rename_threshold: int = 50, copy_threshold: int = 50, rename_from_rewrite_threshold: int = 50, break_rewrite_threshold: int = 60, rename_limit: int = 1000)\n"
  "\n"
  "Transform a diff marking file renames, copies, etc.\n"
  "\n"
  "This modifies a diff in place, replacing old entries that look like\n"
  "renames or copies with new entries reflecting those changes. This also "
  "will, if requested, break modified files into add/remove pairs if the "
  "amount of change is above a threshold.\n"
  "\n"
  "flags - Combination of enums.DiffFind.FIND_* and enums.DiffFind.BREAK_* constants."
  );

PyObject *
Diff_find_similar(Diff *self, PyObject *args, PyObject *kwds)
{
    int err;
    git_diff_find_options opts = GIT_DIFF_FIND_OPTIONS_INIT;

    char *keywords[] = {"flags", "rename_threshold", "copy_threshold",
                        "rename_from_rewrite_threshold",
                        "break_rewrite_threshold", "rename_limit", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|iHHHHI", keywords,
            &opts.flags, &opts.rename_threshold, &opts.copy_threshold,
            &opts.rename_from_rewrite_threshold, &opts.break_rewrite_threshold,
            &opts.rename_limit))
        return NULL;

    err = git_diff_find_similar(self->diff, &opts);
    if (err < 0)
        return Error_set(err);

    Py_RETURN_NONE;
}

PyObject *
Diff_iter(Diff *self)
{
    DiffIter *iter;

    iter = PyObject_New(DiffIter, &DiffIterType);
    if (iter != NULL) {
        Py_INCREF(self);
        iter->diff = self;
        iter->i = 0;
        iter->n = git_diff_num_deltas(self->diff);
    }
    return (PyObject*)iter;
}

PyObject *
Diff_getitem(Diff *self, PyObject *value)
{
    size_t i;

    if (!PyLong_Check(value))
        return NULL; /* FIXME Raise error */

    i = PyLong_AsSize_t(value);
    return diff_get_patch_byindex(self->diff, i);
}

PyDoc_STRVAR(Diff_stats__doc__, "Accumulate diff statistics for all patches.");

PyObject *
Diff_stats__get__(Diff *self)
{
    return wrap_diff_stats(self->diff);
}

PyDoc_STRVAR(Diff_parse_diff__doc__,
    "parse_diff(git_diff: str | bytes) -> Diff\n"
    "\n"
    "Parses a git unified diff into a diff object without a repository");

static PyObject *
Diff_parse_diff(PyObject *self, PyObject *py_str)
{
    /* A wrapper around git_diff_from_buffer */
    git_diff *diff;

    const char *content = pgit_borrow(py_str);
    if (content == NULL)
        return NULL;

    int err = git_diff_from_buffer(&diff, content, strlen(content));
    if (err < 0)
        return Error_set(err);

    return wrap_diff(diff, NULL);
}

static void
Diff_dealloc(Diff *self)
{
    git_diff_free(self->diff);
    Py_CLEAR(self->repo);
    PyObject_Del(self);
}

PyGetSetDef Diff_getsetters[] = {
    GETTER(Diff, deltas),
    GETTER(Diff, patch),
    GETTER(Diff, stats),
    GETTER(Diff, patchid),
    {NULL}
};

PyMappingMethods Diff_as_mapping = {
    (lenfunc)Diff_len,               /* mp_length */
    (binaryfunc)Diff_getitem,        /* mp_subscript */
    0,                               /* mp_ass_subscript */
};

static PyMethodDef Diff_methods[] = {
    METHOD(Diff, merge, METH_VARARGS),
    METHOD(Diff, find_similar, METH_VARARGS | METH_KEYWORDS),
    METHOD(Diff, from_c, METH_STATIC | METH_VARARGS),
    {"parse_diff", (PyCFunction) Diff_parse_diff,
      METH_O | METH_STATIC, Diff_parse_diff__doc__},
    {NULL}
};

/* TODO Implement Diff.patches, deprecate Diff_iter and Diff_getitem */

PyDoc_STRVAR(Diff__doc__, "Diff objects.");

PyTypeObject DiffType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Diff",                            /* tp_name           */
    sizeof(Diff),                              /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Diff_dealloc,                  /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    0,                                         /* tp_repr           */
    0,                                         /* tp_as_number      */
    0,                                         /* tp_as_sequence    */
    &Diff_as_mapping,                          /* tp_as_mapping     */
    0,                                         /* tp_hash           */
    0,                                         /* tp_call           */
    0,                                         /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,  /* tp_flags          */
    Diff__doc__,                               /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    (getiterfunc)Diff_iter,                    /* tp_iter           */
    0,                                         /* tp_iternext       */
    Diff_methods,                              /* tp_methods        */
    0,                                         /* tp_members        */
    Diff_getsetters,                           /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};
