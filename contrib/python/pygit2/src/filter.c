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
#include <git2.h>
#include <git2/sys/errors.h>
#include <git2/sys/filter.h>
#include "diff.h"
#include "error.h"
#include "object.h"
#include "oid.h"
#include "patch.h"
#include "utils.h"
#include "filter.h"

extern PyObject *GitError;

extern PyTypeObject FilterSourceType;
extern PyTypeObject RepositoryType;

PyDoc_STRVAR(FilterSource_repo__doc__,
    "Repository the source data is from\n");

PyObject *
FilterSource_repo__get__(FilterSource *self)
{
    git_repository *repo = git_filter_source_repo(self->src);
    Repository *py_repo;

    if (repo == NULL)
        Py_RETURN_NONE;

    py_repo = PyObject_New(Repository, &RepositoryType);
    if (py_repo == NULL)
        return NULL;
    py_repo->repo = repo;
    py_repo->config = NULL;
    py_repo->index = NULL;
    py_repo->owned = 0;
    Py_INCREF(py_repo);
    return (PyObject *)py_repo;
}

PyDoc_STRVAR(FilterSource_path__doc__,
    "File path the source data is from.\n");

PyObject *
FilterSource_path__get__(FilterSource *self)
{
    return to_unicode_safe(git_filter_source_path(self->src), NULL);
}

PyDoc_STRVAR(FilterSource_filemode__doc__,
    "Mode of the source file. If this is unknown, `filemode` will be 0.\n");

PyObject *
FilterSource_filemode__get__(FilterSource *self)
{
    return PyLong_FromUnsignedLong(git_filter_source_filemode(self->src));
}

PyDoc_STRVAR(FilterSource_oid__doc__,
    "Oid of the source object. If the oid is unknown "
    "(often the case with FilterMode.CLEAN) then `oid` will be None.\n");
PyObject *
FilterSource_oid__get__(FilterSource *self)
{
    const git_oid *oid = git_filter_source_id(self->src);
    if (oid == NULL)
        Py_RETURN_NONE;
    return git_oid_to_python(oid);
}

PyDoc_STRVAR(FilterSource_mode__doc__,
    "Filter mode (either FilterMode.CLEAN or FilterMode.SMUDGE).\n");

PyObject *
FilterSource_mode__get__(FilterSource *self)
{
    return PyLong_FromUnsignedLong(git_filter_source_mode(self->src));
}

PyDoc_STRVAR(FilterSource_flags__doc__,
    "A combination of filter flags (enums.FilterFlag) to be applied to the data.\n");

PyObject *
FilterSource_flags__get__(FilterSource *self)
{
    return PyLong_FromUnsignedLong(git_filter_source_flags(self->src));
}

PyGetSetDef FilterSource_getseters[] = {
    GETTER(FilterSource, repo),
    GETTER(FilterSource, path),
    GETTER(FilterSource, filemode),
    GETTER(FilterSource, oid),
    GETTER(FilterSource, mode),
    GETTER(FilterSource, flags),
    {NULL}
};

PyDoc_STRVAR(FilterSource__doc__,
    "A filter source represents the file/blob to be processed.\n");

PyTypeObject FilterSourceType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.FilterSource",                    /* tp_name           */
    sizeof(FilterSource),                      /* tp_basicsize      */
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
    FilterSource__doc__,                       /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    0,                                         /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    FilterSource_getseters,                    /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    0,                                         /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyDoc_STRVAR(filter__write_next__doc__,
    "Write to the next writestream in a filter list.\n");

static PyObject *
filter__write_next(PyObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *py_next;
    git_writestream *next;
    const char *buf;
    Py_ssize_t size;
    char *keywords[] = {"next", "data", NULL};
    int err;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "Oy#", keywords,
                                     &py_next, &buf, &size))
        return NULL;

    next = (git_writestream *)PyCapsule_GetPointer(py_next, NULL);
    if (next == NULL)
        goto done;

    Py_BEGIN_ALLOW_THREADS;
    err = next->write(next, buf, size);
    Py_END_ALLOW_THREADS;
    if (err  < 0)
        return Error_set(err);

done:
    Py_RETURN_NONE;
}

static PyMethodDef filter__write_next_method = {
    "_write_next",
    (PyCFunction)filter__write_next,
    METH_VARARGS | METH_KEYWORDS,
    filter__write_next__doc__
};

struct pygit2_filter_stream {
    git_writestream stream;
    git_writestream *next;
    PyObject *py_filter;
    FilterSource *py_src;
    PyObject *py_write_next;
};

struct pygit2_filter_payload {
    PyObject *py_filter;
    FilterSource *src;
    struct pygit2_filter_stream *stream;
};

static void pygit2_filter_payload_free(
    struct pygit2_filter_payload *payload)
{
    if (payload == NULL)
        return;
    if (payload->py_filter != NULL)
        Py_DECREF(payload->py_filter);
    if (payload->src != NULL)
        Py_DECREF(payload->src);
    if (payload->stream != NULL)
        free(payload->stream);
    free(payload);
}

static struct pygit2_filter_payload * pygit2_filter_payload_new(
    PyObject *py_filter_cls, const git_filter_source *src)
{
    struct pygit2_filter_payload *payload = NULL;

    payload = malloc(sizeof(struct pygit2_filter_payload));
    if (payload == NULL)
        return NULL;
    memset(payload, 0, sizeof(struct pygit2_filter_payload));

    payload->py_filter = PyObject_CallFunction(py_filter_cls, NULL);
    if (payload->py_filter == NULL)
    {
        PyErr_Clear();
        goto error;
    }
    payload->src = PyObject_New(FilterSource, &FilterSourceType);
    if (payload->src == NULL)
    {
        PyErr_Clear();
        goto error;
    }
    payload->src->src = src;
    goto done;

error:
    pygit2_filter_payload_free(payload);
    payload = NULL;
done:
    return payload;
}

static int pygit2_filter_stream_write(
    git_writestream *s, const char *buffer, size_t len)
{
    struct pygit2_filter_stream *stream = (struct pygit2_filter_stream *)s;
    PyObject *result = NULL;
    PyGILState_STATE gil = PyGILState_Ensure();
    int err = 0;

    result = PyObject_CallMethod(stream->py_filter, "write", "y#OO",
                                 buffer, len, stream->py_src,
                                 stream->py_write_next);
    if (result == NULL)
    {
        PyErr_Clear();
        git_error_set(GIT_ERROR_OS, "failed to write to filter stream");
        err = GIT_ERROR;
        goto done;
    }
    Py_DECREF(result);

done:
    PyGILState_Release(gil);
    return err;
}

static int pygit2_filter_stream_close(git_writestream *s)
{
    struct pygit2_filter_stream *stream = (struct pygit2_filter_stream *)s;
    PyObject *result = NULL;
    PyGILState_STATE gil = PyGILState_Ensure();
    int err = 0;
    int nexterr;

    result = PyObject_CallMethod(stream->py_filter, "close", "O",
                                 stream->py_write_next);
    if (result == NULL)
    {
        PyErr_Clear();
        git_error_set(GIT_ERROR_OS, "failed to close filter stream");
        err = GIT_ERROR;
        goto done;
    }
    Py_DECREF(result);

done:
    if (stream->py_write_next != NULL)
        Py_DECREF(stream->py_write_next);
    PyGILState_Release(gil);
    if (stream->next != NULL) {
        nexterr = stream->next->close(stream->next);
        if (err == 0)
            err = nexterr;
    }
    return err;
}

static void pygit2_filter_stream_free(git_writestream *s)
{
}

static int pygit2_filter_stream_init(
    struct pygit2_filter_stream *stream, git_writestream *next, PyObject *py_filter, FilterSource *py_src)
{
    int err = 0;
    PyObject *py_next = NULL;
    PyObject *py_functools = NULL;
    PyObject *py_write_next = NULL;
    PyObject *py_partial_write = NULL;
    PyGILState_STATE gil = PyGILState_Ensure();

    memset(stream, 0, sizeof(struct pygit2_filter_stream));
    stream->stream.write = pygit2_filter_stream_write;
    stream->stream.close = pygit2_filter_stream_close;
    stream->stream.free = pygit2_filter_stream_free;
    stream->next = next;
    stream->py_filter = py_filter;
    stream->py_src = py_src;

    py_functools = PyImport_ImportModule("functools");
    if (py_functools == NULL)
    {
        PyErr_Clear();
        git_error_set(GIT_ERROR_OS, "failed to import module");
        err = GIT_ERROR;
        goto error;
    }
    py_next = PyCapsule_New(stream->next, NULL, NULL);
    if (py_next == NULL)
    {
        PyErr_Clear();
        giterr_set_oom();
        err = GIT_ERROR;
        goto error;
    }
    py_write_next = PyCFunction_New(&filter__write_next_method, NULL);
    if (py_write_next == NULL)
    {
        PyErr_Clear();
        err = GIT_ERROR;
        goto error;
    }
    py_partial_write = PyObject_CallMethod(py_functools, "partial", "OO",
                                     py_write_next, py_next);
    if (py_partial_write == NULL)
    {
        PyErr_Clear();
        err = GIT_ERROR;
        goto error;
    }
    stream->py_write_next = py_partial_write;
    goto done;

error:
    if (py_partial_write != NULL)
        Py_DECREF(py_partial_write);
done:
    if (py_write_next != NULL)
        Py_DECREF(py_write_next);
    if (py_functools != NULL)
        Py_DECREF(py_functools);
    if (py_next != NULL)
        Py_DECREF(py_next);
    PyGILState_Release(gil);
    return err;
}

static PyObject * get_passthrough()
{
    PyObject *py_passthrough;
    PyObject *py_errors = PyImport_ImportModule("pygit2.errors");
    if (py_errors == NULL)
        return NULL;
    py_passthrough = PyObject_GetAttrString(py_errors, "Passthrough");
    Py_DECREF(py_errors);
    return py_passthrough;
}

int pygit2_filter_check(
    git_filter *self, void **payload, const git_filter_source *src, const char **attr_values)
{
    pygit2_filter *filter = (pygit2_filter *)self;
    struct pygit2_filter_payload *pl = NULL;
    PyObject *py_attrs = NULL;
    Py_ssize_t nattrs;
    Py_ssize_t i;
    PyObject *result;
    PyObject *py_passthrough = NULL;
    PyGILState_STATE gil = PyGILState_Ensure();
    int err = 0;

    py_passthrough = get_passthrough();
    if (py_passthrough == NULL)
    {
        PyErr_Clear();
        err = GIT_ERROR;
        goto error;
    }

    pl = pygit2_filter_payload_new(filter->py_filter_cls, src);
    if (pl == NULL)
    {
        giterr_set_oom();
        err = GIT_ERROR;
        goto done;
    }

    result = PyObject_CallMethod(pl->py_filter, "nattrs", NULL);
    if (result == NULL)
    {
        PyErr_Clear();
        err = GIT_ERROR;
        goto error;
    }
    nattrs = PyLong_AsSsize_t(result);
    Py_DECREF(result);
    py_attrs = PyList_New(nattrs);
    if (py_attrs == NULL)
    {
        PyErr_Clear();
        err = GIT_ERROR;
        goto error;
    }
    for (i = 0; i < nattrs; ++i)
    {
        if (attr_values[i] == NULL)
        {
            if (PyList_SetItem(py_attrs, i, Py_None) < 0)
            {
                PyErr_Clear();
                err = GIT_ERROR;
                goto error;
            }
        }
        else if (PyList_SetItem(py_attrs, i, to_unicode_safe(attr_values[i], NULL)) < 0)
        {
            PyErr_Clear();
            err = GIT_ERROR;
            goto error;
        }
    }
    result = PyObject_CallMethod(pl->py_filter, "check", "OO", pl->src, py_attrs);
    if (result == NULL)
    {
        if (PyErr_ExceptionMatches(py_passthrough))
        {
            PyErr_Clear();
            err = GIT_PASSTHROUGH;
        }
        else
        {
            PyErr_Clear();
            err = GIT_ERROR;
            goto error;
        }
    }
    else
    {
        Py_DECREF(result);
        *payload = pl;
    }
    goto done;

error:
    if (pl != NULL)
        pygit2_filter_payload_free(pl);
done:
    if (py_attrs != NULL)
        Py_DECREF(py_attrs);
    if (py_passthrough != NULL)
        Py_DECREF(py_passthrough);
    PyGILState_Release(gil);
    return err;
}

int pygit2_filter_stream(
    git_writestream **out, git_filter *self, void **payload, const git_filter_source *src, git_writestream *next)
{
    pygit2_filter *filter = (pygit2_filter *)self;
    struct pygit2_filter_stream *stream = NULL;
    struct pygit2_filter_payload *pl = NULL;
    PyGILState_STATE gil = PyGILState_Ensure();
    int err = 0;

    if (*payload == NULL)
    {
        pl = pygit2_filter_payload_new(filter->py_filter_cls, src);
        if (pl == NULL)
        {
            giterr_set_oom();
            err = GIT_ERROR;
            goto done;
        }
        *payload = pl;
    }
    else
    {
        pl = *payload;
    }

    stream = malloc(sizeof(struct pygit2_filter_stream));
    if ((err = pygit2_filter_stream_init(stream, next, pl->py_filter, pl->src)) < 0)
        goto error;
    *out = &stream->stream;
    goto done;

error:
    if (stream != NULL)
        free(stream);
done:
    PyGILState_Release(gil);
    return err;
}

void pygit2_filter_cleanup(git_filter *self, void *payload)
{
    struct pygit2_filter_payload *pl = (struct pygit2_filter_payload *)payload;

    PyGILState_STATE gil = PyGILState_Ensure();
    pygit2_filter_payload_free(pl);
    PyGILState_Release(gil);
}

void pygit2_filter_shutdown(git_filter *self)
{
    pygit2_filter *filter = (pygit2_filter *)self;
    PyGILState_STATE gil = PyGILState_Ensure();
    Py_DECREF(filter->py_filter_cls);
    free(filter);
    PyGILState_Release(gil);
}
