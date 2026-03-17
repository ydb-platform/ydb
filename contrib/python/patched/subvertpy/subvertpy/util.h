/*
 * Copyright © 2008 Jelmer Vernooij <jelmer@jelmer.uk>
 * -*- coding: utf-8 -*-
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 */

#ifndef _SUBVERTPY_UTIL_H_
#define _SUBVERTPY_UTIL_H_

#include <svn_version.h>

#if SVN_VER_MAJOR != 1
#error "only svn 1.x is supported"
#endif

#ifdef SUBVERTPY_OVERRIDE_SVN_VER_MINOR
#define ONLY_SINCE_SVN(maj, min) (SUBVERTPY_OVERRIDE_SVN_VER_MINOR >= (min))
#else
#define ONLY_SINCE_SVN(maj, min) (SVN_VER_MINOR >= (min))
#endif

#define ONLY_BEFORE_SVN(maj, min) (!(ONLY_SINCE_SVN(maj, min)))

#ifdef __GNUC__
#pragma GCC visibility push(hidden)
#endif

svn_error_t *py_cancel_check(void *cancel_baton);
__attribute__((warn_unused_result)) apr_pool_t *Pool(apr_pool_t *parent);
void handle_svn_error(svn_error_t *error);
bool string_list_to_apr_array(apr_pool_t *pool, PyObject *l, apr_array_header_t **);
bool relpath_list_to_apr_array(apr_pool_t *pool, PyObject *l, apr_array_header_t **);
PyObject *prop_hash_to_dict(apr_hash_t *props);
apr_hash_t *prop_dict_to_hash(apr_pool_t *pool, PyObject *py_props);
svn_error_t *py_svn_log_wrapper(
    void *baton, apr_hash_t *changed_paths, long revision, const char *author,
    const char *date, const char *message, apr_pool_t *pool);
svn_error_t *py_svn_error(void);
void PyErr_SetSubversionException(svn_error_t *error);
PyTypeObject *PyErr_GetSubversionExceptionTypeObject(void);

#define RUN_SVN(cmd) { \
    svn_error_t *err; \
    PyThreadState *_save; \
    _save = PyEval_SaveThread(); \
    err = (cmd); \
    PyEval_RestoreThread(_save); \
    if (err != NULL) { \
        handle_svn_error(err); \
        svn_error_clear(err); \
        return NULL; \
    } \
}

#define RUN_SVN_WITH_POOL(pool, cmd) { \
    svn_error_t *err; \
    PyThreadState *_save; \
    _save = PyEval_SaveThread(); \
    err = (cmd); \
    PyEval_RestoreThread(_save); \
    if (err != NULL) { \
        handle_svn_error(err); \
        svn_error_clear(err); \
        apr_pool_destroy(pool); \
        return NULL; \
    } \
}

PyObject *wrap_lock(svn_lock_t *lock);
apr_array_header_t *revnum_list_to_apr_array(apr_pool_t *pool, PyObject *l);
svn_stream_t *new_py_stream(apr_pool_t *pool, PyObject *py);
PyObject *PyErr_NewSubversionException(svn_error_t *error);
apr_hash_t *config_hash_from_object(PyObject *config, apr_pool_t *pool);
void PyErr_SetAprStatus(apr_status_t status);
PyObject *py_dirent(const svn_dirent_t *dirent, int dirent_fields);
PyObject *PyOS_tmpfile(void);
PyObject *pyify_changed_paths(apr_hash_t *changed_paths, bool node_kind, apr_pool_t *pool);
bool pyify_log_message(
    apr_hash_t *changed_paths, const char *author,
    const char *date, const char *message, bool node_kind,
    apr_pool_t *pool, PyObject **py_changed_paths, PyObject **revprops);
#if ONLY_SINCE_SVN(1, 6)
PyObject *pyify_changed_paths2(apr_hash_t *changed_paths2, apr_pool_t *pool);
#endif
apr_file_t *apr_file_from_object(PyObject *object, apr_pool_t *pool);

#if ONLY_SINCE_SVN(1, 5)
svn_error_t *py_svn_log_entry_receiver(void *baton, svn_log_entry_t *log_entry, apr_pool_t *pool);
#endif

#ifdef __GNUC__
#pragma GCC visibility pop
#endif

#define CB_CHECK_PYRETVAL(ret) \
    if (ret == NULL) { \
        PyGILState_Release(state); \
        return py_svn_error(); \
    }

#if SVN_VER_MINOR < 5
typedef enum svn_depth_t {
    svn_depth_unknown = -2,
    svn_depth_exclude = -1,
    svn_depth_empty = 0,
    svn_depth_files = 1,
    svn_depth_immediates = 2,
    svn_depth_infinity = 3
} svn_depth_t;
#endif

typedef struct {
    PyObject_HEAD
    apr_hash_t *config;
    apr_pool_t *pool;
} ConfigObject;

typedef struct {
    PyObject_HEAD
    svn_stream_t *stream;
    apr_pool_t *pool;
    bool closed;
} StreamObject;

extern PyTypeObject Stream_Type;

const char *py_object_to_svn_path_or_url(PyObject *obj, apr_pool_t *pool);
const char *py_object_to_svn_abspath(PyObject *obj, apr_pool_t *pool);

#if ONLY_BEFORE_SVN(1, 7)
const char *
svn_uri_canonicalize(const char *uri,
                     apr_pool_t *result_pool);
const char *
svn_relpath_canonicalize(const char *relpath,
                         apr_pool_t *result_pool);
#endif

const char *py_object_to_svn_uri(PyObject *obj, apr_pool_t *pool);
const char *py_object_to_svn_dirent(PyObject *obj, apr_pool_t *pool);
const char *py_object_to_svn_relpath(PyObject *obj, apr_pool_t *pool);
char *py_object_to_svn_string(PyObject *obj, apr_pool_t *pool);
#define py_object_from_svn_abspath PyBytes_FromString

#if PY_MAJOR_VERSION >= 3
#define PyRepr_FromFormat PyUnicode_FromFormat
#else
#define PyRepr_FromFormat PyString_FromFormat
#endif

#if PY_MAJOR_VERSION >= 3
#define py_from_svn_revnum PyLong_FromLong
#define py_to_svn_revnum PyLong_AsLong
#else
#define py_from_svn_revnum PyInt_FromLong
#define py_to_svn_revnum PyInt_AsLong
#endif

#endif /* _SUBVERTPY_UTIL_H_ */
