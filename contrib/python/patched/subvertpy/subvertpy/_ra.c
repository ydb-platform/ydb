/*
 * Copyright © 2008-2009 Jelmer Vernooij <jelmer@jelmer.uk>
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
#include <stdbool.h>
#include <Python.h>
#include <apr_general.h>
#include <svn_types.h>
#include <svn_ra.h>
#include <svn_path.h>
#include <svn_props.h>
#include <apr_file_io.h>
#include <apr_portable.h>

#include <structmember.h>

#include "editor.h"
#include "util.h"
#include "ra.h"

#if ONLY_SINCE_SVN(1, 5)
#define REPORTER_T svn_ra_reporter3_t
#else
#define REPORTER_T svn_ra_reporter2_t
#endif

static PyObject *busy_exc;

static PyTypeObject Reporter_Type;
static PyTypeObject RemoteAccess_Type;
static PyTypeObject AuthProvider_Type;
static PyTypeObject CredentialsIter_Type;
static PyTypeObject Auth_Type;

static bool ra_check_svn_path(const char *path)
{
	/* svn_ra_check_path will raise an assertion error if the path has a
	 * leading '/'. Raise a Python exception if there ar eleading '/'s so that
	 * the Python interpreter won't crash and die. */
	if (*path == '/') {
		PyErr_SetString(PyExc_ValueError, "invalid path has a leading '/'");
		return true;
	}
	return false;
}

static svn_error_t *py_commit_callback(const svn_commit_info_t *commit_info, void *baton, apr_pool_t *pool)
{
	PyObject *fn = (PyObject *)baton, *ret;
	PyGILState_STATE state;

	if (fn == Py_None)
		return NULL;

	state = PyGILState_Ensure();

	ret = PyObject_CallFunction(fn, "lzz",
					commit_info->revision, commit_info->date,
					commit_info->author);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static PyObject *pyify_lock(const svn_lock_t *lock)
{
	return Py_BuildValue("(ssszbLL)",
						 lock->path, lock->token,
						 lock->owner, lock->comment,
						 lock->is_dav_comment,
						 lock->creation_date,
						 lock->expiration_date);
}

static svn_error_t *py_lock_func (void *baton, const char *path, int do_lock,
						   const svn_lock_t *lock, svn_error_t *ra_err,
						   apr_pool_t *pool)
{
	PyObject *py_ra_err, *ret, *py_lock;
	PyGILState_STATE state = PyGILState_Ensure();
	if (ra_err != NULL) {
		py_ra_err = PyErr_NewSubversionException(ra_err);
	} else {
		py_ra_err = Py_None;
		Py_INCREF(py_ra_err);
	}
	py_lock = pyify_lock(lock);
	ret = PyObject_CallFunction((PyObject *)baton, "zbOO", path, do_lock?true:false,
						  py_lock, py_ra_err);
	Py_DECREF(py_lock);
	Py_DECREF(py_ra_err);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

/** Connection to a remote Subversion repository. */
typedef struct {
	PyObject_VAR_HEAD
	svn_ra_session_t *ra;
	apr_pool_t *pool;
	const char *url;
	PyObject *progress_func;
	AuthObject *auth;
	bool busy;
	PyObject *client_string_func;
	PyObject *open_tmp_file_func;
	const char *root;
	const char *corrected_url;
} RemoteAccessObject;

typedef struct {
	PyObject_VAR_HEAD
	const REPORTER_T *reporter;
	void *report_baton;
	apr_pool_t *pool;
	RemoteAccessObject *ra;
} ReporterObject;

static PyObject *reporter_set_path(PyObject *self, PyObject *args)
{
	char *path;
	svn_revnum_t revision;
	bool start_empty;
	char *lock_token = NULL;
	svn_depth_t depth = svn_depth_infinity;
	ReporterObject *reporter = (ReporterObject *)self;

	if (!PyArg_ParseTuple(args, "slb|zi:set_path", &path, &revision, &start_empty,
						  &lock_token, &depth))
		return NULL;

	if (reporter->ra == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
			"Reporter already finished.");
		return NULL;
	}

#if ONLY_SINCE_SVN(1, 5)
	RUN_SVN(reporter->reporter->set_path(reporter->report_baton,
												  path, revision, depth, start_empty,
					 lock_token, reporter->pool));
#else
	if (depth != svn_depth_infinity) {
		PyErr_SetString(PyExc_NotImplementedError,
						"depth != infinity only supported for svn >= 1.5");
		return NULL;
	}
	RUN_SVN(reporter->reporter->set_path(reporter->report_baton,
												  path, revision, start_empty,
					 lock_token, reporter->pool));
#endif
	Py_RETURN_NONE;
}

static PyObject *reporter_delete_path(PyObject *self, PyObject *args)
{
	ReporterObject *reporter = (ReporterObject *)self;
	char *path;
	if (!PyArg_ParseTuple(args, "s:delete_path", &path))
		return NULL;

	if (reporter->ra == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
			"Reporter already finished.");
		return NULL;
	}

	RUN_SVN(reporter->reporter->delete_path(reporter->report_baton,
		path, reporter->pool));

	Py_RETURN_NONE;
}

static PyObject *reporter_link_path(PyObject *self, PyObject *args)
{
	char *path, *url;
	svn_revnum_t revision;
	bool start_empty;
	char *lock_token = NULL;
	ReporterObject *reporter = (ReporterObject *)self;
	svn_depth_t depth = svn_depth_infinity;

	if (!PyArg_ParseTuple(args, "sslb|zi:link_path", &path, &url, &revision,
			&start_empty, &lock_token, &depth))
		return NULL;

	if (reporter->ra == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
			"Reporter already finished.");
		return NULL;
	}

#if ONLY_SINCE_SVN(1, 5)
	RUN_SVN(reporter->reporter->link_path(reporter->report_baton, path, url,
				revision, depth, start_empty, lock_token, reporter->pool));
#else
	if (depth != svn_depth_infinity) {
		PyErr_SetString(PyExc_NotImplementedError,
						"depth != infinity only supported for svn >= 1.5");
		return NULL;
	}
	RUN_SVN(reporter->reporter->link_path(reporter->report_baton, path, url,
				revision, start_empty, lock_token, reporter->pool));
#endif

	Py_RETURN_NONE;
}

static PyObject *reporter_finish(PyObject *self)
{
	ReporterObject *reporter = (ReporterObject *)self;

	if (reporter->ra == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
			"Reporter already finished.");
		return NULL;
	}

	reporter->ra->busy = false;

	RUN_SVN(reporter->reporter->finish_report(
		reporter->report_baton, reporter->pool));

	apr_pool_destroy(reporter->pool);
	Py_XDECREF(reporter->ra);
	reporter->ra = NULL;

	Py_RETURN_NONE;
}

static PyObject *reporter_abort(PyObject *self)
{
	ReporterObject *reporter = (ReporterObject *)self;

	if (reporter->ra == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
			"Reporter already finished.");
		return NULL;
	}

	reporter->ra->busy = false;

	RUN_SVN(reporter->reporter->abort_report(reporter->report_baton,
													 reporter->pool));

	apr_pool_destroy(reporter->pool);
	Py_XDECREF(reporter->ra);
	reporter->ra = NULL;

	Py_RETURN_NONE;
}

static PyMethodDef reporter_methods[] = {
	{ "abort", (PyCFunction)reporter_abort, METH_NOARGS,
		"S.abort()\n"
		"Abort this report." },
	{ "finish", (PyCFunction)reporter_finish, METH_NOARGS,
		"S.finish()\n"
		"Finish this report." },
	{ "link_path", (PyCFunction)reporter_link_path, METH_VARARGS,
		"S.link_path(path, url, revision, start_empty, lock_token=None)\n" },
	{ "set_path", (PyCFunction)reporter_set_path, METH_VARARGS,
		"S.set_path(path, revision, start_empty, lock_token=None)\n" },
	{ "delete_path", (PyCFunction)reporter_delete_path, METH_VARARGS,
		"S.delete_path(path)\n" },
	{ NULL, }
};

static void reporter_dealloc(PyObject *self)
{
	ReporterObject *reporter = (ReporterObject *)self;
	if (reporter->ra != NULL) {
		/* FIXME: Warn */
		apr_pool_destroy(reporter->pool);
		Py_DECREF(reporter->ra);
	}
	PyObject_Del(self);
}

static PyTypeObject Reporter_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.Reporter", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(ReporterObject),
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

	/* Methods to implement standard operations */

	reporter_dealloc, /*	destructor tp_dealloc;	*/
	0, /*	printfunc tp_print;	*/
	NULL, /*	getattrfunc tp_getattr;	*/
	NULL, /*	setattrfunc tp_setattr;	*/
	NULL, /*	cmpfunc tp_compare;	*/
	NULL, /*	reprfunc tp_repr;	*/

	/* Method suites for standard classes */

	NULL, /*	PyNumberMethods *tp_as_number;	*/
	NULL, /*	PySequenceMethods *tp_as_sequence;	*/
	NULL, /*	PyMappingMethods *tp_as_mapping;	*/

	/* More standard operations (here for binary compatibility) */

	NULL, /*	hashfunc tp_hash;	*/
	NULL, /*	ternaryfunc tp_call;	*/
	NULL, /*	reprfunc tp_str;	*/
	NULL, /*	getattrofunc tp_getattro;	*/
	NULL, /*	setattrofunc tp_setattro;	*/

	/* Functions to access object as input/output buffer */
	NULL, /*	PyBufferProcs *tp_as_buffer;	*/

	/* Flags to define presence of optional/expanded features */
	0, /*	long tp_flags;	*/

	NULL, /*	const char *tp_doc;  Documentation string */

	/* Assigned meaning in release 2.0 */
	/* call function for all accessible objects */
	NULL, /*	traverseproc tp_traverse;	*/

	/* delete references to contained objects */
	NULL, /*	inquiry tp_clear;	*/

	/* Assigned meaning in release 2.1 */
	/* rich comparisons */
	NULL, /*	richcmpfunc tp_richcompare;	*/

	/* weak reference enabler */
	0, /*	Py_ssize_t tp_weaklistoffset;	*/

	/* Added in release 2.2 */
	/* Iterators */
	NULL, /*	getiterfunc tp_iter;	*/
	NULL, /*	iternextfunc tp_iternext;	*/

	/* Attribute descriptor and subclassing stuff */
	reporter_methods, /*	struct PyMethodDef *tp_methods;	*/

};

/**
 * Get libsvn_ra version information.
 *
 * :return: tuple with major, minor, patch version number and tag.
 */
static PyObject *version(PyObject *self)
{
	const svn_version_t *ver = svn_ra_version();
	return Py_BuildValue("(iiis)", ver->major, ver->minor,
						 ver->patch, ver->tag);
}

SVN_VERSION_DEFINE(svn_api_version);

/**
 * Get compile-time libsvn_ra version information.
 *
 * :return: tuple with major, minor, patch version number and tag.
 */
static PyObject *api_version(PyObject *self)
{
	const svn_version_t *ver = &svn_api_version;
	return Py_BuildValue("(iiis)", ver->major, ver->minor,
						 ver->patch, ver->tag);
}

#if ONLY_SINCE_SVN(1, 5)
static svn_error_t *py_file_rev_handler(void *baton, const char *path, svn_revnum_t rev, apr_hash_t *rev_props, svn_boolean_t result_of_merge, svn_txdelta_window_handler_t *delta_handler, void **delta_baton, apr_array_header_t *prop_diffs, apr_pool_t *pool)
{
	PyObject *fn = (PyObject *)baton, *ret, *py_rev_props;
	PyGILState_STATE state = PyGILState_Ensure();

	py_rev_props = prop_hash_to_dict(rev_props);
	CB_CHECK_PYRETVAL(py_rev_props);

	ret = PyObject_CallFunction(fn, "slOi", path, rev, py_rev_props, result_of_merge);
	Py_DECREF(py_rev_props);
	CB_CHECK_PYRETVAL(ret);

	if (delta_baton != NULL && delta_handler != NULL) {
		*delta_baton = (void *)ret;
		*delta_handler = py_txdelta_window_handler;
	} else {
		Py_DECREF(ret);
	}
	PyGILState_Release(state);
	return NULL;
}
#else
static svn_error_t *py_ra_file_rev_handler(void *baton, const char *path, svn_revnum_t rev, apr_hash_t *rev_props, svn_txdelta_window_handler_t *delta_handler, void **delta_baton, apr_array_header_t *prop_diffs, apr_pool_t *pool)
{
	PyObject *fn = (PyObject *)baton, *ret, *py_rev_props;
	PyGILState_STATE state = PyGILState_Ensure();

	py_rev_props = prop_hash_to_dict(rev_props);
	CB_CHECK_PYRETVAL(py_rev_props);

	ret = PyObject_CallFunction(fn, "slO", path, rev, py_rev_props);
	Py_DECREF(py_rev_props);
	CB_CHECK_PYRETVAL(ret);

	if (delta_baton != NULL && delta_handler != NULL) {
		*delta_baton = (void *)ret;
		*delta_handler = py_txdelta_window_handler;
	} else {
		Py_DECREF(ret);
	}
	PyGILState_Release(state);
	return NULL;
}


#endif

static void ra_done_handler(void *_ra)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)_ra;

	ra->busy = false;

	Py_DECREF(ra);
}

#define RUN_RA_WITH_POOL(pool, ra, cmd) { \
	svn_error_t *err; \
	PyThreadState *_save; \
	_save = PyEval_SaveThread(); \
	err = (cmd); \
	PyEval_RestoreThread(_save); \
	if (err != NULL) { \
		handle_svn_error(err); \
		svn_error_clear(err); \
		apr_pool_destroy(pool); \
		ra->busy = false; \
		return NULL; \
	} \
	ra->busy = false; \
}

static bool ra_check_busy(RemoteAccessObject *raobj)
{
	if (raobj->busy) {
		PyErr_SetString(busy_exc, "Remote access object already in use");
		return true;
	}
	raobj->busy = true;
	return false;
}

#if ONLY_SINCE_SVN(1, 5)
static svn_error_t *py_get_client_string(void *baton, const char **name, apr_pool_t *pool)
{
	RemoteAccessObject *self = (RemoteAccessObject *)baton;
	PyObject *ret;
	PyGILState_STATE state;

	if (self->client_string_func == Py_None) {
		*name = NULL;
		return NULL;
	}

	state = PyGILState_Ensure();

	ret = PyObject_CallFunction(self->client_string_func, "");

	CB_CHECK_PYRETVAL(ret);

	*name = py_object_to_svn_string(ret, pool);
	Py_DECREF(ret);

	PyGILState_Release(state);
	return NULL;
}
#endif

/* Based on svn_swig_py_make_file() from Subversion */
static svn_error_t *py_open_tmp_file(apr_file_t **fp, void *callback,
									 apr_pool_t *pool)
{
	RemoteAccessObject *self = (RemoteAccessObject *)callback;
	PyObject *ret;
	apr_status_t status;
	PyGILState_STATE state;

	if (self->open_tmp_file_func == Py_None) {
		const char *path;

		SVN_ERR (svn_io_temp_dir (&path, pool));
#if ONLY_SINCE_SVN(1, 7)
		path = svn_dirent_join (path, "subvertpy", pool);
#else
		path = svn_path_join (path, "subvertpy", pool);
#endif
#if ONLY_SINCE_SVN(1, 6)
		SVN_ERR (svn_io_open_unique_file3(fp, NULL, path, svn_io_file_del_on_pool_cleanup, pool, pool));
#else
		SVN_ERR (svn_io_open_unique_file (fp, NULL, path, ".tmp", TRUE, pool));
#endif

		return NULL;
	}

	state = PyGILState_Ensure();

	ret = PyObject_CallFunction(self->open_tmp_file_func, "");

	CB_CHECK_PYRETVAL(ret);

	if (PyUnicode_Check(ret)) {
		PyObject *orig_ret = ret;
		ret = PyUnicode_AsUTF8String(ret);
		Py_DECREF(orig_ret);
	}

	if (PyBytes_Check(ret)) {
		char* fname = PyBytes_AsString(ret);
		status = apr_file_open(fp, fname, APR_CREATE | APR_READ | APR_WRITE, APR_OS_DEFAULT,
								pool);
		if (status) {
			PyErr_SetAprStatus(status);
			goto fail_file;
		}
		Py_DECREF(ret);
	} else if (PyObject_AsFileDescriptor(ret) != -1) {
		*fp = apr_file_from_object(ret, pool);
		Py_DECREF(ret);
		if (!*fp) {
			goto fail;
		}
	} else {
		PyErr_SetString(PyExc_TypeError, "Unknown type for file variable");
		Py_DECREF(ret);
		PyGILState_Release(state);
		return py_svn_error();
	}

	PyGILState_Release(state);
	return NULL;

fail_file:
	Py_DECREF(ret);
fail:
	PyGILState_Release(state);
	return py_svn_error();
}

static void py_progress_func(apr_off_t progress, apr_off_t total, void *baton, apr_pool_t *pool)
{
	PyGILState_STATE state = PyGILState_Ensure();
	RemoteAccessObject *ra = (RemoteAccessObject *)baton;
	PyObject *fn = (PyObject *)ra->progress_func, *ret;
	if (fn != Py_None) {
		ret = PyObject_CallFunction(fn, "LL", progress, total);
		Py_XDECREF(ret);
	}
	PyGILState_Release(state);
}

static PyObject *ra_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
	char *kwnames[] = { "url", "progress_cb", "auth", "config",
				"client_string_func", "open_tmp_file_func", "uuid",
						NULL };
	char *uuid = NULL;
	PyObject *py_url;
	PyObject *progress_cb = Py_None;
	AuthObject *auth = (AuthObject *)Py_None;
	PyObject *config = Py_None;
	PyObject *client_string_func = Py_None, *open_tmp_file_func = Py_None;
	RemoteAccessObject *ret;
	apr_hash_t *config_hash;
	svn_ra_callbacks2_t *callbacks2;
	svn_auth_baton_t *auth_baton;
	svn_error_t *err;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOOOOz", kwnames, &py_url,
									 &progress_cb, (PyObject **)&auth, &config,
									 &client_string_func, &open_tmp_file_func,
									 &uuid))
		return NULL;

	ret = PyObject_New(RemoteAccessObject, &RemoteAccess_Type);
	if (ret == NULL)
		return NULL;

	ret->client_string_func = client_string_func;
	ret->open_tmp_file_func = open_tmp_file_func;
	Py_INCREF(client_string_func);

	Py_INCREF(progress_cb);
	ret->progress_func = progress_cb;

	ret->auth = NULL;
	ret->corrected_url = NULL;

	ret->root = NULL;
	ret->pool = Pool(NULL);
	if (ret->pool == NULL) {
		Py_DECREF(ret);
		return NULL;
	}

	ret->url = py_object_to_svn_uri(py_url, ret->pool);
	if (ret->url == NULL) {
		Py_DECREF(ret);
		return NULL;
	}

	if ((PyObject *)auth == Py_None) {
		ret->auth = NULL;
		svn_auth_open(&auth_baton, apr_array_make(ret->pool, 0, sizeof(svn_auth_provider_object_t *)), ret->pool);
	} else if (PyObject_TypeCheck(auth, &Auth_Type)) {
		Py_INCREF(auth);
		ret->auth = auth;
		auth_baton = ret->auth->auth_baton;
	} else {
		PyErr_SetString(PyExc_TypeError, "auth argument is not an Auth object");
		Py_DECREF(ret);
		return NULL;
	}

	err = svn_ra_create_callbacks(&callbacks2, ret->pool);
	if (err != NULL) {
		handle_svn_error(err);
		svn_error_clear(err);
		Py_DECREF(ret);
		return NULL;
	}

	callbacks2->progress_baton = (void *)ret;
	callbacks2->progress_func = py_progress_func;
	callbacks2->auth_baton = auth_baton;
	callbacks2->open_tmp_file = py_open_tmp_file;
	callbacks2->cancel_func = py_cancel_check;
#if ONLY_SINCE_SVN(1, 5)
	callbacks2->get_client_string = py_get_client_string;
#endif
	config_hash = config_hash_from_object(config, ret->pool);
	if (config_hash == NULL) {
		Py_DECREF(ret);
		return NULL;
	}
	Py_BEGIN_ALLOW_THREADS
#if ONLY_SINCE_SVN(1, 7)
	err = svn_ra_open4(&ret->ra, &ret->corrected_url, ret->url, uuid,
			   callbacks2, ret, config_hash, ret->pool);
#elif ONLY_SINCE_SVN(1, 5)
	err = svn_ra_open3(&ret->ra, ret->url, uuid,
			   callbacks2, ret, config_hash, ret->pool);
#else
	if (uuid != NULL) {
		PyErr_SetString(PyExc_TypeError,
			"uuid argument not supported with svn 1.4");
		Py_DECREF(ret);
		return NULL;
	}
	err = svn_ra_open2(&ret->ra, ret->url,
			   callbacks2, ret, config_hash, ret->pool);
#endif
	Py_END_ALLOW_THREADS
	if (err != NULL) {
		handle_svn_error(err);
		svn_error_clear(err);
		Py_DECREF(ret);
		return NULL;
	}
	ret->busy = false;
	return (PyObject *)ret;
}

 /**
  * Obtain the globally unique identifier for this repository.
  */
static PyObject *ra_get_uuid(PyObject *self)
{
	const char *uuid;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	PyObject *ret;
	apr_pool_t *temp_pool;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
#if ONLY_SINCE_SVN(1, 5)
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_uuid2(ra->ra, &uuid, temp_pool));
#else
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_uuid(ra->ra, &uuid, temp_pool));
#endif
	ret = PyUnicode_FromString(uuid);
	apr_pool_destroy(temp_pool);
	return ret;
}

/** Switch to a different url. */
static PyObject *ra_reparent(PyObject *self, PyObject *args)
{
	PyObject *py_url;
	apr_pool_t *temp_pool;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;

	if (!PyArg_ParseTuple(args, "O:reparent", &py_url))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	ra->url = py_object_to_svn_uri(py_url, ra->pool);
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_reparent(ra->ra, ra->url, temp_pool));
	apr_pool_destroy(temp_pool);
	Py_RETURN_NONE;
}

/**
 * Obtain the number of the latest committed revision in the
 * connected repository.
 */
static PyObject *ra_get_latest_revnum(PyObject *self)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_revnum_t latest_revnum;
	apr_pool_t *temp_pool;
	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	RUN_RA_WITH_POOL(temp_pool, ra,
				  svn_ra_get_latest_revnum(ra->ra, &latest_revnum, temp_pool));
	apr_pool_destroy(temp_pool);
	return py_from_svn_revnum(latest_revnum);
}

static bool ra_get_log_prepare(RemoteAccessObject *ra, PyObject *paths,
bool include_merged_revisions, PyObject *revprops, apr_pool_t **pool,
apr_array_header_t **apr_paths, apr_array_header_t **apr_revprops)
{
	if (ra_check_busy(ra))
		goto fail_busy;

	*pool = Pool(NULL);
	if (*pool == NULL)
		goto fail_pool;
	if (paths == Py_None) {
		/* The subversion libraries don't behave as expected,
		 * so tweak our own parameters a bit. */
		*apr_paths = apr_array_make(*pool, 1, sizeof(char *));
		APR_ARRAY_PUSH(*apr_paths, char *) = apr_pstrdup(*pool, "");
	} else if (!relpath_list_to_apr_array(*pool, paths, apr_paths)) {
		goto fail_prep;
	}

#if ONLY_BEFORE_SVN(1, 5)
	if (revprops == Py_None) {
		PyErr_SetString(PyExc_NotImplementedError,
			"fetching all revision properties not supported");
		goto fail_prep;
	} else if (!PySequence_Check(revprops)) {
		PyErr_SetString(PyExc_TypeError, "revprops should be a sequence");
		goto fail_prep;
	} else {
		int i;
		for (i = 0; i < PySequence_Size(revprops); i++) {
			PyObject *n = PySequence_GetItem(revprops, i);
			char *ns;

			ns = py_object_to_svn_string(n, *pool);
			if (ns == NULL) {
				goto fail_prep;
			}

			if (strcmp(SVN_PROP_REVISION_LOG, ns) &&
				strcmp(SVN_PROP_REVISION_AUTHOR, ns) &&
				strcmp(SVN_PROP_REVISION_DATE, ns)) {
				PyErr_SetString(PyExc_NotImplementedError,
								"fetching custom revision properties not supported");
				goto fail_prep;
			}
		}
	}

	if (include_merged_revisions) {
		PyErr_SetString(PyExc_NotImplementedError,
			"include_merged_revisions not supported in Subversion 1.4");
		goto fail_prep;
	}
#endif

	if (!string_list_to_apr_array(*pool, revprops, apr_revprops)) {
		goto fail_prep;
	}

	return true;

fail_prep:
	apr_pool_destroy(*pool);
fail_pool:
	ra->busy = false;
fail_busy:
	return false;
}

static PyObject *ra_get_log(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char *kwnames[] = { "callback", "paths", "start", "end", "limit",
		"discover_changed_paths", "strict_node_history", "include_merged_revisions", "revprops", NULL };
	PyObject *callback, *paths;
	svn_revnum_t start = 0, end = 0;
	int limit=0;
	bool discover_changed_paths=false, strict_node_history=true,include_merged_revisions=false;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	PyObject *revprops = Py_None;
	apr_pool_t *temp_pool;
	apr_array_header_t *apr_paths;
	apr_array_header_t *apr_revprops;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOll|ibbbO:get_log", kwnames,
						 &callback, &paths, &start, &end, &limit,
						 &discover_changed_paths, &strict_node_history,
						 &include_merged_revisions, &revprops))
		return NULL;

	if (!ra_get_log_prepare(ra, paths, include_merged_revisions,
	revprops, &temp_pool, &apr_paths, &apr_revprops)) {
		return NULL;
	}

#if ONLY_SINCE_SVN(1, 5)
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_log2(ra->ra,
			apr_paths, start, end, limit,
			discover_changed_paths, strict_node_history,
			include_merged_revisions,
			apr_revprops,
			py_svn_log_entry_receiver,
			callback, temp_pool));
#else
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_log(ra->ra,
			apr_paths, start, end, limit,
			discover_changed_paths, strict_node_history, py_svn_log_wrapper,
			callback, temp_pool));
#endif
	apr_pool_destroy(temp_pool);
	Py_RETURN_NONE;
}

/**
 * Obtain the URL of the root of this repository.
 */
static PyObject *ra_get_repos_root(PyObject *self)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	const char *root;
	apr_pool_t *temp_pool;

	if (ra->root == NULL) {
		if (ra_check_busy(ra))
			return NULL;

		temp_pool = Pool(NULL);
		if (temp_pool == NULL)
			return NULL;
#if ONLY_SINCE_SVN(1, 5)
		RUN_RA_WITH_POOL(temp_pool, ra,
						  svn_ra_get_repos_root2(ra->ra, &root, temp_pool));
#else
		RUN_RA_WITH_POOL(temp_pool, ra,
						  svn_ra_get_repos_root(ra->ra, &root, temp_pool));
#endif
		ra->root = svn_uri_canonicalize(root, ra->pool);
		apr_pool_destroy(temp_pool);
	}

	return PyUnicode_FromString(ra->root);
}

/**
 * Obtain the URL of this repository.
 */
static PyObject *ra_get_url(PyObject *self, void *closure)
{
	const char *url;
	apr_pool_t *temp_pool;
	PyObject *r;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;

	if (ra_check_busy(ra))
		return NULL;

#if ONLY_SINCE_SVN(1, 5)
	temp_pool = Pool(NULL);

	RUN_RA_WITH_POOL(temp_pool, ra,
						svn_ra_get_session_url(ra->ra, &url, temp_pool));

	r = PyUnicode_FromString(url);

	apr_pool_destroy(temp_pool);

	return r;
#else
	PyErr_SetString(PyExc_NotImplementedError,
					"svn_ra_get_session_url only supported for svn >= 1.5");
	return NULL;
#endif
}

static PyObject *ra_do_update(PyObject *self, PyObject *args)
{
	svn_revnum_t revision_to_update_to;
	char *update_target;
	bool recurse;
	bool ignore_ancestry = true;
	PyObject *update_editor;
	const REPORTER_T *reporter;
	void *report_baton;
	svn_error_t *err;
	apr_pool_t *temp_pool, *result_pool;
	ReporterObject *ret;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	bool send_copyfrom_args = false;

	if (!PyArg_ParseTuple(args, "lsbO|bb:do_update", &revision_to_update_to, &update_target, &recurse, &update_editor,
						  &send_copyfrom_args, &ignore_ancestry))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

#if ONLY_BEFORE_SVN(1, 8)
	if (!ignore_ancestry) {
		PyErr_SetString(PyExc_NotImplementedError, "ignore_ancestry only supported on svn >= 1.8");
		ra->busy = false;
		return NULL;
	}
#endif

	temp_pool = Pool(NULL);
	if (temp_pool == NULL) {
		ra->busy = false;
		return NULL;
	}

	result_pool = Pool(NULL);
	if (result_pool == NULL) {
		apr_pool_destroy(temp_pool);
		ra->busy = false;
		return NULL;
	}

	Py_INCREF(update_editor);
#if ONLY_SINCE_SVN(1, 8)
	Py_BEGIN_ALLOW_THREADS
	err = svn_ra_do_update3(ra->ra, &reporter,
												  &report_baton,
												  revision_to_update_to,
												  update_target, recurse?svn_depth_infinity:svn_depth_files,
												  send_copyfrom_args,
												  ignore_ancestry,
												  &py_editor, update_editor,
												  result_pool, temp_pool);
#elif ONLY_SINCE_SVN(1, 5)
	Py_BEGIN_ALLOW_THREADS
	err = svn_ra_do_update2(ra->ra, &reporter,
												  &report_baton,
												  revision_to_update_to,
												  update_target, recurse?svn_depth_infinity:svn_depth_files,
												  send_copyfrom_args,
												  &py_editor, update_editor,
												  result_pool);
#else
	if (send_copyfrom_args) {
		PyErr_SetString(PyExc_NotImplementedError, "send_copyfrom_args only supported for svn >= 1.5");
		apr_pool_destroy(temp_pool);
		apr_pool_destroy(result_pool);
		ra->busy = false;
		return NULL;
	}
	Py_BEGIN_ALLOW_THREADS
	err = svn_ra_do_update(ra->ra, &reporter,
		&report_baton, revision_to_update_to,
		update_target, recurse,
		&py_editor, update_editor,
		result_pool);

#endif
	Py_END_ALLOW_THREADS
	apr_pool_destroy(temp_pool);
	if (err != NULL) {
		handle_svn_error(err);
		svn_error_clear(err);
		apr_pool_destroy(result_pool);
		ra->busy = false;
		return NULL;
	}

	ret = PyObject_New(ReporterObject, &Reporter_Type);
	if (ret == NULL) {
		apr_pool_destroy(result_pool);
		ra->busy = false;
		return NULL;
	}
	ret->reporter = reporter;
	ret->report_baton = report_baton;
	ret->pool = result_pool;
	Py_INCREF(ra);
	ret->ra = ra;
	return (PyObject *)ret;
}

static PyObject *ra_do_switch(PyObject *self, PyObject *args)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_revnum_t revision_to_update_to;
	char *update_target;
	bool recurse;
	bool send_copyfrom_args = false;
	bool ignore_ancestry = true;
	char *switch_url;
	PyObject *update_editor;
	const REPORTER_T *reporter;
	void *report_baton;
	apr_pool_t *temp_pool, *result_pool;
	ReporterObject *ret;
	svn_error_t *err;

	if (!PyArg_ParseTuple(args, "lsbsO|bb:do_switch", &revision_to_update_to, &update_target,
						  &recurse, &switch_url, &update_editor, &send_copyfrom_args, &ignore_ancestry))
		return NULL;
	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL) {
		ra->busy = false;
		return NULL;
	}

	result_pool = Pool(NULL);
	if (result_pool == NULL) {
		apr_pool_destroy(temp_pool);
		ra->busy = false;
		return NULL;
	}

	Py_INCREF(update_editor);
	Py_BEGIN_ALLOW_THREADS

#if ONLY_SINCE_SVN(1, 8)
	err = svn_ra_do_switch3(
						ra->ra, &reporter, &report_baton,
						revision_to_update_to, update_target,
						recurse?svn_depth_infinity:svn_depth_files, switch_url,
						send_copyfrom_args, ignore_ancestry,
						&py_editor, update_editor, result_pool, temp_pool);
#elif ONLY_SINCE_SVN(1, 5)
	err = svn_ra_do_switch2(
						ra->ra, &reporter, &report_baton,
						revision_to_update_to, update_target,
						recurse?svn_depth_infinity:svn_depth_files, switch_url, &py_editor,
						update_editor, result_pool);
#else
	err = svn_ra_do_switch(
						ra->ra, &reporter, &report_baton,
						revision_to_update_to, update_target,
						recurse, switch_url, &py_editor,
						update_editor, result_pool);
#endif

	Py_END_ALLOW_THREADS
	apr_pool_destroy(temp_pool);

	if (err != NULL) {
		handle_svn_error(err);
		svn_error_clear(err);
		apr_pool_destroy(result_pool);
		ra->busy = false;
		return NULL;
	}
	ret = PyObject_New(ReporterObject, &Reporter_Type);
	if (ret == NULL) {
		apr_pool_destroy(result_pool);
		ra->busy = false;
		return NULL;
	}
	ret->reporter = reporter;
	ret->report_baton = report_baton;
	ret->pool = result_pool;
	Py_INCREF(ra);
	ret->ra = ra;
	return (PyObject *)ret;
}

static PyObject *ra_do_diff(PyObject *self, PyObject *args)
{
	svn_revnum_t revision_to_update_to;
	char *diff_target, *versus_url;
	PyObject *diff_editor;
	const REPORTER_T *reporter;
	void *report_baton;
	svn_error_t *err;
	apr_pool_t *temp_pool;
	bool ignore_ancestry = false, text_deltas = false, recurse=true;
	ReporterObject *ret;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;

	if (!PyArg_ParseTuple(args, "lssO|bbb:do_diff", &revision_to_update_to, &diff_target, &versus_url, &diff_editor, &recurse, &ignore_ancestry, &text_deltas))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	Py_INCREF(diff_editor);
	Py_BEGIN_ALLOW_THREADS
#if ONLY_SINCE_SVN(1, 5)
	err = svn_ra_do_diff3(ra->ra, &reporter, &report_baton,
												  revision_to_update_to,
												  diff_target, recurse?svn_depth_infinity:svn_depth_files,
												  ignore_ancestry,
												  text_deltas,
												  versus_url,
												  &py_editor, diff_editor,
												  temp_pool);
#else
	err = svn_ra_do_diff2(ra->ra, &reporter, &report_baton,
												  revision_to_update_to,
												  diff_target, recurse,
												  ignore_ancestry,
												  text_deltas,
												  versus_url,
												  &py_editor, diff_editor,
												  temp_pool);
#endif
	Py_END_ALLOW_THREADS
	if (err != NULL) {
		handle_svn_error(err);
		svn_error_clear(err);
		apr_pool_destroy(temp_pool);
		ra->busy = false;
		return NULL;
	}

	ret = PyObject_New(ReporterObject, &Reporter_Type);
	if (ret == NULL)
		return NULL;
	ret->reporter = reporter;
	ret->report_baton = report_baton;
	ret->pool = temp_pool;
	Py_INCREF(ra);
	ret->ra = ra;
	return (PyObject *)ret;
}

static PyObject *ra_replay(PyObject *self, PyObject *args)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	apr_pool_t *temp_pool;
	svn_revnum_t revision, low_water_mark;
	PyObject *update_editor;
	bool send_deltas = true;

	if (!PyArg_ParseTuple(args, "llO|b:replay", &revision, &low_water_mark, &update_editor, &send_deltas))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	/* Only INCREF here, py_editor takes care of the DECREF */
	Py_INCREF(update_editor);
	RUN_RA_WITH_POOL(temp_pool, ra,
					  svn_ra_replay(ra->ra, revision, low_water_mark,
									send_deltas, &py_editor, update_editor,
									temp_pool));
	apr_pool_destroy(temp_pool);

	Py_RETURN_NONE;
}

#if ONLY_SINCE_SVN(1, 5)
static svn_error_t *py_revstart_cb(svn_revnum_t revision, void *replay_baton,
   const svn_delta_editor_t **editor, void **edit_baton, apr_hash_t *rev_props, apr_pool_t *pool)
{
	PyObject *cbs = (PyObject *)replay_baton;
	PyObject *py_start_fn = PyTuple_GetItem(cbs, 0);
	PyObject *py_revprops = prop_hash_to_dict(rev_props);
	PyObject *ret;
	PyGILState_STATE state = PyGILState_Ensure();

	ret = PyObject_CallFunction(py_start_fn, "lO", revision, py_revprops);
	CB_CHECK_PYRETVAL(ret);

	*editor = &py_editor;
	*edit_baton = ret;

	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_revfinish_cb(svn_revnum_t revision, void *replay_baton,
									const svn_delta_editor_t *editor, void *edit_baton,
									apr_hash_t *rev_props, apr_pool_t *pool)
{
	PyObject *cbs = (PyObject *)replay_baton;
	PyObject *py_finish_fn = PyTuple_GetItem(cbs, 1);
	PyObject *py_revprops = prop_hash_to_dict(rev_props);
	PyObject *ret;
	PyGILState_STATE state = PyGILState_Ensure();

	ret = PyObject_CallFunction(py_finish_fn, "lOO", revision, py_revprops, edit_baton);
	CB_CHECK_PYRETVAL(ret);

	Py_DECREF((PyObject *)edit_baton);
	Py_DECREF(ret);

	PyGILState_Release(state);
	return NULL;
}
#endif

static PyObject *ra_replay_range(PyObject *self, PyObject *args)
{
#if ONLY_SINCE_SVN(1, 5)
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	apr_pool_t *temp_pool;
	svn_revnum_t start_revision, end_revision, low_water_mark;
	PyObject *cbs;
	bool send_deltas = true;

	if (!PyArg_ParseTuple(args, "lllO|b:replay_range", &start_revision, &end_revision, &low_water_mark, &cbs, &send_deltas))
		return NULL;

	if (!PyTuple_Check(cbs)) {
		PyErr_SetString(PyExc_TypeError, "Expected tuple with callbacks");
		return NULL;
	}

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	Py_INCREF(cbs);
	RUN_RA_WITH_POOL(temp_pool, ra,
					  svn_ra_replay_range(ra->ra, start_revision, end_revision, low_water_mark,
									send_deltas, py_revstart_cb, py_revfinish_cb, cbs,
									temp_pool));
	apr_pool_destroy(temp_pool);

	Py_RETURN_NONE;
#else
	PyErr_SetString(PyExc_NotImplementedError,
		"svn_ra_replay not available with Subversion 1.4");
	return NULL;
#endif
}

static PyObject *ra_rev_proplist(PyObject *self, PyObject *args)
{
	apr_pool_t *temp_pool;
	apr_hash_t *props;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_revnum_t rev;
	PyObject *py_props;
	if (!PyArg_ParseTuple(args, "l:rev_proplist", &rev))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	RUN_RA_WITH_POOL(temp_pool, ra,
					  svn_ra_rev_proplist(ra->ra, rev, &props, temp_pool));
	py_props = prop_hash_to_dict(props);
	apr_pool_destroy(temp_pool);
	return py_props;
}

static PyObject *get_commit_editor(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char *kwnames[] = { "revprops", "callback", "lock_tokens", "keep_locks",
		NULL };
	PyObject *revprops, *commit_callback = Py_None, *lock_tokens = Py_None;
	bool keep_locks = false;
	apr_pool_t *pool;
	const svn_delta_editor_t *editor;
	void *edit_baton;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	apr_hash_t *hash_lock_tokens;
#if ONLY_SINCE_SVN(1, 5)
	apr_hash_t *hash_revprops;
#else
	char *log_msg;
	PyObject *py_log_msg;
#endif
	svn_error_t *err;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOb:get_commit_editor",
		kwnames, &revprops, &commit_callback, &lock_tokens, &keep_locks))
		return NULL;

	pool = Pool(NULL);
	if (pool == NULL)
		goto fail_pool;
	if (lock_tokens == Py_None) {
		hash_lock_tokens = NULL;
	} else {
		Py_ssize_t idx = 0;
		PyObject *k, *v;
		hash_lock_tokens = apr_hash_make(pool);
		while (PyDict_Next(lock_tokens, &idx, &k, &v)) {
			if (!PyBytes_Check(k)) {
				PyErr_SetString(PyExc_TypeError, "token not bytes");
				goto fail_prep;
			}
			apr_hash_set(hash_lock_tokens, PyBytes_AsString(k),
						 PyBytes_Size(k), PyBytes_AsString(v));
		}
	}

	if (!PyDict_Check(revprops)) {
		PyErr_SetString(PyExc_TypeError, "Expected dictionary with revision properties");
		goto fail_prep;
	}

	if (ra_check_busy(ra))
		goto fail_prep;

	Py_INCREF(commit_callback);

#if ONLY_SINCE_SVN(1, 5)
	hash_revprops = prop_dict_to_hash(pool, revprops);
	if (hash_revprops == NULL) {
		goto fail_prep2;
	}
	Py_BEGIN_ALLOW_THREADS
	err = svn_ra_get_commit_editor3(ra->ra, &editor,
		&edit_baton,
		hash_revprops, py_commit_callback,
		commit_callback, hash_lock_tokens, keep_locks, pool);
#else
	/* Check that revprops has only one member named SVN_PROP_REVISION_LOG */
	if (PyDict_Size(revprops) != 1) {
		PyErr_SetString(PyExc_ValueError, "Only svn:log can be set with Subversion 1.4");
		goto fail_prep2;
	}

	py_log_msg = PyDict_GetItemString(revprops, SVN_PROP_REVISION_LOG);
	if (py_log_msg == NULL) {
		PyErr_SetString(PyExc_ValueError, "Only svn:log can be set with Subversion 1.4.");
		goto fail_prep2;
	}

	log_msg = py_object_to_svn_string(py_log_msg, pool);
	if (log_msg == NULL) {
		goto fail_prep2;
	}

	Py_BEGIN_ALLOW_THREADS
	err = svn_ra_get_commit_editor2(ra->ra, &editor,
		&edit_baton,
		log_msg, py_commit_callback,
		commit_callback, hash_lock_tokens, keep_locks, pool);
#endif
	Py_END_ALLOW_THREADS

	if (err != NULL) {
		handle_svn_error(err);
		svn_error_clear(err);
		goto fail_prep2;
	}

	Py_INCREF(ra);
	return new_editor_object(NULL, editor, edit_baton, pool,
			  &Editor_Type, ra_done_handler, ra, commit_callback);

fail_prep2:
	Py_DECREF(commit_callback);
	ra->busy = false;
fail_prep:
	apr_pool_destroy(pool);
fail_pool:
	return NULL;
}

static PyObject *ra_change_rev_prop(PyObject *self, PyObject *args)
{
	svn_revnum_t rev;
	char *name;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	char *value, *oldvalue = NULL;
	int vallen, oldvallen = -2;
	apr_pool_t *temp_pool;
	svn_string_t *val_string;
	const svn_string_t *old_val_string;
	const svn_string_t *const *old_val_string_p;

	if (!PyArg_ParseTuple(args, "lss#|z#:change_rev_prop", &rev, &name, &value,
			&vallen, &oldvalue, &oldvallen))
		return NULL;
	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	val_string = svn_string_ncreate(value, vallen, temp_pool);
#if ONLY_BEFORE_SVN(1, 7)
	if (oldvallen != -2) {
		PyErr_SetString(PyExc_NotImplementedError,
						"Atomic revision property updates only supported on svn >= 1.7");
		ra->busy = false;
		apr_pool_destroy(temp_pool);
		return NULL;
	}
#else
	if (oldvallen != -2) {
		if (oldvalue == NULL) {
			old_val_string = NULL;
		} else {
			old_val_string = svn_string_ncreate(oldvalue, oldvallen, temp_pool);
		}
		old_val_string_p = &old_val_string;
	} else {
		old_val_string_p = NULL;
	}
#endif
#if ONLY_SINCE_SVN(1, 7)
	RUN_RA_WITH_POOL(temp_pool, ra,
					  svn_ra_change_rev_prop2(ra->ra, rev, name, old_val_string_p, val_string,
											 temp_pool));
#else
	RUN_RA_WITH_POOL(temp_pool, ra,
					  svn_ra_change_rev_prop(ra->ra, rev, name, val_string,
											 temp_pool));
#endif
	apr_pool_destroy(temp_pool);
	Py_RETURN_NONE;
}


static PyObject *ra_get_dir(PyObject *self, PyObject *args, PyObject *kwargs)
{
	apr_pool_t *temp_pool;
	apr_hash_t *dirents;
	apr_hash_index_t *idx;
	apr_hash_t *props;
	svn_revnum_t fetch_rev;
	const char *key;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_dirent_t *dirent;
	apr_ssize_t klen;
	const char *path;
	PyObject *py_path;
	svn_revnum_t revision = -1;
	unsigned int dirent_fields = 0;
	PyObject *py_dirents, *py_props;
	char *kwnames[] = { "path", "revision", "fields", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|lI:get_dir", kwnames,
                                     &py_path, &revision, &dirent_fields))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	if (revision != SVN_INVALID_REVNUM)
		fetch_rev = revision;

	path = py_object_to_svn_relpath(py_path, temp_pool);
	if (path == NULL)
		return NULL;

	/* Yuck. Subversion doesn't like leading slashes.. */
	while (*path == '/') path++;

	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_dir2(ra->ra, &dirents, &fetch_rev, &props,
					 path, revision, dirent_fields, temp_pool));

	if (dirents == NULL) {
		py_dirents = Py_None;
		Py_INCREF(py_dirents);
	} else {
		py_dirents = PyDict_New();
		if (py_dirents == NULL) {
			goto fail;
		}
		idx = apr_hash_first(temp_pool, dirents);
		while (idx != NULL) {
			PyObject *item, *pykey;
			apr_hash_this(idx, (const void **)&key, &klen, (void **)&dirent);
			item = py_dirent(dirent, dirent_fields);
			if (item == NULL) {
				goto fail_dirents;
			}
			if (key == NULL) {
				pykey = Py_None;
				Py_INCREF(pykey);
			} else {
				pykey = PyUnicode_FromString((char *)key);
			}
			if (PyDict_SetItem(py_dirents, pykey, item) != 0) {
				Py_DECREF(item);
				Py_DECREF(pykey);
				goto fail_dirents;
			}
			Py_DECREF(pykey);
			Py_DECREF(item);
			idx = apr_hash_next(idx);
		}
	}

	py_props = prop_hash_to_dict(props);
	if (py_props == NULL) {
		goto fail_dirents;
	}
	apr_pool_destroy(temp_pool);
	return Py_BuildValue("(NlN)", py_dirents, fetch_rev, py_props);

fail_dirents:
	Py_DECREF(py_dirents);
fail:
	apr_pool_destroy(temp_pool);
	return NULL;
}

static PyObject *ra_get_file(PyObject *self, PyObject *args)
{
	const char *path;
	PyObject *py_path;
	svn_revnum_t revision = -1;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	apr_hash_t *props;
	svn_revnum_t fetch_rev;
	PyObject *py_stream, *py_props;
	apr_pool_t *temp_pool;
	svn_stream_t *stream;

	if (!PyArg_ParseTuple(args, "OO|l:get_file", &py_path, &py_stream, &revision))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	if (revision != SVN_INVALID_REVNUM)
		fetch_rev = revision;

	path = py_object_to_svn_relpath(py_path, temp_pool);
	if (path == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	/* Yuck. Subversion doesn't like leading slashes.. */
	while (*path == '/') path++;

	stream = new_py_stream(temp_pool, py_stream);
	if (stream == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_file(ra->ra, path, revision,
													stream,
													&fetch_rev, &props, temp_pool));

	py_props = prop_hash_to_dict(props);
	if (py_props == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	apr_pool_destroy(temp_pool);

	return Py_BuildValue("(lN)", fetch_rev, py_props);
}

static PyObject *ra_get_lock(PyObject *self, PyObject *args)
{
	char *path;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_lock_t *lock;
	apr_pool_t *temp_pool;

	if (!PyArg_ParseTuple(args, "s:get_lock", &path))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	RUN_RA_WITH_POOL(temp_pool, ra,
				  svn_ra_get_lock(ra->ra, &lock, path, temp_pool));
	apr_pool_destroy(temp_pool);
	return wrap_lock(lock);
}

static PyObject *ra_check_path(PyObject *self, PyObject *args)
{
	const char *path;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_revnum_t revision;
	svn_node_kind_t kind;
	PyObject *py_path;
	apr_pool_t *temp_pool;

	if (!PyArg_ParseTuple(args, "Ol:check_path", &py_path, &revision))
		return NULL;
	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	path = py_object_to_svn_relpath(py_path, temp_pool);
	if (path == NULL)
		return NULL;

	if (ra_check_svn_path(path))
		return NULL;

	RUN_RA_WITH_POOL(temp_pool, ra,
					  svn_ra_check_path(ra->ra, path, revision, &kind,
					 temp_pool));
	apr_pool_destroy(temp_pool);
#if PY_MAJOR_VERSION < 3
	return PyInt_FromLong(kind);
#else
	return PyLong_FromLong(kind);
#endif
}

static PyObject *ra_stat(PyObject *self, PyObject *args)
{
	const char *path;
	PyObject *py_path;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	PyObject *ret;
	svn_revnum_t revision;
	svn_dirent_t *dirent;
	apr_pool_t *temp_pool;

	if (!PyArg_ParseTuple(args, "Ol:stat", &py_path, &revision))
		return NULL;
	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	path = py_object_to_svn_relpath(py_path, temp_pool);
	if (path == NULL)
		return NULL;

	if (ra_check_svn_path(path))
		return NULL;

	RUN_RA_WITH_POOL(temp_pool, ra,
					  svn_ra_stat(ra->ra, path, revision, &dirent,
					 temp_pool));
	ret = py_dirent(dirent, SVN_DIRENT_ALL);
	apr_pool_destroy(temp_pool);
	return ret;
}

static PyObject *ra_has_capability(PyObject *self, PyObject *args)
{
#if ONLY_SINCE_SVN(1, 5)
	char *capability;
	apr_pool_t *temp_pool;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	int has = 0;

	if (!PyArg_ParseTuple(args, "s:has_capability", &capability))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	RUN_RA_WITH_POOL(temp_pool, ra,
					  svn_ra_has_capability(ra->ra, &has, capability, temp_pool));
	apr_pool_destroy(temp_pool);
	return PyBool_FromLong(has);
#else
	PyErr_SetString(PyExc_NotImplementedError, "has_capability is only supported in Subversion >= 1.5");
	return NULL;
#endif
}

static PyObject *ra_unlock(PyObject *self, PyObject *args)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	PyObject *path_tokens, *lock_func, *k, *v;
	bool break_lock;
	Py_ssize_t idx;
	apr_pool_t *temp_pool;
	apr_hash_t *hash_path_tokens;

	if (!PyArg_ParseTuple(args, "ObO:unlock", &path_tokens, &break_lock, &lock_func))
		goto fail_busy;

	if (ra_check_busy(ra))
		goto fail_busy;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		goto fail_pool;
	hash_path_tokens = apr_hash_make(temp_pool);
	while (PyDict_Next(path_tokens, &idx, &k, &v)) {
		if (!PyBytes_Check(k)) {
			PyErr_SetString(PyExc_TypeError, "token not bytes");
			goto fail_dict;
		}
		if (PyUnicode_Check(v)) {
			v = PyUnicode_AsUTF8String(v);
		} else {
			Py_INCREF(v);
		}
		if (!PyBytes_Check(v)) {
			PyErr_SetString(PyExc_TypeError, "path not bytestring or unicode string");
			goto fail_dict;
		}

		apr_hash_set(hash_path_tokens, PyBytes_AsString(k), PyBytes_Size(k), (char *)PyBytes_AsString(v));
	}
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_unlock(ra->ra, hash_path_tokens, break_lock,
					 py_lock_func, lock_func, temp_pool));

	apr_pool_destroy(temp_pool);
	Py_RETURN_NONE;

fail_dict:
	apr_pool_destroy(temp_pool);
fail_pool:
	ra->busy = false;
fail_busy:
	return NULL;
}

static PyObject *ra_lock(PyObject *self, PyObject *args)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	PyObject *path_revs;
	char *comment;
	int steal_lock;
	PyObject *lock_func, *k, *v;
	apr_pool_t *temp_pool;
	apr_hash_t *hash_path_revs;
	svn_revnum_t *rev;
	Py_ssize_t idx = 0;

	if (!PyArg_ParseTuple(args, "OsbO:lock", &path_revs, &comment, &steal_lock,
						  &lock_func))
		goto fail_busy;

	if (ra_check_busy(ra))
		goto fail_busy;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		goto fail_pool;
	if (path_revs == Py_None) {
		hash_path_revs = NULL;
	} else {
		hash_path_revs = apr_hash_make(temp_pool);
	}

	while (PyDict_Next(path_revs, &idx, &k, &v)) {
		rev = (svn_revnum_t *)apr_palloc(temp_pool, sizeof(svn_revnum_t));
		*rev = py_to_svn_revnum(v);
		if (*rev == -1 && PyErr_Occurred()) {
			goto fail_prep;
		}
		if (!PyBytes_Check(k)) {
			PyErr_SetString(PyExc_TypeError, "token not bytes");
			goto fail_prep;
		}
		apr_hash_set(hash_path_revs, PyBytes_AsString(k), PyBytes_Size(k), rev);
	}
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_lock(ra->ra, hash_path_revs, comment, steal_lock,
					 py_lock_func, lock_func, temp_pool));
	apr_pool_destroy(temp_pool);
	Py_RETURN_NONE;

fail_prep:
	apr_pool_destroy(temp_pool);
fail_pool:
	ra->busy = false;
fail_busy:
	return NULL;
}

static PyObject *ra_get_locks(PyObject *self, PyObject *args)
{
	const char *path;
	PyObject *py_path;
	apr_pool_t *temp_pool;
	apr_hash_t *hash_locks;
	apr_hash_index_t *idx;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_depth_t depth = svn_depth_infinity;
	char *key;
	apr_ssize_t klen;
	svn_lock_t *lock;
	PyObject *ret;

	if (!PyArg_ParseTuple(args, "O|i:get_locks", &py_path, &depth))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	path = py_object_to_svn_relpath(py_path, temp_pool);
	if (path == NULL)
		return NULL;

	if (ra_check_svn_path(path))
		return NULL;

#if ONLY_SINCE_SVN(1, 7)
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_locks2(ra->ra, &hash_locks, path, depth, temp_pool));
#else
	if (depth != svn_depth_infinity) {
		PyErr_SetString(PyExc_NotImplementedError,
						"depth != infinity only supported for svn >= 1.7");
		return NULL;
	}

	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_locks(ra->ra, &hash_locks, path, temp_pool));
#endif

	ret = PyDict_New();
	if (ret == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}
	for (idx = apr_hash_first(temp_pool, hash_locks); idx != NULL;
		 idx = apr_hash_next(idx)) {
		PyObject *pyval;
		apr_hash_this(idx, (const void **)&key, &klen, (void **)&lock);
		pyval = pyify_lock(lock);
		if (pyval == NULL) {
			Py_DECREF(ret);
			apr_pool_destroy(temp_pool);
			return NULL;
		}
		if (PyDict_SetItemString(ret, key, pyval) != 0) {
			apr_pool_destroy(temp_pool);
			Py_DECREF(pyval);
			Py_DECREF(ret);
			return NULL;
		}
		Py_DECREF(pyval);
	}

	apr_pool_destroy(temp_pool);
	return ret;
}

static PyObject *ra_get_locations(PyObject *self, PyObject *args)
{
	const char *path;
	PyObject *py_path;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_revnum_t peg_revision;
	PyObject *location_revisions;
	apr_pool_t *temp_pool;
	apr_hash_t *hash_locations;
	apr_hash_index_t *idx;
	svn_revnum_t *key;
	PyObject *ret;
	apr_ssize_t klen;
	char *val;

	if (!PyArg_ParseTuple(args, "OlO:get_locations", &py_path, &peg_revision, &location_revisions))
		goto fail_busy;

	if (ra_check_busy(ra))
		goto fail_busy;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		goto fail_pool;

	path = py_object_to_svn_relpath(py_path, temp_pool);
	if (path == NULL)
		goto fail_dict;

	if (ra_check_svn_path(path))
		goto fail_dict;

	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_locations(ra->ra, &hash_locations,
					path, peg_revision,
					revnum_list_to_apr_array(temp_pool, location_revisions),
					temp_pool));
	ret = PyDict_New();
	if (ret == NULL) {
		goto fail_dict;
	}

	for (idx = apr_hash_first(temp_pool, hash_locations); idx != NULL;
		idx = apr_hash_next(idx)) {
		PyObject *py_key, *py_val;
		apr_hash_this(idx, (const void **)&key, &klen, (void **)&val);
		py_key = py_from_svn_revnum(*key);
		if (py_key == NULL) {
			goto fail_conv;
		}
		py_val = PyUnicode_FromString(val);
		if (py_val == NULL) {
			goto fail_conv;
		}
		if (PyDict_SetItem(ret, py_key, py_val) != 0) {
			goto fail_conv;
		}
	}
	apr_pool_destroy(temp_pool);
	return ret;

fail_conv:
	Py_DECREF(ret);
fail_dict:
	apr_pool_destroy(temp_pool);
fail_pool:
	ra->busy = false;
fail_busy:
	return NULL;
}

#if ONLY_SINCE_SVN(1, 5)
static PyObject *range_to_tuple(svn_merge_range_t *range)
{
	return Py_BuildValue("(llb)", range->start, range->end, range->inheritable?true:false);
}

static PyObject *merge_rangelist_to_list(apr_array_header_t *rangelist)
{
	PyObject *ret;
	int i;

	ret = PyList_New(rangelist->nelts);
	if (ret == NULL)
		return NULL;

	for (i = 0; i < rangelist->nelts; i++) {
		PyObject *pyval;
		pyval = range_to_tuple(APR_ARRAY_IDX(rangelist, i, svn_merge_range_t *));
		if (pyval == NULL) {
			Py_DECREF(ret);
			return NULL;
		}
		if (PyList_SetItem(ret, i, pyval) != 0) {
			Py_DECREF(ret);
			Py_DECREF(pyval);
			return NULL;
		}
	}

	return ret;
}

static PyObject *mergeinfo_to_dict(svn_mergeinfo_t mergeinfo, apr_pool_t *temp_pool)
{
	PyObject *ret;
	char *key;
	apr_ssize_t klen;
	apr_hash_index_t *idx;
	apr_array_header_t *range;

	ret = PyDict_New();
	if (ret == NULL) {
		return NULL;
	}

	for (idx = apr_hash_first(temp_pool, mergeinfo); idx != NULL;
		idx = apr_hash_next(idx)) {
		PyObject *pyval;
		apr_hash_this(idx, (const void **)&key, &klen, (void **)&range);
		pyval = merge_rangelist_to_list(range);
		if (pyval == NULL) {
			Py_DECREF(ret);
			return NULL;
		}
		if (PyDict_SetItemString(ret, key, pyval) != 0) {
			Py_DECREF(ret);
			Py_DECREF(pyval);
			return NULL;
		}
		Py_DECREF(pyval);
	}

	return ret;
}
#endif

static PyObject *ra_mergeinfo(PyObject *self, PyObject *args)
{
#if ONLY_SINCE_SVN(1, 5)
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	apr_array_header_t *apr_paths;
	apr_pool_t *temp_pool;
	svn_mergeinfo_catalog_t catalog;
	apr_ssize_t klen;
	apr_hash_index_t *idx;
	svn_mergeinfo_t val;
	char *key;
	PyObject *ret;
	svn_revnum_t revision = -1;
	PyObject *paths;
	svn_mergeinfo_inheritance_t inherit = svn_mergeinfo_explicit;
	bool include_descendants;

	if (!PyArg_ParseTuple(args, "O|lib:mergeinfo", &paths, &revision, &inherit, &include_descendants))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	if (!relpath_list_to_apr_array(temp_pool, paths, &apr_paths)) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_mergeinfo(ra->ra,
                     &catalog, apr_paths, revision, inherit,
					 include_descendants,
                     temp_pool));

	ret = PyDict_New();
	if (ret == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	if (catalog != NULL) {
		for (idx = apr_hash_first(temp_pool, catalog); idx != NULL;
			idx = apr_hash_next(idx)) {
			PyObject *pyval;
			apr_hash_this(idx, (const void **)&key, &klen, (void **)&val);
			pyval = mergeinfo_to_dict(val, temp_pool);
			if (pyval == NULL) {
				apr_pool_destroy(temp_pool);
				Py_DECREF(ret);
				return NULL;
			}
			if (PyDict_SetItemString(ret, key, pyval) != 0) {
				apr_pool_destroy(temp_pool);
				Py_DECREF(pyval);
				Py_DECREF(ret);
				return NULL;
			}

			Py_DECREF(pyval);
		}
	}

	apr_pool_destroy(temp_pool);

	return ret;
#else
	PyErr_SetString(PyExc_NotImplementedError, "mergeinfo is only supported in Subversion >= 1.5");
	return NULL;
#endif
}

#if ONLY_SINCE_SVN(1, 5)
static svn_error_t *py_location_segment_receiver(svn_location_segment_t *segment, void *baton, apr_pool_t *pool)
{
	PyObject *fn = baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();

	ret = PyObject_CallFunction(fn, "llz", segment->range_start, segment->range_end, segment->path);
	CB_CHECK_PYRETVAL(ret);
	Py_XDECREF(ret);
	PyGILState_Release(state);
	return NULL;
}
#endif

static PyObject *ra_get_location_segments(PyObject *self, PyObject *args)
{
#if ONLY_SINCE_SVN(1, 5)
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	svn_revnum_t peg_revision, start_revision, end_revision;
	const char *path;
	PyObject *py_path;
	PyObject *py_rcvr;
	apr_pool_t *temp_pool;

	if (!PyArg_ParseTuple(args, "OlllO:get_location_segments", &py_path,
			&peg_revision, &start_revision, &end_revision, &py_rcvr))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

	path = py_object_to_svn_relpath(py_path, temp_pool);
	if (path == NULL)
		return NULL;

	if (ra_check_svn_path(path))
		return NULL;

	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_location_segments(ra->ra,
					 path, peg_revision, start_revision, end_revision,
					 py_location_segment_receiver,
					 py_rcvr, temp_pool));

	apr_pool_destroy(temp_pool);
	Py_RETURN_NONE;
#else
	PyErr_SetString(PyExc_NotImplementedError, "mergeinfo is only supported in Subversion >= 1.5");
	return NULL;
#endif
}


static PyObject *ra_get_file_revs(PyObject *self, PyObject *args)
{
	char *path;
	svn_revnum_t start, end;
	PyObject *file_rev_handler;
	apr_pool_t *temp_pool;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	bool include_merged_revisions = false;

	if (!PyArg_ParseTuple(args, "sllO|b:get_file_revs", &path, &start,
			&end, &file_rev_handler, &include_merged_revisions))
		return NULL;

	if (ra_check_svn_path(path))
		return NULL;

	if (ra_check_busy(ra))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;

#if ONLY_SINCE_SVN(1, 5)
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_file_revs2(ra->ra, path, start, end,
				include_merged_revisions,
				py_file_rev_handler, (void *)file_rev_handler,
					temp_pool));
#else
	if (include_merged_revisions) {
		PyErr_SetString(PyExc_NotImplementedError,
						"include_merged_revisions only supported with svn >= 1.5");
		apr_pool_destroy(temp_pool);
		return NULL;
	}
	RUN_RA_WITH_POOL(temp_pool, ra, svn_ra_get_file_revs(ra->ra, path, start, end,
				py_ra_file_rev_handler, (void *)file_rev_handler, temp_pool));
#endif

	apr_pool_destroy(temp_pool);

	Py_RETURN_NONE;
}

static void ra_dealloc(PyObject *self)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	Py_XDECREF(ra->client_string_func);
	Py_XDECREF(ra->progress_func);
	Py_XDECREF(ra->auth);
	apr_pool_destroy(ra->pool);
	PyObject_Del(self);
}

static PyObject *ra_repr(PyObject *self)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	return PyRepr_FromFormat("RemoteAccess(\"%s\")", ra->url);
}

static int ra_set_progress_func(PyObject *self, PyObject *value, void *closure)
{
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	Py_XDECREF(ra->progress_func);
	ra->progress_func = value;
	Py_INCREF(ra->progress_func);
	return 0;
}

static PyGetSetDef ra_getsetters[] = {
	{ "progress_func", NULL, ra_set_progress_func, NULL },
	{ NULL }
};

#include "_ra_iter_log.c"

static PyMethodDef ra_methods[] = {
	{ "get_file_revs", ra_get_file_revs, METH_VARARGS,
		"S.get_file_revs(path, start_rev, end_revs, handler)" },
	{ "get_locations", ra_get_locations, METH_VARARGS,
		"S.get_locations(path, peg_revision, location_revisions)" },
	{ "get_locks", ra_get_locks, METH_VARARGS,
		"S.get_locks(path, depth=DEPTH_INFINITY)" },
	{ "lock", ra_lock, METH_VARARGS,
		"S.lock(path_revs, comment, steal_lock, lock_func)\n" },
	{ "unlock", ra_unlock, METH_VARARGS,
		"S.unlock(path_tokens, break_lock, lock_func)\n" },
	{ "mergeinfo", ra_mergeinfo, METH_VARARGS,
		"S.mergeinfo(paths, revision, inherit, include_descendants)\n" },
	{ "get_location_segments", ra_get_location_segments, METH_VARARGS,
		"S.get_location_segments(path, peg_revision, start_revision, "
			"end_revision, rcvr)\n"
		"The receiver is called as rcvr(range_start, range_end, path)\n"
	},
	{ "has_capability", ra_has_capability, METH_VARARGS,
		"S.has_capability(name) -> bool\n"
		"Check whether the specified capability is supported by the client and server" },
	{ "check_path", ra_check_path, METH_VARARGS,
		"S.check_path(path, revnum) -> node_kind\n"
		"Check the type of a path (one of NODE_DIR, NODE_FILE, NODE_UNKNOWN)" },
	{ "stat", ra_stat, METH_VARARGS,
		"S.stat(path, revnum) -> dirent\n" },
	{ "get_lock", ra_get_lock, METH_VARARGS,
		"S.get_lock(path) -> lock\n"
	},
	{ "get_dir", (PyCFunction)ra_get_dir, METH_VARARGS|METH_KEYWORDS,
		"S.get_dir(path, revision, dirent_fields=-1) -> (dirents, fetched_rev, properties)\n"
		"Get the contents of a directory. "},
	{ "get_file", ra_get_file, METH_VARARGS,
		"S.get_file(path, stream, revnum=-1) -> (fetched_rev, properties)\n"
		"Fetch a file. The contents will be written to stream." },
	{ "change_rev_prop", ra_change_rev_prop, METH_VARARGS,
		"S.change_rev_prop(revnum, name, value)\n"
		"Change a revision property" },
	{ "get_commit_editor", (PyCFunction)get_commit_editor, METH_VARARGS|METH_KEYWORDS,
		"S.get_commit_editor(revprops, commit_callback, lock_tokens, keep_locks) -> editor\n"
	},
	{ "rev_proplist", ra_rev_proplist, METH_VARARGS,
		"S.rev_proplist(revnum) -> properties\n"
		"Return a dictionary with the properties set on the specified revision" },
	{ "replay", ra_replay, METH_VARARGS,
		"S.replay(revision, low_water_mark, update_editor, send_deltas=True)\n"
		"Replay a revision, reporting changes to update_editor." },
	{ "replay_range", ra_replay_range, METH_VARARGS,
		"S.replay_range(start_rev, end_rev, low_water_mark, cbs, send_deltas=True)\n"
		"Replay a range of revisions, reporting them to an update editor.\n"
		"cbs is a two-tuple with two callbacks:\n"
		"- start_rev_cb(revision, revprops) -> editor\n"
		"- finish_rev_cb(revision, revprops, editor)\n"
	},
	{ "do_switch", ra_do_switch, METH_VARARGS,
		"S.do_switch(revision_to_update_to, update_target, recurse, switch_url, update_editor, send_copyfrom_args=False, ignore_ancestry=True)\n" },
	{ "do_update", ra_do_update, METH_VARARGS,
		"S.do_update(revision_to_update_to, update_target, recurse, update_editor, send_copyfrom_args=False, ignore_ancestry=True)\n" },
	{ "do_diff", ra_do_diff, METH_VARARGS,
		"S.do_diff(revision_to_update_to, diff_target, versus_url, diff_editor, recurse, ignore_ancestry, text_deltas) -> Reporter object\n"
	},
	{ "get_repos_root", (PyCFunction)ra_get_repos_root, METH_NOARGS,
		"S.get_repos_root() -> url\n"
		"Return the URL to the root of the repository." },
	{ "get_url", (PyCFunction)ra_get_url, METH_NOARGS,
		"S.get_url() -> url\n"
		"Return the URL of the repository." },
	{ "get_log", (PyCFunction)ra_get_log, METH_VARARGS|METH_KEYWORDS,
		"S.get_log(callback, paths, start, end, limit=0, "
		"discover_changed_paths=False, strict_node_history=True, "
		"include_merged_revisions=False, revprops=None)\n"
		"The callback is passed three or four arguments:\n"
		"callback(changed_paths, revision, revprops[, has_children])\n"
		"The changed_paths argument may be None, or a dictionary mapping each\n"
		"path to a tuple:\n"
		"(action, from_path, from_rev)\n"
	},
	{ "iter_log", (PyCFunction)ra_iter_log, METH_VARARGS|METH_KEYWORDS,
		"S.iter_log(paths, start, end, limit=0, "
		"discover_changed_paths=False, strict_node_history=True, "
		"include_merged_revisions=False, revprops=None)\n"
		"Yields tuples of three or four elements:\n"
		"(changed_paths, revision, revprops[, has_children])\n"
		"The changed_paths element may be None, or a dictionary mapping each\n"
		"path to a tuple:\n"
		"(action, from_path, from_rev, node_kind)\n"
		"This method collects the log entries in another thread. Before calling\n"
		"any further methods, make sure the thread has completed by running the\n"
		"iterator to exhaustion (i.e. until StopIteration is raised, the \"for\"\n"
		"loop finishes, etc).\n"
	},
	{ "get_latest_revnum", (PyCFunction)ra_get_latest_revnum, METH_NOARGS,
		"S.get_latest_revnum() -> int\n"
		"Return the last revision committed in the repository." },
	{ "reparent", ra_reparent, METH_VARARGS,
		"S.reparent(url)\n"
		"Reparent to a new URL" },
	{ "get_uuid", (PyCFunction)ra_get_uuid, METH_NOARGS,
		"S.get_uuid() -> uuid\n"
		"Return the UUID of the repository." },
	{ NULL, }
};

static PyMemberDef ra_members[] = {
	{ "busy", T_BYTE, offsetof(RemoteAccessObject, busy), READONLY,
		"Whether this connection is in use at the moment" },
	{ "url", T_STRING, offsetof(RemoteAccessObject, url), READONLY,
		"URL this connection is to" },
	{ "corrected_url", T_STRING, offsetof(RemoteAccessObject, corrected_url), READONLY,
		"Corrected URL" },
	{ NULL, }
};

static PyTypeObject RemoteAccess_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.RemoteAccess", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(RemoteAccessObject),
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

	/* Methods to implement standard operations */

	ra_dealloc, /*	destructor tp_dealloc;	*/
	0, /*	printfunc tp_print;	*/
	NULL, /*	getattrfunc tp_getattr;	*/
	NULL, /*	setattrfunc tp_setattr;	*/
	NULL, /*	cmpfunc tp_compare;	*/
	ra_repr, /*	reprfunc tp_repr;	*/

	/* Method suites for standard classes */

	NULL, /*	PyNumberMethods *tp_as_number;	*/
	NULL, /*	PySequenceMethods *tp_as_sequence;	*/
	NULL, /*	PyMappingMethods *tp_as_mapping;	*/

	/* More standard operations (here for binary compatibility) */

	NULL, /*	hashfunc tp_hash;	*/
	NULL, /*	ternaryfunc tp_call;	*/
	NULL, /*	reprfunc tp_str;	*/
	NULL, /*	getattrofunc tp_getattro;	*/
	NULL, /*	setattrofunc tp_setattro;	*/

	/* Functions to access object as input/output buffer */
	NULL, /*	PyBufferProcs *tp_as_buffer;	*/

	/* Flags to define presence of optional/expanded features */
	Py_TPFLAGS_BASETYPE, /*	long tp_flags;	*/

	NULL, /*	const char *tp_doc;  Documentation string */

	/* Assigned meaning in release 2.0 */
	/* call function for all accessible objects */
	NULL, /*	traverseproc tp_traverse;	*/

	/* delete references to contained objects */
	NULL, /*	inquiry tp_clear;	*/

	/* Assigned meaning in release 2.1 */
	/* rich comparisons */
	NULL, /*	richcmpfunc tp_richcompare;	*/

	/* weak reference enabler */
	0, /*	Py_ssize_t tp_weaklistoffset;	*/

	/* Added in release 2.2 */
	/* Iterators */
	NULL, /*	getiterfunc tp_iter;	*/
	NULL, /*	iternextfunc tp_iternext;	*/

	/* Attribute descriptor and subclassing stuff */
	ra_methods, /*	struct PyMethodDef *tp_methods;	*/
	ra_members, /*	struct PyMemberDef *tp_members;	*/
	ra_getsetters, /*	struct PyGetSetDef *tp_getset;	*/
	NULL, /*	struct _typeobject *tp_base;	*/
	NULL, /*	PyObject *tp_dict;	*/
	NULL, /*	descrgetfunc tp_descr_get;	*/
	NULL, /*	descrsetfunc tp_descr_set;	*/
	0, /*	Py_ssize_t tp_dictoffset;	*/
	NULL, /*	initproc tp_init;	*/
	NULL, /*	allocfunc tp_alloc;	*/
	ra_new, /*	newfunc tp_new;	*/

};

typedef struct {
	PyObject_VAR_HEAD
	apr_pool_t *pool;
	svn_auth_provider_object_t *provider;
	PyObject *callback;
} AuthProviderObject;

static void auth_provider_dealloc(PyObject *self)
{
	AuthProviderObject *auth_provider = (AuthProviderObject *)self;
	Py_XDECREF(auth_provider->callback);
	auth_provider->callback = NULL;
	apr_pool_destroy(auth_provider->pool);
	PyObject_Del(self);
}

static PyTypeObject AuthProvider_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.AuthProvider", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(AuthProviderObject),
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

	/* Methods to implement standard operations */

	auth_provider_dealloc, /*	destructor tp_dealloc;	*/

};

static PyObject *auth_init(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
	char *kwnames[] = { "providers", NULL };
	apr_array_header_t *c_providers;
	svn_auth_provider_object_t **el;
	PyObject *providers;
	AuthObject *ret;
	int i;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwnames, &providers))
		return NULL;

	ret = PyObject_New(AuthObject, &Auth_Type);
	if (ret == NULL)
		return NULL;

	ret->providers = NULL;

	ret->pool = Pool(NULL);
	if (ret->pool == NULL) {
		PyErr_NoMemory();
		Py_DECREF(ret);
		return NULL;
	}

	if (!PySequence_Check(providers)) {
		PyErr_SetString(PyExc_TypeError, "Auth providers should be a sequence");
		Py_DECREF(ret);
		return NULL;
	}

	Py_INCREF(providers);
	ret->providers = providers;

	c_providers = apr_array_make(ret->pool, PySequence_Size(providers),
								 sizeof(svn_auth_provider_object_t *));
	if (c_providers == NULL) {
		PyErr_NoMemory();
		Py_DECREF(ret);
		return NULL;
	}
	for (i = 0; i < PySequence_Size(providers); i++) {
		AuthProviderObject *provider;
		el = (svn_auth_provider_object_t **)apr_array_push(c_providers);
		provider = (AuthProviderObject *)PySequence_GetItem(providers, i);
		if (!PyObject_TypeCheck(provider, &AuthProvider_Type)) {
			PyErr_SetString(PyExc_TypeError, "Invalid auth provider");
			Py_DECREF(ret);
			return NULL;
		}
		*el = provider->provider;
	}
	svn_auth_open(&ret->auth_baton, c_providers, ret->pool);
	return (PyObject *)ret;
}

static PyObject *auth_set_parameter(PyObject *self, PyObject *args)
{
	AuthObject *auth = (AuthObject *)self;
	char *name;
	PyObject *value;
	void *vvalue;
	if (!PyArg_ParseTuple(args, "sO:set_parameter", &name, &value))
		return NULL;

	if (!strcmp(name, SVN_AUTH_PARAM_SSL_SERVER_FAILURES)) {
		long ret = PyLong_AsLong(value);
		if (ret == -1 && PyErr_Occurred())
			return NULL;
		vvalue = apr_pcalloc(auth->pool, sizeof(apr_uint32_t));
		*((apr_uint32_t *)vvalue) = ret;
	} else if (!strcmp(name, SVN_AUTH_PARAM_DEFAULT_USERNAME) ||
			   !strcmp(name, SVN_AUTH_PARAM_DEFAULT_PASSWORD)) {
		vvalue = py_object_to_svn_string(value, auth->pool);
		if (vvalue == NULL) {
			return NULL;
		}
	} else {
		PyErr_Format(PyExc_TypeError, "Unsupported auth parameter %s", name);
		return NULL;
	}

	svn_auth_set_parameter(auth->auth_baton, name, (char *)vvalue);

	Py_RETURN_NONE;
}

static PyObject *auth_get_parameter(PyObject *self, PyObject *args)
{
	char *name;
	const void *value;
	AuthObject *auth = (AuthObject *)self;

	if (!PyArg_ParseTuple(args, "s:get_parameter", &name))
		return NULL;

	value = svn_auth_get_parameter(auth->auth_baton, name);

	if (!strcmp(name, SVN_AUTH_PARAM_SSL_SERVER_FAILURES)) {
		return PyLong_FromLong(*((apr_uint32_t *)value));
	} else if (!strcmp(name, SVN_AUTH_PARAM_DEFAULT_USERNAME) ||
			   !strcmp(name, SVN_AUTH_PARAM_DEFAULT_PASSWORD)) {
		return PyUnicode_FromString((const char *)value);
	} else {
		PyErr_Format(PyExc_TypeError, "Unsupported auth parameter %s", name);
		return NULL;
	}
}

typedef struct {
	PyObject_VAR_HEAD
	apr_pool_t *pool;
	char *cred_kind;
	svn_auth_iterstate_t *state;
	void *credentials;
} CredentialsIterObject;

static PyObject *auth_first_credentials(PyObject *self, PyObject *args)
{
	char *cred_kind;
	char *realmstring;
	AuthObject *auth = (AuthObject *)self;
	void *creds;
	apr_pool_t *pool;
	CredentialsIterObject *ret;
	svn_auth_iterstate_t *state;

	if (!PyArg_ParseTuple(args, "ss:credentials", &cred_kind, &realmstring))
		return NULL;

	pool = Pool(NULL);
	if (pool == NULL)
		return NULL;

	RUN_SVN_WITH_POOL(pool,
					  svn_auth_first_credentials(&creds, &state, cred_kind, realmstring, auth->auth_baton, pool));

	ret = PyObject_New(CredentialsIterObject, &CredentialsIter_Type);
	if (ret == NULL)
		return NULL;

	ret->pool = pool;
	ret->cred_kind = apr_pstrdup(pool, cred_kind);
	ret->state = state;
	ret->credentials = creds;

	return (PyObject *)ret;
}

static void credentials_iter_dealloc(PyObject *self)
{
	CredentialsIterObject *credsiter = (CredentialsIterObject *)self;
	apr_pool_destroy(credsiter->pool);
	PyObject_Del(self);
}

static PyObject *credentials_iter_next(CredentialsIterObject *iterator)
{
	PyObject *ret;

	if (iterator->credentials == NULL) {
		PyErr_SetString(PyExc_StopIteration, "No more credentials available");
		return NULL;
	}

	if (!strcmp(iterator->cred_kind, SVN_AUTH_CRED_SIMPLE)) {
		svn_auth_cred_simple_t *simple = iterator->credentials;
		ret = Py_BuildValue("(zzb)", simple->username, simple->password, simple->may_save?true:false);
	} else if (!strcmp(iterator->cred_kind, SVN_AUTH_CRED_USERNAME)) {
		svn_auth_cred_username_t *uname = iterator->credentials;
		ret = Py_BuildValue("(zb)", uname->username, uname->may_save?true:false);
	} else if (!strcmp(iterator->cred_kind, SVN_AUTH_CRED_SSL_CLIENT_CERT)) {
		svn_auth_cred_ssl_client_cert_t *ccert = iterator->credentials;
		ret = Py_BuildValue("(zb)", ccert->cert_file, ccert->may_save?true:false);
	} else if (!strcmp(iterator->cred_kind, SVN_AUTH_CRED_SSL_CLIENT_CERT_PW)) {
		svn_auth_cred_ssl_client_cert_pw_t *ccert = iterator->credentials;
		ret = Py_BuildValue("(zb)", ccert->password, ccert->may_save?true:false);
	} else if (!strcmp(iterator->cred_kind, SVN_AUTH_CRED_SSL_SERVER_TRUST)) {
		svn_auth_cred_ssl_server_trust_t *ccert = iterator->credentials;
		ret = Py_BuildValue("(ib)", ccert->accepted_failures, ccert->may_save?true:false);
	} else {
		PyErr_Format(PyExc_RuntimeError, "Unknown cred kind %s", iterator->cred_kind);
		return NULL;
	}

	RUN_SVN_WITH_POOL(iterator->pool,
					  svn_auth_next_credentials(&iterator->credentials, iterator->state, iterator->pool));

	return ret;
}

static PyTypeObject CredentialsIter_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.CredentialsIter", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(CredentialsIterObject),
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

	/* Methods to implement standard operations */

	.tp_dealloc = (destructor)credentials_iter_dealloc, /*	destructor tp_dealloc;	*/

#if PY_MAJOR_VERSION < 3
	.tp_flags = Py_TPFLAGS_HAVE_ITER, /*	long tp_flags;	*/
#endif

	.tp_iternext = (iternextfunc)credentials_iter_next, /*	iternextfunc tp_iternext;	*/

};

static PyMethodDef auth_methods[] = {
	{ "set_parameter", auth_set_parameter, METH_VARARGS,
		"S.set_parameter(key, value)\n"
		"Set a parameter" },
	{ "get_parameter", auth_get_parameter, METH_VARARGS,
		"S.get_parameter(key) -> value\n"
		"Get a parameter" },
	{ "credentials", auth_first_credentials, METH_VARARGS,
		"Credentials" },
	{ NULL, }
};

static void auth_dealloc(PyObject *self)
{
	AuthObject *auth = (AuthObject *)self;
	apr_pool_destroy(auth->pool);
	Py_XDECREF(auth->providers);
	PyObject_Del(auth);
}

static PyTypeObject Auth_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.Auth", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(AuthObject),
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

	/* Methods to implement standard operations */

	auth_dealloc, /*	destructor tp_dealloc;	*/
	0, /*	printfunc tp_print;	*/
	NULL, /*	getattrfunc tp_getattr;	*/
	NULL, /*	setattrfunc tp_setattr;	*/
	NULL, /*	cmpfunc tp_compare;	*/
	NULL, /*	reprfunc tp_repr;	*/

	/* Method suites for standard classes */

	NULL, /*	PyNumberMethods *tp_as_number;	*/
	NULL, /*	PySequenceMethods *tp_as_sequence;	*/
	NULL, /*	PyMappingMethods *tp_as_mapping;	*/

	/* More standard operations (here for binary compatibility) */

	NULL, /*	hashfunc tp_hash;	*/
	NULL, /*	ternaryfunc tp_call;	*/
	NULL, /*	reprfunc tp_str;	*/
	NULL, /*	getattrofunc tp_getattro;	*/
	NULL, /*	setattrofunc tp_setattro;	*/

	/* Functions to access object as input/output buffer */
	NULL, /*	PyBufferProcs *tp_as_buffer;	*/

	/* Flags to define presence of optional/expanded features */
	0, /*	long tp_flags;	*/

	NULL, /*	const char *tp_doc;  Documentation string */

	/* Assigned meaning in release 2.0 */
	/* call function for all accessible objects */
	NULL, /*	traverseproc tp_traverse;	*/

	/* delete references to contained objects */
	NULL, /*	inquiry tp_clear;	*/

	/* Assigned meaning in release 2.1 */
	/* rich comparisons */
	NULL, /*	richcmpfunc tp_richcompare;	*/

	/* weak reference enabler */
	0, /*	Py_ssize_t tp_weaklistoffset;	*/

	/* Added in release 2.2 */
	/* Iterators */
	NULL, /*	getiterfunc tp_iter;	*/
	NULL, /*	iternextfunc tp_iternext;	*/

	/* Attribute descriptor and subclassing stuff */
	auth_methods, /*	struct PyMethodDef *tp_methods;	*/
	NULL, /*	struct PyMemberDef *tp_members;	*/
	NULL, /*	struct PyGetSetDef *tp_getset;	*/
	NULL, /*	struct _typeobject *tp_base;	*/
	NULL, /*	PyObject *tp_dict;	*/
	NULL, /*	descrgetfunc tp_descr_get;	*/
	NULL, /*	descrsetfunc tp_descr_set;	*/
	0, /*	Py_ssize_t tp_dictoffset;	*/
	NULL, /*	initproc tp_init;	*/
	NULL, /*	allocfunc tp_alloc;	*/
	auth_init, /*	newfunc tp_new;	*/

};

static svn_error_t *py_username_prompt(svn_auth_cred_username_t **cred, void *baton, const char *realm, int may_save, apr_pool_t *pool)
{
	PyObject *fn = (PyObject *)baton, *ret;
	PyObject *py_username, *py_may_save;
	char *username;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallFunction(fn, "si", realm, may_save);
	CB_CHECK_PYRETVAL(ret);

	if (ret == Py_None) {
		Py_DECREF(ret);
		PyGILState_Release(state);
		return NULL;
	}

	if (!PyTuple_Check(ret)) {
		PyErr_SetString(PyExc_TypeError, "expected tuple with username credentials");
		goto fail;
	}

	if (PyTuple_Size(ret) != 2) {
		PyErr_SetString(PyExc_TypeError, "expected tuple with username credentials to be size 2");
		goto fail;
	}

	py_may_save = PyTuple_GetItem(ret, 1);
	CB_CHECK_PYRETVAL(py_may_save);
	if (!PyBool_Check(py_may_save)) {
		PyErr_SetString(PyExc_TypeError, "may_save should be boolean");
		goto fail;
	}
	py_username = PyTuple_GetItem(ret, 0);
	CB_CHECK_PYRETVAL(py_username);
	username = py_object_to_svn_string(py_username, pool);
	if (username == NULL) {
		goto fail;
	}

	*cred = apr_pcalloc(pool, sizeof(**cred));
	(*cred)->username = username;
	(*cred)->may_save = (py_may_save == Py_True);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;

fail:
	Py_DECREF(ret);
	PyGILState_Release(state);
	return py_svn_error();
}

static PyObject *get_username_prompt_provider(PyObject *self, PyObject *args)
{
	AuthProviderObject *auth;
	PyObject *prompt_func;
	int retry_limit;
	if (!PyArg_ParseTuple(args, "Oi:get_username_prompt_provider",
			  &prompt_func, &retry_limit))
		return NULL;
	auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	Py_INCREF(prompt_func);
	auth->callback = prompt_func;
	svn_auth_get_username_prompt_provider(&auth->provider, py_username_prompt,
		 (void *)prompt_func, retry_limit, auth->pool);
	return (PyObject *)auth;
}

static svn_error_t *py_simple_prompt(svn_auth_cred_simple_t **cred, void *baton, const char *realm, const char *username, int may_save, apr_pool_t *pool)
{
	PyObject *fn = (PyObject *)baton, *ret;
	PyObject *py_may_save, *py_username, *py_password;
	char *ret_username, *password;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallFunction(fn, "ssi", realm, username, may_save);
	CB_CHECK_PYRETVAL(ret);
	if (!PyTuple_Check(ret)) {
		PyErr_SetString(PyExc_TypeError, "expected tuple with simple credentials");
		goto fail;
	}
	if (PyTuple_Size(ret) != 3) {
		PyErr_SetString(PyExc_TypeError, "expected tuple of size 3");
		goto fail;
	}

	py_may_save = PyTuple_GetItem(ret, 2);
	CB_CHECK_PYRETVAL(py_may_save);

	if (!PyBool_Check(py_may_save)) {
		PyErr_SetString(PyExc_TypeError, "may_save should be boolean");
		goto fail;
	}

	py_username = PyTuple_GetItem(ret, 0);
	CB_CHECK_PYRETVAL(py_username);
	ret_username = py_object_to_svn_string(py_username, pool);
	if (ret_username == NULL) {
		goto fail;
	}

	py_password = PyTuple_GetItem(ret, 1);
	CB_CHECK_PYRETVAL(py_password);
	password = py_object_to_svn_string(py_password, pool);
	if (password == NULL) {
		goto fail;
	}

	*cred = apr_pcalloc(pool, sizeof(**cred));
	(*cred)->username = ret_username;
	(*cred)->password = password;
	(*cred)->may_save = (py_may_save == Py_True);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;

fail:
	Py_DECREF(ret);
	PyGILState_Release(state);
	return py_svn_error();
}

static PyObject *get_simple_prompt_provider(PyObject *self, PyObject *args)
{
	PyObject *prompt_func;
	int retry_limit;
	AuthProviderObject *auth;

	if (!PyArg_ParseTuple(args, "Oi", &prompt_func, &retry_limit))
		return NULL;

	auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	Py_INCREF(prompt_func);
	auth->callback = prompt_func;
	svn_auth_get_simple_prompt_provider (&auth->provider, py_simple_prompt, (void *)prompt_func, retry_limit, auth->pool);
	return (PyObject *)auth;
}

static svn_error_t *py_ssl_server_trust_prompt(svn_auth_cred_ssl_server_trust_t **cred, void *baton, const char *realm, apr_uint32_t failures, const svn_auth_ssl_server_cert_info_t *cert_info, svn_boolean_t may_save, apr_pool_t *pool)
{
	PyObject *fn = (PyObject *)baton;
	PyObject *ret;
	PyObject *py_cert;
	PyGILState_STATE state = PyGILState_Ensure();
	int accepted_failures;

	if (cert_info == NULL) {
		py_cert = Py_None;
		Py_INCREF(py_cert);
	} else {
		py_cert = Py_BuildValue("(sssss)", cert_info->hostname, cert_info->fingerprint,
						  cert_info->valid_from, cert_info->valid_until,
						  cert_info->issuer_dname, cert_info->ascii_cert);
	}

	CB_CHECK_PYRETVAL(py_cert);

	ret = PyObject_CallFunction(fn, "slOi", realm, failures, py_cert, may_save);
	Py_DECREF(py_cert);
	CB_CHECK_PYRETVAL(ret);

	if (ret == Py_None) {
		Py_DECREF(ret);
		PyGILState_Release(state);
		return NULL;
	}

	if (!PyArg_ParseTuple(ret, "ii", &accepted_failures, &may_save)) {
		Py_DECREF(ret);
		PyGILState_Release(state);
		return py_svn_error();
	}

	*cred = apr_pcalloc(pool, sizeof(**cred));
	(*cred)->accepted_failures = accepted_failures;
	(*cred)->may_save = may_save;

	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static PyObject *get_ssl_server_trust_prompt_provider(PyObject *self, PyObject *args)
{
	AuthProviderObject *auth;
	PyObject *prompt_func;

	if (!PyArg_ParseTuple(args, "O", &prompt_func))
		return NULL;

	auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	Py_INCREF(prompt_func);
	auth->callback = prompt_func;
	svn_auth_get_ssl_server_trust_prompt_provider (&auth->provider, py_ssl_server_trust_prompt, (void *)prompt_func, auth->pool);
	return (PyObject *)auth;
}

static svn_error_t *py_ssl_client_cert_pw_prompt(svn_auth_cred_ssl_client_cert_pw_t **cred, void *baton, const char *realm, svn_boolean_t may_save, apr_pool_t *pool)
{
	PyObject *fn = (PyObject *)baton, *ret, *py_password;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallFunction(fn, "si", realm, may_save);
	CB_CHECK_PYRETVAL(ret);
	if (!PyArg_ParseTuple(ret, "Oi", &py_password, &may_save)) {
		goto fail;
	}

	*cred = apr_pcalloc(pool, sizeof(**cred));
	(*cred)->password = py_object_to_svn_string(py_password, pool);
	if ((*cred)->password == NULL) {
		goto fail;
	}
	(*cred)->may_save = may_save;
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;

fail:
	Py_DECREF(ret);
	PyGILState_Release(state);
	return py_svn_error();
}

static svn_error_t *py_ssl_client_cert_prompt(svn_auth_cred_ssl_client_cert_t **cred, void *baton, const char *realm, svn_boolean_t may_save, apr_pool_t *pool)
{
	PyObject *fn = (PyObject *)baton, *ret, *py_may_save, *py_cert_file;
	PyGILState_STATE state = PyGILState_Ensure();
	char *cert_file;
	ret = PyObject_CallFunction(fn, "si", realm, may_save);
	CB_CHECK_PYRETVAL(ret);

	if (!PyTuple_Check(ret)) {
		PyErr_SetString(PyExc_TypeError, "expected tuple with client cert credentials");
		goto fail;
	}

	if (PyTuple_Size(ret) != 2) {
		PyErr_SetString(PyExc_TypeError, "expected tuple of size 2");
		goto fail;
	}
	py_may_save = PyTuple_GetItem(ret, 1);
	if (!PyBool_Check(py_may_save)) {
		PyErr_SetString(PyExc_TypeError, "may_save should be boolean");
		goto fail;
	}

	py_cert_file = PyTuple_GetItem(ret, 0);
	cert_file = py_object_to_svn_string(py_cert_file, pool);
	if (!cert_file) {
		goto fail;
	}

	*cred = apr_pcalloc(pool, sizeof(**cred));
	(*cred)->cert_file = cert_file;
	(*cred)->may_save = (py_may_save == Py_True);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;

fail:
	Py_DECREF(ret);
	PyGILState_Release(state);
	return py_svn_error();
}

static PyObject *get_ssl_client_cert_pw_prompt_provider(PyObject *self, PyObject *args)
{
	PyObject *prompt_func;
	int retry_limit;
	AuthProviderObject *auth;

	if (!PyArg_ParseTuple(args, "Oi", &prompt_func, &retry_limit))
		return NULL;

	auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	Py_INCREF(prompt_func);
	auth->callback = prompt_func;
	svn_auth_get_ssl_client_cert_pw_prompt_provider (&auth->provider, py_ssl_client_cert_pw_prompt, (void *)prompt_func, retry_limit, auth->pool);
	return (PyObject *)auth;
}

static PyObject *get_ssl_client_cert_prompt_provider(PyObject *self, PyObject *args)
{
	PyObject *prompt_func;
	int retry_limit;
	AuthProviderObject *auth;

	if (!PyArg_ParseTuple(args, "Oi", &prompt_func, &retry_limit))
		return NULL;

	auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	Py_INCREF(prompt_func);
	auth->callback = prompt_func;
	svn_auth_get_ssl_client_cert_prompt_provider (&auth->provider, py_ssl_client_cert_prompt, (void *)prompt_func, retry_limit, auth->pool);
	return (PyObject *)auth;
}

static PyObject *get_username_provider(PyObject *self)
{
	AuthProviderObject *auth;
	auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->pool = Pool(NULL);
	auth->callback = NULL;
	if (auth->pool == NULL) {
		PyObject_Del(auth);
		return NULL;
	}
	svn_auth_get_username_provider(&auth->provider, auth->pool);
	return (PyObject *)auth;
}

#if ONLY_SINCE_SVN(1, 6)
static svn_error_t *py_cb_get_simple_provider_prompt(svn_boolean_t *may_save_plaintext,
                                                     const char *realmstring,
                                                     void *baton,
                                                     apr_pool_t *pool)
{
	if (baton == Py_None) {
		/* just disallow saving plaintext passwords on 1.6 and later */
		*may_save_plaintext = FALSE;
	} else {
		PyObject *ret;
		PyGILState_STATE state = PyGILState_Ensure();
		ret = PyObject_CallFunction(baton, "s", realmstring);
		CB_CHECK_PYRETVAL(ret);
		if (ret == NULL) {
			PyGILState_Release(state);
			return py_svn_error();
		}
		*may_save_plaintext = PyObject_IsTrue(ret)?TRUE:FALSE;
		Py_DECREF(ret);
		PyGILState_Release(state);
	}

    return NULL;
}
#endif

static PyObject *get_simple_provider(PyObject *self, PyObject *args)
{
	AuthProviderObject *auth;
	PyObject *callback = Py_None;
	apr_pool_t *pool;

	if (!PyArg_ParseTuple(args, "|O:get_simple_provider", &callback))
		return NULL;

	pool = Pool(NULL);
	if (pool == NULL)
		return NULL;
	auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL) {
		apr_pool_destroy(pool);
		return NULL;
	}
	auth->pool = pool;
#if ONLY_SINCE_SVN(1, 6)
	Py_INCREF(callback);
	auth->callback = callback;
	svn_auth_get_simple_provider2(&auth->provider,
		  py_cb_get_simple_provider_prompt, auth->callback, auth->pool);
#else
	auth->callback = NULL;
	auth->provider = NULL;
	if (callback != Py_None) {
		PyErr_SetString(PyExc_NotImplementedError,
			"callback not supported with svn < 1.6");
		Py_DECREF(auth);
		return NULL;
	}
	svn_auth_get_simple_provider(&auth->provider, auth->pool);
#endif
	return (PyObject *)auth;
}

static PyObject *get_ssl_server_trust_file_provider(PyObject *self)
{
	AuthProviderObject *auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->callback = NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	svn_auth_get_ssl_server_trust_file_provider(&auth->provider, auth->pool);
	return (PyObject *)auth;
}

static PyObject *get_ssl_client_cert_file_provider(PyObject *self)
{
	AuthProviderObject *auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->callback = NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	svn_auth_get_ssl_client_cert_file_provider(&auth->provider, auth->pool);
	return (PyObject *)auth;
}

static PyObject *get_ssl_client_cert_pw_file_provider(PyObject *self)
{
	AuthProviderObject *auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->callback = NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;

#if ONLY_SINCE_SVN(1, 6)
	svn_auth_get_ssl_client_cert_pw_file_provider2(&auth->provider, NULL, NULL, auth->pool);
#else
	svn_auth_get_ssl_client_cert_pw_file_provider(&auth->provider, auth->pool);
#endif
	return (PyObject *)auth;
}

static PyObject *print_modules(PyObject *self)
{
	svn_stringbuf_t *stringbuf;
	svn_string_t *string;
	PyObject *ret;
	apr_pool_t *pool = Pool(NULL);
	if (pool == NULL)
		return NULL;
	stringbuf = svn_stringbuf_create("", pool);
	if (stringbuf == NULL) {
		apr_pool_destroy(pool);
		return NULL;
	}
	RUN_SVN_WITH_POOL(pool, svn_ra_print_modules(stringbuf, pool));
	string = svn_string_create_from_buf(stringbuf, pool);
	if (string == NULL) {
		apr_pool_destroy(pool);
		return NULL;
	}
	ret = PyBytes_FromStringAndSize(string->data, string->len);
	apr_pool_destroy(pool);
	return ret;
}

#if defined(WIN32) || defined(__CYGWIN__)
static PyObject *get_windows_simple_provider(PyObject* self)
{
	AuthProviderObject *auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->callback = NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	svn_auth_get_windows_simple_provider(&auth->provider, auth->pool);
	return (PyObject *)auth;
}

#if ONLY_SINCE_SVN(1, 5)
static PyObject *get_windows_ssl_server_trust_provider(PyObject *self)
{
	AuthProviderObject *auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->callback = NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	svn_auth_get_windows_ssl_server_trust_provider(&auth->provider, auth->pool);
	return (PyObject *)auth;
}
#endif
#endif

#if defined(SVN_KEYCHAIN_PROVIDER_AVAILABLE)
static PyObject *get_keychain_simple_provider(PyObject* self)
{
	AuthProviderObject *auth = PyObject_New(AuthProviderObject, &AuthProvider_Type);
	if (auth == NULL)
		return NULL;
	auth->callback = NULL;
	auth->pool = Pool(NULL);
	if (auth->pool == NULL)
		return NULL;
	svn_auth_get_keychain_simple_provider(&auth->provider, auth->pool);
	return (PyObject *)auth;
}
#endif

static PyObject *get_platform_specific_client_providers(PyObject *self)
{
#if ONLY_SINCE_SVN(1, 6)
	/* svn_auth_get_platform_specific_client_providers() allocates all the
	 * providers in a single pool, so we can't use it :/ */
	const char *provider_names[] = {
		"gnome_keyring", "keychain", "kwallet", "windows", NULL,
	};
	const char *provider_types[] = {
		"simple", "ssl_client_cert_pw", "ssl_server_trust", NULL,
	};
	PyObject *pylist;
	int i, j;

	pylist = PyList_New(0);
	if (pylist == NULL) {
		return NULL;
	}

	for (i = 0; provider_names[i] != NULL; i++) {
		for (j = 0; provider_types[j] != NULL; j++) {
			svn_auth_provider_object_t *c_provider = NULL;
			apr_pool_t *pool = Pool(NULL);
			AuthProviderObject *auth;

			if (pool == NULL)
				continue;

			RUN_SVN(svn_auth_get_platform_specific_provider(&c_provider,
															provider_names[i],
															provider_types[j],
															pool));

			auth = PyObject_New(AuthProviderObject,
								&AuthProvider_Type);

			if (c_provider == NULL || auth == NULL) {
				apr_pool_destroy(pool);
				continue;
			}

			auth->pool = pool;
			auth->callback = NULL;
			auth->provider = c_provider;

			PyList_Append(pylist, (PyObject *)auth);

			Py_DECREF(auth);
		}
	}

	return pylist;
#else
	PyObject *pylist = PyList_New(0);
	PyObject *provider = NULL;

	if (pylist == NULL) {
		Py_DECREF(pylist);
		return NULL;
	}

#if defined(WIN32) || defined(__CYGWIN__)
	provider = get_windows_simple_provider(self);
	if (provider == NULL)
		return NULL;
	PyList_Append(pylist, provider);
	Py_DECREF(provider);

#if ONLY_SINCE_SVN(1, 5)
	provider = get_windows_ssl_server_trust_provider(self);
	if (provider == NULL)
		return NULL;
	PyList_Append(pylist, provider);
	Py_DECREF(provider);
#endif /* 1.5 */
#endif /* WIN32 || __CYGWIN__ */

#if defined(SVN_KEYCHAIN_PROVIDER_AVAILABLE)
	provider = get_keychain_simple_provider(self);
	if (provider == NULL)
		return NULL;
	PyList_Append(pylist, provider);
	Py_DECREF(provider);
#endif

	return pylist;
#endif
}

static PyMethodDef ra_module_methods[] = {
	{ "version", (PyCFunction)version, METH_NOARGS,
		"version() -> (major, minor, micro, tag)\n"
		"Version of libsvn_ra currently used." },
	{ "api_version", (PyCFunction)api_version, METH_NOARGS,
		"api_version() -> (major, minor, patch, tag)\n\n"
		"Version of libsvn_ra Subvertpy was compiled against."
	},
	{ "get_ssl_client_cert_pw_file_provider", (PyCFunction)get_ssl_client_cert_pw_file_provider, METH_NOARGS, NULL },
	{ "get_ssl_client_cert_file_provider", (PyCFunction)get_ssl_client_cert_file_provider, METH_NOARGS, NULL },
	{ "get_ssl_server_trust_file_provider", (PyCFunction)get_ssl_server_trust_file_provider, METH_NOARGS, NULL },
	{ "get_simple_provider", (PyCFunction)get_simple_provider, METH_VARARGS, NULL },
#if defined(WIN32) || defined(__CYGWIN__)
	{ "get_windows_simple_provider", (PyCFunction)get_windows_simple_provider, METH_NOARGS, NULL },
#if ONLY_SINCE_SVN(1, 5)
	{ "get_windows_ssl_server_trust_provider", (PyCFunction)get_windows_ssl_server_trust_provider, METH_NOARGS, NULL },
#endif
#endif
#if defined(SVN_KEYCHAIN_PROVIDER_AVAILABLE)
	{ "get_keychain_simple_provider", (PyCFunction)get_keychain_simple_provider, METH_NOARGS, NULL },
#endif
	{ "get_username_prompt_provider", (PyCFunction)get_username_prompt_provider, METH_VARARGS, NULL },
	{ "get_simple_prompt_provider", (PyCFunction)get_simple_prompt_provider, METH_VARARGS, NULL },
	{ "get_ssl_server_trust_prompt_provider", (PyCFunction)get_ssl_server_trust_prompt_provider, METH_VARARGS, NULL },
	{ "get_ssl_client_cert_prompt_provider", (PyCFunction)get_ssl_client_cert_prompt_provider, METH_VARARGS, NULL },
	{ "get_ssl_client_cert_pw_prompt_provider", (PyCFunction)get_ssl_client_cert_pw_prompt_provider, METH_VARARGS, NULL },
	{ "get_username_provider", (PyCFunction)get_username_provider, METH_NOARGS, NULL },
	{ "get_platform_specific_client_providers",
		(PyCFunction)get_platform_specific_client_providers,
		METH_NOARGS,
		"Get a list of all available platform client providers.",
	},
	{ "print_modules", (PyCFunction)print_modules, METH_NOARGS, NULL },
	{ NULL, }
};

static PyObject *
moduleinit(void)
{
	static apr_pool_t *pool;
	PyObject *mod;

	if (PyType_Ready(&RemoteAccess_Type) < 0)
		return NULL;

	if (PyType_Ready(&Editor_Type) < 0)
		return NULL;

	if (PyType_Ready(&FileEditor_Type) < 0)
		return NULL;

	if (PyType_Ready(&DirectoryEditor_Type) < 0)
		return NULL;

	if (PyType_Ready(&Reporter_Type) < 0)
		return NULL;

	if (PyType_Ready(&TxDeltaWindowHandler_Type) < 0)
		return NULL;

	if (PyType_Ready(&Auth_Type) < 0)
		return NULL;

	if (PyType_Ready(&CredentialsIter_Type) < 0)
		return NULL;

	if (PyType_Ready(&AuthProvider_Type) < 0)
		return NULL;

	if (PyType_Ready(&LogIterator_Type) < 0)
		return NULL;

	apr_initialize();
	pool = Pool(NULL);
	if (pool == NULL)
		return NULL;
	svn_ra_initialize(pool);
	PyEval_InitThreads();


#if PY_MAJOR_VERSION >= 3
	static struct PyModuleDef moduledef = {
	  PyModuleDef_HEAD_INIT,
	  "_ra",         /* m_name */
	  "Remote Access",            /* m_doc */
	  -1,              /* m_size */
	  ra_module_methods, /* m_methods */
	  NULL,            /* m_reload */
	  NULL,            /* m_traverse */
	  NULL,            /* m_clear*/
	  NULL,            /* m_free */
	};
	mod = PyModule_Create(&moduledef);
#else
	mod = Py_InitModule3("subvertpy._ra", ra_module_methods, "Remote Access");
#endif
	if (mod == NULL)
		return NULL;

	PyModule_AddObject(mod, "RemoteAccess", (PyObject *)&RemoteAccess_Type);
	Py_INCREF(&RemoteAccess_Type);

	PyModule_AddObject(mod, "Auth", (PyObject *)&Auth_Type);
	Py_INCREF(&Auth_Type);

	PyModule_AddObject(mod, "Editor", (PyObject *)&Editor_Type);
	Py_INCREF(&Editor_Type);

	busy_exc = PyErr_NewException("subvertpy._ra.BusyException", NULL, NULL);
	PyModule_AddObject(mod, "BusyException", busy_exc);

#if ONLY_SINCE_SVN(1, 5)
	PyModule_AddIntConstant(mod, "DEPTH_UNKNOWN", svn_depth_unknown);
	PyModule_AddIntConstant(mod, "DEPTH_EXCLUDE", svn_depth_exclude);
	PyModule_AddIntConstant(mod, "DEPTH_EMPTY", svn_depth_empty);
	PyModule_AddIntConstant(mod, "DEPTH_FILES", svn_depth_files);
	PyModule_AddIntConstant(mod, "DEPTH_IMMEDIATES", svn_depth_immediates);
	PyModule_AddIntConstant(mod, "DEPTH_INFINITY", svn_depth_infinity);
#endif

	PyModule_AddIntConstant(mod, "DIRENT_KIND", SVN_DIRENT_KIND);
	PyModule_AddIntConstant(mod, "DIRENT_SIZE", SVN_DIRENT_SIZE);
	PyModule_AddIntConstant(mod, "DIRENT_HAS_PROPS", SVN_DIRENT_HAS_PROPS);
	PyModule_AddIntConstant(mod, "DIRENT_CREATED_REV", SVN_DIRENT_CREATED_REV);
	PyModule_AddIntConstant(mod, "DIRENT_TIME", SVN_DIRENT_TIME);
	PyModule_AddIntConstant(mod, "DIRENT_LAST_AUTHOR", SVN_DIRENT_LAST_AUTHOR);
	PyModule_AddIntConstant(mod, "DIRENT_ALL", SVN_DIRENT_ALL);

#if ONLY_SINCE_SVN(1, 5)
	PyModule_AddIntConstant(mod, "MERGEINFO_EXPLICIT", svn_mergeinfo_explicit);
	PyModule_AddIntConstant(mod, "MERGEINFO_INHERITED", svn_mergeinfo_inherited);
	PyModule_AddIntConstant(mod, "MERGEINFO_NEAREST_ANCESTOR", svn_mergeinfo_nearest_ancestor);
#endif

#ifdef SVN_VER_REVISION
	PyModule_AddIntConstant(mod, "SVN_REVISION", SVN_VER_REVISION);
#endif

	return mod;
}

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit__ra(void)
{
	return moduleinit();
}
#else
PyMODINIT_FUNC
init_ra(void)
{
	moduleinit();
}
#endif
