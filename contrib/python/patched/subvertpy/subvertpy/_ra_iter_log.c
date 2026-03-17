/*
 * Copyright © 2010 Jelmer Vernooij <jelmer@jelmer.uk>
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
#include <pythread.h>

struct log_entry {
	PyObject *tuple;
	struct log_entry *next;
};

typedef struct {
	PyObject_VAR_HEAD
	svn_revnum_t start, end;
	svn_boolean_t discover_changed_paths;
	svn_boolean_t strict_node_history;
	svn_boolean_t include_merged_revisions;
	int limit;
	apr_pool_t *pool;
	apr_array_header_t *apr_paths;
	apr_array_header_t *apr_revprops;
	RemoteAccessObject *ra;
	svn_boolean_t done;
	PyObject *exc_type;
	PyObject *exc_val;
	int queue_size;
	struct log_entry *head;
	struct log_entry *tail;
} LogIteratorObject;

static void log_iter_dealloc(PyObject *self)
{
	LogIteratorObject *iter = (LogIteratorObject *)self;

	while (iter->head) {
		struct log_entry *e = iter->head;
		Py_DECREF(e->tuple);
		iter->head = e->next;
		free(e);
	}
	Py_XDECREF(iter->exc_type);
	Py_XDECREF(iter->exc_val);
	apr_pool_destroy(iter->pool);
	Py_DECREF(iter->ra);
	PyObject_Del(iter);
}

static PyObject *log_iter_next(LogIteratorObject *iter)
{
	struct log_entry *first;
	PyObject *ret;
	Py_INCREF(iter);

	while (iter->head == NULL) {
		/* Done, raise exception */
		if (iter->exc_type != NULL) {
			PyErr_SetObject(iter->exc_type, iter->exc_val);
			Py_DECREF(iter);
			return NULL;
		} else {
			Py_BEGIN_ALLOW_THREADS
			/* FIXME: Don't waste cycles */
			Py_END_ALLOW_THREADS
		}
	}
	first = iter->head;
	ret = iter->head->tuple;
	iter->head = first->next;
	if (first == iter->tail)
		iter->tail = NULL;
	free(first);
	iter->queue_size--;
	Py_DECREF(iter);
	return ret;
}

static PyObject *py_iter_append(LogIteratorObject *iter, PyObject *tuple)
{
	struct log_entry *entry;

	entry = calloc(sizeof(struct log_entry), 1);
	if (entry == NULL) {
		PyErr_NoMemory();
		return NULL;
	}

	entry->tuple = tuple;
	if (iter->tail == NULL) {
		iter->tail = entry;
	} else {
		iter->tail->next = entry;
		iter->tail = entry;
	}
	if (iter->head == NULL)
		iter->head = entry;

	iter->queue_size++;

	Py_RETURN_NONE;
}

PyTypeObject LogIterator_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.LogIterator", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(LogIteratorObject), 
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */
	
	/* Methods to implement standard operations */

	.tp_dealloc = (destructor)log_iter_dealloc, /*	destructor tp_dealloc;	*/

#if PY_MAJOR_VERSION < 3
	/* Flags to define presence of optional/expanded features */
	.tp_flags = Py_TPFLAGS_HAVE_ITER, /*	long tp_flags;	*/
#endif

	/* Iterators */
	.tp_iter = PyObject_SelfIter,
	.tp_iternext = (iternextfunc)log_iter_next,
};

#if ONLY_SINCE_SVN(1, 5)
static svn_error_t *py_iter_log_entry_cb(void *baton, svn_log_entry_t *log_entry, apr_pool_t *pool)
{
	PyObject *revprops, *py_changed_paths, *ret, *tuple;
	LogIteratorObject *iter = (LogIteratorObject *)baton;

	PyGILState_STATE state;

	state = PyGILState_Ensure();

#if ONLY_SINCE_SVN(1, 6)
	py_changed_paths = pyify_changed_paths2(log_entry->changed_paths2, pool);
#else
	py_changed_paths = pyify_changed_paths(log_entry->changed_paths, true, pool);
#endif
	if (py_changed_paths == NULL) {
		PyGILState_Release(state);
		return py_svn_error();
	}

	revprops = prop_hash_to_dict(log_entry->revprops);
	if (revprops == NULL) {
		Py_DECREF(py_changed_paths);
		PyGILState_Release(state);
		return py_svn_error();
	}

	tuple = Py_BuildValue("NlNb", py_changed_paths,
						log_entry->revision, revprops, log_entry->has_children);
	if (tuple == NULL) {
		Py_DECREF(revprops);
		Py_DECREF(py_changed_paths);
		PyGILState_Release(state);
		return py_svn_error();
	}

	ret = py_iter_append(iter, tuple);
	if (ret == NULL) {
		Py_DECREF(tuple);
		PyGILState_Release(state);
		return py_svn_error();
	}

	Py_DECREF(ret);

	PyGILState_Release(state);

	return NULL;
}
#else
static svn_error_t *py_iter_log_cb(void *baton, apr_hash_t *changed_paths, svn_revnum_t revision, const char *author, const char *date, const char *message, apr_pool_t *pool)
{
	PyObject *revprops, *py_changed_paths, *ret, *tuple;
	LogIteratorObject *iter = (LogIteratorObject *)baton;

	PyGILState_STATE state;

	state = PyGILState_Ensure();

	if (!pyify_log_message(changed_paths, author, date, message, true,
	pool, &py_changed_paths, &revprops)) {
		goto fail;
	}
	tuple = Py_BuildValue("NlN", py_changed_paths, revision, revprops);
	if (tuple == NULL) {
		goto fail_tuple;
	}

	ret = py_iter_append(iter, tuple);

	if (ret == NULL) {
		Py_DECREF(tuple);
		goto fail;
	}

	Py_DECREF(ret);

	PyGILState_Release(state);

	return NULL;
	
fail_tuple:
	Py_DECREF(revprops);
	Py_DECREF(py_changed_paths);
fail:
	PyGILState_Release(state);
	return py_svn_error();
}
#endif


static void py_iter_log(void *baton)
{
	LogIteratorObject *iter = (LogIteratorObject *)baton;
	svn_error_t *error;
	PyGILState_STATE state;

#if ONLY_SINCE_SVN(1, 5)
	error = svn_ra_get_log2(iter->ra->ra, 
			iter->apr_paths, iter->start, iter->end, iter->limit,
			iter->discover_changed_paths, iter->strict_node_history, 
			iter->include_merged_revisions, iter->apr_revprops,
			py_iter_log_entry_cb, iter, iter->pool);
#else
	error = svn_ra_get_log(iter->ra->ra, 
			iter->apr_paths, iter->start, iter->end, iter->limit,
			iter->discover_changed_paths, iter->strict_node_history, py_iter_log_cb, 
			iter, iter->pool);
#endif
	state = PyGILState_Ensure();
	if (error != NULL) {
		iter->exc_type = (PyObject *)PyErr_GetSubversionExceptionTypeObject();
		iter->exc_val  = PyErr_NewSubversionException(error);
		svn_error_clear(error);
	} else {
		iter->exc_type = PyExc_StopIteration;
		Py_INCREF(iter->exc_type);
		iter->exc_val = Py_None;
		Py_INCREF(iter->exc_val);
	}
	iter->done = TRUE;
	iter->ra->busy = false;

	Py_DECREF(iter);
	PyGILState_Release(state);
}

PyObject *ra_iter_log(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char *kwnames[] = { "paths", "start", "end", "limit",
		"discover_changed_paths", "strict_node_history", "include_merged_revisions", "revprops", NULL };
	PyObject *paths;
	svn_revnum_t start = 0, end = 0;
	int limit=0; 
	bool discover_changed_paths=false, strict_node_history=true, include_merged_revisions=false;
	RemoteAccessObject *ra = (RemoteAccessObject *)self;
	PyObject *revprops = Py_None;
	LogIteratorObject *ret;
	apr_pool_t *pool;
	apr_array_header_t *apr_paths;
	apr_array_header_t *apr_revprops;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oll|ibbbO:iter_log", kwnames, 
						 &paths, &start, &end, &limit,
						 &discover_changed_paths, &strict_node_history,
						 &include_merged_revisions, &revprops))
		return NULL;

	if (!ra_get_log_prepare(ra, paths, include_merged_revisions,
	revprops, &pool, &apr_paths, &apr_revprops)) {
		return NULL;
	}

	ret = PyObject_New(LogIteratorObject, &LogIterator_Type);
	ret->ra = ra;
	Py_INCREF(ret->ra);
	ret->start = start;
	ret->exc_type = NULL;
	ret->exc_val = NULL;
	ret->discover_changed_paths = discover_changed_paths;
	ret->end = end;
	ret->limit = limit;
	ret->apr_paths = apr_paths;
	ret->pool = pool;
	ret->include_merged_revisions = include_merged_revisions;
	ret->strict_node_history = strict_node_history;
	ret->apr_revprops = apr_revprops;
	ret->done = FALSE;
	ret->queue_size = 0;
	ret->head = NULL;
	ret->tail = NULL;

	Py_INCREF(ret);
	PyThread_start_new_thread(py_iter_log, ret);

	return (PyObject *)ret;
}


