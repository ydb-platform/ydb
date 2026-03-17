/* Copyright © 2008 Jelmer Vernooij <jelmer@jelmer.uk>
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
 * GNU Lesser Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 */
#include <stdbool.h>
#include <Python.h>
#include <apr_general.h>
#include <svn_types.h>
#include <svn_delta.h>
#include <svn_path.h>

#include "editor.h"
#include "util.h"

typedef struct EditorObject {
	PyObject_VAR_HEAD
	const svn_delta_editor_t *editor;
	void *baton;
	apr_pool_t *pool;
	void (*done_cb) (void *baton);
	void *done_baton;
	bool done;
	PyObject *commit_callback;
	bool active_child;
	struct EditorObject *parent;
} EditorObject;

static PyObject *py_editor_ctx_enter(PyObject *self)
{
	Py_INCREF(self);
	return self;
}

PyObject *new_editor_object(struct EditorObject *parent,
							const svn_delta_editor_t *editor, void *baton,
							apr_pool_t *pool, PyTypeObject *type,
							void (*done_cb) (void *), void *done_baton, PyObject *commit_callback)
{
	EditorObject *obj = PyObject_New(EditorObject, type);
	if (obj == NULL)
		return NULL;
	obj->editor = editor;
	obj->baton = baton;
	obj->pool = pool;
	obj->done_cb = done_cb;
	obj->done = false;
	obj->done_baton = done_baton;
	obj->commit_callback = commit_callback;
	obj->active_child = false;
	if (parent != NULL) {
		Py_INCREF(parent);
		parent->active_child = true;
	}
	obj->parent = (EditorObject *)parent;
	return (PyObject *)obj;
}

static void py_editor_dealloc(PyObject *self)
{
	EditorObject *editor = (EditorObject *)self;
	Py_XDECREF(editor->commit_callback);
	if (editor->pool != NULL) {
		apr_pool_destroy(editor->pool);
		editor->pool = NULL;
	}
	PyObject_Del(self);
}

#ifndef _WIN64
/* paranoia check */
#if defined(SIZEOF_SIZE_T) && SIZEOF_SIZE_T != SIZEOF_LONG
#error "Unable to determine PyArg_Parse format for size_t"
#endif
#endif

/* svn_filesize_t is always 64 bits */
#if SIZEOF_LONG == 8
#define SVN_FILESIZE_T_PYFMT "k"
#elif SIZEOF_LONG_LONG == 8
#define SVN_FILESIZE_T_PYFMT "K"
#else
#error "Unable to determine PyArg_Parse format for size_t"
#endif

static PyObject *txdelta_call(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char *kwnames[] = { "window", NULL };
	svn_txdelta_window_t window;
	TxDeltaWindowHandlerObject *obj = (TxDeltaWindowHandlerObject *)self;
	PyObject *py_window, *py_ops, *py_new_data;
	int i;
	svn_string_t new_data;
	svn_error_t *error;
	svn_txdelta_op_t *ops;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwnames, &py_window))
		return NULL;

	if (py_window == Py_None) {
		RUN_SVN(obj->txdelta_handler(NULL, obj->txdelta_baton));
		Py_RETURN_NONE;
	}

	if (!PyArg_ParseTuple(py_window, SVN_FILESIZE_T_PYFMT "kkiOO",
		&window.sview_offset, &window.sview_len, &window.tview_len,
		&window.src_ops, &py_ops, &py_new_data))
		return NULL;

	if (py_new_data == Py_None) {
		window.new_data = NULL;
	} else {
		if (!PyBytes_Check(py_new_data)) {
			PyErr_SetString(PyExc_TypeError, "delta data should be bytes");
			return NULL;
		}
		new_data.data = PyBytes_AsString(py_new_data);
		new_data.len = PyBytes_Size(py_new_data);
		window.new_data = &new_data;
	}

	if (!PyList_Check(py_ops)) {
		PyErr_SetString(PyExc_TypeError, "ops not a list");
		return NULL;
	}

	window.num_ops = PyList_Size(py_ops);

	window.ops = ops = malloc(sizeof(svn_txdelta_op_t) * window.num_ops);

	for (i = 0; i < window.num_ops; i++) {
		PyObject *windowitem = PyList_GetItem(py_ops, i);
		if (!PyArg_ParseTuple(windowitem, "ikk", &ops[i].action_code, 
							  &ops[i].offset, &ops[i].length)) {
			free(ops);
			return NULL;
		}
	}

	Py_BEGIN_ALLOW_THREADS
	error = obj->txdelta_handler(&window, obj->txdelta_baton);
	Py_END_ALLOW_THREADS
	if (error != NULL) {
		handle_svn_error(error);
		svn_error_clear(error);
		free(ops);
		return NULL;
	}

	free(ops);

	Py_RETURN_NONE;
}

static void py_txdelta_window_handler_dealloc(PyObject *self)
{
	PyObject_Del(self);
}

PyTypeObject TxDeltaWindowHandler_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.TxDeltaWindowHandler", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(TxDeltaWindowHandlerObject), 
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */
	
	/* Methods to implement standard operations */
	
	py_txdelta_window_handler_dealloc, /* destructor tp_dealloc; */
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
	txdelta_call, /*	ternaryfunc tp_call;	*/
	
};

static PyObject *py_file_editor_apply_textdelta(PyObject *self, PyObject *args)
{
	EditorObject *editor = (EditorObject *)self;
	char *c_base_checksum = NULL;
	svn_txdelta_window_handler_t txdelta_handler;
	void *txdelta_baton;
	TxDeltaWindowHandlerObject *py_txdelta;

	if (!PyArg_ParseTuple(args, "|z", &c_base_checksum))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "file editor already closed");
		return NULL;
	}

	RUN_SVN(editor->editor->apply_textdelta(editor->baton,
				c_base_checksum, editor->pool, 
				&txdelta_handler, &txdelta_baton));
	py_txdelta = PyObject_New(TxDeltaWindowHandlerObject, &TxDeltaWindowHandler_Type);
	py_txdelta->txdelta_handler = txdelta_handler;
	py_txdelta->txdelta_baton = txdelta_baton;
	return (PyObject *)py_txdelta;
}

static PyObject *py_file_editor_change_prop(PyObject *self, PyObject *args)
{
	EditorObject *editor = (EditorObject *)self;
	char *name;
	svn_string_t c_value;
	int vallen;

	if (!PyArg_ParseTuple(args, "sz#", &name, &c_value.data, &vallen))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "file editor already closed");
		return NULL;
	}

	c_value.len = vallen;

	RUN_SVN(editor->editor->change_file_prop(editor->baton, name, 
				(c_value.data == NULL)?NULL:&c_value, editor->pool));

	Py_RETURN_NONE;
}

static PyObject *py_file_editor_close(PyObject *self, PyObject *args)
{
	EditorObject *editor = (EditorObject *)self;
	char *c_checksum = NULL;

	if (!PyArg_ParseTuple(args, "|z", &c_checksum))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "file editor was already closed");
		return NULL;
	}

	RUN_SVN(editor->editor->close_file(editor->baton, c_checksum, 
					editor->pool));

	editor->parent->active_child = false;
	Py_DECREF(editor->parent);

	editor->done = true;
	apr_pool_destroy(editor->pool);
	editor->pool = NULL;

	Py_RETURN_NONE;
}

static PyObject *py_file_editor_ctx_enter(PyObject *self)
{
	Py_INCREF(self);
	return self;
}

static PyObject *py_file_editor_ctx_exit(PyObject *self, PyObject *args)
{
	EditorObject *editor = (EditorObject *)self;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "file editor already closed");
		return NULL;
	}

	RUN_SVN(editor->editor->close_file(editor->baton, NULL, editor->pool));

	editor->parent->active_child = false;
	Py_DECREF(editor->parent);

	editor->done = true;
	apr_pool_destroy(editor->pool);
	editor->pool = NULL;

	Py_RETURN_FALSE;
}

static PyMethodDef py_file_editor_methods[] = {
	{ "change_prop", py_file_editor_change_prop, METH_VARARGS, NULL },
	{ "close", py_file_editor_close, METH_VARARGS, NULL },
	{ "apply_textdelta", py_file_editor_apply_textdelta, METH_VARARGS, NULL },
	{ "__enter__", (PyCFunction)py_file_editor_ctx_enter, METH_NOARGS, NULL },
	{ "__exit__", py_file_editor_ctx_exit, METH_VARARGS, NULL },
	{ NULL }
};

PyTypeObject FileEditor_Type = { 
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.FileEditor", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(EditorObject), 
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */
	
	/* Methods to implement standard operations */
	
	py_editor_dealloc, /*	destructor tp_dealloc; 	*/
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
	py_file_editor_methods, /*	struct PyMethodDef *tp_methods;	*/
};

static PyObject *py_dir_editor_delete_entry(PyObject *self, PyObject *args)
{
	EditorObject *editor = (EditorObject *)self;
	const char *path;
	PyObject *py_path;
	svn_revnum_t revision = -1;

	if (!PyArg_ParseTuple(args, "O|l", &py_path, &revision))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "a child is already open");
		return NULL;
	}

	path = py_object_to_svn_relpath(py_path, editor->pool);
	if (path == NULL) {
		return NULL;
	}

	RUN_SVN(editor->editor->delete_entry(path, revision, editor->baton, editor->pool));

	Py_RETURN_NONE;
}

static PyObject *py_dir_editor_add_directory(PyObject *self, PyObject *args)
{
	PyObject *py_path;
	const char *path;
	char *copyfrom_path = NULL;
	svn_revnum_t copyfrom_rev = -1;
	void *child_baton;
	EditorObject *editor = (EditorObject *)self;
	apr_pool_t *subpool;

	if (!PyArg_ParseTuple(args, "O|zl", &py_path, &copyfrom_path, &copyfrom_rev))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "child is already open");
		return NULL;
	}

	path = py_object_to_svn_relpath(py_path, editor->pool);
	if (path == NULL) {
		return NULL;
	}

	RUN_SVN(editor->editor->add_directory(
		path, editor->baton,
		copyfrom_path == NULL?NULL:svn_uri_canonicalize(copyfrom_path, editor->pool),
		copyfrom_rev, editor->pool, &child_baton));

	subpool = Pool(editor->pool);
	if (subpool == NULL)
		return NULL;

	return new_editor_object(editor, editor->editor, child_baton, subpool, 
							 &DirectoryEditor_Type, NULL, NULL, NULL);
}

static PyObject *py_dir_editor_open_directory(PyObject *self, PyObject *args)
{
	const char *path;
	PyObject *py_path;
	EditorObject *editor = (EditorObject *)self;
	svn_revnum_t base_revision=-1;
	void *child_baton;
	apr_pool_t *subpool;

	if (!PyArg_ParseTuple(args, "O|l", &py_path, &base_revision))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "child is already open");
		return NULL;
	}

	path = py_object_to_svn_relpath(py_path, editor->pool);
	if (path == NULL) {
		return NULL;
	}

	RUN_SVN(editor->editor->open_directory(
		path, editor->baton, base_revision, editor->pool, &child_baton));

	subpool = Pool(NULL);
	if (subpool == NULL)
		return NULL;

	return new_editor_object(editor, editor->editor, child_baton, subpool,
							 &DirectoryEditor_Type, NULL, NULL, NULL);
}

static PyObject *py_dir_editor_change_prop(PyObject *self, PyObject *args)
{
	char *name;
	svn_string_t c_value;
	EditorObject *editor = (EditorObject *)self;
	int vallen;

	if (!PyArg_ParseTuple(args, "sz#", &name, &c_value.data, &vallen))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "child is already open");
		return NULL;
	}

	c_value.len = vallen;

	RUN_SVN(editor->editor->change_dir_prop(editor->baton, name,
					(c_value.data == NULL)?NULL:&c_value, editor->pool));

	Py_RETURN_NONE;
}

static PyObject *py_dir_editor_close(PyObject *self)
{
	EditorObject *editor = (EditorObject *)self;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor was already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "child is still open");
		return NULL;
	}

	RUN_SVN(editor->editor->close_directory(editor->baton, editor->pool));

	if (editor->parent != NULL) {
		editor->parent->active_child = false;
		Py_DECREF(editor->parent);
	}

	editor->done = true;
	apr_pool_destroy(editor->pool);
	editor->pool = NULL;

	Py_RETURN_NONE;
}

static PyObject *py_dir_editor_absent_directory(PyObject *self, PyObject *args)
{
	const char *path;
	PyObject *py_path;
	EditorObject *editor = (EditorObject *)self;

	if (!PyArg_ParseTuple(args, "O", &py_path))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "another child is still open");
		return NULL;
	}

	path = py_object_to_svn_relpath(py_path, editor->pool);
	if (path == NULL) {
		return NULL;
	}

	RUN_SVN(editor->editor->absent_directory(
		path, editor->baton, editor->pool));

	Py_RETURN_NONE;
}

static PyObject *py_dir_editor_add_file(PyObject *self, PyObject *args)
{
	const char *path;
	char *copy_path=NULL;
	PyObject *py_path;
	svn_revnum_t copy_rev=-1;
	void *file_baton = NULL;
	EditorObject *editor = (EditorObject *)self;
	apr_pool_t *subpool;

	if (!PyArg_ParseTuple(args, "O|zl", &py_path, &copy_path, &copy_rev))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "another child is still open");
		return NULL;
	}

	path = py_object_to_svn_relpath(py_path, editor->pool);
	if (path == NULL) {
		return NULL;
	}

	RUN_SVN(editor->editor->add_file(path, editor->baton,
		copy_path == NULL?NULL:svn_uri_canonicalize(copy_path, editor->pool),
		copy_rev, editor->pool, &file_baton));

	subpool = Pool(NULL);
	if (subpool == NULL)
		return NULL;

	return new_editor_object(editor, editor->editor, file_baton, subpool,
							 &FileEditor_Type, NULL, NULL, NULL);
}

static PyObject *py_dir_editor_open_file(PyObject *self, PyObject *args)
{
	const char *path;
	PyObject *py_path;
	svn_revnum_t base_revision=-1;
	void *file_baton;
	EditorObject *editor = (EditorObject *)self;
	apr_pool_t *subpool;

	if (!PyArg_ParseTuple(args, "O|l", &py_path, &base_revision))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "another child is still open");
		return NULL;
	}

	path = py_object_to_svn_relpath(py_path, editor->pool);
	if (path == NULL) {
		return NULL;
	}

	RUN_SVN(editor->editor->open_file(path, editor->baton, base_revision,
									  editor->pool, &file_baton));

	subpool = Pool(NULL);
	if (subpool == NULL)
		return NULL;

	return new_editor_object(editor, editor->editor, file_baton, subpool,
							 &FileEditor_Type, NULL, NULL, NULL);
}

static PyObject *py_dir_editor_absent_file(PyObject *self, PyObject *args)
{
	const char *path;
	PyObject *py_path;
	EditorObject *editor = (EditorObject *)self;

	if (!PyArg_ParseTuple(args, "O", &py_path))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "another child is still open");
		return NULL;
	}

	path = py_object_to_svn_relpath(py_path, editor->pool);
	if (path == NULL) {
		return NULL;
	}

	RUN_SVN(editor->editor->absent_file(
		path, editor->baton, editor->pool));

	Py_RETURN_NONE;
}

static PyObject *py_dir_editor_ctx_enter(PyObject *self)
{
	Py_INCREF(self);
	return self;
}

static PyObject *py_dir_editor_ctx_exit(PyObject *self, PyObject *args)
{
	EditorObject *editor = (EditorObject *)self;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "directory editor already closed");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "a child is still open");
		return NULL;
	}

	RUN_SVN(editor->editor->close_directory(editor->baton, editor->pool));

	if (editor->parent != NULL) {
		editor->parent->active_child = false;
		Py_DECREF(editor->parent);
	}

	editor->done = true;
	apr_pool_destroy(editor->pool);
	editor->pool = NULL;

	Py_RETURN_FALSE;
}

static PyMethodDef py_dir_editor_methods[] = {
	{ "absent_file", py_dir_editor_absent_file, METH_VARARGS,
		"S.absent_file(path)\n\n"
		"Indicate a file is not present." },
	{ "absent_directory", py_dir_editor_absent_directory, METH_VARARGS, NULL },
	{ "delete_entry", py_dir_editor_delete_entry, METH_VARARGS, NULL },
	{ "add_file", py_dir_editor_add_file, METH_VARARGS, NULL },
	{ "open_file", py_dir_editor_open_file, METH_VARARGS, NULL },
	{ "add_directory", py_dir_editor_add_directory, METH_VARARGS, NULL },
	{ "open_directory", py_dir_editor_open_directory, METH_VARARGS, NULL },
	{ "close", (PyCFunction)py_dir_editor_close, METH_NOARGS, NULL },
	{ "change_prop", py_dir_editor_change_prop, METH_VARARGS, NULL },
	{ "__enter__", (PyCFunction)py_dir_editor_ctx_enter, METH_NOARGS, NULL },
	{ "__exit__", py_dir_editor_ctx_exit, METH_VARARGS, NULL },
	{ NULL, }
};

PyTypeObject DirectoryEditor_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.DirEditor", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(EditorObject),
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

	/* Methods to implement standard operations */

	py_editor_dealloc, /* destructor tp_dealloc;  */
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
	py_dir_editor_methods, /*	struct PyMethodDef *tp_methods;	*/
};

static PyObject *py_editor_set_target_revision(PyObject *self, PyObject *args)
{
	svn_revnum_t target_revision;
	EditorObject *editor = (EditorObject *)self;

	if (!PyArg_ParseTuple(args, "l", &target_revision))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "Editor already closed/aborted");
		return NULL;
	}

	RUN_SVN(editor->editor->set_target_revision(editor->baton,
					target_revision, editor->pool));

	Py_RETURN_NONE;
}

static PyObject *py_editor_open_root(PyObject *self, PyObject *args)
{
	svn_revnum_t base_revision=-1;
	void *root_baton;
	EditorObject *editor = (EditorObject *)self;
	apr_pool_t *subpool;

	if (!PyArg_ParseTuple(args, "|l:open_root", &base_revision))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "Editor already closed/aborted");
		return NULL;
	}

	RUN_SVN(editor->editor->open_root(editor->baton, base_revision,
					editor->pool, &root_baton));

	subpool = Pool(NULL);
	if (subpool == NULL)
		return NULL;

	return new_editor_object(editor, editor->editor, root_baton, subpool,
							 &DirectoryEditor_Type, NULL, NULL, NULL);
}

static PyObject *py_editor_close(PyObject *self)
{
	EditorObject *editor = (EditorObject *)self;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "Editor already closed/aborted");
		return NULL;
	}

	if (editor->active_child) {
		PyErr_SetString(PyExc_RuntimeError, "a child is still open");
		return NULL;
	}

	RUN_SVN(editor->editor->close_edit(editor->baton, editor->pool));

	editor->done = true;
	apr_pool_destroy(editor->pool);
	editor->pool = NULL;

	if (editor->done_cb != NULL)
		editor->done_cb(editor->done_baton);

	Py_RETURN_NONE;
}

static PyObject *py_editor_abort(PyObject *self)
{
	EditorObject *editor = (EditorObject *)self;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "Editor already closed/aborted");
		return NULL;
	}

	/* FIXME: Check for open active childs ? */

	RUN_SVN(editor->editor->abort_edit(editor->baton, editor->pool));

	editor->done = true;
	apr_pool_destroy(editor->pool);
	editor->pool = NULL;

	if (editor->done_cb != NULL)
		editor->done_cb(editor->done_baton);

	Py_RETURN_NONE;
}

static PyObject *py_editor_ctx_exit(PyObject *self, PyObject *args)
{
	EditorObject *editor = (EditorObject *)self;
	PyObject *exc_type, *exc_val, *exc_tb;

	if (!PyArg_ParseTuple(args, "OOO", &exc_type, &exc_val, &exc_tb))
		return NULL;

	if (editor->done) {
		PyErr_SetString(PyExc_RuntimeError, "Editor already closed/aborted");
		return NULL;
	}

	if (exc_type != Py_None) {
		RUN_SVN(editor->editor->abort_edit(editor->baton, editor->pool));
	} else {
		if (editor->active_child) {
			PyErr_SetString(PyExc_RuntimeError, "a child is still open");
			return NULL;
		}

		RUN_SVN(editor->editor->close_edit(editor->baton, editor->pool));
	}

	if (editor->done_cb != NULL)
		editor->done_cb(editor->done_baton);

	Py_RETURN_FALSE;
}

static PyMethodDef py_editor_methods[] = { 
	{ "abort", (PyCFunction)py_editor_abort, METH_NOARGS, 
		"S.abort()\n"
		"Close the editor, aborting the commit." },
	{ "close", (PyCFunction)py_editor_close, METH_NOARGS, 
		"S.close()\n"
		"Close the editor, finalizing the commit." },
	{ "open_root", py_editor_open_root, METH_VARARGS, 
		"S.open_root(base_revision=None) -> DirectoryEditor\n"
		"Open the root directory." },
	{ "set_target_revision", py_editor_set_target_revision, METH_VARARGS,
		"S.set_target_revision(target_revision)\n"
		"Set the target revision created by the reported revision."},
	{ "__enter__", (PyCFunction)py_editor_ctx_enter, METH_NOARGS, NULL },
	{ "__exit__", py_editor_ctx_exit, METH_VARARGS, NULL },
	{ NULL }
};

PyTypeObject Editor_Type = { 
	PyVarObject_HEAD_INIT(NULL, 0)
	"_ra.Editor", /*	const char *tp_name;  For printing, in format "<module>.<name>" */
	sizeof(EditorObject), 
	0,/*	Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */
	
	/* Methods to implement standard operations */
	
	py_editor_dealloc, /*	destructor tp_dealloc;	*/
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
	py_editor_methods, /*	struct PyMethodDef *tp_methods;	*/
};


static svn_error_t *py_cb_editor_set_target_revision(void *edit_baton, svn_revnum_t target_revision, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)edit_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();

	ret = PyObject_CallMethod(self, "set_target_revision", "l", target_revision);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_open_root(void *edit_baton, svn_revnum_t base_revision, apr_pool_t *pool, void **root_baton)
{
	PyObject *self = (PyObject *)edit_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	*root_baton = NULL;
	ret = PyObject_CallMethod(self, "open_root", "l", base_revision);
	CB_CHECK_PYRETVAL(ret);
	*root_baton = (void *)ret;
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_delete_entry(const char *path, svn_revnum_t revision, void *parent_baton, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)parent_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallMethod(self, "delete_entry", "sl", path, revision);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_add_directory(const char *path, void *parent_baton, const char *copyfrom_path, svn_revnum_t copyfrom_revision, apr_pool_t *pool, void **child_baton)
{
	PyObject *self = (PyObject *)parent_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	*child_baton = NULL;

	if (copyfrom_path == NULL) {
		ret = PyObject_CallMethod(self, "add_directory", "s", path);
	} else {
		ret = PyObject_CallMethod(self, "add_directory", "ssl", path, copyfrom_path, copyfrom_revision);
	}
	CB_CHECK_PYRETVAL(ret);
	*child_baton = (void *)ret;
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_open_directory(const char *path, void *parent_baton, svn_revnum_t base_revision, apr_pool_t *pool, void **child_baton)
{
	PyObject *self = (PyObject *)parent_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	*child_baton = NULL;
	ret = PyObject_CallMethod(self, "open_directory", "sl", path, base_revision);
	CB_CHECK_PYRETVAL(ret);
	*child_baton = (void *)ret;
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_change_prop(void *dir_baton, const char *name, const svn_string_t *value, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)dir_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();

	if (value != NULL) {
		ret = PyObject_CallMethod(self, "change_prop", "sz#", name, value->data, value->len);
	} else {
		ret = PyObject_CallMethod(self, "change_prop", "sO", name, Py_None);
	}
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_close_directory(void *dir_baton, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)dir_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallMethod(self, "close", "");
	Py_DECREF(self);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_absent_directory(const char *path, void *parent_baton, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)parent_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallMethod(self, "absent_directory", "s", path);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_add_file(const char *path, void *parent_baton, const char *copy_path, svn_revnum_t copy_revision, apr_pool_t *file_pool, void **file_baton)
{
	PyObject *self = (PyObject *)parent_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	if (copy_path == NULL) {
		ret = PyObject_CallMethod(self, "add_file", "s", path);
	} else {
		ret = PyObject_CallMethod(self, "add_file", "ssl", path, copy_path, 
								  copy_revision);
	}
	CB_CHECK_PYRETVAL(ret);
	*file_baton = (void *)ret;
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_open_file(const char *path, void *parent_baton, svn_revnum_t base_revision, apr_pool_t *file_pool, void **file_baton)
{
	PyObject *self = (PyObject *)parent_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallMethod(self, "open_file", "sl", path, base_revision);
	CB_CHECK_PYRETVAL(ret);
	*file_baton = (void *)ret;
	PyGILState_Release(state);
	return NULL;
}

svn_error_t *py_txdelta_window_handler(svn_txdelta_window_t *window, void *baton)
{
	int i;
	PyObject *ops, *ret;
	PyObject *fn = (PyObject *)baton, *py_new_data, *py_window;
	PyGILState_STATE state;
	if (fn == Py_None) {
		/* User doesn't care about deltas */
		return NULL;
	}

	state = PyGILState_Ensure();

	if (window == NULL) {
		py_window = Py_None;
		Py_INCREF(py_window);
	} else {
		ops = PyList_New(window->num_ops);
		if (ops == NULL) {
			PyGILState_Release(state);
			return NULL;
		}
		for (i = 0; i < window->num_ops; i++) {
			PyObject *pyval = Py_BuildValue("(iII)", 
											window->ops[i].action_code, 
											window->ops[i].offset, 
											window->ops[i].length);
			CB_CHECK_PYRETVAL(pyval);
			if (PyList_SetItem(ops, i, pyval) != 0) {
				Py_DECREF(ops);
				Py_DECREF(pyval);
				PyGILState_Release(state);
				return NULL;
			}
		}
		if (window->new_data != NULL && window->new_data->data != NULL) {
			py_new_data = PyBytes_FromStringAndSize(window->new_data->data,
													window->new_data->len);
		} else {
			py_new_data = Py_None;
			Py_INCREF(py_new_data);
		}
		if (py_new_data == NULL) {
			Py_DECREF(ops);
			PyGILState_Release(state);
			return NULL;
		}

		py_window = Py_BuildValue("((LIIiNN))", 
								  window->sview_offset, 
								  window->sview_len, 
								  window->tview_len, 
								  window->src_ops, ops, py_new_data);
		CB_CHECK_PYRETVAL(py_window); /* FIXME: free ops and py_new_data */
	}
	ret = PyObject_CallFunction(fn, "O", py_window);
	Py_DECREF(py_window);
	if (window == NULL) {
		/* Signals all delta windows have been received */
		Py_DECREF(fn);
	}
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_apply_textdelta(void *file_baton, const char *base_checksum, apr_pool_t *pool, svn_txdelta_window_handler_t *handler, void **handler_baton)
{
	PyObject *self = (PyObject *)file_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	*handler_baton = NULL;

	ret = PyObject_CallMethod(self, "apply_textdelta", "z", base_checksum);
	CB_CHECK_PYRETVAL(ret);
	*handler_baton = (void *)ret;
	*handler = py_txdelta_window_handler;
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_close_file(void *file_baton, 
										 const char *text_checksum, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)file_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();

	if (text_checksum != NULL) {
		ret = PyObject_CallMethod(self, "close", "");
	} else {
		ret = PyObject_CallMethod(self, "close", "s", text_checksum);
	}
	Py_DECREF(self);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_absent_file(const char *path, void *parent_baton, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)parent_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallMethod(self, "absent_file", "s", path);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_close_edit(void *edit_baton, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)edit_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallMethod(self, "close", "");
	Py_DECREF(self);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

static svn_error_t *py_cb_editor_abort_edit(void *edit_baton, apr_pool_t *pool)
{
	PyObject *self = (PyObject *)edit_baton, *ret;
	PyGILState_STATE state = PyGILState_Ensure();
	ret = PyObject_CallMethod(self, "abort", "");
	Py_DECREF(self);
	CB_CHECK_PYRETVAL(ret);
	Py_DECREF(ret);
	PyGILState_Release(state);
	return NULL;
}

const svn_delta_editor_t py_editor = {
	py_cb_editor_set_target_revision,
	py_cb_editor_open_root,
	py_cb_editor_delete_entry,
	py_cb_editor_add_directory,
	py_cb_editor_open_directory,
	py_cb_editor_change_prop,
	py_cb_editor_close_directory,
	py_cb_editor_absent_directory,
	py_cb_editor_add_file,
	py_cb_editor_open_file,
	py_cb_editor_apply_textdelta,
	py_cb_editor_change_prop,
	py_cb_editor_close_file,
	py_cb_editor_absent_file,
	py_cb_editor_close_edit,
	py_cb_editor_abort_edit
};


