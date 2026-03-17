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

#ifndef _BZR_SVN_EDITOR_H_
#define _BZR_SVN_EDITOR_H_

#ifdef __GNUC__
#pragma GCC visibility push(hidden)
#endif

extern PyTypeObject DirectoryEditor_Type;
extern PyTypeObject FileEditor_Type;
extern PyTypeObject Editor_Type;
extern PyTypeObject TxDeltaWindowHandler_Type;
struct EditorObject;
PyObject *new_editor_object(
     struct EditorObject *parent, const
     svn_delta_editor_t *editor, void *baton, apr_pool_t
     *pool, PyTypeObject *type, void (*done_cb) (void *baton),
     void *done_baton, PyObject *commit_callback);

#define DirectoryEditor_Check(op) PyObject_TypeCheck(op, &DirectoryEditor_Type)
#define FileEditor_Check(op) PyObject_TypeCheck(op, &FileEditor_Type)
#define Editor_Check(op) PyObject_TypeCheck(op, &Editor_Type)
#define TxDeltaWindowHandler_Check(op) PyObject_TypeCheck(op, &TxDeltaWindowHandler_Type)

typedef struct {
    PyObject_HEAD
    svn_txdelta_window_handler_t txdelta_handler;
    void *txdelta_baton;
} TxDeltaWindowHandlerObject;

svn_error_t *py_txdelta_window_handler(svn_txdelta_window_t *window, void *baton);

#ifdef __GNUC__
#pragma GCC visibility pop
#endif

extern const svn_delta_editor_t py_editor;

#endif /* _BZR_SVN_EDITOR_H_ */
