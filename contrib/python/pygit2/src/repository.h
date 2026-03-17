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

#ifndef INCLUDE_pygit2_repository_h
#define INCLUDE_pygit2_repository_h

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <git2.h>
#include "types.h"

typedef enum {
	GIT_REFERENCES_ALL = 0,
	GIT_REFERENCES_BRANCHES = 1,
	GIT_REFERENCES_TAGS = 2,
} git_reference_iterator_return_t;

PyObject *wrap_repository(git_repository *c_repo);

int  Repository_init(Repository *self, PyObject *args, PyObject *kwds);
int  Repository_traverse(Repository *self, visitproc visit, void *arg);
int  Repository_clear(Repository *self);

PyObject* Repository_walk(Repository *self, PyObject *args);
PyObject* Repository_create_blob(Repository *self, PyObject *args);
PyObject* Repository_create_blob_fromdisk(Repository *self, PyObject *args);
PyObject* Repository_create_commit(Repository *self, PyObject *args);
PyObject* Repository_create_commit_string(Repository *self, PyObject *args);
PyObject* Repository_create_commit_with_signature(Repository *self, PyObject *args);
PyObject* Repository_create_tag(Repository *self, PyObject *args);
PyObject* Repository_create_branch(Repository *self, PyObject *args);
PyObject* Repository_references_iterator_init(Repository *self, PyObject *args);
PyObject* Repository_references_iterator_next(Repository *self, PyObject *args);
PyObject* Repository_listall_branches(Repository *self, PyObject *args);
PyObject* Repository_lookup_reference(Repository *self, PyObject *py_name);
PyObject* Repository_add_worktree(Repository *self, PyObject *args);
PyObject* Repository_lookup_worktree(Repository *self, PyObject *py_name);
PyObject* Repository_list_worktrees(Repository *self, PyObject *args);

PyObject* Repository_create_reference_direct(Repository *self, PyObject *args, PyObject* kw);
PyObject* Repository_create_reference_symbolic(Repository *self, PyObject *args, PyObject* kw);

PyObject* Repository_compress_references(Repository *self);
PyObject* Repository_status(Repository *self, PyObject *args, PyObject *kw);
PyObject* Repository_status_file(Repository *self, PyObject *value);
PyObject* Repository_TreeBuilder(Repository *self, PyObject *args);

PyObject* Repository_cherrypick(Repository *self, PyObject *py_oid);
PyObject* Repository_apply(Repository *self, PyObject *py_diff, PyObject *kwds);
PyObject* Repository_merge_analysis(Repository *self, PyObject *args);

#endif
