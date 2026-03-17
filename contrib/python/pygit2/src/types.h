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

#ifndef INCLUDE_pygit2_objects_h
#define INCLUDE_pygit2_objects_h

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <git2.h>
#include <git2/sys/filter.h>

#if !(LIBGIT2_VER_MAJOR == 1 && LIBGIT2_VER_MINOR == 9)
#error You need a compatible libgit2 version (1.9.x)
#endif

/*
 * Python objects
 *
 **/

/* git_repository */
typedef struct {
    PyObject_HEAD
    git_repository *repo;
    PyObject *index;  /* It will be None for a bare repository */
    PyObject *config; /* It will be None for a bare repository */
    int owned;    /* _from_c() sometimes means we don't own the C pointer */
} Repository;


typedef struct {
    PyObject_HEAD
    git_oid oid;
} Oid;

typedef struct {
    PyObject_HEAD
    git_odb *odb;
} Odb;

typedef struct {
    PyObject_HEAD
    git_odb_backend *odb_backend;
} OdbBackend;

typedef struct {
    OdbBackend super;
} OdbBackendPack;

typedef struct {
    OdbBackend super;
} OdbBackendLoose;

typedef struct {
    PyObject_HEAD
    git_refdb *refdb;
} Refdb;

typedef struct {
    PyObject_HEAD
    git_refdb_backend *refdb_backend;
} RefdbBackend;

typedef struct {
    RefdbBackend super;
} RefdbFsBackend;

typedef struct {
    PyObject_HEAD
    git_reference_iterator *iterator;
} RefsIterator;

#define SIMPLE_TYPE(_name, _ptr_type, _ptr_name) \
        typedef struct {\
            PyObject_HEAD\
            Repository *repo;\
            _ptr_type *_ptr_name;\
        } _name;

#define OBJECT_TYPE(_name, _ptr_type, _ptr_name) \
        typedef struct {\
            PyObject_HEAD\
            Repository *repo;\
            _ptr_type *_ptr_name;\
            const git_tree_entry *entry;\
        } _name;


/* git object types
 *
 * The structs for some of the object subtypes are identical except for
 * the type of their object pointers. */
OBJECT_TYPE(Object, git_object, obj)
OBJECT_TYPE(Commit, git_commit, commit)
OBJECT_TYPE(Tree, git_tree, tree)
OBJECT_TYPE(Blob, git_blob, blob)
OBJECT_TYPE(Tag, git_tag, tag)

SIMPLE_TYPE(Worktree, git_worktree, worktree)

/* git_note */
typedef struct {
    PyObject_HEAD
    Repository *repo;
    const char *ref;
    PyObject *annotated_id;
    PyObject *id;
    git_note *note;
} Note;

typedef struct {
    PyObject_HEAD
    Repository *repo;
    git_note_iterator* iter;
    char* ref;
} NoteIter;

/* git_patch */
typedef struct {
    PyObject_HEAD
    git_patch *patch;
    Blob* oldblob;
    Blob* newblob;
} Patch;

/* git_diff */
SIMPLE_TYPE(Diff, git_diff, diff)

typedef struct {
    PyObject_HEAD
    Diff *diff;
    size_t i;
    size_t n;
} DeltasIter;

typedef struct {
    PyObject_HEAD
    Diff *diff;
    size_t i;
    size_t n;
} DiffIter;

typedef struct {
    PyObject_HEAD
    PyObject *id;
    char *path;
    PyObject *raw_path;
    git_off_t size;
    uint32_t flags;
    uint16_t mode;
} DiffFile;

typedef struct {
    PyObject_HEAD
    git_delta_t status;
    uint32_t flags;
    uint16_t similarity;
    uint16_t nfiles;
    PyObject *old_file;
    PyObject *new_file;
} DiffDelta;

typedef struct {
    PyObject_HEAD
    Patch *patch;
    const git_diff_hunk *hunk;
    size_t idx;
    size_t n_lines;
} DiffHunk;

typedef struct {
    PyObject_HEAD
    DiffHunk *hunk;
    const git_diff_line *line;
} DiffLine;

SIMPLE_TYPE(DiffStats, git_diff_stats, stats);

/* git_tree_walk , git_treebuilder*/
SIMPLE_TYPE(TreeBuilder, git_treebuilder, bld)

typedef struct {
    PyObject_HEAD
    Tree *owner;
    int i;
} TreeIter;


/* git_index */
typedef struct {
    PyObject_HEAD
    git_index_entry entry;
} IndexEntry;


/* git_reference, git_reflog */
SIMPLE_TYPE(Walker, git_revwalk, walk)

SIMPLE_TYPE(Reference, git_reference, reference)

typedef Reference Branch;

typedef struct {
    PyObject_HEAD
    git_signature *signature;
    PyObject *oid_old;
    PyObject *oid_new;
    char *message;
} RefLogEntry;

typedef struct {
    PyObject_HEAD
    git_reflog *reflog;
    size_t i;
    size_t size;
} RefLogIter;

/* git_revspec */
typedef struct {
    PyObject_HEAD
    PyObject *from;
    PyObject *to;
    unsigned int flags;
} RevSpec;

/* git_signature */
typedef struct {
    PyObject_HEAD
    Object *obj;
    const git_signature *signature;
    char *encoding;
} Signature;

/* git_mailmap */
typedef struct {
    PyObject_HEAD
    git_mailmap *mailmap;
} Mailmap;

typedef struct {
    PyObject_HEAD
    PyObject *commit_id;
    char *message;
} Stash;

typedef struct {
    PyObject_HEAD
    const git_filter_source *src;
} FilterSource;

#endif
