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

#ifndef INCLUDE_pygit2_utils_h
#define INCLUDE_pygit2_utils_h

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <git2.h>
#include "types.h"

#ifdef __GNUC__
#  define PYGIT2_FN_UNUSED __attribute__((unused))
#else
#  define PYGIT2_FN_UNUSED
#endif

#if defined(PYPY_VERSION)
#define Py_FileSystemDefaultEncodeErrors "surrogateescape"
#endif

#define to_encoding(x) PyUnicode_DecodeASCII(x, strlen(x), "strict")


#define CHECK_REFERENCE(self)\
    if (self->reference == NULL) {\
        PyErr_SetString(GitError, "deleted reference");\
        return NULL;\
    }

#define CHECK_REFERENCE_INT(self)\
    if (self->reference == NULL) {\
        PyErr_SetString(GitError, "deleted reference");\
        return -1;\
    }


/* Utilities */
#define to_unicode(x, encoding, errors) to_unicode_n(x, strlen(x), encoding, errors)

PyObject *to_unicode_safe(const char *value, const char *encoding);

PYGIT2_FN_UNUSED
Py_LOCAL_INLINE(PyObject*)
to_unicode_n(const char *value, size_t len, const char *encoding,
             const char *errors)
{
    if (encoding == NULL) {
        encoding = "utf-8"; // Default to UTF-8

        /* If the encoding is not explicit, it may not be UTF-8, so it is not
         * safe to decode it strictly.  This is rare in the wild, but does
         * occur in old commits to git itself (e.g. c31820c2).
         * https://github.com/libgit2/pygit2/issues/77
         */
        if (errors == NULL) {
            errors = "replace";
        }
    }

    return PyUnicode_Decode(value, len, encoding, errors);
}

#define value_or_default(x, _default) ((x) == NULL ? (_default) : (x))

const char* pgit_borrow(PyObject *value);
const char* pgit_borrow_encoding(PyObject *value, const char *encoding, const char *errors, PyObject **tvalue);
char* pgit_borrow_fsdefault(PyObject *value, PyObject **tvalue);


//PyObject * get_pylist_from_git_strarray(git_strarray *strarray);
//int get_strarraygit_from_pylist(git_strarray *array, PyObject *pylist);

git_otype py_object_to_otype(PyObject *py_type);


/* Enum utilities (pygit2.enums) */
PyObject *pygit2_enum(PyObject *enum_type, int value);


/* Helpers to make shorter PyMethodDef and PyGetSetDef blocks */
#define METHOD(type, name, args)\
  {#name, (PyCFunction) type ## _ ## name, args, type ## _ ## name ## __doc__}

#define GETTER(type, attr)\
  {         #attr,\
   (getter) type ## _ ## attr ## __get__,\
            NULL,\
            type ## _ ## attr ## __doc__,\
            NULL}

#define GETSET(type, attr)\
  {         #attr,\
   (getter) type ## _ ## attr ## __get__,\
   (setter) type ## _ ## attr ## __set__,\
            type ## _ ## attr ## __doc__,\
            NULL}

#define MEMBER(type, attr, attr_type, docstr)\
  {#attr, attr_type, offsetof(type, attr), 0, PyDoc_STR(docstr)}

#define RMEMBER(type, attr, attr_type, docstr)\
  {#attr, attr_type, offsetof(type, attr), READONLY, PyDoc_STR(docstr)}


/* Helpers for memory allocation */
#define CALLOC(ptr, num, size, label) \
        ptr = calloc((num), size);\
        if (ptr == NULL) {\
            err = GIT_ERROR;\
            giterr_set_oom();\
            goto label;\
        }

#define MALLOC(ptr, size, label) \
        ptr = malloc(size);\
        if (ptr == NULL) {\
            err = GIT_ERROR;\
            giterr_set_oom();\
            goto label;\
        }

/* Helpers to make type init shorter. */
#define INIT_TYPE(type, base, new) \
    type.tp_base = base; \
    type.tp_new = new; \
    if (PyType_Ready(&type) < 0) return NULL;

#define ADD_TYPE(module, type) \
    Py_INCREF(& type ## Type);\
    if (PyModule_AddObject(module, #type, (PyObject*) & type ## Type) == -1)\
        return NULL;

#define ADD_EXC(m, name, base)\
    name = PyErr_NewException("_pygit2." #name, base, NULL);\
    if (name == NULL) goto fail;\
    Py_INCREF(name);\
    if (PyModule_AddObject(m, #name, name)) {\
        Py_DECREF(name);\
        goto fail;\
    }

#define ADD_CONSTANT_INT(m, name) \
    if (PyModule_AddIntConstant(m, #name, name) == -1) return NULL;

#define ADD_CONSTANT_STR(m, name) \
    if (PyModule_AddStringConstant(m, #name, name) == -1) return NULL;


#endif
