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
#include "error.h"
#include "utils.h"

extern PyTypeObject ReferenceType;
extern PyTypeObject TreeType;
extern PyTypeObject CommitType;
extern PyTypeObject BlobType;
extern PyTypeObject TagType;

/**
 * Attempt to convert a C string to a Python string with the given encoding.
 * If the conversion fails, return a fallback string.
 */
PyObject *
to_unicode_safe(const char *value, const char *encoding)
{
    PyObject *py_str;

    if (!value) {
        py_str = PyUnicode_FromString("None");
    } else {
        py_str = to_unicode(value, encoding, "replace");

        if (!py_str) {
            assert(PyErr_Occurred());
            py_str = PyUnicode_FromString("(error)");
            PyErr_Clear();
        }
    }

    assert(!PyErr_Occurred());
    assert(py_str);

    return py_str;
}

char*
pgit_borrow_fsdefault(PyObject *value, PyObject **tvalue)
{
    PyObject *str = PyOS_FSPath(value);
    if (str == NULL) {
        return NULL;
    }

    PyObject *bytes = PyUnicode_EncodeFSDefault(str);
    if (bytes == NULL) {
        return NULL;
    }

    *tvalue = bytes;
    return PyBytes_AS_STRING(bytes);
}

/**
 * Return a pointer to the underlying C string in 'value'. The pointer is
 * guaranteed by 'tvalue', decrease its refcount when done with the string.
 */
const char*
pgit_borrow_encoding(PyObject *value, const char *encoding, const char *errors, PyObject **tvalue)
{
    PyObject *py_value = NULL;
    PyObject *py_str = NULL;

    py_value = PyOS_FSPath(value);
    if (py_value == NULL) {
        Error_type_error("unexpected %.200s", value);
        return NULL;
    }

    // Get new PyBytes reference from value
    if (PyUnicode_Check(py_value)) { // Text string
        py_str = PyUnicode_AsEncodedString(
            py_value,
            encoding ? encoding : "utf-8",
            errors ? errors : "strict"
        );

        Py_DECREF(py_value);
        if (py_str == NULL)
            return NULL;
    } else if (PyBytes_Check(py_value)) { // Byte string
        py_str = py_value;
    } else { // Type error
        Error_type_error("unexpected %.200s", value);
        Py_DECREF(py_value);
        return NULL;
    }

    // Borrow c string from the new PyBytes reference
    char *c_str = PyBytes_AsString(py_str);
    if (c_str == NULL) {
        Py_DECREF(py_str);
        return NULL;
    }

    // Return the borrowed c string and the new PyBytes reference
    *tvalue = py_str;
    return c_str;
}


/**
 * Return a borrowed c string with the representation of the given Unicode or
 * Bytes object:
 * - If value is Unicode return the UTF-8 representation
 * - If value is Bytes return the raw sttring
 * In both cases the returned string is owned by value and must not be
 * modified, nor freed.
 */
const char*
pgit_borrow(PyObject *value)
{
    if (PyUnicode_Check(value)) { // Text string
        return PyUnicode_AsUTF8(value);
    } else if (PyBytes_Check(value)) { // Byte string
        return PyBytes_AsString(value);
    }

    // Type error
    Error_type_error("unexpected %.200s", value);
    return NULL;
}


static git_otype
py_type_to_git_type(PyTypeObject *py_type)
{
    if (py_type == &CommitType)
        return GIT_OBJECT_COMMIT;
    else if (py_type == &TreeType)
        return GIT_OBJECT_TREE;
    else if (py_type == &BlobType)
        return GIT_OBJECT_BLOB;
    else if (py_type == &TagType)
        return GIT_OBJECT_TAG;

    PyErr_SetString(PyExc_ValueError, "invalid target type");
    return GIT_OBJECT_INVALID; /* -1 */
}

git_otype
py_object_to_otype(PyObject *py_type)
{
    long value;

    if (py_type == Py_None)
        return GIT_OBJECT_ANY;

    if (PyLong_Check(py_type)) {
        value = PyLong_AsLong(py_type);
        if (value == -1 && PyErr_Occurred())
            return GIT_OBJECT_INVALID;

        /* TODO Check whether the value is a valid value */
        return (git_otype)value;
    }

    if (PyType_Check(py_type))
        return py_type_to_git_type((PyTypeObject *) py_type);

    PyErr_SetString(PyExc_ValueError, "invalid target type");
    return GIT_OBJECT_INVALID; /* -1 */
}


/**
 * Convert an integer to a reference to an IntEnum or IntFlag in pygit2.enums.
 */
PyObject *
pygit2_enum(PyObject *enum_type, int value)
{
    if (!enum_type) {
        PyErr_SetString(PyExc_TypeError, "an enum has not been cached in _pygit2.cache_enums()");
        return NULL;
    }
    PyObject *enum_instance = PyObject_CallFunction(enum_type, "(i)", value);
    return enum_instance;
}
