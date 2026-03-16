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
#include "utils.h"
#include "error.h"
#include "oid.h"

PyTypeObject OidType;

static const git_oid oid_zero = GIT_OID_SHA1_ZERO;


PyObject *
git_oid_to_python(const git_oid *oid)
{
    Oid *py_oid;

    py_oid = PyObject_New(Oid, &OidType);
    if (py_oid == NULL)
        return NULL;

    git_oid_cpy(&(py_oid->oid), oid);
    return (PyObject*)py_oid;
}

size_t
py_hex_to_git_oid(PyObject *py_oid, git_oid *oid)
{
    PyObject *py_hex;
    int err;
    char *hex;
    Py_ssize_t len;

    /* Unicode */
    if (PyUnicode_Check(py_oid)) {
        py_hex = PyUnicode_AsASCIIString(py_oid);
        if (py_hex == NULL)
            return 0;

        err = PyBytes_AsStringAndSize(py_hex, &hex, &len);
        if (err) {
            Py_DECREF(py_hex);
            return 0;
        }

        err = git_oid_fromstrn(oid, hex, len);
        Py_DECREF(py_hex);
        if (err < 0) {
            PyErr_SetObject(Error_type(err), py_oid);
            return 0;
        }

        return (size_t)len;
    }

    /* Type error */
    PyErr_SetObject(PyExc_TypeError, py_oid);
    return 0;
}

size_t
py_oid_to_git_oid(PyObject *py_oid, git_oid *oid)
{
    /* Oid */
    if (PyObject_TypeCheck(py_oid, (PyTypeObject*)&OidType)) {
        git_oid_cpy(oid, &((Oid*)py_oid)->oid);
        return GIT_OID_HEXSZ;
    }

    /* Hex */
    return py_hex_to_git_oid(py_oid, oid);
}

int
py_oid_to_git_oid_expand(git_repository *repo, PyObject *py_str, git_oid *oid)
{
    int err;
    size_t len;
    git_odb *odb = NULL;
    git_oid tmp;

    len = py_oid_to_git_oid(py_str, oid);
    if (len == 0)
        return -1;

    if (len == GIT_OID_HEXSZ)
        return 0;

    /* Short oid */
    err = git_repository_odb(&odb, repo);
    if (err < 0)
        goto error;

    err = git_odb_exists_prefix(&tmp, odb, oid, len);
    if (err < 0)
        goto error;

    git_oid_cpy(oid, &tmp);

    git_odb_free(odb);
    return 0;

error:
    git_odb_free(odb);
    Error_set(err);
    return -1;
}

PyObject *
git_oid_to_py_str(const git_oid *oid)
{
    char hex[GIT_OID_HEXSZ];

    git_oid_fmt(hex, oid);
    return to_unicode_n(hex, GIT_OID_HEXSZ, "utf-8", "strict");
}


int
Oid_init(Oid *self, PyObject *args, PyObject *kw)
{
    char *keywords[] = {"raw", "hex", NULL};
    PyObject *raw = NULL, *hex = NULL;
    int err;
    char *bytes;
    Py_ssize_t len;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "|OO", keywords, &raw, &hex))
        return -1;

    /* We expect one or the other, but not both. */
    if (raw == NULL && hex == NULL) {
        PyErr_SetString(PyExc_ValueError, "Expected raw or hex.");
        return -1;
    }
    if (raw != NULL && hex != NULL) {
        PyErr_SetString(PyExc_ValueError, "Expected raw or hex, not both.");
        return -1;
    }

    /* Case 1: raw */
    if (raw != NULL) {
        err = PyBytes_AsStringAndSize(raw, &bytes, &len);
        if (err)
            return -1;

        if (len > GIT_OID_RAWSZ) {
            PyErr_SetObject(PyExc_ValueError, raw);
            return -1;
        }

        memcpy(self->oid.id, (const unsigned char*)bytes, len);
        return 0;
    }

    /* Case 2: hex */
    len = py_hex_to_git_oid(hex, &self->oid);
    if (len == 0)
        return -1;

    return 0;
}


Py_hash_t
Oid_hash(PyObject *oid)
{
    PyObject *py_oid = git_oid_to_py_str(&((Oid *)oid)->oid);
    Py_hash_t ret = PyObject_Hash(py_oid);
    Py_DECREF(py_oid);
    return ret;
}


PyObject *
Oid_richcompare(PyObject *self, PyObject *other, int op)
{
    git_oid *oid = &((Oid*)self)->oid;
    int cmp;

    // Can compare an oid against another oid or a unicode string
    if (PyObject_TypeCheck(other, &OidType)) {
        cmp = git_oid_cmp(oid, &((Oid*)other)->oid);
    }
    else if (PyObject_TypeCheck(other, &PyUnicode_Type)) {
        const char * str = PyUnicode_AsUTF8(other);
        if (str == NULL) {
            return NULL;
        }
        cmp = git_oid_strcmp(oid, str);
    }
    else {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    // Return boolean
    PyObject *res;
    switch (op) {
        case Py_LT:
            res = (cmp <= 0) ? Py_True: Py_False;
            break;
        case Py_LE:
            res = (cmp < 0) ? Py_True: Py_False;
            break;
        case Py_EQ:
            res = (cmp == 0) ? Py_True: Py_False;
            break;
        case Py_NE:
            res = (cmp != 0) ? Py_True: Py_False;
            break;
        case Py_GT:
            res = (cmp > 0) ? Py_True: Py_False;
            break;
        case Py_GE:
            res = (cmp >= 0) ? Py_True: Py_False;
            break;
        default:
            PyErr_Format(PyExc_RuntimeError, "Unexpected '%d' op", op);
            return NULL;
    }

    Py_INCREF(res);
    return res;
}

PyObject *
Oid__str__(Oid *self)
{
    return git_oid_to_py_str(&self->oid);
}

int
Oid__bool(PyObject *self)
{
    git_oid *oid = &((Oid*)self)->oid;
    return !git_oid_equal(oid, &oid_zero);
}

PyDoc_STRVAR(Oid_raw__doc__, "Raw oid, a 20 bytes string.");

PyObject *
Oid_raw__get__(Oid *self)
{
    return PyBytes_FromStringAndSize((const char*)self->oid.id, GIT_OID_RAWSZ);
}


PyGetSetDef Oid_getseters[] = {
    GETTER(Oid, raw),
    {NULL},
};

PyNumberMethods Oid_as_number = {
     0,                          /* nb_add */
     0,                          /* nb_subtract */
     0,                          /* nb_multiply */
     0,                          /* nb_remainder */
     0,                          /* nb_divmod */
     0,                          /* nb_power */
     0,                          /* nb_negative */
     0,                          /* nb_positive */
     0,                          /* nb_absolute */
     Oid__bool,                  /* nb_bool */
     0,                          /* nb_invert */
     0,                          /* nb_lshift */
     0,                          /* nb_rshift */
     0,                          /* nb_and */
     0,                          /* nb_xor */
     0,                          /* nb_or */
     0,                          /* nb_int */
     0,                          /* nb_reserved */
     0,                          /* nb_float */
     0,                          /* nb_inplace_add */
     0,                          /* nb_inplace_subtract */
     0,                          /* nb_inplace_multiply */
     0,                          /* nb_inplace_remainder */
     0,                          /* nb_inplace_power */
     0,                          /* nb_inplace_lshift */
     0,                          /* nb_inplace_rshift */
     0,                          /* nb_inplace_and */
     0,                          /* nb_inplace_xor */
     0,                          /* nb_inplace_or */
     0,                          /* nb_floor_divide */
     0,                          /* nb_true_divide */
     0,                          /* nb_inplace_floor_divide */
     0,                          /* nb_inplace_true_divide */
     0,                          /* nb_index */
     0,                          /* nb_matrix_multiply */
     0,                          /* nb_inplace_matrix_multiply */
};

PyDoc_STRVAR(Oid__doc__, "Object id.");

PyTypeObject OidType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Oid",                             /* tp_name           */
    sizeof(Oid),                               /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    0,                                         /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    (reprfunc)Oid__str__,                      /* tp_repr           */
    &Oid_as_number,                            /* tp_as_number      */
    0,                                         /* tp_as_sequence    */
    0,                                         /* tp_as_mapping     */
    (hashfunc)Oid_hash,                        /* tp_hash           */
    0,                                         /* tp_call           */
    (reprfunc)Oid__str__,                      /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT,                        /* tp_flags          */
    Oid__doc__,                                /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    (richcmpfunc)Oid_richcompare,              /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    Oid_getseters,                             /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)Oid_init,                        /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};
