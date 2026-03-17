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
#include "types.h"
#include "utils.h"
#include "oid.h"
#include "signature.h"

extern PyTypeObject SignatureType;

int
Signature_init(Signature *self, PyObject *args, PyObject *kwds)
{
    char *keywords[] = {"name", "email", "time", "offset", "encoding", NULL};
    PyObject *py_name;
    char *email, *encoding = NULL;
    long long time = -1;
    int offset = 0;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "Os|Liz", keywords,
            &py_name, &email, &time, &offset, &encoding))
        return -1;

    PyObject *tname;
    const char *name = pgit_borrow_encoding(
        py_name, value_or_default(encoding, "utf-8"), NULL, &tname);
    if (name == NULL)
        return -1;

    git_signature *signature;
    int err = (time == -1) ? git_signature_now(&signature, name, email)
                           : git_signature_new(&signature, name, email, time, offset);
    Py_DECREF(tname);
    if (err < 0) {
        Error_set(err);
        return -1;
    }

    self->obj = NULL;
    self->signature = signature;

    if (encoding) {
        self->encoding = strdup(encoding);
        if (self->encoding == NULL) {
            PyErr_NoMemory();
            return -1;
        }
    }

    return 0;
}

void
Signature_dealloc(Signature *self)
{
    /* self->obj is the owner of the git_signature C structure, so we mustn't free it */
    if (self->obj) {
        Py_CLEAR(self->obj);
    } else {
        git_signature_free((git_signature *) self->signature);
    }

    /* we own self->encoding */
    free(self->encoding);

    PyObject_Del(self);
}

PyDoc_STRVAR(Signature__pointer__doc__, "Get the signature's pointer. For internal use only.");
PyObject *
Signature__pointer__get__(Signature *self)
{
    /* Bytes means a raw buffer */
    return PyBytes_FromStringAndSize((char *) &self->signature, sizeof(git_signature *));
}

PyDoc_STRVAR(Signature__encoding__doc__, "Encoding.");

PyObject *
Signature__encoding__get__(Signature *self)
{
    const char *encoding = self->encoding;
    if (encoding == NULL) {
        encoding = "utf-8";
    }

    return to_encoding(encoding);
}


PyDoc_STRVAR(Signature_raw_name__doc__, "Name (bytes).");

PyObject *
Signature_raw_name__get__(Signature *self)
{
    return PyBytes_FromString(self->signature->name);
}


PyDoc_STRVAR(Signature_raw_email__doc__, "Email (bytes).");

PyObject *
Signature_raw_email__get__(Signature *self)
{
    return PyBytes_FromString(self->signature->email);
}


PyDoc_STRVAR(Signature_name__doc__, "Name.");

PyObject *
Signature_name__get__(Signature *self)
{
    return to_unicode(self->signature->name, self->encoding, NULL);
}


PyDoc_STRVAR(Signature_email__doc__, "Email address.");

PyObject *
Signature_email__get__(Signature *self)
{
    return to_unicode(self->signature->email, self->encoding, NULL);
}


PyDoc_STRVAR(Signature_time__doc__, "Unix time.");

PyObject *
Signature_time__get__(Signature *self)
{
    return PyLong_FromLongLong(self->signature->when.time);
}


PyDoc_STRVAR(Signature_offset__doc__, "Offset from UTC in minutes.");

PyObject *
Signature_offset__get__(Signature *self)
{
    return PyLong_FromLong(self->signature->when.offset);
}

PyGetSetDef Signature_getseters[] = {
    GETTER(Signature, _encoding),
    GETTER(Signature, raw_name),
    GETTER(Signature, raw_email),
    GETTER(Signature, name),
    GETTER(Signature, email),
    GETTER(Signature, time),
    GETTER(Signature, offset),
    GETTER(Signature, _pointer),
    {NULL}
};

PyObject *
Signature_richcompare(PyObject *a, PyObject *b, int op)
{
    int eq;
    Signature *sa, *sb;

    /* We only support comparing to another signature */
    if (!PyObject_TypeCheck(b, &SignatureType)) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    sa = (Signature *)a;
    sb = (Signature *)b;

    eq = (
        strcmp(sa->signature->name, sb->signature->name) == 0 &&
        strcmp(sa->signature->email, sb->signature->email) == 0 &&
        sa->signature->when.time == sb->signature->when.time &&
        sa->signature->when.offset == sb->signature->when.offset &&
        sa->signature->when.sign == sb->signature->when.sign &&
        strcmp(value_or_default(sa->encoding, "utf-8"),
               value_or_default(sb->encoding, "utf-8")) == 0);

    switch (op) {
        case Py_EQ:
            if (eq) {
                Py_RETURN_TRUE;
            } else {
                Py_RETURN_FALSE;
            }
        case Py_NE:
            if (eq) {
                Py_RETURN_FALSE;
            } else {
                Py_RETURN_TRUE;
            }
        default:
            Py_INCREF(Py_NotImplemented);
            return Py_NotImplemented;
    }

}

static PyObject *
Signature__str__(Signature *self)
{
    PyObject *name, *email, *str;
    name = to_unicode_safe(self->signature->name, self->encoding);
    email = to_unicode_safe(self->signature->email, self->encoding);
    assert(name);
    assert(email);

    str = PyUnicode_FromFormat("%U <%U>", name, email);
    Py_DECREF(name);
    Py_DECREF(email);
    return str;
}

static PyObject *
Signature__repr__(Signature *self)
{
    PyObject *name, *email, *encoding, *str;
    name = to_unicode_safe(self->signature->name, self->encoding);
    email = to_unicode_safe(self->signature->email, self->encoding);

    if (self->encoding) {
        encoding = to_unicode_safe(self->encoding, self->encoding);
    } else {
        encoding = Py_None;
        Py_INCREF(Py_None);
    }

    assert(name);
    assert(email);
    assert(encoding);

    str = PyUnicode_FromFormat(
        "pygit2.Signature(%R, %R, %lld, %ld, %R)",
        name,
        email,
        self->signature->when.time,
        self->signature->when.offset,
        encoding);
    Py_DECREF(name);
    Py_DECREF(email);
    Py_DECREF(encoding);
    return str;
}


PyDoc_STRVAR(Signature__doc__, "Signature.");

PyTypeObject SignatureType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_pygit2.Signature",                       /* tp_name           */
    sizeof(Signature),                         /* tp_basicsize      */
    0,                                         /* tp_itemsize       */
    (destructor)Signature_dealloc,             /* tp_dealloc        */
    0,                                         /* tp_print          */
    0,                                         /* tp_getattr        */
    0,                                         /* tp_setattr        */
    0,                                         /* tp_compare        */
    (reprfunc)Signature__repr__,               /* tp_repr           */
    0,                                         /* tp_as_number      */
    0,                                         /* tp_as_sequence    */
    0,                                         /* tp_as_mapping     */
    0,                                         /* tp_hash           */
    0,                                         /* tp_call           */
    (reprfunc)Signature__str__,                /* tp_str            */
    0,                                         /* tp_getattro       */
    0,                                         /* tp_setattro       */
    0,                                         /* tp_as_buffer      */
    Py_TPFLAGS_DEFAULT,                        /* tp_flags          */
    Signature__doc__,                          /* tp_doc            */
    0,                                         /* tp_traverse       */
    0,                                         /* tp_clear          */
    (richcmpfunc)Signature_richcompare,        /* tp_richcompare    */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter           */
    0,                                         /* tp_iternext       */
    0,                                         /* tp_methods        */
    0,                                         /* tp_members        */
    Signature_getseters,                       /* tp_getset         */
    0,                                         /* tp_base           */
    0,                                         /* tp_dict           */
    0,                                         /* tp_descr_get      */
    0,                                         /* tp_descr_set      */
    0,                                         /* tp_dictoffset     */
    (initproc)Signature_init,                  /* tp_init           */
    0,                                         /* tp_alloc          */
    0,                                         /* tp_new            */
};

PyObject *
build_signature(Object *obj, const git_signature *signature,
                const char *encoding)
{
    Signature *py_signature;

    py_signature = PyObject_New(Signature, &SignatureType);
    if (!py_signature)
        goto on_error;

    py_signature->encoding = NULL;
    if (encoding) {
        py_signature->encoding = strdup(encoding);
        if (!py_signature->encoding)
            goto on_error;
    }

    Py_XINCREF(obj);
    py_signature->obj = obj;
    py_signature->signature = signature;

    return (PyObject*)py_signature;

on_error:
    git_signature_free((git_signature *) signature);
    return NULL;
}
