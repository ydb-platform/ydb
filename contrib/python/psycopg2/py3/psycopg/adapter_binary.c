/* adapter_binary.c - Binary objects
 *
 * Copyright (C) 2003-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020-2021 The Psycopg Team
 *
 * This file is part of psycopg.
 *
 * psycopg2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link this program with the OpenSSL library (or with
 * modified versions of OpenSSL that use the same license as OpenSSL),
 * and distribute linked combinations including the two.
 *
 * You must obey the GNU Lesser General Public License in all respects for
 * all of the code used other than OpenSSL.
 *
 * psycopg2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 */

#define PSYCOPG_MODULE
#include "psycopg/psycopg.h"

#include "psycopg/adapter_binary.h"
#include "psycopg/microprotocols_proto.h"
#include "psycopg/connection.h"

#include <string.h>


/** the quoting code */

static unsigned char *
binary_escape(unsigned char *from, size_t from_length,
               size_t *to_length, PGconn *conn)
{
    if (conn)
        return PQescapeByteaConn(conn, from, from_length, to_length);
    else
        return PQescapeBytea(from, from_length, to_length);
}

/* binary_quote - do the quote process on plain and unicode strings */

static PyObject *
binary_quote(binaryObject *self)
{
    char *to = NULL;
    const char *buffer = NULL;
    Py_ssize_t buffer_len;
    size_t len = 0;
    PyObject *rv = NULL;
    Py_buffer view;
    int got_view = 0;

    /* Allow Binary(None) to work */
    if (self->wrapped == Py_None) {
        Py_INCREF(psyco_null);
        rv = psyco_null;
        goto exit;
    }

    /* if we got a plain string or a buffer we escape it and save the buffer */
    if (PyObject_CheckBuffer(self->wrapped)) {
        if (0 > PyObject_GetBuffer(self->wrapped, &view, PyBUF_CONTIG_RO)) {
            goto exit;
        }
        got_view = 1;
        buffer = (const char *)(view.buf);
        buffer_len = view.len;
    }

    if (!buffer) {
        goto exit;
    }

    /* escape and build quoted buffer */

    to = (char *)binary_escape((unsigned char*)buffer, (size_t)buffer_len,
        &len, self->conn ? ((connectionObject*)self->conn)->pgconn : NULL);
    if (to == NULL) {
        PyErr_NoMemory();
        goto exit;
    }

    if (len > 0)
        rv = Bytes_FromFormat(
            (self->conn && ((connectionObject*)self->conn)->equote)
                ? "E'%s'::bytea" : "'%s'::bytea" , to);
    else
        rv = Bytes_FromString("''::bytea");

exit:
    if (to) { PQfreemem(to); }
    if (got_view) { PyBuffer_Release(&view); }

    /* if the wrapped object is not bytes or a buffer, this is an error */
    if (!rv && !PyErr_Occurred()) {
        PyErr_Format(PyExc_TypeError, "can't escape %s to binary",
            Py_TYPE(self->wrapped)->tp_name);
    }

    return rv;
}

/* binary_str, binary_getquoted - return result of quoting */

static PyObject *
binary_getquoted(binaryObject *self, PyObject *args)
{
    if (self->buffer == NULL) {
        self->buffer = binary_quote(self);
    }
    Py_XINCREF(self->buffer);
    return self->buffer;
}

static PyObject *
binary_str(binaryObject *self)
{
    return psyco_ensure_text(binary_getquoted(self, NULL));
}

static PyObject *
binary_prepare(binaryObject *self, PyObject *args)
{
    PyObject *conn;

    if (!PyArg_ParseTuple(args, "O!", &connectionType, &conn))
        return NULL;

    Py_XDECREF(self->conn);
    self->conn = conn;
    Py_INCREF(self->conn);

    Py_RETURN_NONE;
}

static PyObject *
binary_conform(binaryObject *self, PyObject *args)
{
    PyObject *res, *proto;

    if (!PyArg_ParseTuple(args, "O", &proto)) return NULL;

    if (proto == (PyObject*)&isqlquoteType)
        res = (PyObject*)self;
    else
        res = Py_None;

    Py_INCREF(res);
    return res;
}

/** the Binary object **/

/* object member list */

static struct PyMemberDef binaryObject_members[] = {
    {"adapted", T_OBJECT, offsetof(binaryObject, wrapped), READONLY},
    {"buffer", T_OBJECT, offsetof(binaryObject, buffer), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef binaryObject_methods[] = {
    {"getquoted", (PyCFunction)binary_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL-quoted binary string"},
    {"prepare", (PyCFunction)binary_prepare, METH_VARARGS,
     "prepare(conn) -> prepare for binary encoding using conn"},
    {"__conform__", (PyCFunction)binary_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
binary_setup(binaryObject *self, PyObject *str)
{
    Dprintf("binary_setup: init binary object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );

    self->buffer = NULL;
    self->conn = NULL;
    Py_INCREF(str);
    self->wrapped = str;

    Dprintf("binary_setup: good binary object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self));
    return 0;
}

static void
binary_dealloc(PyObject* obj)
{
    binaryObject *self = (binaryObject *)obj;

    Py_CLEAR(self->wrapped);
    Py_CLEAR(self->buffer);
    Py_CLEAR(self->conn);

    Dprintf("binary_dealloc: deleted binary object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
      );

    Py_TYPE(obj)->tp_free(obj);
}

static int
binary_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *str;

    if (!PyArg_ParseTuple(args, "O", &str))
        return -1;

    return binary_setup((binaryObject *)obj, str);
}

static PyObject *
binary_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define binaryType_doc \
"Binary(buffer) -> new binary object"

PyTypeObject binaryType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.Binary",
    sizeof(binaryObject), 0,
    binary_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    0,          /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    (reprfunc)binary_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    binaryType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    binaryObject_methods, /*tp_methods*/
    binaryObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    binary_init, /*tp_init*/
    0,          /*tp_alloc*/
    binary_new, /*tp_new*/
};
