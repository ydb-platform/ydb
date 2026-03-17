/* lobject_type.c - python interface to lobject objects
 *
 * Copyright (C) 2006-2019 Federico Di Gregorio <fog@debian.org>
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

#include "psycopg/lobject.h"
#include "psycopg/connection.h"
#include "psycopg/microprotocols.h"
#include "psycopg/microprotocols_proto.h"
#include "psycopg/pqpath.h"

#include <string.h>


/** public methods **/

/* close method - close the lobject */

#define psyco_lobj_close_doc \
"close() -- Close the lobject."

static PyObject *
psyco_lobj_close(lobjectObject *self, PyObject *args)
{
    /* file-like objects can be closed multiple times and remember that
       closing the current transaction is equivalent to close all the
       opened large objects */
    if (!lobject_is_closed(self)
        && !self->conn->autocommit
        && self->conn->mark == self->mark)
    {
        Dprintf("psyco_lobj_close: closing lobject at %p", self);
        if (lobject_close(self) < 0)
            return NULL;
    }

    Py_RETURN_NONE;
}

/* write method - write data to the lobject */

#define psyco_lobj_write_doc \
"write(str | bytes) -- Write a string or bytes to the large object."

static PyObject *
psyco_lobj_write(lobjectObject *self, PyObject *args)
{
    char *buffer;
    Py_ssize_t len;
    Py_ssize_t res;
    PyObject *obj;
    PyObject *data = NULL;
    PyObject *rv = NULL;

    if (!PyArg_ParseTuple(args, "O", &obj)) return NULL;

    EXC_IF_LOBJ_CLOSED(self);
    EXC_IF_LOBJ_LEVEL0(self);
    EXC_IF_LOBJ_UNMARKED(self);

    if (Bytes_Check(obj)) {
        Py_INCREF(obj);
        data = obj;
    }
    else if (PyUnicode_Check(obj)) {
        if (!(data = conn_encode(self->conn, obj))) { goto exit; }
    }
    else {
        PyErr_Format(PyExc_TypeError,
            "lobject.write requires a string; got %s instead",
            Py_TYPE(obj)->tp_name);
        goto exit;
    }

    if (-1 == Bytes_AsStringAndSize(data, &buffer, &len)) {
        goto exit;
    }

    if (0 > (res = lobject_write(self, buffer, (size_t)len))) {
        goto exit;
    }

    rv = PyInt_FromSsize_t((Py_ssize_t)res);

exit:
    Py_XDECREF(data);
    return rv;
}

/* read method - read data from the lobject */

#define psyco_lobj_read_doc \
"read(size=-1) -- Read at most size bytes or to the end of the large object."

static PyObject *
psyco_lobj_read(lobjectObject *self, PyObject *args)
{
    PyObject *res;
    Py_ssize_t where, end;
    Py_ssize_t size = -1;
    char *buffer;

    if (!PyArg_ParseTuple(args, "|n",  &size)) return NULL;

    EXC_IF_LOBJ_CLOSED(self);
    EXC_IF_LOBJ_LEVEL0(self);
    EXC_IF_LOBJ_UNMARKED(self);

    if (size < 0) {
        if ((where = lobject_tell(self)) < 0) return NULL;
        if ((end = lobject_seek(self, 0, SEEK_END)) < 0) return NULL;
        if (lobject_seek(self, where, SEEK_SET) < 0) return NULL;
        size = end - where;
    }

    if ((buffer = PyMem_Malloc(size)) == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    if ((size = lobject_read(self, buffer, size)) < 0) {
        PyMem_Free(buffer);
        return NULL;
    }

    if (self->mode & LOBJECT_BINARY) {
        res = Bytes_FromStringAndSize(buffer, size);
    } else {
        res = conn_decode(self->conn, buffer, size);
    }
    PyMem_Free(buffer);

    return res;
}

/* seek method - seek in the lobject */

#define psyco_lobj_seek_doc \
"seek(offset, whence=0) -- Set the lobject's current position."

static PyObject *
psyco_lobj_seek(lobjectObject *self, PyObject *args)
{
    Py_ssize_t offset, pos=0;
    int whence=0;

    if (!PyArg_ParseTuple(args, "n|i", &offset, &whence))
        return NULL;

    EXC_IF_LOBJ_CLOSED(self);
    EXC_IF_LOBJ_LEVEL0(self);
    EXC_IF_LOBJ_UNMARKED(self);

#ifdef HAVE_LO64
    if ((offset < INT_MIN || offset > INT_MAX)
            && self->conn->server_version < 90300) {
        PyErr_Format(NotSupportedError,
            "offset out of range (%ld): server version %d "
            "does not support the lobject 64 API",
            offset, self->conn->server_version);
        return NULL;
    }
#else
    if (offset < INT_MIN || offset > INT_MAX) {
        PyErr_Format(InterfaceError,
            "offset out of range (" FORMAT_CODE_PY_SSIZE_T "): "
            "this psycopg version was not built with lobject 64 API support",
            offset);
        return NULL;
    }
#endif

    if ((pos = lobject_seek(self, offset, whence)) < 0)
        return NULL;

    return PyInt_FromSsize_t(pos);
}

/* tell method - tell current position in the lobject */

#define psyco_lobj_tell_doc \
"tell() -- Return the lobject's current position."

static PyObject *
psyco_lobj_tell(lobjectObject *self, PyObject *args)
{
    Py_ssize_t pos;

    EXC_IF_LOBJ_CLOSED(self);
    EXC_IF_LOBJ_LEVEL0(self);
    EXC_IF_LOBJ_UNMARKED(self);

    if ((pos = lobject_tell(self)) < 0)
        return NULL;

    return PyInt_FromSsize_t(pos);
}

/* unlink method - unlink (destroy) the lobject */

#define psyco_lobj_unlink_doc \
"unlink() -- Close and then remove the lobject."

static PyObject *
psyco_lobj_unlink(lobjectObject *self, PyObject *args)
{
    if (lobject_unlink(self) < 0)
        return NULL;

    Py_RETURN_NONE;
}

/* export method - export lobject's content to given file */

#define psyco_lobj_export_doc \
"export(filename) -- Export large object to given file."

static PyObject *
psyco_lobj_export(lobjectObject *self, PyObject *args)
{
    const char *filename;

    if (!PyArg_ParseTuple(args, "s", &filename))
        return NULL;

    EXC_IF_LOBJ_LEVEL0(self);

    if (lobject_export(self, filename) < 0)
        return NULL;

    Py_RETURN_NONE;
}


static PyObject *
psyco_lobj_get_closed(lobjectObject *self, void *closure)
{
    return PyBool_FromLong(lobject_is_closed(self));
}

#define psyco_lobj_truncate_doc \
"truncate(len=0) -- Truncate large object to given size."

static PyObject *
psyco_lobj_truncate(lobjectObject *self, PyObject *args)
{
    Py_ssize_t len = 0;

    if (!PyArg_ParseTuple(args, "|n", &len))
        return NULL;

    EXC_IF_LOBJ_CLOSED(self);
    EXC_IF_LOBJ_LEVEL0(self);
    EXC_IF_LOBJ_UNMARKED(self);

#ifdef HAVE_LO64
    if (len > INT_MAX && self->conn->server_version < 90300) {
        PyErr_Format(NotSupportedError,
            "len out of range (" FORMAT_CODE_PY_SSIZE_T "): "
            "server version %d does not support the lobject 64 API",
            len, self->conn->server_version);
        return NULL;
    }
#else
    if (len > INT_MAX) {
        PyErr_Format(InterfaceError,
            "len out of range (" FORMAT_CODE_PY_SSIZE_T "): "
            "this psycopg version was not built with lobject 64 API support",
            len);
        return NULL;
    }
#endif

    if (lobject_truncate(self, len) < 0)
        return NULL;

    Py_RETURN_NONE;
}


/** the lobject object **/

/* object method list */

static struct PyMethodDef lobjectObject_methods[] = {
    {"read", (PyCFunction)psyco_lobj_read,
     METH_VARARGS, psyco_lobj_read_doc},
    {"write", (PyCFunction)psyco_lobj_write,
     METH_VARARGS, psyco_lobj_write_doc},
    {"seek", (PyCFunction)psyco_lobj_seek,
     METH_VARARGS, psyco_lobj_seek_doc},
    {"tell", (PyCFunction)psyco_lobj_tell,
     METH_NOARGS, psyco_lobj_tell_doc},
    {"close", (PyCFunction)psyco_lobj_close,
     METH_NOARGS, psyco_lobj_close_doc},
    {"unlink",(PyCFunction)psyco_lobj_unlink,
     METH_NOARGS, psyco_lobj_unlink_doc},
    {"export",(PyCFunction)psyco_lobj_export,
     METH_VARARGS, psyco_lobj_export_doc},
    {"truncate",(PyCFunction)psyco_lobj_truncate,
     METH_VARARGS, psyco_lobj_truncate_doc},
    {NULL}
};

/* object member list */

static struct PyMemberDef lobjectObject_members[] = {
    {"oid", T_OID, offsetof(lobjectObject, oid), READONLY,
        "The backend OID associated to this lobject."},
    {"mode", T_STRING, offsetof(lobjectObject, smode), READONLY,
        "Open mode."},
    {NULL}
};

/* object getset list */

static struct PyGetSetDef lobjectObject_getsets[] = {
    {"closed", (getter)psyco_lobj_get_closed, NULL,
     "The if the large object is closed (no file-like methods)."},
    {NULL}
};

/* initialization and finalization methods */

static int
lobject_setup(lobjectObject *self, connectionObject *conn,
              Oid oid, const char *smode, Oid new_oid, const char *new_file)
{
    Dprintf("lobject_setup: init lobject object at %p", self);

    if (conn->autocommit) {
        psyco_set_error(ProgrammingError, NULL,
            "can't use a lobject outside of transactions");
        return -1;
    }

    Py_INCREF((PyObject*)conn);
    self->conn = conn;
    self->mark = conn->mark;

    self->fd = -1;
    self->oid = InvalidOid;

    if (0 != lobject_open(self, conn, oid, smode, new_oid, new_file))
        return -1;

   Dprintf("lobject_setup: good lobject object at %p, refcnt = "
           FORMAT_CODE_PY_SSIZE_T, self, Py_REFCNT(self));
   Dprintf("lobject_setup:    oid = %u, fd = %d", self->oid, self->fd);
   return 0;
}

static void
lobject_dealloc(PyObject* obj)
{
    lobjectObject *self = (lobjectObject *)obj;

    if (self->conn && self->fd != -1) {
        if (lobject_close(self) < 0)
            PyErr_Print();
    }
    Py_CLEAR(self->conn);
    PyMem_Free(self->smode);

    Dprintf("lobject_dealloc: deleted lobject object at %p, refcnt = "
            FORMAT_CODE_PY_SSIZE_T, obj, Py_REFCNT(obj));

    Py_TYPE(obj)->tp_free(obj);
}

static int
lobject_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    Oid oid = InvalidOid, new_oid = InvalidOid;
    const char *smode = NULL;
    const char *new_file = NULL;
    PyObject *conn = NULL;

    if (!PyArg_ParseTuple(args, "O!|IzIz",
         &connectionType, &conn,
         &oid, &smode, &new_oid, &new_file))
        return -1;

    if (!smode)
        smode = "";

    return lobject_setup((lobjectObject *)obj,
        (connectionObject *)conn, oid, smode, new_oid, new_file);
}

static PyObject *
lobject_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static PyObject *
lobject_repr(lobjectObject *self)
{
    return PyString_FromFormat(
        "<lobject object at %p; closed: %d>", self, lobject_is_closed(self));
}


/* object type */

#define lobjectType_doc \
"A database large object."

PyTypeObject lobjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.lobject",
    sizeof(lobjectObject), 0,
    lobject_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)lobject_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    (reprfunc)lobject_repr, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_ITER, /*tp_flags*/
    lobjectType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    lobjectObject_methods, /*tp_methods*/
    lobjectObject_members, /*tp_members*/
    lobjectObject_getsets, /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    lobject_init, /*tp_init*/
    0,          /*tp_alloc*/
    lobject_new, /*tp_new*/
};
