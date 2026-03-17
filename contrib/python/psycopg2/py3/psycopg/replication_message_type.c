/* replication_message_type.c - python interface to ReplcationMessage objects
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

#include "psycopg/replication_message.h"

#include "datetime.h"

RAISES_NEG int
replmsg_datetime_init(void)
{
    PyDateTime_IMPORT;

    if (!PyDateTimeAPI) {
        PyErr_SetString(PyExc_ImportError, "datetime initialization failed");
        return -1;
    }
    return 0;
}


static PyObject *
replmsg_repr(replicationMessageObject *self)
{
    return PyString_FromFormat(
        "<ReplicationMessage object at %p; data_size: %d; "
        "data_start: "XLOGFMTSTR"; wal_end: "XLOGFMTSTR"; send_time: %ld>",
        self, self->data_size, XLOGFMTARGS(self->data_start), XLOGFMTARGS(self->wal_end),
        (long int)self->send_time);
}

static int
replmsg_init(PyObject *obj, PyObject *args, PyObject *kwargs)
{
    PyObject *cur = NULL;
    replicationMessageObject *self = (replicationMessageObject *)obj;

    if (!PyArg_ParseTuple(
            args, "O!O", &cursorType, &cur, &self->payload)) {
        return -1;
    }

    Py_INCREF(cur);
    self->cursor = (cursorObject *)cur;
    Py_INCREF(self->payload);

    self->data_size = 0;
    self->data_start = 0;
    self->wal_end = 0;
    self->send_time = 0;

    return 0;
}

static int
replmsg_traverse(replicationMessageObject *self, visitproc visit, void *arg)
{
    Py_VISIT((PyObject *)self->cursor);
    Py_VISIT(self->payload);
    return 0;
}

static int
replmsg_clear(replicationMessageObject *self)
{
    Py_CLEAR(self->cursor);
    Py_CLEAR(self->payload);
    return 0;
}

static void
replmsg_dealloc(PyObject* obj)
{
    PyObject_GC_UnTrack(obj);

    replmsg_clear((replicationMessageObject*) obj);

    Py_TYPE(obj)->tp_free(obj);
}

#define replmsg_send_time_doc \
"send_time - Timestamp of the replication message departure from the server."

static PyObject *
replmsg_get_send_time(replicationMessageObject *self)
{
    PyObject *tval, *res = NULL;
    double t;

    t = (double)self->send_time / USECS_PER_SEC +
        ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

    tval = Py_BuildValue("(d)", t);
    if (tval) {
        res = PyDateTime_FromTimestamp(tval);
        Py_DECREF(tval);
    }

    return res;
}

#define OFFSETOF(x) offsetof(replicationMessageObject, x)

/* object member list */

static struct PyMemberDef replicationMessageObject_members[] = {
    {"cursor", T_OBJECT, OFFSETOF(cursor), READONLY,
        "Related ReplcationCursor object."},
    {"payload", T_OBJECT, OFFSETOF(payload), READONLY,
        "The actual message data."},
    {"data_size", T_INT, OFFSETOF(data_size), READONLY,
        "Raw size of the message data in bytes."},
    {"data_start", T_ULONGLONG, OFFSETOF(data_start), READONLY,
        "LSN position of the start of this message."},
    {"wal_end", T_ULONGLONG, OFFSETOF(wal_end), READONLY,
        "LSN position of the current end of WAL on the server."},
    {NULL}
};

static struct PyGetSetDef replicationMessageObject_getsets[] = {
    { "send_time", (getter)replmsg_get_send_time, NULL,
      replmsg_send_time_doc, NULL },
    {NULL}
};

/* object type */

#define replicationMessageType_doc \
"A replication protocol message."

PyTypeObject replicationMessageType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.ReplicationMessage",
    sizeof(replicationMessageObject), 0,
    replmsg_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)replmsg_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
      Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    replicationMessageType_doc, /*tp_doc*/
    (traverseproc)replmsg_traverse, /*tp_traverse*/
    (inquiry)replmsg_clear, /*tp_clear*/
    0,          /*tp_richcompare*/
    0, /*tp_weaklistoffset*/
    0, /*tp_iter*/
    0, /*tp_iternext*/
    0, /*tp_methods*/
    replicationMessageObject_members, /*tp_members*/
    replicationMessageObject_getsets, /*tp_getset*/
    0, /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    replmsg_init, /*tp_init*/
    0,          /*tp_alloc*/
    PyType_GenericNew, /*tp_new*/
};
