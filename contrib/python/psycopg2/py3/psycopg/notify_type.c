/* notify_type.c - python interface to Notify objects
 *
 * Copyright (C) 2010-2019  Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

#include "psycopg/notify.h"


static const char notify_doc[] =
    "A notification received from the backend.\n\n"
    "`!Notify` instances are made available upon reception on the\n"
    "`~connection.notifies` member of the listening connection. The object\n"
    "can be also accessed as a 2 items tuple returning the members\n"
    ":samp:`({pid},{channel})` for backward compatibility.\n\n"
    "See :ref:`async-notify` for details.";

static const char pid_doc[] =
    "The ID of the backend process that sent the notification.\n\n"
    "Note: if the sending session was handled by Psycopg, you can use\n"
    "`~connection.info.backend_pid` to know its PID.";

static const char channel_doc[] =
    "The name of the channel to which the notification was sent.";

static const char payload_doc[] =
    "The payload message of the notification.\n\n"
    "Attaching a payload to a notification is only available since\n"
    "PostgreSQL 9.0: for notifications received from previous versions\n"
    "of the server this member is always the empty string.";

static PyMemberDef notify_members[] = {
    { "pid", T_OBJECT, offsetof(notifyObject, pid), READONLY, (char *)pid_doc },
    { "channel", T_OBJECT, offsetof(notifyObject, channel), READONLY, (char *)channel_doc },
    { "payload", T_OBJECT, offsetof(notifyObject, payload), READONLY, (char *)payload_doc },
    { NULL }
};

static PyObject *
notify_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    return type->tp_alloc(type, 0);
}

static int
notify_init(notifyObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"pid", "channel", "payload", NULL};
    PyObject *pid = NULL, *channel = NULL, *payload = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|O", kwlist,
                                     &pid, &channel, &payload)) {
        return -1;
    }

    if (!payload) {
        payload = Text_FromUTF8("");
    }

    Py_INCREF(pid);
    self->pid = pid;

    Py_INCREF(channel);
    self->channel = channel;

    Py_INCREF(payload);
    self->payload = payload;

    return 0;
}

static void
notify_dealloc(notifyObject *self)
{
    Py_CLEAR(self->pid);
    Py_CLEAR(self->channel);
    Py_CLEAR(self->payload);

    Py_TYPE(self)->tp_free((PyObject *)self);
}


/* Convert a notify into a 2 or 3 items tuple. */
static PyObject *
notify_astuple(notifyObject *self, int with_payload)
{
    PyObject *tself;
    if (!(tself = PyTuple_New(with_payload ? 3 : 2))) { return NULL; }

    Py_INCREF(self->pid);
    PyTuple_SET_ITEM(tself, 0, self->pid);

    Py_INCREF(self->channel);
    PyTuple_SET_ITEM(tself, 1, self->channel);

    if (with_payload) {
        Py_INCREF(self->payload);
        PyTuple_SET_ITEM(tself, 2, self->payload);
    }

    return tself;
}

/* note on Notify-tuple comparison.
 *
 * Such a comparison is required otherwise a check n == (pid, channel)
 * would fail. We also want to compare two notifies, and the obvious meaning is
 * "check that all the attributes are equal". Unfortunately this leads to an
 * inconsistent situation:
 *      Notify(pid, channel, payload1)
 *   == (pid, channel)
 *   == Notify(pid, channel, payload2)
 * even when payload1 != payload2. We can probably live with that, but hashing
 * makes things worse: hashability is a desirable property for a Notify, and
 * to maintain compatibility we should put a notify object in the same bucket
 * of a 2-item tuples... but we can't put all the payloads with the same
 * (pid, channel) in the same bucket: it would be an extremely poor hash.
 * So we maintain compatibility in the sense that notify without payload
 * behave as 2-item tuples in term of hashability, but if a payload is present
 * the (pid, channel) pair is no more equivalent as dict key to the Notify.
 */
static PyObject *
notify_richcompare(notifyObject *self, PyObject *other, int op)
{
    PyObject *rv = NULL;
    PyObject *tself = NULL;
    PyObject *tother = NULL;

    if (Py_TYPE(other) == &notifyType) {
        if (!(tself = notify_astuple(self, 1))) { goto exit; }
        if (!(tother = notify_astuple((notifyObject *)other, 1))) { goto exit; }
        rv = PyObject_RichCompare(tself, tother, op);
    }
    else if (PyTuple_Check(other)) {
        if (!(tself = notify_astuple(self, 0))) { goto exit; }
        rv = PyObject_RichCompare(tself, other, op);
    }
    else {
        Py_INCREF(Py_False);
        rv = Py_False;
    }

exit:
    Py_XDECREF(tself);
    Py_XDECREF(tother);
    return rv;
}


static Py_hash_t
notify_hash(notifyObject *self)
{
    Py_hash_t rv = -1L;
    PyObject *tself = NULL;

    /* if self == a tuple, then their hashes are the same. */
    int has_payload = PyObject_IsTrue(self->payload);
    if (!(tself = notify_astuple(self, has_payload))) { goto exit; }
    rv = PyObject_Hash(tself);

exit:
    Py_XDECREF(tself);
    return rv;
}


static PyObject*
notify_repr(notifyObject *self)
{
    PyObject *rv = NULL;
    PyObject *format = NULL;
    PyObject *args = NULL;

    if (!(format = Text_FromUTF8("Notify(%r, %r, %r)"))) {
        goto exit;
    }

    if (!(args = PyTuple_New(3))) { goto exit; }
    Py_INCREF(self->pid);
    PyTuple_SET_ITEM(args, 0, self->pid);
    Py_INCREF(self->channel);
    PyTuple_SET_ITEM(args, 1, self->channel);
    Py_INCREF(self->payload);
    PyTuple_SET_ITEM(args, 2, self->payload);

    rv = Text_Format(format, args);

exit:
    Py_XDECREF(args);
    Py_XDECREF(format);

    return rv;
}

/* Notify can be accessed as a 2 items tuple for backward compatibility */

static Py_ssize_t
notify_len(notifyObject *self)
{
    return 2;
}

static PyObject *
notify_getitem(notifyObject *self, Py_ssize_t item)
{
    if (item < 0)
        item += 2;

    switch (item) {
    case 0:
        Py_INCREF(self->pid);
        return self->pid;
    case 1:
        Py_INCREF(self->channel);
        return self->channel;
    default:
        PyErr_SetString(PyExc_IndexError, "index out of range");
        return NULL;
    }
}

static PySequenceMethods notify_sequence = {
    (lenfunc)notify_len,          /* sq_length */
    0,                         /* sq_concat */
    0,                         /* sq_repeat */
    (ssizeargfunc)notify_getitem, /* sq_item */
    0,                         /* sq_slice */
    0,                         /* sq_ass_item */
    0,                         /* sq_ass_slice */
    0,                         /* sq_contains */
    0,                         /* sq_inplace_concat */
    0,                         /* sq_inplace_repeat */
};


PyTypeObject notifyType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.Notify",
    sizeof(notifyObject), 0,
    (destructor)notify_dealloc, /* tp_dealloc */
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)notify_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    &notify_sequence, /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    (hashfunc)notify_hash, /*tp_hash */
    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    /* Notify is not GC as it only has string attributes */
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    notify_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    (richcmpfunc)notify_richcompare, /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    0,          /*tp_methods*/
    notify_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    (initproc)notify_init, /*tp_init*/
    0,          /*tp_alloc*/
    notify_new, /*tp_new*/
};
