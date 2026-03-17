/* xid_type.c - python interface to Xid objects
 *
 * Copyright (C) 2008  Canonical Ltd.
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

#include "psycopg/xid.h"
#include "psycopg/cursor.h"


static const char xid_doc[] =
    "A transaction identifier used for two-phase commit.\n\n"
    "Usually returned by the connection methods `~connection.xid()` and\n"
    "`~connection.tpc_recover()`.\n"
    "`!Xid` instances can be unpacked as a 3-item tuples containing the items\n"
    ":samp:`({format_id},{gtrid},{bqual})`.\n"
    "The `!str()` of the object returns the *transaction ID* used\n"
    "in the commands sent to the server.\n\n"
    "See :ref:`tpc` for an introduction.";

static const char format_id_doc[] =
    "Format ID in a XA transaction.\n\n"
    "A non-negative 32 bit integer.\n"
    "`!None` if the transaction doesn't follow the XA standard.";

static const char gtrid_doc[] =
    "Global transaction ID in a XA transaction.\n\n"
    "If the transaction doesn't follow the XA standard, it is the plain\n"
    "*transaction ID* used in the server commands.";

static const char bqual_doc[] =
    "Branch qualifier of the transaction.\n\n"
    "In a XA transaction every resource participating to a transaction\n"
    "receives a distinct branch qualifier.\n"
    "`!None` if the transaction doesn't follow the XA standard.";

static const char prepared_doc[] =
    "Timestamp (with timezone) in which a recovered transaction was prepared.";

static const char owner_doc[] =
    "Name of the user who prepared a recovered transaction.";

static const char database_doc[] =
    "Database the recovered transaction belongs to.";

static PyMemberDef xid_members[] = {
    { "format_id", T_OBJECT, offsetof(xidObject, format_id), READONLY, (char *)format_id_doc },
    { "gtrid", T_OBJECT, offsetof(xidObject, gtrid), READONLY, (char *)gtrid_doc },
    { "bqual", T_OBJECT, offsetof(xidObject, bqual), READONLY, (char *)bqual_doc },
    { "prepared", T_OBJECT, offsetof(xidObject, prepared), READONLY, (char *)prepared_doc },
    { "owner", T_OBJECT, offsetof(xidObject, owner), READONLY, (char *)owner_doc },
    { "database", T_OBJECT, offsetof(xidObject, database), READONLY, (char *)database_doc },
    { NULL }
};

static PyObject *
xid_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    return type->tp_alloc(type, 0);
}

static int
xid_init(xidObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"format_id", "gtrid", "bqual", NULL};
    int format_id;
    size_t i, gtrid_len, bqual_len;
    const char *gtrid, *bqual;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iss", kwlist,
                                     &format_id, &gtrid, &bqual))
        return -1;

    if (format_id < 0 || format_id > 0x7fffffff) {
        PyErr_SetString(PyExc_ValueError,
                        "format_id must be a non-negative 32-bit integer");
        return -1;
    }

    /* make sure that gtrid is no more than 64 characters long and
       made of printable characters (which we're defining as those
       between 0x20 and 0x7f). */
    gtrid_len = strlen(gtrid);
    if (gtrid_len > 64) {
        PyErr_SetString(PyExc_ValueError,
                        "gtrid must be a string no longer than 64 characters");
        return -1;
    }
    for (i = 0; i < gtrid_len; i++) {
        if (gtrid[i] < 0x20 || gtrid[i] >= 0x7f) {
            PyErr_SetString(PyExc_ValueError,
                            "gtrid must contain only printable characters.");
            return -1;
        }
    }
    /* Same for bqual */
    bqual_len = strlen(bqual);
    if (bqual_len > 64) {
        PyErr_SetString(PyExc_ValueError,
                        "bqual must be a string no longer than 64 characters");
        return -1;
    }
    for (i = 0; i < bqual_len; i++) {
        if (bqual[i] < 0x20 || bqual[i] >= 0x7f) {
            PyErr_SetString(PyExc_ValueError,
                            "bqual must contain only printable characters.");
            return -1;
        }
    }

    if (!(self->format_id = PyInt_FromLong(format_id))) { return -1; }
    if (!(self->gtrid = Text_FromUTF8(gtrid))) { return -1; }
    if (!(self->bqual = Text_FromUTF8(bqual))) { return -1; }
    Py_INCREF(Py_None); self->prepared = Py_None;
    Py_INCREF(Py_None); self->owner = Py_None;
    Py_INCREF(Py_None); self->database = Py_None;

    return 0;
}

static void
xid_dealloc(xidObject *self)
{
    Py_CLEAR(self->format_id);
    Py_CLEAR(self->gtrid);
    Py_CLEAR(self->bqual);
    Py_CLEAR(self->prepared);
    Py_CLEAR(self->owner);
    Py_CLEAR(self->database);

    Py_TYPE(self)->tp_free((PyObject *)self);
}

static Py_ssize_t
xid_len(xidObject *self)
{
    return 3;
}

static PyObject *
xid_getitem(xidObject *self, Py_ssize_t item)
{
    if (item < 0)
        item += 3;

    switch (item) {
    case 0:
        Py_INCREF(self->format_id);
        return self->format_id;
    case 1:
        Py_INCREF(self->gtrid);
        return self->gtrid;
    case 2:
        Py_INCREF(self->bqual);
        return self->bqual;
    default:
        PyErr_SetString(PyExc_IndexError, "index out of range");
        return NULL;
    }
}

static PyObject *
xid_str(xidObject *self)
{
    return xid_get_tid(self);
}

static PyObject *
xid_repr(xidObject *self)
{
    PyObject *rv = NULL;
    PyObject *format = NULL;
    PyObject *args = NULL;

    if (Py_None == self->format_id) {
        if (!(format = Text_FromUTF8("<Xid: %r (unparsed)>"))) {
            goto exit;
        }
        if (!(args = PyTuple_New(1))) { goto exit; }
        Py_INCREF(self->gtrid);
        PyTuple_SET_ITEM(args, 0, self->gtrid);
    }
    else {
        if (!(format = Text_FromUTF8("<Xid: (%r, %r, %r)>"))) {
            goto exit;
        }
        if (!(args = PyTuple_New(3))) { goto exit; }
        Py_INCREF(self->format_id);
        PyTuple_SET_ITEM(args, 0, self->format_id);
        Py_INCREF(self->gtrid);
        PyTuple_SET_ITEM(args, 1, self->gtrid);
        Py_INCREF(self->bqual);
        PyTuple_SET_ITEM(args, 2, self->bqual);
    }

    rv = Text_Format(format, args);

exit:
    Py_XDECREF(args);
    Py_XDECREF(format);

    return rv;
}


static const char xid_from_string_doc[] =
    "Create a `!Xid` object from a string representation. Static method.\n\n"
    "If *s* is a PostgreSQL transaction ID produced by a XA transaction,\n"
    "the returned object will have `format_id`, `gtrid`, `bqual` set to\n"
    "the values of the preparing XA id.\n"
    "Otherwise only the `!gtrid` is populated with the unparsed string.\n"
    "The operation is the inverse of the one performed by `!str(xid)`.";

static PyObject *
xid_from_string_method(PyObject *cls, PyObject *args)
{
    PyObject *s = NULL;

    if (!PyArg_ParseTuple(args, "O", &s)) { return NULL; }

    return (PyObject *)xid_from_string(s);
}


static PySequenceMethods xid_sequence = {
    (lenfunc)xid_len,          /* sq_length */
    0,                         /* sq_concat */
    0,                         /* sq_repeat */
    (ssizeargfunc)xid_getitem, /* sq_item */
    0,                         /* sq_slice */
    0,                         /* sq_ass_item */
    0,                         /* sq_ass_slice */
    0,                         /* sq_contains */
    0,                         /* sq_inplace_concat */
    0,                         /* sq_inplace_repeat */
};

static struct PyMethodDef xid_methods[] = {
    {"from_string", (PyCFunction)xid_from_string_method,
     METH_VARARGS|METH_STATIC, xid_from_string_doc},
    {NULL}
};

PyTypeObject xidType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.Xid",
    sizeof(xidObject), 0,
    (destructor)xid_dealloc, /* tp_dealloc */
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)xid_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    &xid_sequence, /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    (reprfunc)xid_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    /* Notify is not GC as it only has string attributes */
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    xid_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    xid_methods, /*tp_methods*/
    xid_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    (initproc)xid_init, /*tp_init*/
    0,          /*tp_alloc*/
    xid_new, /*tp_new*/
};


/* Convert a Python object into a proper xid.
 *
 * Return a new reference to the object or set an exception.
 *
 * The idea is that people can either create a xid from connection.xid
 * or use a regular string they have found in PostgreSQL's pg_prepared_xacts
 * in order to recover a transaction not generated by psycopg.
 */
xidObject *xid_ensure(PyObject *oxid)
{
    xidObject *rv = NULL;

    if (PyObject_TypeCheck(oxid, &xidType)) {
        Py_INCREF(oxid);
        rv = (xidObject *)oxid;
    }
    else {
        rv = xid_from_string(oxid);
    }

    return rv;
}


/* Encode or decode a string in base64. */

static PyObject *
_xid_base64_enc_dec(const char *funcname, PyObject *s)
{
    PyObject *base64 = NULL;
    PyObject *func = NULL;
    PyObject *rv = NULL;

    if (!(base64 = PyImport_ImportModule("base64"))) { goto exit; }
    if (!(func = PyObject_GetAttrString(base64, funcname))) { goto exit; }

    Py_INCREF(s);
    if (!(s = psyco_ensure_bytes(s))) { goto exit; }
    rv = psyco_ensure_text(PyObject_CallFunctionObjArgs(func, s, NULL));
    Py_DECREF(s);

exit:
    Py_XDECREF(func);
    Py_XDECREF(base64);

    return rv;
}

/* Return a base64-encoded string. */

static PyObject *
_xid_encode64(PyObject *s)
{
    return _xid_base64_enc_dec("b64encode", s);
}

/* Decode a base64-encoded string */

static PyObject *
_xid_decode64(PyObject *s)
{
    return _xid_base64_enc_dec("b64decode", s);
}


/* Return the PostgreSQL transaction_id for this XA xid.
 *
 * PostgreSQL wants just a string, while the DBAPI supports the XA standard
 * and thus a triple. We use the same conversion algorithm implemented by JDBC
 * in order to allow some form of interoperation.
 *
 * The function must be called while holding the GIL.
 *
 * see also: the pgjdbc implementation
 *   http://cvs.pgfoundry.org/cgi-bin/cvsweb.cgi/jdbc/pgjdbc/org/postgresql/xa/RecoveredXid.java?rev=1.2
 */
PyObject *
xid_get_tid(xidObject *self)
{
    PyObject *rv = NULL;
    PyObject *egtrid = NULL;
    PyObject *ebqual = NULL;
    PyObject *format = NULL;
    PyObject *args = NULL;

    if (Py_None == self->format_id) {
        /* Unparsed xid: return the gtrid. */
        Py_INCREF(self->gtrid);
        rv = self->gtrid;
    }
    else {
        /* XA xid: mash together the components. */
        if (!(egtrid = _xid_encode64(self->gtrid))) { goto exit; }
        if (!(ebqual = _xid_encode64(self->bqual))) { goto exit; }

        /* rv = "%d_%s_%s" % (format_id, egtrid, ebqual) */
        if (!(format = Text_FromUTF8("%d_%s_%s"))) { goto exit; }

        if (!(args = PyTuple_New(3))) { goto exit; }
        Py_INCREF(self->format_id);
        PyTuple_SET_ITEM(args, 0, self->format_id);
        PyTuple_SET_ITEM(args, 1, egtrid); egtrid = NULL;
        PyTuple_SET_ITEM(args, 2, ebqual); ebqual = NULL;

        if (!(rv = Text_Format(format, args))) { goto exit; }
    }

exit:
    Py_XDECREF(args);
    Py_XDECREF(format);
    Py_XDECREF(egtrid);
    Py_XDECREF(ebqual);

    return rv;
}


/* Return the regex object to parse a Xid string.
 *
 * Return a borrowed reference. */

BORROWED static PyObject *
_xid_get_parse_regex(void) {
    static PyObject *rv;

    if (!rv) {
        PyObject *re_mod = NULL;
        PyObject *comp = NULL;
        PyObject *regex = NULL;

        Dprintf("compiling regexp to parse transaction id");

        if (!(re_mod = PyImport_ImportModule("re"))) { goto exit; }
        if (!(comp = PyObject_GetAttrString(re_mod, "compile"))) { goto exit; }
        if (!(regex = PyObject_CallFunction(comp, "s",
                "^(\\d+)_([^_]*)_([^_]*)$"))) {
            goto exit;
        }

        /* Good, compiled. */
        rv = regex;
        regex = NULL;

exit:
        Py_XDECREF(regex);
        Py_XDECREF(comp);
        Py_XDECREF(re_mod);
    }

    return rv;
}

/* Try to parse a Xid string representation in a Xid object.
 *
 *
 * Return NULL + exception if parsing failed. Else a new Xid object. */

static xidObject *
_xid_parse_string(PyObject *str) {
    PyObject *regex;
    PyObject *m = NULL;
    PyObject *group = NULL;
    PyObject *item = NULL;
    PyObject *format_id = NULL;
    PyObject *egtrid = NULL;
    PyObject *ebqual = NULL;
    PyObject *gtrid = NULL;
    PyObject *bqual = NULL;
    xidObject *rv = NULL;

    /* check if the string is a possible XA triple with a regexp */
    if (!(regex = _xid_get_parse_regex())) { goto exit; }
    if (!(m = PyObject_CallMethod(regex, "match", "O", str))) { goto exit; }
    if (m == Py_None) {
        PyErr_SetString(PyExc_ValueError, "bad xid format");
        goto exit;
    }

    /* Extract the components from the regexp */
    if (!(group = PyObject_GetAttrString(m, "group"))) { goto exit; }
    if (!(item = PyObject_CallFunction(group, "i", 1))) { goto exit; }
    if (!(format_id = PyObject_CallFunctionObjArgs(
            (PyObject *)&PyInt_Type, item, NULL))) {
        goto exit;
    }
    if (!(egtrid = PyObject_CallFunction(group, "i", 2))) { goto exit; }
    if (!(gtrid = _xid_decode64(egtrid))) { goto exit; }

    if (!(ebqual = PyObject_CallFunction(group, "i", 3))) { goto exit; }
    if (!(bqual = _xid_decode64(ebqual))) { goto exit; }

    /* Try to build the xid with the parsed material */
    rv = (xidObject *)PyObject_CallFunctionObjArgs((PyObject *)&xidType,
        format_id, gtrid, bqual, NULL);

exit:
    Py_XDECREF(bqual);
    Py_XDECREF(ebqual);
    Py_XDECREF(gtrid);
    Py_XDECREF(egtrid);
    Py_XDECREF(format_id);
    Py_XDECREF(item);
    Py_XDECREF(group);
    Py_XDECREF(m);

    return rv;
}

/* Return a new Xid object representing a transaction ID not conform to
 * the XA specifications. */

static xidObject *
_xid_unparsed_from_string(PyObject *str) {
    xidObject *xid = NULL;
    xidObject *rv = NULL;

    /* fake args to work around the checks performed by the xid init */
    if (!(xid = (xidObject *)PyObject_CallFunction((PyObject *)&xidType,
            "iss", 0, "", ""))) {
        goto exit;
    }

    /* set xid.gtrid = str */
    Py_CLEAR(xid->gtrid);
    Py_INCREF(str);
    xid->gtrid = str;

    /* set xid.format_id = None */
    Py_CLEAR(xid->format_id);
    Py_INCREF(Py_None);
    xid->format_id = Py_None;

    /* set xid.bqual = None */
    Py_CLEAR(xid->bqual);
    Py_INCREF(Py_None);
    xid->bqual = Py_None;

    /* return the finished object */
    rv = xid;
    xid = NULL;

exit:
    Py_XDECREF(xid);

    return rv;
}

/* Build a Xid from a string representation.
 *
 * If the xid is in the format generated by Psycopg, unpack the tuple into
 * the struct members. Otherwise generate an "unparsed" xid.
 */
xidObject *
xid_from_string(PyObject *str) {
    xidObject *rv;

    if (!(Bytes_Check(str) || PyUnicode_Check(str))) {
        PyErr_SetString(PyExc_TypeError, "not a valid transaction id");
        return NULL;
    }

    /* Try to parse an XA triple from the string. This may fail for several
     * reasons, such as the rules stated in Xid.__init__. */
    rv = _xid_parse_string(str);
    if (!rv) {
        /* If parsing failed, treat the string as an unparsed id */
        PyErr_Clear();
        rv = _xid_unparsed_from_string(str);
    }

    return rv;
}


/* conn_tpc_recover -- return a list of pending TPC Xid */

PyObject *
xid_recover(PyObject *conn)
{
    PyObject *rv = NULL;
    PyObject *curs = NULL;
    PyObject *xids = NULL;
    xidObject *xid = NULL;
    PyObject *recs = NULL;
    PyObject *rec = NULL;
    PyObject *item = NULL;
    PyObject *tmp;
    Py_ssize_t len, i;

    /* curs = conn.cursor()
     * (sort of. Use the real cursor in case the connection returns
     * something non-dbapi -- see ticket #114) */
    if (!(curs = PyObject_CallFunctionObjArgs(
        (PyObject *)&cursorType, conn, NULL))) { goto exit; }

    /* curs.execute(...) */
    if (!(tmp = PyObject_CallMethod(curs, "execute", "s",
        "SELECT gid, prepared, owner, database FROM pg_prepared_xacts")))
    {
        goto exit;
    }
    Py_DECREF(tmp);

    /* recs = curs.fetchall() */
    if (!(recs = PyObject_CallMethod(curs, "fetchall", NULL))) { goto exit; }

    /* curs.close() */
    if (!(tmp = PyObject_CallMethod(curs, "close", NULL))) { goto exit; }
    Py_DECREF(tmp);

    /* Build the list with return values. */
    if (0 > (len = PySequence_Size(recs))) { goto exit; }
    if (!(xids = PyList_New(len))) { goto exit; }

    /* populate the xids list */
    for (i = 0; i < len; ++i) {
        if (!(rec = PySequence_GetItem(recs, i))) { goto exit; }

        /* Get the xid with the XA triple set */
        if (!(item = PySequence_GetItem(rec, 0))) { goto exit; }
        if (!(xid = xid_from_string(item))) { goto exit; }
        Py_CLEAR(item);

        /* set xid.prepared */
        Py_CLEAR(xid->prepared);
        if (!(xid->prepared = PySequence_GetItem(rec, 1))) { goto exit; }

        /* set xid.owner */
        Py_CLEAR(xid->owner);
        if (!(xid->owner = PySequence_GetItem(rec, 2))) { goto exit; }

        /* set xid.database */
        Py_CLEAR(xid->database);
        if (!(xid->database = PySequence_GetItem(rec, 3))) { goto exit; }

        /* xid finished: add it to the returned list */
        PyList_SET_ITEM(xids, i, (PyObject *)xid);
        xid = NULL;  /* ref stolen */

        Py_CLEAR(rec);
    }

    /* set the return value. */
    rv = xids;
    xids = NULL;

exit:
    Py_XDECREF(xids);
    Py_XDECREF(xid);
    Py_XDECREF(curs);
    Py_XDECREF(recs);
    Py_XDECREF(rec);
    Py_XDECREF(item);

    return rv;
}
