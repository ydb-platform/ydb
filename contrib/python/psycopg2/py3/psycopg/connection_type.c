/* connection_type.c - python interface to connection objects
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

#include "psycopg/connection.h"
#include "psycopg/cursor.h"
#include "psycopg/pqpath.h"
#include "psycopg/conninfo.h"
#include "psycopg/lobject.h"
#include "psycopg/green.h"
#include "psycopg/xid.h"

#include <stdlib.h>
#include <string.h>
#include <ctype.h>

extern HIDDEN const char *srv_isolevels[];
extern HIDDEN const char *srv_readonly[];
extern HIDDEN const char *srv_deferrable[];
extern HIDDEN const int SRV_STATE_UNCHANGED;

/** DBAPI methods **/

/* cursor method - allocate a new cursor */

#define psyco_conn_cursor_doc \
"cursor(name=None, cursor_factory=extensions.cursor, withhold=False) -- new cursor\n\n"     \
"Return a new cursor.\n\nThe ``cursor_factory`` argument can be used to\n"  \
"create non-standard cursors by passing a class different from the\n"       \
"default. Note that the new class *should* be a sub-class of\n"             \
"`extensions.cursor`.\n\n"                                                  \
":rtype: `extensions.cursor`"

static PyObject *
psyco_conn_cursor(connectionObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *obj = NULL;
    PyObject *rv = NULL;
    PyObject *name = Py_None;
    PyObject *factory = Py_None;
    PyObject *withhold = Py_False;
    PyObject *scrollable = Py_None;

    static char *kwlist[] = {
        "name", "cursor_factory", "withhold", "scrollable", NULL};

    EXC_IF_CONN_CLOSED(self);

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "|OOOO", kwlist,
            &name, &factory, &withhold, &scrollable)) {
        goto exit;
    }

    if (factory == Py_None) {
        if (self->cursor_factory && self->cursor_factory != Py_None) {
            factory = self->cursor_factory;
        }
        else {
            factory = (PyObject *)&cursorType;
        }
    }

    if (self->status != CONN_STATUS_READY &&
        self->status != CONN_STATUS_BEGIN &&
        self->status != CONN_STATUS_PREPARED) {
        PyErr_SetString(OperationalError,
                        "asynchronous connection attempt underway");
        goto exit;
    }

    if (name != Py_None && self->async == 1) {
        PyErr_SetString(ProgrammingError,
                        "asynchronous connections "
                        "cannot produce named cursors");
        goto exit;
    }

    Dprintf("psyco_conn_cursor: new %s cursor for connection at %p",
        (name == Py_None ? "unnamed" : "named"), self);

    if (!(obj = PyObject_CallFunctionObjArgs(factory, self, name, NULL))) {
        goto exit;
    }

    if (PyObject_IsInstance(obj, (PyObject *)&cursorType) == 0) {
        PyErr_SetString(PyExc_TypeError,
            "cursor factory must be subclass of psycopg2.extensions.cursor");
        goto exit;
    }

    if (0 > curs_withhold_set((cursorObject *)obj, withhold)) {
        goto exit;
    }
    if (0 > curs_scrollable_set((cursorObject *)obj, scrollable)) {
        goto exit;
    }

    Dprintf("psyco_conn_cursor: new cursor at %p: refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
    );

    rv = obj;
    obj = NULL;

exit:
    Py_XDECREF(obj);
    return rv;
}


/* close method - close the connection and all related cursors */

#define psyco_conn_close_doc "close() -- Close the connection."

static PyObject *
psyco_conn_close(connectionObject *self, PyObject *dummy)
{
    Dprintf("psyco_conn_close: closing connection at %p", self);
    conn_close(self);
    Dprintf("psyco_conn_close: connection at %p closed", self);

    Py_RETURN_NONE;
}


/* commit method - commit all changes to the database */

#define psyco_conn_commit_doc "commit() -- Commit all changes to database."

static PyObject *
psyco_conn_commit(connectionObject *self, PyObject *dummy)
{
    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, commit);
    EXC_IF_TPC_BEGIN(self, commit);

    if (conn_commit(self) < 0)
        return NULL;

    Py_RETURN_NONE;
}


/* rollback method - roll back all changes done to the database */

#define psyco_conn_rollback_doc \
"rollback() -- Roll back all changes done to database."

static PyObject *
psyco_conn_rollback(connectionObject *self, PyObject *dummy)
{
    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, rollback);
    EXC_IF_TPC_BEGIN(self, rollback);

    if (conn_rollback(self) < 0)
        return NULL;

    Py_RETURN_NONE;
}


#define psyco_conn_xid_doc \
"xid(format_id, gtrid, bqual) -- create a transaction identifier."

static PyObject *
psyco_conn_xid(connectionObject *self, PyObject *args, PyObject *kwargs)
{
    EXC_IF_CONN_CLOSED(self);
    EXC_IF_TPC_NOT_SUPPORTED(self);

    return PyObject_Call((PyObject *)&xidType, args, kwargs);
}


#define psyco_conn_tpc_begin_doc \
"tpc_begin(xid) -- begin a TPC transaction with given transaction ID xid."

static PyObject *
psyco_conn_tpc_begin(connectionObject *self, PyObject *args)
{
    PyObject *rv = NULL;
    xidObject *xid = NULL;
    PyObject *oxid;

    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, tpc_begin);
    EXC_IF_TPC_NOT_SUPPORTED(self);
    EXC_IF_IN_TRANSACTION(self, tpc_begin);

    if (!PyArg_ParseTuple(args, "O", &oxid)) {
        goto exit;
    }

    if (NULL == (xid = xid_ensure(oxid))) {
        goto exit;
    }

    /* two phase commit and autocommit make no point */
    if (self->autocommit) {
        PyErr_SetString(ProgrammingError,
            "tpc_begin can't be called in autocommit mode");
        goto exit;
    }

    if (conn_tpc_begin(self, xid) < 0) {
        goto exit;
    }

    Py_INCREF(Py_None);
    rv = Py_None;

exit:
    Py_XDECREF(xid);
    return rv;
}


#define psyco_conn_tpc_prepare_doc \
"tpc_prepare() -- perform the first phase of a two-phase transaction."

static PyObject *
psyco_conn_tpc_prepare(connectionObject *self, PyObject *dummy)
{
    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, tpc_prepare);
    EXC_IF_TPC_PREPARED(self, tpc_prepare);

    if (NULL == self->tpc_xid) {
        PyErr_SetString(ProgrammingError,
            "prepare must be called inside a two-phase transaction");
        return NULL;
    }

    if (0 > conn_tpc_command(self, "PREPARE TRANSACTION", self->tpc_xid)) {
        return NULL;
    }

    /* transaction prepared: set the state so that no operation
     * can be performed until commit. */
    self->status = CONN_STATUS_PREPARED;

    Py_RETURN_NONE;
}


/* the type of conn_commit/conn_rollback */
typedef int (*_finish_f)(connectionObject *self);

/* Implement tpc_commit/tpc_rollback.
 *
 * This is a common framework performing the chechs and state manipulation
 * common to the two functions.
 *
 * Parameters are:
 * - self, args: passed by Python
 * - opc_f: the function to call in case of one-phase commit/rollback
 *          one of conn_commit/conn_rollback
 * - tpc_cmd: the command to execute for a two-phase commit/rollback
 *
 * The function can be called in three cases:
 * - If xid is specified, the status must be "ready";
 *   issue the commit/rollback prepared.
 * - if xid is not specified and status is "begin" with a xid,
 *   issue a normal commit/rollback.
 * - if xid is not specified and status is "prepared",
 *   issue the commit/rollback prepared.
 */
static PyObject *
_psyco_conn_tpc_finish(connectionObject *self, PyObject *args,
    _finish_f opc_f, char *tpc_cmd)
{
    PyObject *oxid = NULL;
    xidObject *xid = NULL;
    PyObject *rv = NULL;

    if (!PyArg_ParseTuple(args, "|O", &oxid)) { goto exit; }

    if (oxid) {
        if (!(xid = xid_ensure(oxid))) { goto exit; }
    }

    if (xid) {
        /* committing/aborting a recovered transaction. */
        if (self->status != CONN_STATUS_READY) {
            PyErr_SetString(ProgrammingError,
                "tpc_commit/tpc_rollback with a xid "
                "must be called outside a transaction");
            goto exit;
        }
        if (0 > conn_tpc_command(self, tpc_cmd, xid)) {
            goto exit;
        }
    } else {
        /* committing/aborting our own transaction. */
        if (!self->tpc_xid) {
            PyErr_SetString(ProgrammingError,
                "tpc_commit/tpc_rollback with no parameter "
                "must be called in a two-phase transaction");
            goto exit;
        }

        switch (self->status) {
          case CONN_STATUS_BEGIN:
            if (0 > opc_f(self)) { goto exit; }
            break;

          case CONN_STATUS_PREPARED:
            if (0 > conn_tpc_command(self, tpc_cmd, self->tpc_xid)) {
                goto exit;
            }
            break;

          default:
            PyErr_SetString(InterfaceError,
                "unexpected state in tpc_commit/tpc_rollback");
            goto exit;
        }

        Py_CLEAR(self->tpc_xid);

        /* connection goes ready */
        self->status = CONN_STATUS_READY;
    }

    Py_INCREF(Py_None);
    rv = Py_None;

exit:
    Py_XDECREF(xid);
    return rv;
}

#define psyco_conn_tpc_commit_doc \
"tpc_commit([xid]) -- commit a transaction previously prepared."

static PyObject *
psyco_conn_tpc_commit(connectionObject *self, PyObject *args)
{
    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, tpc_commit);
    EXC_IF_TPC_NOT_SUPPORTED(self);

    return _psyco_conn_tpc_finish(self, args,
                                  conn_commit, "COMMIT PREPARED");
}

#define psyco_conn_tpc_rollback_doc \
"tpc_rollback([xid]) -- abort a transaction previously prepared."

static PyObject *
psyco_conn_tpc_rollback(connectionObject *self, PyObject *args)
{
    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, tpc_rollback);
    EXC_IF_TPC_NOT_SUPPORTED(self);

    return _psyco_conn_tpc_finish(self, args,
                                  conn_rollback, "ROLLBACK PREPARED");
}

#define psyco_conn_tpc_recover_doc \
"tpc_recover() -- returns a list of pending transaction IDs."

static PyObject *
psyco_conn_tpc_recover(connectionObject *self, PyObject *dummy)
{
    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, tpc_recover);
    EXC_IF_TPC_PREPARED(self, tpc_recover);
    EXC_IF_TPC_NOT_SUPPORTED(self);

    return conn_tpc_recover(self);
}


#define psyco_conn_enter_doc \
"__enter__ -> self"

static PyObject *
psyco_conn_enter(connectionObject *self, PyObject *dummy)
{
    PyObject *rv = NULL;

    EXC_IF_CONN_CLOSED(self);

    if (self->entered) {
        PyErr_SetString(ProgrammingError,
            "the connection cannot be re-entered recursively");
        goto exit;
    }

    self->entered = 1;
    Py_INCREF(self);
    rv = (PyObject *)self;

exit:
    return rv;
}


#define psyco_conn_exit_doc \
"__exit__ -- commit if no exception, else roll back"

static PyObject *
psyco_conn_exit(connectionObject *self, PyObject *args)
{
    PyObject *type, *name, *tb;
    PyObject *tmp = NULL;
    PyObject *rv = NULL;

    if (!PyArg_ParseTuple(args, "OOO", &type, &name, &tb)) {
        goto exit;
    }

    /* even if there will be an error, consider ourselves out */
    self->entered = 0;

    if (type == Py_None) {
        if (!(tmp = PyObject_CallMethod((PyObject *)self, "commit", NULL))) {
            goto exit;
        }
    } else {
        if (!(tmp = PyObject_CallMethod((PyObject *)self, "rollback", NULL))) {
            goto exit;
        }
    }

    /* success (of the commit or rollback, there may have been an exception in
     * the block). Return None to avoid swallowing the exception */
    rv = Py_None;
    Py_INCREF(rv);

exit:
    Py_XDECREF(tmp);
    return rv;
}


/* parse a python object into one of the possible isolation level values */

RAISES_NEG static int
_psyco_conn_parse_isolevel(PyObject *pyval)
{
    int rv = -1;
    long level;

    Py_INCREF(pyval);   /* for ensure_bytes */

    /* None is default. This is only used when setting the property, because
     * set_session() has None used as "don't change" */
    if (pyval == Py_None) {
        rv = ISOLATION_LEVEL_DEFAULT;
    }

    /* parse from one of the level constants */
    else if (PyInt_Check(pyval)) {
        level = PyInt_AsLong(pyval);
        if (level == -1 && PyErr_Occurred()) { goto exit; }
        if (level < 1 || level > 4) {
            PyErr_SetString(PyExc_ValueError,
                "isolation_level must be between 1 and 4");
            goto exit;
        }

        rv = level;
    }

    /* parse from the string -- this includes "default" */
    else {
        if (!(pyval = psyco_ensure_bytes(pyval))) {
            goto exit;
        }
        for (level = 1; level <= 4; level++) {
            if (0 == strcasecmp(srv_isolevels[level], Bytes_AS_STRING(pyval))) {
                rv = level;
                break;
            }
        }
        if (rv < 0 && 0 == strcasecmp("default", Bytes_AS_STRING(pyval))) {
            rv = ISOLATION_LEVEL_DEFAULT;
        }
        if (rv < 0) {
            PyErr_Format(PyExc_ValueError,
                "bad value for isolation_level: '%s'", Bytes_AS_STRING(pyval));
            goto exit;
        }
    }

exit:
    Py_XDECREF(pyval);

    return rv;
}

/* convert False/True/"default" -> 0/1/2 */

RAISES_NEG static int
_psyco_conn_parse_onoff(PyObject *pyval)
{
    int rv = -1;

    Py_INCREF(pyval);   /* for ensure_bytes */

    if (pyval == Py_None) {
        rv = STATE_DEFAULT;
    }
    else if (PyUnicode_CheckExact(pyval) || Bytes_CheckExact(pyval)) {
        if (!(pyval = psyco_ensure_bytes(pyval))) {
            goto exit;
        }
        if (0 == strcasecmp("default", Bytes_AS_STRING(pyval))) {
            rv = STATE_DEFAULT;
        }
        else {
            PyErr_Format(PyExc_ValueError,
                "the only string accepted is 'default'; got %s",
                Bytes_AS_STRING(pyval));
            goto exit;
        }
    }
    else {
        int istrue;
        if (0 > (istrue = PyObject_IsTrue(pyval))) { goto exit; }
        rv = istrue ? STATE_ON : STATE_OFF;
    }

exit:
    Py_XDECREF(pyval);

    return rv;
}

#define _set_session_checks(self,what) \
do { \
    EXC_IF_CONN_CLOSED(self); \
    EXC_IF_CONN_ASYNC(self, what); \
    EXC_IF_IN_TRANSACTION(self, what); \
    EXC_IF_TPC_PREPARED(self, what); \
} while(0)

/* set_session - set default transaction characteristics */

#define psyco_conn_set_session_doc \
"set_session(...) -- Set one or more parameters for the next transactions.\n\n" \
"Accepted arguments are 'isolation_level', 'readonly', 'deferrable', 'autocommit'."

static PyObject *
psyco_conn_set_session(connectionObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *isolevel = Py_None;
    PyObject *readonly = Py_None;
    PyObject *deferrable = Py_None;
    PyObject *autocommit = Py_None;

    int c_isolevel = SRV_STATE_UNCHANGED;
    int c_readonly = SRV_STATE_UNCHANGED;
    int c_deferrable = SRV_STATE_UNCHANGED;
    int c_autocommit = SRV_STATE_UNCHANGED;

    static char *kwlist[] =
        {"isolation_level", "readonly", "deferrable", "autocommit", NULL};

    _set_session_checks(self, set_session);

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOOO", kwlist,
            &isolevel, &readonly, &deferrable, &autocommit)) {
        return NULL;
    }

    if (Py_None != isolevel) {
        if (0 > (c_isolevel = _psyco_conn_parse_isolevel(isolevel))) {
            return NULL;
        }
    }

    if (Py_None != readonly) {
        if (0 > (c_readonly = _psyco_conn_parse_onoff(readonly))) {
            return NULL;
        }
    }
    if (Py_None != deferrable) {
        if (0 > (c_deferrable = _psyco_conn_parse_onoff(deferrable))) {
            return NULL;
        }
    }

    if (Py_None != autocommit) {
        if (-1 == (c_autocommit = PyObject_IsTrue(autocommit))) { return NULL; }
    }

    if (0 > conn_set_session(
                self, c_autocommit, c_isolevel, c_readonly, c_deferrable)) {
        return NULL;
    }

    Py_RETURN_NONE;
}


/* autocommit - return or set the current autocommit status */

#define psyco_conn_autocommit_doc \
"Set or return the autocommit status."

static PyObject *
psyco_conn_autocommit_get(connectionObject *self)
{
    return PyBool_FromLong(self->autocommit);
}

BORROWED static PyObject *
_psyco_set_session_check_setter_wrapper(connectionObject *self)
{
    /* wrapper to use the EXC_IF macros.
     * return NULL in case of error, else whatever */
    _set_session_checks(self, set_session);
    return Py_None;     /* borrowed */
}

static int
psyco_conn_autocommit_set(connectionObject *self, PyObject *pyvalue)
{
    int value;

    if (!_psyco_set_session_check_setter_wrapper(self)) { return -1; }
    if (-1 == (value = PyObject_IsTrue(pyvalue))) { return -1; }
    if (0 > conn_set_session(self, value,
                SRV_STATE_UNCHANGED, SRV_STATE_UNCHANGED, SRV_STATE_UNCHANGED)) {
        return -1;
    }

    return 0;
}


/* isolation_level - return or set the current isolation level */

#define psyco_conn_isolation_level_doc \
"Set or return the connection transaction isolation level."

static PyObject *
psyco_conn_isolation_level_get(connectionObject *self)
{
    if (self->isolevel == ISOLATION_LEVEL_DEFAULT) {
        Py_RETURN_NONE;
    } else {
        return PyInt_FromLong((long)self->isolevel);
    }
}


static int
psyco_conn_isolation_level_set(connectionObject *self, PyObject *pyvalue)
{
    int value;

    if (!_psyco_set_session_check_setter_wrapper(self)) { return -1; }
    if (0 > (value = _psyco_conn_parse_isolevel(pyvalue))) { return -1; }
    if (0 > conn_set_session(self, SRV_STATE_UNCHANGED,
                value, SRV_STATE_UNCHANGED, SRV_STATE_UNCHANGED)) {
        return -1;
    }

    return 0;
}


/* set_isolation_level method - switch connection isolation level */

#define psyco_conn_set_isolation_level_doc \
"set_isolation_level(level) -- Switch isolation level to ``level``."

static PyObject *
psyco_conn_set_isolation_level(connectionObject *self, PyObject *args)
{
    int level = 1;
    PyObject *pyval = NULL;

    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, "isolation_level");
    EXC_IF_TPC_PREPARED(self, "isolation_level");

    if (!PyArg_ParseTuple(args, "O", &pyval)) return NULL;

    if (pyval == Py_None) {
        level = ISOLATION_LEVEL_DEFAULT;
    }

    /* parse from one of the level constants */
    else if (PyInt_Check(pyval)) {
        level = PyInt_AsLong(pyval);

        if (level < 0 || level > 4) {
            PyErr_SetString(PyExc_ValueError,
                "isolation level must be between 0 and 4");
            return NULL;
        }
    }

    if (0 > conn_rollback(self)) {
        return NULL;
    }

    if (level == 0) {
        if (0 > conn_set_session(self, 1,
                SRV_STATE_UNCHANGED, SRV_STATE_UNCHANGED, SRV_STATE_UNCHANGED)) {
            return NULL;
        }
    }
    else {
        if (0 > conn_set_session(self, 0,
                level, SRV_STATE_UNCHANGED, SRV_STATE_UNCHANGED)) {
            return NULL;
        }
    }

    Py_RETURN_NONE;
}


/* readonly - return or set the current read-only status */

#define psyco_conn_readonly_doc \
"Set or return the connection read-only status."

static PyObject *
psyco_conn_readonly_get(connectionObject *self)
{
    PyObject *rv = NULL;

    switch (self->readonly) {
        case STATE_OFF:
            rv = Py_False;
            break;
        case STATE_ON:
            rv = Py_True;
            break;
        case STATE_DEFAULT:
            rv = Py_None;
            break;
        default:
            PyErr_Format(InternalError,
                "bad internal value for readonly: %d", self->readonly);
            break;
    }

    Py_XINCREF(rv);
    return rv;
}


static int
psyco_conn_readonly_set(connectionObject *self, PyObject *pyvalue)
{
    int value;

    if (!_psyco_set_session_check_setter_wrapper(self)) { return -1; }
    if (0 > (value = _psyco_conn_parse_onoff(pyvalue))) { return -1; }
    if (0 > conn_set_session(self, SRV_STATE_UNCHANGED,
                SRV_STATE_UNCHANGED, value, SRV_STATE_UNCHANGED)) {
        return -1;
    }

    return 0;
}


/* deferrable - return or set the current deferrable status */

#define psyco_conn_deferrable_doc \
"Set or return the connection deferrable status."

static PyObject *
psyco_conn_deferrable_get(connectionObject *self)
{
    PyObject *rv = NULL;

    switch (self->deferrable) {
        case STATE_OFF:
            rv = Py_False;
            break;
        case STATE_ON:
            rv = Py_True;
            break;
        case STATE_DEFAULT:
            rv = Py_None;
            break;
        default:
            PyErr_Format(InternalError,
                "bad internal value for deferrable: %d", self->deferrable);
            break;
    }

    Py_XINCREF(rv);
    return rv;
}


static int
psyco_conn_deferrable_set(connectionObject *self, PyObject *pyvalue)
{
    int value;

    if (!_psyco_set_session_check_setter_wrapper(self)) { return -1; }
    if (0 > (value = _psyco_conn_parse_onoff(pyvalue))) { return -1; }
    if (0 > conn_set_session(self, SRV_STATE_UNCHANGED,
                SRV_STATE_UNCHANGED, SRV_STATE_UNCHANGED, value)) {
        return -1;
    }

    return 0;
}

/* psyco_get_native_connection - expose PGconn* as a Python capsule */

#define psyco_get_native_connection_doc \
"get_native_connection() -- Return the internal PGconn* as a Python Capsule."

static PyObject *
psyco_get_native_connection(connectionObject *self, PyObject *dummy)
{
    EXC_IF_CONN_CLOSED(self);

    return PyCapsule_New(self->pgconn, "psycopg2.connection.native_connection", NULL);
}


/* set_client_encoding method - set client encoding */

#define psyco_conn_set_client_encoding_doc \
"set_client_encoding(encoding) -- Set client encoding to ``encoding``."

static PyObject *
psyco_conn_set_client_encoding(connectionObject *self, PyObject *args)
{
    const char *enc;
    PyObject *rv = NULL;

    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, set_client_encoding);
    EXC_IF_TPC_PREPARED(self, set_client_encoding);

    if (!PyArg_ParseTuple(args, "s", &enc)) return NULL;

    if (conn_set_client_encoding(self, enc) >= 0) {
        Py_INCREF(Py_None);
        rv = Py_None;
    }
    return rv;
}

/* get_transaction_status method - Get backend transaction status */

#define psyco_conn_get_transaction_status_doc \
"get_transaction_status() -- Get backend transaction status."

static PyObject *
psyco_conn_get_transaction_status(connectionObject *self, PyObject *dummy)
{
    return PyInt_FromLong((long)PQtransactionStatus(self->pgconn));
}

/* get_parameter_status method - Get server parameter status */

#define psyco_conn_get_parameter_status_doc \
"get_parameter_status(parameter) -- Get backend parameter status.\n\n" \
"Potential values for ``parameter``:\n" \
"  server_version, server_encoding, client_encoding, is_superuser,\n" \
"  session_authorization, DateStyle, TimeZone, integer_datetimes,\n" \
"  and standard_conforming_strings\n" \
"If server did not report requested parameter, None is returned.\n\n" \
"See libpq docs for PQparameterStatus() for further details."

static PyObject *
psyco_conn_get_parameter_status(connectionObject *self, PyObject *args)
{
    const char *param = NULL;
    const char *val = NULL;

    EXC_IF_CONN_CLOSED(self);

    if (!PyArg_ParseTuple(args, "s", &param)) return NULL;

    val = PQparameterStatus(self->pgconn, param);
    if (!val) {
        Py_RETURN_NONE;
    }
    return conn_text_from_chars(self, val);
}

/* get_dsn_parameters method - Get connection parameters */

#define psyco_conn_get_dsn_parameters_doc \
"get_dsn_parameters() -- Get effective connection parameters.\n\n"

static PyObject *
psyco_conn_get_dsn_parameters(connectionObject *self, PyObject *dummy)
{
#if PG_VERSION_NUM >= 90300
    PyObject *res = NULL;
    PQconninfoOption *options = NULL;

    EXC_IF_CONN_CLOSED(self);

    if (!(options = PQconninfo(self->pgconn))) {
        PyErr_NoMemory();
        goto exit;
    }

    res = psyco_dict_from_conninfo_options(options, /* include_password = */ 0);

exit:
    PQconninfoFree(options);

    return res;
#else
    PyErr_SetString(NotSupportedError, "PQconninfo not available in libpq < 9.3");
    return NULL;
#endif
}


/* lobject method - allocate a new lobject */

#define psyco_conn_lobject_doc \
"lobject(oid=0, mode=0, new_oid=0, new_file=None,\n"                        \
"       lobject_factory=extensions.lobject) -- new lobject\n\n"             \
"Return a new lobject.\n\nThe ``lobject_factory`` argument can be used\n"   \
"to create non-standard lobjects by passing a class different from the\n"   \
"default. Note that the new class *should* be a sub-class of\n"             \
"`extensions.lobject`.\n\n"                                                 \
":rtype: `extensions.lobject`"

static PyObject *
psyco_conn_lobject(connectionObject *self, PyObject *args, PyObject *keywds)
{
    Oid oid = InvalidOid, new_oid = InvalidOid;
    const char *new_file = NULL;
    const char *smode = "";
    PyObject *factory = (PyObject *)&lobjectType;
    PyObject *obj;

    static char *kwlist[] = {"oid", "mode", "new_oid", "new_file",
                             "lobject_factory", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, keywds, "|IzIzO", kwlist,
                                     &oid, &smode, &new_oid, &new_file,
                                     &factory)) {
        return NULL;
    }

    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, lobject);
    EXC_IF_GREEN(lobject);
    EXC_IF_TPC_PREPARED(self, lobject);

    Dprintf("psyco_conn_lobject: new lobject for connection at %p", self);
    Dprintf("psyco_conn_lobject:     parameters: oid = %u, mode = %s",
            oid, smode);
    Dprintf("psyco_conn_lobject:     parameters: new_oid = %u, new_file = %s",
            new_oid, new_file);

    if (new_file)
        obj = PyObject_CallFunction(factory, "OIsIs",
            self, oid, smode, new_oid, new_file);
    else
        obj = PyObject_CallFunction(factory, "OIsI",
            self, oid, smode, new_oid);

    if (obj == NULL) return NULL;
    if (PyObject_IsInstance(obj, (PyObject *)&lobjectType) == 0) {
        PyErr_SetString(PyExc_TypeError,
            "lobject factory must be subclass of psycopg2.extensions.lobject");
        Py_DECREF(obj);
        return NULL;
    }

    Dprintf("psyco_conn_lobject: new lobject at %p: refcnt = "
            FORMAT_CODE_PY_SSIZE_T,
            obj, Py_REFCNT(obj));
    return obj;
}

/* get the current backend pid */

#define psyco_conn_get_backend_pid_doc \
"get_backend_pid() -- Get backend process id."

static PyObject *
psyco_conn_get_backend_pid(connectionObject *self, PyObject *dummy)
{
    EXC_IF_CONN_CLOSED(self);

    return PyInt_FromLong((long)PQbackendPID(self->pgconn));
}


/* get info about the connection */

#define psyco_conn_info_doc \
"info -- Get connection info."

static PyObject *
psyco_conn_info_get(connectionObject *self)
{
    return PyObject_CallFunctionObjArgs(
        (PyObject *)&connInfoType, (PyObject *)self, NULL);
}


/* return the pointer to the PGconn structure */

#define psyco_conn_pgconn_ptr_doc \
"pgconn_ptr -- Get the PGconn structure pointer."

static PyObject *
psyco_conn_pgconn_ptr_get(connectionObject *self)
{
    if (self->pgconn) {
        return PyLong_FromVoidPtr((void *)self->pgconn);
    }
    else {
        Py_RETURN_NONE;
    }
}


/* reset the currect connection */

#define psyco_conn_reset_doc \
"reset() -- Reset current connection to defaults."

static PyObject *
psyco_conn_reset(connectionObject *self, PyObject *dummy)
{
    int res;

    EXC_IF_CONN_CLOSED(self);
    EXC_IF_CONN_ASYNC(self, reset);

    if (pq_reset(self) < 0)
        return NULL;

    res = conn_setup(self);
    if (res < 0)
        return NULL;

    Py_RETURN_NONE;
}

static PyObject *
psyco_conn_get_exception(PyObject *self, void *closure)
{
    PyObject *exception = *(PyObject **)closure;

    Py_INCREF(exception);
    return exception;
}


#define psyco_conn_poll_doc \
"poll() -> int -- Advance the connection or query process without blocking."

static PyObject *
psyco_conn_poll(connectionObject *self, PyObject *dummy)
{
    int res;

    EXC_IF_CONN_CLOSED(self);

    res = conn_poll(self);
    if (res != PSYCO_POLL_ERROR || !PyErr_Occurred()) {
        return PyInt_FromLong(res);
    } else {
        /* There is an error and an exception is already in place */
        return NULL;
    }
}


#define psyco_conn_fileno_doc \
"fileno() -> int -- Return file descriptor associated to database connection."

static PyObject *
psyco_conn_fileno(connectionObject *self, PyObject *dummy)
{
    long int socket;

    EXC_IF_CONN_CLOSED(self);

    socket = (long int)PQsocket(self->pgconn);

    return PyInt_FromLong(socket);
}


#define psyco_conn_isexecuting_doc                           \
"isexecuting() -> bool -- Return True if the connection is " \
 "executing an asynchronous operation."

static PyObject *
psyco_conn_isexecuting(connectionObject *self, PyObject *dummy)
{
    /* synchronous connections will always return False */
    if (self->async == 0) {
        Py_RETURN_FALSE;
    }

    /* check if the connection is still being built */
    if (self->status != CONN_STATUS_READY) {
        Py_RETURN_TRUE;
    }

    /* check if there is a query being executed */
    if (self->async_cursor != NULL) {
        Py_RETURN_TRUE;
    }

    /* otherwise it's not executing */
    Py_RETURN_FALSE;
}


#define psyco_conn_cancel_doc                           \
"cancel() -- cancel the current operation"

static PyObject *
psyco_conn_cancel(connectionObject *self, PyObject *dummy)
{
    char errbuf[256];

    EXC_IF_CONN_CLOSED(self);
    EXC_IF_TPC_PREPARED(self, cancel);

    /* do not allow cancellation while the connection is being built */
    Dprintf("psyco_conn_cancel: cancelling with key %p", self->cancel);
    if (self->status != CONN_STATUS_READY &&
        self->status != CONN_STATUS_BEGIN) {
        PyErr_SetString(OperationalError,
                        "asynchronous connection attempt underway");
        return NULL;
    }

    if (PQcancel(self->cancel, errbuf, sizeof(errbuf)) == 0) {
        Dprintf("psyco_conn_cancel: cancelling failed: %s", errbuf);
        PyErr_SetString(OperationalError, errbuf);
        return NULL;
    }
    Py_RETURN_NONE;
}


/** the connection object **/


/* object method list */

static struct PyMethodDef connectionObject_methods[] = {
    {"cursor", (PyCFunction)psyco_conn_cursor,
     METH_VARARGS|METH_KEYWORDS, psyco_conn_cursor_doc},
    {"close", (PyCFunction)psyco_conn_close,
     METH_NOARGS, psyco_conn_close_doc},
    {"commit", (PyCFunction)psyco_conn_commit,
     METH_NOARGS, psyco_conn_commit_doc},
    {"rollback", (PyCFunction)psyco_conn_rollback,
     METH_NOARGS, psyco_conn_rollback_doc},
    {"xid", (PyCFunction)psyco_conn_xid,
     METH_VARARGS|METH_KEYWORDS, psyco_conn_xid_doc},
    {"tpc_begin", (PyCFunction)psyco_conn_tpc_begin,
     METH_VARARGS, psyco_conn_tpc_begin_doc},
    {"tpc_prepare", (PyCFunction)psyco_conn_tpc_prepare,
     METH_NOARGS, psyco_conn_tpc_prepare_doc},
    {"tpc_commit", (PyCFunction)psyco_conn_tpc_commit,
     METH_VARARGS, psyco_conn_tpc_commit_doc},
    {"tpc_rollback", (PyCFunction)psyco_conn_tpc_rollback,
     METH_VARARGS, psyco_conn_tpc_rollback_doc},
    {"tpc_recover", (PyCFunction)psyco_conn_tpc_recover,
     METH_NOARGS, psyco_conn_tpc_recover_doc},
    {"__enter__", (PyCFunction)psyco_conn_enter,
     METH_NOARGS, psyco_conn_enter_doc},
    {"__exit__", (PyCFunction)psyco_conn_exit,
     METH_VARARGS, psyco_conn_exit_doc},
    {"set_session", (PyCFunction)psyco_conn_set_session,
     METH_VARARGS|METH_KEYWORDS, psyco_conn_set_session_doc},
    {"set_isolation_level", (PyCFunction)psyco_conn_set_isolation_level,
     METH_VARARGS, psyco_conn_set_isolation_level_doc},
    {"set_client_encoding", (PyCFunction)psyco_conn_set_client_encoding,
     METH_VARARGS, psyco_conn_set_client_encoding_doc},
    {"get_transaction_status", (PyCFunction)psyco_conn_get_transaction_status,
     METH_NOARGS, psyco_conn_get_transaction_status_doc},
    {"get_parameter_status", (PyCFunction)psyco_conn_get_parameter_status,
     METH_VARARGS, psyco_conn_get_parameter_status_doc},
    {"get_dsn_parameters", (PyCFunction)psyco_conn_get_dsn_parameters,
     METH_NOARGS, psyco_conn_get_dsn_parameters_doc},
    {"get_backend_pid", (PyCFunction)psyco_conn_get_backend_pid,
     METH_NOARGS, psyco_conn_get_backend_pid_doc},
    {"lobject", (PyCFunction)psyco_conn_lobject,
     METH_VARARGS|METH_KEYWORDS, psyco_conn_lobject_doc},
    {"reset", (PyCFunction)psyco_conn_reset,
     METH_NOARGS, psyco_conn_reset_doc},
    {"poll", (PyCFunction)psyco_conn_poll,
     METH_NOARGS, psyco_conn_poll_doc},
    {"fileno", (PyCFunction)psyco_conn_fileno,
     METH_NOARGS, psyco_conn_fileno_doc},
    {"isexecuting", (PyCFunction)psyco_conn_isexecuting,
     METH_NOARGS, psyco_conn_isexecuting_doc},
    {"cancel", (PyCFunction)psyco_conn_cancel,
     METH_NOARGS, psyco_conn_cancel_doc},
    {"get_native_connection", (PyCFunction)psyco_get_native_connection,
     METH_NOARGS, psyco_get_native_connection_doc},
    {NULL}
};

/* object member list */

static struct PyMemberDef connectionObject_members[] = {
    {"closed", T_LONG, offsetof(connectionObject, closed), READONLY,
        "True if the connection is closed."},
    {"encoding", T_STRING, offsetof(connectionObject, encoding), READONLY,
        "The current client encoding."},
    {"notices", T_OBJECT, offsetof(connectionObject, notice_list), 0},
    {"notifies", T_OBJECT, offsetof(connectionObject, notifies), 0},
    {"dsn", T_STRING, offsetof(connectionObject, dsn), READONLY,
        "The current connection string."},
    {"async", T_LONG, offsetof(connectionObject, async), READONLY,
        "True if the connection is asynchronous."},
    {"async_", T_LONG, offsetof(connectionObject, async), READONLY,
        "True if the connection is asynchronous."},
    {"status", T_INT,
        offsetof(connectionObject, status), READONLY,
        "The current transaction status."},
    {"cursor_factory", T_OBJECT, offsetof(connectionObject, cursor_factory), 0,
        "Default cursor_factory for cursor()."},
    {"string_types", T_OBJECT, offsetof(connectionObject, string_types), READONLY,
        "A set of typecasters to convert textual values."},
    {"binary_types", T_OBJECT, offsetof(connectionObject, binary_types), READONLY,
        "A set of typecasters to convert binary values."},
    {"protocol_version", T_INT,
        offsetof(connectionObject, protocol), READONLY,
        "Protocol version used for this connection. Currently always 3."},
    {"server_version", T_INT,
        offsetof(connectionObject, server_version), READONLY,
        "Server version."},
    {NULL}
};

#define EXCEPTION_GETTER(exc) \
    { #exc, psyco_conn_get_exception, NULL, exc ## _doc, &exc }

static struct PyGetSetDef connectionObject_getsets[] = {
    EXCEPTION_GETTER(Error),
    EXCEPTION_GETTER(Warning),
    EXCEPTION_GETTER(InterfaceError),
    EXCEPTION_GETTER(DatabaseError),
    EXCEPTION_GETTER(InternalError),
    EXCEPTION_GETTER(OperationalError),
    EXCEPTION_GETTER(ProgrammingError),
    EXCEPTION_GETTER(IntegrityError),
    EXCEPTION_GETTER(DataError),
    EXCEPTION_GETTER(NotSupportedError),
    { "autocommit",
        (getter)psyco_conn_autocommit_get,
        (setter)psyco_conn_autocommit_set,
        psyco_conn_autocommit_doc },
    { "isolation_level",
        (getter)psyco_conn_isolation_level_get,
        (setter)psyco_conn_isolation_level_set,
        psyco_conn_isolation_level_doc },
    { "readonly",
        (getter)psyco_conn_readonly_get,
        (setter)psyco_conn_readonly_set,
        psyco_conn_readonly_doc },
    { "deferrable",
        (getter)psyco_conn_deferrable_get,
        (setter)psyco_conn_deferrable_set,
        psyco_conn_deferrable_doc },
    { "info",
        (getter)psyco_conn_info_get, NULL,
        psyco_conn_info_doc },
    { "pgconn_ptr",
        (getter)psyco_conn_pgconn_ptr_get, NULL,
        psyco_conn_pgconn_ptr_doc },
    {NULL}
};
#undef EXCEPTION_GETTER

/* initialization and finalization methods */

static int
connection_setup(connectionObject *self, const char *dsn, long int async)
{
    int rv = -1;

    Dprintf("connection_setup: init connection object at %p, "
	    "async %ld, refcnt = " FORMAT_CODE_PY_SSIZE_T,
            self, async, Py_REFCNT(self)
      );

    if (!(self->dsn = conn_obscure_password(dsn))) { goto exit; }
    if (!(self->notice_list = PyList_New(0))) { goto exit; }
    if (!(self->notifies = PyList_New(0))) { goto exit; }
    self->async = async;
    self->status = CONN_STATUS_SETUP;
    self->async_status = ASYNC_DONE;
    if (!(self->string_types = PyDict_New())) { goto exit; }
    if (!(self->binary_types = PyDict_New())) { goto exit; }
    self->isolevel = ISOLATION_LEVEL_DEFAULT;
    self->readonly = STATE_DEFAULT;
    self->deferrable = STATE_DEFAULT;
#ifdef CONN_CHECK_PID
    self->procpid = getpid();
#endif

    /* other fields have been zeroed by tp_alloc */

    if (0 != pthread_mutex_init(&(self->lock), NULL)) {
        PyErr_SetString(InternalError, "lock initialization failed");
        goto exit;
    }

    if (conn_connect(self, dsn, async) != 0) {
        Dprintf("connection_init: FAILED");
        goto exit;
    }

    rv = 0;

    Dprintf("connection_setup: good connection object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self));

exit:
    return rv;
}


static int
connection_clear(connectionObject *self)
{
    Py_CLEAR(self->tpc_xid);
    Py_CLEAR(self->async_cursor);
    Py_CLEAR(self->notice_list);
    Py_CLEAR(self->notifies);
    Py_CLEAR(self->string_types);
    Py_CLEAR(self->binary_types);
    Py_CLEAR(self->cursor_factory);
    Py_CLEAR(self->pyencoder);
    Py_CLEAR(self->pydecoder);
    return 0;
}

static void
connection_dealloc(PyObject* obj)
{
    connectionObject *self = (connectionObject *)obj;

    /* Make sure to untrack the connection before calling conn_close, which may
     * allow a different thread to try and dealloc the connection again,
     * resulting in a double-free segfault (ticket #166). */
    PyObject_GC_UnTrack(self);

    /* close the connection only if this is the same process it was created
     * into, otherwise using multiprocessing we may close the connection
     * belonging to another process. */
#ifdef CONN_CHECK_PID
    if (self->procpid == getpid())
#endif
    {
        conn_close(self);
    }

    if (self->weakreflist) {
        PyObject_ClearWeakRefs(obj);
    }

    conn_notice_clean(self);

    PyMem_Free(self->dsn);
    PyMem_Free(self->encoding);
    if (self->error) free(self->error);
    if (self->cancel) PQfreeCancel(self->cancel);
    PQclear(self->pgres);

    connection_clear(self);

    pthread_mutex_destroy(&(self->lock));

    Dprintf("connection_dealloc: deleted connection object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
      );

    Py_TYPE(obj)->tp_free(obj);
}

static int
connection_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    const char *dsn;
    long int async = 0, async_ = 0;
    static char *kwlist[] = {"dsn", "async", "async_", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|ll", kwlist,
            &dsn, &async, &async_))
        return -1;

    if (async_) { async = async_; }
    return connection_setup((connectionObject *)obj, dsn, async);
}

static PyObject *
connection_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static PyObject *
connection_repr(connectionObject *self)
{
    return PyString_FromFormat(
        "<connection object at %p; dsn: '%s', closed: %ld>",
        self, (self->dsn ? self->dsn : "<unintialized>"), self->closed);
}

static int
connection_traverse(connectionObject *self, visitproc visit, void *arg)
{
    Py_VISIT((PyObject *)(self->tpc_xid));
    Py_VISIT(self->async_cursor);
    Py_VISIT(self->notice_list);
    Py_VISIT(self->notifies);
    Py_VISIT(self->string_types);
    Py_VISIT(self->binary_types);
    Py_VISIT(self->cursor_factory);
    Py_VISIT(self->pyencoder);
    Py_VISIT(self->pydecoder);
    return 0;
}


/* object type */

#define connectionType_doc \
"connection(dsn, ...) -> new connection object\n\n" \
":Groups:\n" \
"  * `DBAPI-2.0 errors`: Error, Warning, InterfaceError,\n" \
"    DatabaseError, InternalError, OperationalError,\n" \
"    ProgrammingError, IntegrityError, DataError, NotSupportedError"

PyTypeObject connectionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.connection",
    sizeof(connectionObject), 0,
    connection_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)connection_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    (reprfunc)connection_repr, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC |
        Py_TPFLAGS_HAVE_WEAKREFS,
                /*tp_flags*/
    connectionType_doc, /*tp_doc*/
    (traverseproc)connection_traverse, /*tp_traverse*/
    (inquiry)connection_clear, /*tp_clear*/
    0,          /*tp_richcompare*/
    offsetof(connectionObject, weakreflist), /* tp_weaklistoffset */
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    connectionObject_methods, /*tp_methods*/
    connectionObject_members, /*tp_members*/
    connectionObject_getsets, /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    connection_init, /*tp_init*/
    0,          /*tp_alloc*/
    connection_new, /*tp_new*/
};
