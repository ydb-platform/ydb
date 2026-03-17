/* pqpath.c - single path into libpq
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

/* IMPORTANT NOTE: no function in this file do its own connection locking
   except for pg_execute and pq_fetch (that are somehow high-level). This means
   that all the other functions should be called while holding a lock to the
   connection.
*/

#define PSYCOPG_MODULE
#include "psycopg/psycopg.h"

#include "psycopg/pqpath.h"
#include "psycopg/connection.h"
#include "psycopg/cursor.h"
#include "psycopg/replication_cursor.h"
#include "psycopg/replication_message.h"
#include "psycopg/green.h"
#include "psycopg/typecast.h"
#include "psycopg/pgtypes.h"
#include "psycopg/error.h"
#include "psycopg/column.h"

#include "psycopg/libpq_support.h"
#include "libpq-fe.h"

#include <stdlib.h>
#ifdef _WIN32
/* select() */
#include <winsock2.h>
/* gettimeofday() */
#include "win32_support.h"
#elif defined(__sun) && defined(__SVR4)
#include "solaris_support.h"
#elif defined(_AIX)
#include "aix_support.h"
#else
#include <sys/time.h>
#endif

extern HIDDEN PyObject *psyco_DescriptionType;
extern HIDDEN const char *srv_isolevels[];
extern HIDDEN const char *srv_readonly[];
extern HIDDEN const char *srv_deferrable[];

/* Strip off the severity from a Postgres error message. */
static const char *
strip_severity(const char *msg)
{
    if (!msg)
        return NULL;

    if (strlen(msg) > 8 && (!strncmp(msg, "ERROR:  ", 8) ||
                            !strncmp(msg, "FATAL:  ", 8) ||
                            !strncmp(msg, "PANIC:  ", 8)))
        return &msg[8];
    else
        return msg;
}


/* pq_raise - raise a python exception of the right kind

   This function should be called while holding the GIL.

   The function passes the ownership of the pgres to the returned exception,
   where the pgres was the explicit argument or taken from the cursor.
   So, after calling it curs->pgres will be set to null */

RAISES static void
pq_raise(connectionObject *conn, cursorObject *curs, PGresult **pgres)
{
    PyObject *exc = NULL;
    const char *err = NULL;
    const char *err2 = NULL;
    const char *code = NULL;
    PyObject *pyerr = NULL;
    PyObject *pgerror = NULL, *pgcode = NULL;

    if (conn == NULL) {
        PyErr_SetString(DatabaseError,
            "psycopg went psychotic and raised a null error");
        return;
    }

    /* if the connection has somehow been broken, we mark the connection
       object as closed but requiring cleanup */
    if (conn->pgconn != NULL && PQstatus(conn->pgconn) == CONNECTION_BAD) {
        conn->closed = 2;
        exc = OperationalError;
    }

    if (pgres == NULL && curs != NULL)
        pgres = &curs->pgres;

    if (pgres && *pgres) {
        err = PQresultErrorMessage(*pgres);
        if (err != NULL) {
            Dprintf("pq_raise: PQresultErrorMessage: err=%s", err);
            code = PQresultErrorField(*pgres, PG_DIAG_SQLSTATE);
        }
    }
    if (err == NULL) {
        err = PQerrorMessage(conn->pgconn);
        Dprintf("pq_raise: PQerrorMessage: err=%s", err);
    }

    /* if the is no error message we probably called pq_raise without reason:
       we need to set an exception anyway because the caller will probably
       raise and a meaningful message is better than an empty one.
       Note: it can happen without it being our error: see ticket #82 */
    if (err == NULL || err[0] == '\0') {
        PyErr_Format(DatabaseError,
            "error with status %s and no message from the libpq",
            PQresStatus(pgres == NULL ?
                PQstatus(conn->pgconn) : PQresultStatus(*pgres)));
        return;
    }

    /* Analyze the message and try to deduce the right exception kind
       (only if we got the SQLSTATE from the pgres, obviously) */
    if (code != NULL) {
        exc = exception_from_sqlstate(code);
    }
    else if (exc == NULL) {
        /* Fallback if there is no exception code (unless we already
           determined that the connection was closed). */
        exc = DatabaseError;
    }

    /* try to remove the initial "ERROR: " part from the postgresql error */
    err2 = strip_severity(err);
    Dprintf("pq_raise: err2=%s", err2);

    /* decode now the details of the error, because after psyco_set_error
     * decoding will fail.
     */
    if (!(pgerror = conn_text_from_chars(conn, err))) {
        /* we can't really handle an exception while handling this error
         * so just print it. */
        PyErr_Print();
        PyErr_Clear();
    }

    if (!(pgcode = conn_text_from_chars(conn, code))) {
        PyErr_Print();
        PyErr_Clear();
    }

    pyerr = psyco_set_error(exc, curs, err2);

    if (pyerr && PyObject_TypeCheck(pyerr, &errorType)) {
        errorObject *perr = (errorObject *)pyerr;

        Py_CLEAR(perr->pydecoder);
        Py_XINCREF(conn->pydecoder);
        perr->pydecoder = conn->pydecoder;

        Py_CLEAR(perr->pgerror);
        perr->pgerror = pgerror;
        pgerror = NULL;

        Py_CLEAR(perr->pgcode);
        perr->pgcode = pgcode;
        pgcode = NULL;

        CLEARPGRES(perr->pgres);
        if (pgres && *pgres) {
            perr->pgres = *pgres;
            *pgres = NULL;
        }
    }

    Py_XDECREF(pgerror);
    Py_XDECREF(pgcode);
}

/* pq_clear_async - clear the effects of a previous async query

   note that this function does block because it needs to wait for the full
   result sets of the previous query to clear them.

   this function does not call any Py_*_ALLOW_THREADS macros */

void
pq_clear_async(connectionObject *conn)
{
    PGresult *pgres;

    /* this will get all pending results (if the submitted query consisted of
       many parts, i.e. "select 1; select 2", there will be many) and also
       finalize asynchronous processing so the connection will be ready to
       accept another query */

    while ((pgres = PQgetResult(conn->pgconn))) {
        Dprintf("pq_clear_async: clearing PGresult at %p", pgres);
        PQclear(pgres);
    }
    Py_CLEAR(conn->async_cursor);
}


/* pq_set_non_blocking - set the nonblocking status on a connection.

   Accepted arg values are 1 (nonblocking) and 0 (blocking).

   Return 0 if everything ok, else < 0 and set an exception.
 */
RAISES_NEG int
pq_set_non_blocking(connectionObject *conn, int arg)
{
    int ret = PQsetnonblocking(conn->pgconn, arg);
    if (0 != ret) {
        Dprintf("PQsetnonblocking(%d) FAILED", arg);
        PyErr_SetString(OperationalError, "PQsetnonblocking() failed");
        ret = -1;
    }
    return ret;
}


/* pg_execute_command_locked - execute a no-result query on a locked connection.

   This function should only be called on a locked connection without
   holding the global interpreter lock.

   On error, -1 is returned, and the conn->pgres will hold the
   relevant result structure.

   The tstate parameter should be the pointer of the _save variable created by
   Py_BEGIN_ALLOW_THREADS: this enables the function to acquire and release
   again the GIL if needed, i.e. if a Python wait callback must be invoked.
 */
int
pq_execute_command_locked(
    connectionObject *conn, const char *query, PyThreadState **tstate)
{
    int pgstatus, retvalue = -1;
    Dprintf("pq_execute_command_locked: pgconn = %p, query = %s",
            conn->pgconn, query);

    if (!psyco_green()) {
        conn_set_result(conn, PQexec(conn->pgconn, query));
    } else {
        PyEval_RestoreThread(*tstate);
        conn_set_result(conn, psyco_exec_green(conn, query));
        *tstate = PyEval_SaveThread();
    }
    if (conn->pgres == NULL) {
        Dprintf("pq_execute_command_locked: PQexec returned NULL");
        PyEval_RestoreThread(*tstate);
        if (!PyErr_Occurred()) {
            conn_set_error(conn, PQerrorMessage(conn->pgconn));
        }
        *tstate = PyEval_SaveThread();
        goto cleanup;
    }

    pgstatus = PQresultStatus(conn->pgres);
    if (pgstatus != PGRES_COMMAND_OK ) {
        Dprintf("pq_execute_command_locked: result was not COMMAND_OK (%d)",
                pgstatus);
        goto cleanup;
    }

    retvalue = 0;
    CLEARPGRES(conn->pgres);

cleanup:
    return retvalue;
}

/* pq_complete_error: handle an error from pq_execute_command_locked()

   If pq_execute_command_locked() returns -1, this function should be
   called to convert the result to a Python exception.

   This function should be called while holding the global interpreter
   lock.
 */
RAISES void
pq_complete_error(connectionObject *conn)
{
    Dprintf("pq_complete_error: pgconn = %p, error = %s",
            conn->pgconn, conn->error);
    if (conn->pgres) {
        pq_raise(conn, NULL, &conn->pgres);
        /* now conn->pgres is null */
    }
    else {
        if (conn->error) {
            PyErr_SetString(OperationalError, conn->error);
        } else if (PyErr_Occurred()) {
            /* There was a Python error (e.g. in the callback). Don't clobber
             * it with an unknown exception. (see #410) */
            Dprintf("pq_complete_error: forwarding Python exception");
        } else {
            PyErr_SetString(OperationalError, "unknown error");
        }
        /* Trivia: with a broken socket connection PQexec returns NULL, so we
         * end up here. With a TCP connection we get a pgres with an error
         * instead, and the connection gets closed in the pq_raise call above
         * (see ticket #196)
         */
        if (CONNECTION_BAD == PQstatus(conn->pgconn)) {
            conn->closed = 2;
        }
    }
    conn_set_error(conn, NULL);
}


/* pq_begin_locked - begin a transaction, if necessary

   This function should only be called on a locked connection without
   holding the global interpreter lock.

   On error, -1 is returned, and the conn->pgres argument will hold the
   relevant result structure.
 */
int
pq_begin_locked(connectionObject *conn, PyThreadState **tstate)
{
    const size_t bufsize = 256;
    char buf[256];  /* buf size must be same as bufsize */
    int result;

    Dprintf("pq_begin_locked: pgconn = %p, %d, status = %d",
            conn->pgconn, conn->autocommit, conn->status);

    if (conn->status != CONN_STATUS_READY) {
        Dprintf("pq_begin_locked: transaction in progress");
        return 0;
    }

    if (conn->autocommit && !conn->entered) {
        Dprintf("pq_begin_locked: autocommit and no with block");
        return 0;
    }

    if (conn->isolevel == ISOLATION_LEVEL_DEFAULT
            && conn->readonly == STATE_DEFAULT
            && conn->deferrable == STATE_DEFAULT) {
        strcpy(buf, "BEGIN");
    }
    else {
        snprintf(buf, bufsize,
            conn->server_version >= 80000 ?
                "BEGIN%s%s%s%s" : "BEGIN;SET TRANSACTION%s%s%s%s",
            (conn->isolevel >= 1 && conn->isolevel <= 4)
                ? " ISOLATION LEVEL " : "",
            (conn->isolevel >= 1 && conn->isolevel <= 4)
                ? srv_isolevels[conn->isolevel] : "",
            srv_readonly[conn->readonly],
            srv_deferrable[conn->deferrable]);
    }

    result = pq_execute_command_locked(conn, buf, tstate);
    if (result == 0)
        conn->status = CONN_STATUS_BEGIN;

    return result;
}

/* pq_commit - send an END, if necessary

   This function should be called while holding the global interpreter
   lock.
*/

int
pq_commit(connectionObject *conn)
{
    int retvalue = -1;

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&conn->lock);

    Dprintf("pq_commit: pgconn = %p, status = %d",
            conn->pgconn, conn->status);

    if (conn->status != CONN_STATUS_BEGIN) {
        Dprintf("pq_commit: no transaction to commit");
        retvalue = 0;
    }
    else {
        conn->mark += 1;
        retvalue = pq_execute_command_locked(conn, "COMMIT", &_save);
    }

    Py_BLOCK_THREADS;
    conn_notifies_process(conn);
    conn_notice_process(conn);
    Py_UNBLOCK_THREADS;

    /* Even if an error occurred, the connection will be rolled back,
       so we unconditionally set the connection status here. */
    conn->status = CONN_STATUS_READY;

    pthread_mutex_unlock(&conn->lock);
    Py_END_ALLOW_THREADS;

    if (retvalue < 0)
        pq_complete_error(conn);

    return retvalue;
}

RAISES_NEG int
pq_abort_locked(connectionObject *conn, PyThreadState **tstate)
{
    int retvalue = -1;

    Dprintf("pq_abort_locked: pgconn = %p, status = %d",
            conn->pgconn, conn->status);

    if (conn->status != CONN_STATUS_BEGIN) {
        Dprintf("pq_abort_locked: no transaction to abort");
        return 0;
    }

    conn->mark += 1;
    retvalue = pq_execute_command_locked(conn, "ROLLBACK", tstate);
    if (retvalue == 0)
        conn->status = CONN_STATUS_READY;

    return retvalue;
}

/* pq_abort - send an ABORT, if necessary

   This function should be called while holding the global interpreter
   lock. */

RAISES_NEG int
pq_abort(connectionObject *conn)
{
    int retvalue = -1;

    Dprintf("pq_abort: pgconn = %p, autocommit = %d, status = %d",
            conn->pgconn, conn->autocommit, conn->status);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&conn->lock);

    retvalue = pq_abort_locked(conn, &_save);

    Py_BLOCK_THREADS;
    conn_notifies_process(conn);
    conn_notice_process(conn);
    Py_UNBLOCK_THREADS;

    pthread_mutex_unlock(&conn->lock);
    Py_END_ALLOW_THREADS;

    if (retvalue < 0)
        pq_complete_error(conn);

    return retvalue;
}

/* pq_reset - reset the connection

   This function should be called while holding the global interpreter
   lock.

   The _locked version of this function should be called on a locked
   connection without holding the global interpreter lock.
*/

RAISES_NEG int
pq_reset_locked(connectionObject *conn, PyThreadState **tstate)
{
    int retvalue = -1;

    Dprintf("pq_reset_locked: pgconn = %p, status = %d",
            conn->pgconn, conn->status);

    conn->mark += 1;

    if (conn->status == CONN_STATUS_BEGIN) {
        retvalue = pq_execute_command_locked(conn, "ABORT", tstate);
        if (retvalue != 0) return retvalue;
    }

    if (conn->server_version >= 80300) {
        retvalue = pq_execute_command_locked(conn, "DISCARD ALL", tstate);
        if (retvalue != 0) return retvalue;
    }
    else {
        retvalue = pq_execute_command_locked(conn, "RESET ALL", tstate);
        if (retvalue != 0) return retvalue;

        retvalue = pq_execute_command_locked(conn,
            "SET SESSION AUTHORIZATION DEFAULT", tstate);
        if (retvalue != 0) return retvalue;
    }

    /* should set the tpc xid to null: postponed until we get the GIL again */
    conn->status = CONN_STATUS_READY;

    return retvalue;
}

int
pq_reset(connectionObject *conn)
{
    int retvalue = -1;

    Dprintf("pq_reset: pgconn = %p, autocommit = %d, status = %d",
            conn->pgconn, conn->autocommit, conn->status);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&conn->lock);

    retvalue = pq_reset_locked(conn, &_save);

    Py_BLOCK_THREADS;
    conn_notice_process(conn);
    conn_notifies_process(conn);
    Py_UNBLOCK_THREADS;

    pthread_mutex_unlock(&conn->lock);
    Py_END_ALLOW_THREADS;

    if (retvalue < 0) {
        pq_complete_error(conn);
    }
    else {
        Py_CLEAR(conn->tpc_xid);
    }
    return retvalue;
}


/* Get a session parameter.
 *
 * The function should be called on a locked connection without
 * holding the GIL.
 *
 * The result is a new string allocated with malloc.
 */

char *
pq_get_guc_locked(connectionObject *conn, const char *param, PyThreadState **tstate)
{
    char query[256];
    int size;
    char *rv = NULL;

    Dprintf("pq_get_guc_locked: reading %s", param);

    size = PyOS_snprintf(query, sizeof(query), "SHOW %s", param);
    if (size < 0 || (size_t)size >= sizeof(query)) {
        conn_set_error(conn, "SHOW: query too large");
        goto cleanup;
    }

    Dprintf("pq_get_guc_locked: pgconn = %p, query = %s", conn->pgconn, query);

    if (!psyco_green()) {
        conn_set_result(conn, PQexec(conn->pgconn, query));
    } else {
        PyEval_RestoreThread(*tstate);
        conn_set_result(conn, psyco_exec_green(conn, query));
        *tstate = PyEval_SaveThread();
    }

    if (!conn->pgres) {
        Dprintf("pq_get_guc_locked: PQexec returned NULL");
        PyEval_RestoreThread(*tstate);
        if (!PyErr_Occurred()) {
            conn_set_error(conn, PQerrorMessage(conn->pgconn));
        }
        *tstate = PyEval_SaveThread();
        goto cleanup;
    }
    if (PQresultStatus(conn->pgres) != PGRES_TUPLES_OK) {
        Dprintf("pq_get_guc_locked: result was not TUPLES_OK (%s)",
                PQresStatus(PQresultStatus(conn->pgres)));
        goto cleanup;
    }

    rv = strdup(PQgetvalue(conn->pgres, 0, 0));
    CLEARPGRES(conn->pgres);

cleanup:
    return rv;
}

/* Set a session parameter.
 *
 * The function should be called on a locked connection without
 * holding the GIL
 */

int
pq_set_guc_locked(
    connectionObject *conn, const char *param, const char *value,
    PyThreadState **tstate)
{
    char query[256];
    int size;
    int rv = -1;

    Dprintf("pq_set_guc_locked: setting %s to %s", param, value);

    if (0 == strcmp(value, "default")) {
        size = PyOS_snprintf(query, sizeof(query),
            "SET %s TO DEFAULT", param);
    }
    else {
        size = PyOS_snprintf(query, sizeof(query),
            "SET %s TO '%s'", param, value);
    }
    if (size < 0 || (size_t)size >= sizeof(query)) {
        conn_set_error(conn, "SET: query too large");
        goto exit;
    }

    rv = pq_execute_command_locked(conn, query, tstate);

exit:
    return rv;
}

/* Call one of the PostgreSQL tpc-related commands.
 *
 * This function should only be called on a locked connection without
 * holding the global interpreter lock. */

int
pq_tpc_command_locked(
    connectionObject *conn, const char *cmd, const char *tid,
    PyThreadState **tstate)
{
    int rv = -1;
    char *etid = NULL, *buf = NULL;
    Py_ssize_t buflen;

    Dprintf("_pq_tpc_command: pgconn = %p, command = %s",
            conn->pgconn, cmd);

    conn->mark += 1;

    PyEval_RestoreThread(*tstate);

    /* convert the xid into the postgres transaction_id and quote it. */
    if (!(etid = psyco_escape_string(conn, tid, -1, NULL, NULL)))
    { goto exit; }

    /* prepare the command to the server */
    buflen = 2 + strlen(cmd) + strlen(etid); /* add space, zero */
    if (!(buf = PyMem_Malloc(buflen))) {
        PyErr_NoMemory();
        goto exit;
    }
    if (0 > PyOS_snprintf(buf, buflen, "%s %s", cmd, etid)) { goto exit; }

    /* run the command and let it handle the error cases */
    *tstate = PyEval_SaveThread();
    rv = pq_execute_command_locked(conn, buf, tstate);
    PyEval_RestoreThread(*tstate);

exit:
    PyMem_Free(buf);
    PyMem_Free(etid);

    *tstate = PyEval_SaveThread();
    return rv;
}


/* pq_get_result_async - read an available result without blocking.
 *
 * Return 0 if the result is ready, 1 if it will block, -1 on error.
 * The last result will be returned in conn->pgres.
 *
 * The function should be called with the lock and holding the GIL.
 */

RAISES_NEG int
pq_get_result_async(connectionObject *conn)
{
    int rv = -1;

    Dprintf("pq_get_result_async: calling PQconsumeInput()");
    if (PQconsumeInput(conn->pgconn) == 0) {
        Dprintf("pq_get_result_async: PQconsumeInput() failed");

        /* if the libpq says pgconn is lost, close the py conn */
        if (CONNECTION_BAD == PQstatus(conn->pgconn)) {
            conn->closed = 2;
        }

        PyErr_SetString(OperationalError, PQerrorMessage(conn->pgconn));
        goto exit;
    }

    conn_notifies_process(conn);
    conn_notice_process(conn);

    for (;;) {
        int busy;
        PGresult *res;
        ExecStatusType status;

        Dprintf("pq_get_result_async: calling PQisBusy()");
        busy = PQisBusy(conn->pgconn);

        if (busy) {
            /* try later */
            Dprintf("pq_get_result_async: PQisBusy() = 1");
            rv = 1;
            goto exit;
        }

        if (!(res = PQgetResult(conn->pgconn))) {
            Dprintf("pq_get_result_async: got no result");
            /* the result is ready: it was the previously read one */
            rv = 0;
            goto exit;
        }

        status = PQresultStatus(res);
        Dprintf("pq_get_result_async: got result %s", PQresStatus(status));

        /* Store the result outside because we want to return the last non-null
         * one and we may have to do it across poll calls. However if there is
         * an error in the stream of results we want to handle the *first*
         * error. So don't clobber it with the following ones. */
        if (conn->pgres && PQresultStatus(conn->pgres) == PGRES_FATAL_ERROR) {
            Dprintf("previous pgres is error: discarding");
            PQclear(res);
        }
        else {
            conn_set_result(conn, res);
        }

        switch (status) {
            case PGRES_COPY_OUT:
            case PGRES_COPY_IN:
            case PGRES_COPY_BOTH:
                /* After entering copy mode, libpq will make a phony
                 * PGresult for us every time we query for it, so we need to
                 * break out of this endless loop. */
                rv = 0;
                goto exit;

            default:
                /* keep on reading to check if there are other results or
                 * we have finished. */
                continue;
        }
    }

exit:
    return rv;
}

/* pq_flush - flush output and return connection status

   a status of 1 means that a some data is still pending to be flushed, while a
   status of 0 means that there is no data waiting to be sent. -1 means an
   error and an exception will be set accordingly.

   this function locks the connection object
   this function call Py_*_ALLOW_THREADS macros */

int
pq_flush(connectionObject *conn)
{
    int res;

    Dprintf("pq_flush: flushing output");

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(conn->lock));
    res = PQflush(conn->pgconn);
    pthread_mutex_unlock(&(conn->lock));
    Py_END_ALLOW_THREADS;

    return res;
}

/* pq_execute - execute a query, possibly asynchronously
 *
 * With no_result an eventual query result is discarded.
 * Currently only used to implement cursor.executemany().
 *
 * This function locks the connection object
 * This function call Py_*_ALLOW_THREADS macros
*/

RAISES_NEG int
_pq_execute_sync(cursorObject *curs, const char *query, int no_result, int no_begin)
{
    connectionObject *conn = curs->conn;

    CLEARPGRES(curs->pgres);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(conn->lock));

    if (!no_begin && pq_begin_locked(conn, &_save) < 0) {
        pthread_mutex_unlock(&(conn->lock));
        Py_BLOCK_THREADS;
        pq_complete_error(conn);
        return -1;
    }

    Dprintf("pq_execute: executing SYNC query: pgconn = %p", conn->pgconn);
    Dprintf("    %-.200s", query);
    if (!psyco_green()) {
        conn_set_result(conn, PQexec(conn->pgconn, query));
    }
    else {
        Py_BLOCK_THREADS;
        conn_set_result(conn, psyco_exec_green(conn, query));
        Py_UNBLOCK_THREADS;
    }

    /* don't let pgres = NULL go to pq_fetch() */
    if (!conn->pgres) {
        if (CONNECTION_BAD == PQstatus(conn->pgconn)) {
            conn->closed = 2;
        }
        pthread_mutex_unlock(&(conn->lock));
        Py_BLOCK_THREADS;
        if (!PyErr_Occurred()) {
            PyErr_SetString(OperationalError,
                            PQerrorMessage(conn->pgconn));
        }
        return -1;
    }

    Py_BLOCK_THREADS;

    /* assign the result back to the cursor now that we have the GIL */
    curs_set_result(curs, conn->pgres);
    conn->pgres = NULL;

    /* Process notifies here instead of when fetching the tuple as we are
     * into the same critical section that received the data. Without this
     * care, reading notifies may disrupt other thread communications.
     * (as in ticket #55). */
    conn_notifies_process(conn);
    conn_notice_process(conn);
    Py_UNBLOCK_THREADS;

    pthread_mutex_unlock(&(conn->lock));
    Py_END_ALLOW_THREADS;

    /* if the execute was sync, we call pq_fetch() immediately,
       to respect the old DBAPI-2.0 compatible behaviour */
    Dprintf("pq_execute: entering synchronous DBAPI compatibility mode");
    if (pq_fetch(curs, no_result) < 0) return -1;

    return 1;
}

RAISES_NEG int
_pq_execute_async(cursorObject *curs, const char *query, int no_result)
{
    int async_status = ASYNC_WRITE;
    connectionObject *conn = curs->conn;
    int ret;

    CLEARPGRES(curs->pgres);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(conn->lock));

    Dprintf("pq_execute: executing ASYNC query: pgconn = %p", conn->pgconn);
    Dprintf("    %-.200s", query);

    if (PQsendQuery(conn->pgconn, query) == 0) {
        if (CONNECTION_BAD == PQstatus(conn->pgconn)) {
            conn->closed = 2;
        }
        pthread_mutex_unlock(&(conn->lock));
        Py_BLOCK_THREADS;
        PyErr_SetString(OperationalError,
                        PQerrorMessage(conn->pgconn));
        return -1;
    }
    Dprintf("pq_execute: async query sent to backend");

    ret = PQflush(conn->pgconn);
    if (ret == 0) {
        /* the query got fully sent to the server */
        Dprintf("pq_execute: query got flushed immediately");
        /* the async status will be ASYNC_READ */
        async_status = ASYNC_READ;
    }
    else if (ret == 1) {
        /* not all of the query got sent to the server */
        async_status = ASYNC_WRITE;
    }
    else {
        /* there was an error */
        pthread_mutex_unlock(&(conn->lock));
        Py_BLOCK_THREADS;
        PyErr_SetString(OperationalError,
                        PQerrorMessage(conn->pgconn));
        return -1;
    }

    pthread_mutex_unlock(&(conn->lock));
    Py_END_ALLOW_THREADS;

    conn->async_status = async_status;
    if (!(conn->async_cursor
            = PyWeakref_NewRef((PyObject *)curs, NULL))) {
        return -1;
    }

    return 0;
}

RAISES_NEG int
pq_execute(cursorObject *curs, const char *query, int async, int no_result, int no_begin)
{
    /* check status of connection, raise error if not OK */
    if (PQstatus(curs->conn->pgconn) != CONNECTION_OK) {
        Dprintf("pq_execute: connection NOT OK");
        PyErr_SetString(OperationalError, PQerrorMessage(curs->conn->pgconn));
        return -1;
    }
    Dprintf("pq_execute: pg connection at %p OK", curs->conn->pgconn);

    if (!async) {
        return _pq_execute_sync(curs, query, no_result, no_begin);
    } else {
        return _pq_execute_async(curs, query, no_result);
    }
}


/* send an async query to the backend.
 *
 * Return 1 if command succeeded, else 0.
 *
 * The function should be called helding the connection lock and the GIL.
 */
int
pq_send_query(connectionObject *conn, const char *query)
{
    int rv;

    Dprintf("pq_send_query: sending ASYNC query:");
    Dprintf("    %-.200s", query);

    CLEARPGRES(conn->pgres);
    if (0 == (rv = PQsendQuery(conn->pgconn, query))) {
        Dprintf("pq_send_query: error: %s", PQerrorMessage(conn->pgconn));
    }

    return rv;
}


/* pq_fetch - fetch data after a query

   this function locks the connection object
   this function call Py_*_ALLOW_THREADS macros

   return value:
     -1 - some error occurred while calling libpq
      0 - no result from the backend but no libpq errors
      1 - result from backend (possibly data is ready)
*/

static PyObject *
_get_cast(cursorObject *curs, PGresult *pgres, int i)
{
    /* fill the right cast function by accessing three different dictionaries:
       - the per-cursor dictionary, if available (can be NULL or None)
       - the per-connection dictionary (always exists but can be null)
       - the global dictionary (at module level)
       if we get no defined cast use the default one */
    PyObject *type = NULL;
    PyObject *cast = NULL;
    PyObject *rv = NULL;

    Oid ftype = PQftype(pgres, i);
    if (!(type = PyLong_FromOid(ftype))) { goto exit; }

    Dprintf("_pq_fetch_tuples: looking for cast %u:", ftype);
    if (!(cast = curs_get_cast(curs, type))) { goto exit; }

    /* else if we got binary tuples and if we got a field that
       is binary use the default cast
       FIXME: what the hell am I trying to do here? This just can't work..
    */
    if (cast == psyco_default_binary_cast && PQbinaryTuples(pgres)) {
        Dprintf("_pq_fetch_tuples: Binary cursor and "
                "binary field: %u using default cast", ftype);
        cast = psyco_default_cast;
    }

    Dprintf("_pq_fetch_tuples: using cast at %p for type %u", cast, ftype);

    /* success */
    Py_INCREF(cast);
    rv = cast;

exit:
    Py_XDECREF(type);
    return rv;
}

static PyObject *
_make_column(connectionObject *conn, PGresult *pgres, int i)
{
    Oid ftype = PQftype(pgres, i);
    int fsize = PQfsize(pgres, i);
    int fmod =  PQfmod(pgres, i);
    Oid ftable = PQftable(pgres, i);
    int ftablecol = PQftablecol(pgres, i);

    columnObject *column = NULL;
    PyObject *rv = NULL;

    if (!(column = (columnObject *)PyObject_CallObject(
            (PyObject *)&columnType, NULL))) {
        goto exit;
    }

    /* fill the type and name fields */
    {
        PyObject *tmp;
        if (!(tmp = PyLong_FromOid(ftype))) {
            goto exit;
        }
        column->type_code = tmp;
    }

    {
        PyObject *tmp;
        if (!(tmp = conn_text_from_chars(conn, PQfname(pgres, i)))) {
            goto exit;
        }
        column->name = tmp;
    }

    /* display size is the maximum size of this field result tuples. */
    Py_INCREF(Py_None);
    column->display_size = Py_None;

    /* size on the backend */
    if (fmod > 0) {
        fmod = fmod - sizeof(int);
    }
    if (fsize == -1) {
        if (ftype == NUMERICOID) {
            PyObject *tmp;
            if (!(tmp = PyInt_FromLong((fmod >> 16)))) { goto exit; }
            column->internal_size = tmp;
        }
        else { /* If variable length record, return maximum size */
            PyObject *tmp;
            if (!(tmp = PyInt_FromLong(fmod))) { goto exit; }
            column->internal_size = tmp;
        }
    }
    else {
        PyObject *tmp;
        if (!(tmp = PyInt_FromLong(fsize))) { goto exit; }
        column->internal_size = tmp;
    }

    /* scale and precision */
    if (ftype == NUMERICOID) {
        PyObject *tmp;

        if (!(tmp = PyInt_FromLong((fmod >> 16) & 0xFFFF))) {
            goto exit;
        }
        column->precision = tmp;

        if (!(tmp = PyInt_FromLong(fmod & 0xFFFF))) {
            goto exit;
        }
        column->scale = tmp;
    }

    /* table_oid, table_column */
    if (ftable != InvalidOid) {
        PyObject *tmp;
        if (!(tmp = PyLong_FromOid(ftable))) { goto exit; }
        column->table_oid = tmp;
    }

    if (ftablecol > 0) {
        PyObject *tmp;
        if (!(tmp = PyInt_FromLong((long)ftablecol))) { goto exit; }
        column->table_column = tmp;
    }

    /* success */
    rv = (PyObject *)column;
    column = NULL;

exit:
    Py_XDECREF(column);
    return rv;
}

RAISES_NEG static int
_pq_fetch_tuples(cursorObject *curs)
{
    int i;
    int pgnfields;
    int rv = -1;
    PyObject *description = NULL;
    PyObject *casts = NULL;

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(curs->conn->lock));
    Py_END_ALLOW_THREADS;

    pgnfields = PQnfields(curs->pgres);

    curs->notuples = 0;

    /* create the tuple for description and typecasting */
    Py_CLEAR(curs->description);
    Py_CLEAR(curs->casts);
    if (!(description = PyTuple_New(pgnfields))) { goto exit; }
    if (!(casts = PyTuple_New(pgnfields))) { goto exit; }
    curs->columns = pgnfields;

    /* calculate each field's parameters and typecasters */
    for (i = 0; i < pgnfields; i++) {
        PyObject *column = NULL;
        PyObject *cast = NULL;

        if (!(column = _make_column(curs->conn, curs->pgres, i))) {
            goto exit;
        }
        PyTuple_SET_ITEM(description, i, (PyObject *)column);

        if (!(cast = _get_cast(curs, curs->pgres, i))) {
            goto exit;
        }
        PyTuple_SET_ITEM(casts, i, cast);
    }

    curs->description = description;
    description = NULL;

    curs->casts = casts;
    casts = NULL;

    rv = 0;

exit:
    Py_XDECREF(description);
    Py_XDECREF(casts);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_unlock(&(curs->conn->lock));
    Py_END_ALLOW_THREADS;

    return rv;
}


void
_read_rowcount(cursorObject *curs)
{
    const char *rowcount;

    rowcount = PQcmdTuples(curs->pgres);
    Dprintf("_read_rowcount: PQcmdTuples = %s", rowcount);
    if (!rowcount || !rowcount[0]) {
        curs->rowcount = -1;
    } else {
        curs->rowcount = atol(rowcount);
    }
}

static int
_pq_copy_in_v3(cursorObject *curs)
{
    /* COPY FROM implementation when protocol 3 is available: this function
       uses the new PQputCopyData() and can detect errors and set the correct
       exception */
    PyObject *o, *func = NULL, *size = NULL;
    Py_ssize_t length = 0;
    int res, error = 0;

    if (!curs->copyfile) {
        PyErr_SetString(ProgrammingError,
            "can't execute COPY FROM: use the copy_from() method instead");
        error = 1;
        goto exit;
    }

    if (!(func = PyObject_GetAttrString(curs->copyfile, "read"))) {
        Dprintf("_pq_copy_in_v3: can't get o.read");
        error = 1;
        goto exit;
    }
    if (!(size = PyInt_FromSsize_t(curs->copysize))) {
        Dprintf("_pq_copy_in_v3: can't get int from copysize");
        error = 1;
        goto exit;
    }

    while (1) {
        if (!(o = PyObject_CallFunctionObjArgs(func, size, NULL))) {
            Dprintf("_pq_copy_in_v3: read() failed");
            error = 1;
            break;
        }

        /* a file may return unicode if implements io.TextIOBase */
        if (PyUnicode_Check(o)) {
            PyObject *tmp;
            if (!(tmp = conn_encode(curs->conn, o))) {
                Dprintf("_pq_copy_in_v3: encoding() failed");
                error = 1;
                break;
            }
            Py_DECREF(o);
            o = tmp;
        }

        if (!Bytes_Check(o)) {
            Dprintf("_pq_copy_in_v3: got %s instead of bytes",
                Py_TYPE(o)->tp_name);
            error = 1;
            break;
        }

        if (0 == (length = Bytes_GET_SIZE(o))) {
            break;
        }
        if (length > INT_MAX) {
            Dprintf("_pq_copy_in_v3: bad length: " FORMAT_CODE_PY_SSIZE_T,
                length);
            error = 1;
            break;
        }

        Py_BEGIN_ALLOW_THREADS;
        res = PQputCopyData(curs->conn->pgconn, Bytes_AS_STRING(o),
            /* Py_ssize_t->int cast was validated above */
            (int) length);
        Dprintf("_pq_copy_in_v3: sent " FORMAT_CODE_PY_SSIZE_T " bytes of data; res = %d",
            length, res);

        if (res == 0) {
            /* FIXME: in theory this should not happen but adding a check
               here would be a nice idea */
        }
        else if (res == -1) {
            Dprintf("_pq_copy_in_v3: PQerrorMessage = %s",
                PQerrorMessage(curs->conn->pgconn));
            error = 2;
        }
        Py_END_ALLOW_THREADS;

        if (error == 2) break;

        Py_DECREF(o);
    }

    Py_XDECREF(o);

    Dprintf("_pq_copy_in_v3: error = %d", error);

    /* 0 means that the copy went well, 2 that there was an error on the
       backend: in both cases we'll get the error message from the PQresult */
    if (error == 0)
        res = PQputCopyEnd(curs->conn->pgconn, NULL);
    else if (error == 2)
        res = PQputCopyEnd(curs->conn->pgconn, "error in PQputCopyData() call");
    else {
        char buf[1024];
        strcpy(buf, "error in .read() call");
        if (PyErr_Occurred()) {
            PyObject *t, *ex, *tb;
            PyErr_Fetch(&t, &ex, &tb);
            if (ex) {
                PyObject *str;
                str = PyObject_Str(ex);
                str = psyco_ensure_bytes(str);
                if (str) {
                    PyOS_snprintf(buf, sizeof(buf),
                        "error in .read() call: %s %s",
                        ((PyTypeObject *)t)->tp_name, Bytes_AsString(str));
                    Py_DECREF(str);
                }
            }
            /* Clear the Py exception: it will be re-raised from the libpq */
            Py_XDECREF(t);
            Py_XDECREF(ex);
            Py_XDECREF(tb);
            PyErr_Clear();
        }
        res = PQputCopyEnd(curs->conn->pgconn, buf);
    }

    CLEARPGRES(curs->pgres);

    Dprintf("_pq_copy_in_v3: copy ended; res = %d", res);

    /* if the result is -1 we should not even try to get a result from the
       because that will lock the current thread forever */
    if (res == -1) {
        pq_raise(curs->conn, curs, NULL);
        /* FIXME: pq_raise check the connection but for some reason even
           if the error message says "server closed the connection unexpectedly"
           the status returned by PQstatus is CONNECTION_OK! */
        curs->conn->closed = 2;
    }
    else {
        /* and finally we grab the operation result from the backend */
        for (;;) {
            Py_BEGIN_ALLOW_THREADS;
            curs_set_result(curs, PQgetResult(curs->conn->pgconn));
            Py_END_ALLOW_THREADS;

            if (NULL == curs->pgres)
                break;
            _read_rowcount(curs);
            if (PQresultStatus(curs->pgres) == PGRES_FATAL_ERROR)
                pq_raise(curs->conn, curs, NULL);
            CLEARPGRES(curs->pgres);
        }
    }

exit:
    Py_XDECREF(func);
    Py_XDECREF(size);
    return (error == 0 ? 1 : -1);
}

static int
_pq_copy_out_v3(cursorObject *curs)
{
    PyObject *tmp = NULL;
    PyObject *func = NULL;
    PyObject *obj = NULL;
    int ret = -1;
    int is_text;

    char *buffer;
    Py_ssize_t len;

    if (!curs->copyfile) {
        PyErr_SetString(ProgrammingError,
            "can't execute COPY TO: use the copy_to() method instead");
        goto exit;
    }

    if (!(func = PyObject_GetAttrString(curs->copyfile, "write"))) {
        Dprintf("_pq_copy_out_v3: can't get o.write");
        goto exit;
    }

    /* if the file is text we must pass it unicode. */
    if (-1 == (is_text = psyco_is_text_file(curs->copyfile))) {
        goto exit;
    }

    while (1) {
        Py_BEGIN_ALLOW_THREADS;
        len = PQgetCopyData(curs->conn->pgconn, &buffer, 0);
        Py_END_ALLOW_THREADS;

        if (len > 0 && buffer) {
            if (is_text) {
                obj = conn_decode(curs->conn, buffer, len);
            } else {
                obj = Bytes_FromStringAndSize(buffer, len);
            }

            PQfreemem(buffer);
            if (!obj) { goto exit; }
            tmp = PyObject_CallFunctionObjArgs(func, obj, NULL);
            Py_DECREF(obj);

            if (tmp == NULL) {
                goto exit;
            } else {
                Py_DECREF(tmp);
            }
        }
        /* we break on len == 0 but note that that should *not* happen,
           because we are not doing an async call (if it happens blame
           postgresql authors :/) */
        else if (len <= 0) break;
    }

    if (len == -2) {
        pq_raise(curs->conn, curs, NULL);
        goto exit;
    }

    /* and finally we grab the operation result from the backend */
    for (;;) {
        Py_BEGIN_ALLOW_THREADS;
        curs_set_result(curs, PQgetResult(curs->conn->pgconn));
        Py_END_ALLOW_THREADS;

        if (NULL == curs->pgres)
            break;
        _read_rowcount(curs);
        if (PQresultStatus(curs->pgres) == PGRES_FATAL_ERROR)
            pq_raise(curs->conn, curs, NULL);
        CLEARPGRES(curs->pgres);
    }
    ret = 1;

exit:
    Py_XDECREF(func);
    return ret;
}

/* Tries to read the next message from the replication stream, without
   blocking, in both sync and async connection modes.  If no message
   is ready in the CopyData buffer, tries to read from the server,
   again without blocking.  If that doesn't help, returns Py_None.
   The caller is then supposed to block on the socket(s) and call this
   function again.

   Any keepalive messages from the server are silently consumed and
   are never returned to the caller.
 */
int
pq_read_replication_message(replicationCursorObject *repl, replicationMessageObject **msg)
{
    cursorObject *curs = &repl->cur;
    connectionObject *conn = curs->conn;
    PGconn *pgconn = conn->pgconn;
    char *buffer = NULL;
    int len, data_size, consumed, hdr, reply;
    XLogRecPtr data_start, wal_end;
    int64_t send_time;
    PyObject *str = NULL, *result = NULL;
    int ret = -1;
    struct timeval curr_time, feedback_time;

    Dprintf("pq_read_replication_message");

    *msg = NULL;
    consumed = 0;

    /* Is it a time to send the next feedback message? */
    gettimeofday(&curr_time, NULL);
    timeradd(&repl->last_feedback, &repl->status_interval, &feedback_time);
    if (timercmp(&curr_time, &feedback_time, >=) && pq_send_replication_feedback(repl, 0) < 0) {
        goto exit;
    }

retry:
    len = PQgetCopyData(pgconn, &buffer, 1 /* async */);

    if (len == 0) {
        /* If we've tried reading some data, but there was none, bail out. */
        if (consumed) {
            ret = 0;
            goto exit;
        }
        /* We should only try reading more data when there is nothing
           available at the moment.  Otherwise, with a really highly loaded
           server we might be reading a number of messages for every single
           one we process, thus overgrowing the internal buffer until the
           client system runs out of memory. */
        if (!PQconsumeInput(pgconn)) {
            pq_raise(conn, curs, NULL);
            goto exit;
        }
        /* But PQconsumeInput() doesn't tell us if it has actually read
           anything into the internal buffer and there is no (supported) way
           to ask libpq about this directly.  The way we check is setting the
           flag and re-trying PQgetCopyData(): if that returns 0 again,
           there's no more data available in the buffer, so we return None. */
        consumed = 1;
        goto retry;
    }

    if (len == -2) {
        /* serious error */
        pq_raise(conn, curs, NULL);
        goto exit;
    }
    if (len == -1) {
        /* EOF */
        curs_set_result(curs, PQgetResult(pgconn));

        if (curs->pgres && PQresultStatus(curs->pgres) == PGRES_FATAL_ERROR) {
            pq_raise(conn, curs, NULL);
            goto exit;
        }

        CLEARPGRES(curs->pgres);
        ret = 0;
        goto exit;
    }

    /* It also makes sense to set this flag here to make us return early in
       case of retry due to keepalive message.  Any pending data on the socket
       will trigger read condition in select() in the calling code anyway. */
    consumed = 1;

    /* ok, we did really read something: update the io timestamp */
    gettimeofday(&repl->last_io, NULL);

    Dprintf("pq_read_replication_message: msg=%c, len=%d", buffer[0], len);
    if (buffer[0] == 'w') {
        /* XLogData: msgtype(1), dataStart(8), walEnd(8), sendTime(8) */
        hdr = 1 + 8 + 8 + 8;
        if (len < hdr + 1) {
            psyco_set_error(OperationalError, curs, "data message header too small");
            goto exit;
        }

        data_size  = len - hdr;
        data_start = fe_recvint64(buffer + 1);
        wal_end    = fe_recvint64(buffer + 1 + 8);
        send_time  = fe_recvint64(buffer + 1 + 8 + 8);

        Dprintf("pq_read_replication_message: data_start="XLOGFMTSTR", wal_end="XLOGFMTSTR,
                XLOGFMTARGS(data_start), XLOGFMTARGS(wal_end));

        Dprintf("pq_read_replication_message: >>%.*s<<", data_size, buffer + hdr);

        if (repl->decode) {
            str = conn_decode(conn, buffer + hdr, data_size);
        } else {
            str = Bytes_FromStringAndSize(buffer + hdr, data_size);
        }
        if (!str) { goto exit; }

        result = PyObject_CallFunctionObjArgs((PyObject *)&replicationMessageType,
                                              curs, str, NULL);
        Py_DECREF(str);
        if (!result) { goto exit; }

        *msg = (replicationMessageObject *)result;
        (*msg)->data_size  = data_size;
        (*msg)->data_start = data_start;
        (*msg)->wal_end    = wal_end;
        (*msg)->send_time  = send_time;

        repl->wal_end = wal_end;
        repl->last_msg_data_start = data_start;
    }
    else if (buffer[0] == 'k') {
        /* Primary keepalive message: msgtype(1), walEnd(8), sendTime(8), reply(1) */
        hdr = 1 + 8 + 8;
        if (len < hdr + 1) {
            psyco_set_error(OperationalError, curs, "keepalive message header too small");
            goto exit;
        }

        wal_end = fe_recvint64(buffer + 1);
        Dprintf("pq_read_replication_message: wal_end="XLOGFMTSTR, XLOGFMTARGS(wal_end));
        repl->wal_end = wal_end;

        /* We can safely forward flush_lsn to the wal_end from the server keepalive message
         * if we know that the client already processed (confirmed) the last XLogData message */
        if (repl->explicitly_flushed_lsn >= repl->last_msg_data_start
                && wal_end > repl->explicitly_flushed_lsn
                && wal_end > repl->flush_lsn) {
            repl->flush_lsn = wal_end;
        }

        reply = buffer[hdr];
        if (reply && pq_send_replication_feedback(repl, 0) < 0) {
            goto exit;
        }

        PQfreemem(buffer);
        buffer = NULL;
        goto retry;
    }
    else {
        psyco_set_error(OperationalError, curs, "unrecognized replication message type");
        goto exit;
    }

    ret = 0;

exit:
    if (buffer) {
        PQfreemem(buffer);
    }

    return ret;
}

int
pq_send_replication_feedback(replicationCursorObject *repl, int reply_requested)
{
    cursorObject *curs = &repl->cur;
    connectionObject *conn = curs->conn;
    PGconn *pgconn = conn->pgconn;
    char replybuf[1 + 8 + 8 + 8 + 8 + 1];
    int len = 0;

    Dprintf("pq_send_replication_feedback: write="XLOGFMTSTR", flush="XLOGFMTSTR", apply="XLOGFMTSTR,
            XLOGFMTARGS(repl->write_lsn),
            XLOGFMTARGS(repl->flush_lsn),
            XLOGFMTARGS(repl->apply_lsn));

    replybuf[len] = 'r'; len += 1;
    fe_sendint64(repl->write_lsn, &replybuf[len]); len += 8;
    fe_sendint64(repl->flush_lsn, &replybuf[len]); len += 8;
    fe_sendint64(repl->apply_lsn, &replybuf[len]); len += 8;
    fe_sendint64(feGetCurrentTimestamp(), &replybuf[len]); len += 8;
    replybuf[len] = reply_requested ? 1 : 0; len += 1;

    if (PQputCopyData(pgconn, replybuf, len) <= 0 || PQflush(pgconn) != 0) {
        pq_raise(conn, curs, NULL);
        return -1;
    }
    gettimeofday(&repl->last_feedback, NULL);
    repl->last_io = repl->last_feedback;

    return 0;
}

/* Calls pq_read_replication_message in an endless loop, until
   stop_replication is called or a fatal error occurs.  The messages
   are passed to the consumer object.

   When no message is available, blocks on the connection socket, but
   manages to send keepalive messages to the server as needed.
*/
int
pq_copy_both(replicationCursorObject *repl, PyObject *consume)
{
    cursorObject *curs = &repl->cur;
    connectionObject *conn = curs->conn;
    PGconn *pgconn = conn->pgconn;
    replicationMessageObject *msg = NULL;
    PyObject *tmp = NULL;
    int fd, sel, ret = -1;
    fd_set fds;
    struct timeval curr_time, feedback_time, timeout;

    if (!PyCallable_Check(consume)) {
        Dprintf("pq_copy_both: expected callable consume object");
        goto exit;
    }

    CLEARPGRES(curs->pgres);

    while (1) {
        if (pq_read_replication_message(repl, &msg) < 0) {
            goto exit;
        }
        else if (msg == NULL) {
            fd = PQsocket(pgconn);
            if (fd < 0) {
                pq_raise(conn, curs, NULL);
                goto exit;
            }

            FD_ZERO(&fds);
            FD_SET(fd, &fds);

            /* how long can we wait before we need to send a feedback? */
            gettimeofday(&curr_time, NULL);

            timeradd(&repl->last_feedback, &repl->status_interval, &feedback_time);
            timersub(&feedback_time, &curr_time, &timeout);

            if (timeout.tv_sec >= 0) {
                Py_BEGIN_ALLOW_THREADS;
                sel = select(fd + 1, &fds, NULL, NULL, &timeout);
                Py_END_ALLOW_THREADS;

                if (sel < 0) {
                    if (errno != EINTR) {
                        PyErr_SetFromErrno(PyExc_OSError);
                        goto exit;
                    }
                    if (PyErr_CheckSignals()) {
                        goto exit;
                    }
                }
            }
        }
        else {
            tmp = PyObject_CallFunctionObjArgs(consume, msg, NULL);
            Py_DECREF(msg);

            if (tmp == NULL) {
                Dprintf("pq_copy_both: consume returned NULL");
                goto exit;
            }
            Py_DECREF(tmp);
        }
    }

    ret = 1;

exit:
    return ret;
}

int
pq_fetch(cursorObject *curs, int no_result)
{
    int pgstatus, ex = -1;

    /* even if we fail, we remove any information about the previous query */
    curs_reset(curs);

    if (curs->pgres == NULL) return 0;

    pgstatus = PQresultStatus(curs->pgres);
    Dprintf("pq_fetch: pgstatus = %s", PQresStatus(pgstatus));

    /* backend status message */
    Py_CLEAR(curs->pgstatus);
    if (!(curs->pgstatus = conn_text_from_chars(
            curs->conn, PQcmdStatus(curs->pgres)))) {
        ex = -1;
        return ex;
    }

    switch(pgstatus) {

    case PGRES_COMMAND_OK:
        Dprintf("pq_fetch: command returned OK (no tuples)");
        _read_rowcount(curs);
        curs->lastoid = PQoidValue(curs->pgres);
        CLEARPGRES(curs->pgres);
        ex = 1;
        break;

    case PGRES_COPY_OUT:
        Dprintf("pq_fetch: data from a COPY TO (no tuples)");
        curs->rowcount = -1;
        ex = _pq_copy_out_v3(curs);
        /* error caught by out glorious notice handler */
        if (PyErr_Occurred()) ex = -1;
        CLEARPGRES(curs->pgres);
        break;

    case PGRES_COPY_IN:
        Dprintf("pq_fetch: data from a COPY FROM (no tuples)");
        curs->rowcount = -1;
        ex = _pq_copy_in_v3(curs);
        /* error caught by out glorious notice handler */
        if (PyErr_Occurred()) ex = -1;
        CLEARPGRES(curs->pgres);
        break;

    case PGRES_COPY_BOTH:
        Dprintf("pq_fetch: data from a streaming replication slot (no tuples)");
        curs->rowcount = -1;
        ex = 0;
        /* Nothing to do here: pq_copy_both will be called separately.

           Also don't clear the result status: it's checked in
           consume_stream. */
        /*CLEARPGRES(curs->pgres);*/
        break;

    case PGRES_TUPLES_OK:
        if (!no_result) {
            Dprintf("pq_fetch: got tuples");
            curs->rowcount = PQntuples(curs->pgres);
            if (0 == _pq_fetch_tuples(curs)) { ex = 0; }
            /* don't clear curs->pgres, because it contains the results! */
        }
        else {
            Dprintf("pq_fetch: got tuples, discarding them");
            /* TODO: is there any case in which PQntuples == PQcmdTuples? */
            _read_rowcount(curs);
            CLEARPGRES(curs->pgres);
            ex = 0;
        }
        break;

    case PGRES_EMPTY_QUERY:
        PyErr_SetString(ProgrammingError,
            "can't execute an empty query");
        CLEARPGRES(curs->pgres);
        ex = -1;
        break;

    case PGRES_BAD_RESPONSE:
    case PGRES_NONFATAL_ERROR:
    case PGRES_FATAL_ERROR:
        Dprintf("pq_fetch: uh-oh, something FAILED: status = %d pgconn = %p",
            pgstatus, curs->conn);
        pq_raise(curs->conn, curs, NULL);
        ex = -1;
        break;

    default:
        /* PGRES_SINGLE_TUPLE, future statuses */
        Dprintf("pq_fetch: got unsupported result: status = %d pgconn = %p",
            pgstatus, curs->conn);
        PyErr_Format(NotSupportedError,
            "got server response with unsupported status %s",
            PQresStatus(curs->pgres == NULL ?
                PQstatus(curs->conn->pgconn) : PQresultStatus(curs->pgres)));
        CLEARPGRES(curs->pgres);
        ex = -1;
        break;
    }

    return ex;
}
