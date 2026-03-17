/* connection_int.c - code used by the connection object
 *
 * Copyright (C) 2003-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020 The Psycopg Team
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
#include "psycopg/green.h"
#include "psycopg/notify.h"

#include <string.h>

/* String indexes match the ISOLATION_LEVEL_* consts */
const char *srv_isolevels[] = {
    NULL, /* autocommit */
    "READ COMMITTED",
    "REPEATABLE READ",
    "SERIALIZABLE",
    "READ UNCOMMITTED",
    "default"       /* only to set GUC, not for BEGIN */
};

/* Read only false, true */
const char *srv_readonly[] = {
    " READ WRITE",
    " READ ONLY",
    ""      /* default */
};

/* Deferrable false, true */
const char *srv_deferrable[] = {
    " NOT DEFERRABLE",
    " DEFERRABLE",
    ""      /* default */
};

/* On/Off/Default GUC states
 */
const char *srv_state_guc[] = {
    "off",
    "on",
    "default"
};


const int SRV_STATE_UNCHANGED = -1;


/* Return a new "string" from a char* from the database.
 *
 * On Py2 just get a string, on Py3 decode it in the connection codec.
 *
 * Use a fallback if the connection is NULL.
 */
PyObject *
conn_text_from_chars(connectionObject *self, const char *str)
{
    return psyco_text_from_chars_safe(str, -1, self ? self->pydecoder : NULL);
}


/* Encode an unicode object into a bytes object in the connection encoding.
 *
 * If no connection or encoding is available, default to utf8
 */
PyObject *
conn_encode(connectionObject *self, PyObject *u)
{
    PyObject *t = NULL;
    PyObject *rv = NULL;

    if (!(self && self->pyencoder)) {
        rv = PyUnicode_AsUTF8String(u);
        goto exit;
    }

    if (!(t = PyObject_CallFunctionObjArgs(self->pyencoder, u, NULL))) {
        goto exit;
    }

    if (!(rv = PyTuple_GetItem(t, 0))) { goto exit; }
    Py_INCREF(rv);

exit:
    Py_XDECREF(t);

    return rv;
}


/* decode a c string into a Python unicode in the connection encoding
 *
 * len can be < 0: in this case it will be calculated
 *
 * If no connection or encoding is available, default to utf8
 */
PyObject *
conn_decode(connectionObject *self, const char *str, Py_ssize_t len)
{
    if (len < 0) { len = strlen(str); }

    if (self) {
        if (self->cdecoder) {
            return self->cdecoder(str, len, NULL);
        }
        else if (self->pydecoder) {
            PyObject *b = NULL;
            PyObject *t = NULL;
            PyObject *rv = NULL;

            if (!(b = Bytes_FromStringAndSize(str, len))) { goto error; }
            if (!(t = PyObject_CallFunctionObjArgs(self->pydecoder, b, NULL))) {
                goto error;
            }
            if (!(rv = PyTuple_GetItem(t, 0))) { goto error; }
            Py_INCREF(rv);      /* PyTuple_GetItem gives a borrowes one */
error:
            Py_XDECREF(t);
            Py_XDECREF(b);
            return rv;
        }
        else {
            return PyUnicode_FromStringAndSize(str, len);
        }
    }
    else {
        return PyUnicode_FromStringAndSize(str, len);
    }
}

/* conn_notice_callback - process notices */

static void
conn_notice_callback(void *args, const char *message)
{
    struct connectionObject_notice *notice;
    connectionObject *self = (connectionObject *)args;

    Dprintf("conn_notice_callback: %s", message);

    /* NOTE: if we get here and the connection is unlocked then there is a
       problem but this should happen because the notice callback is only
       called from libpq and when we're inside libpq the connection is usually
       locked.
    */
    notice = (struct connectionObject_notice *)
        malloc(sizeof(struct connectionObject_notice));
    if (NULL == notice) {
        /* Discard the notice in case of failed allocation. */
        return;
    }
    notice->next = NULL;
    notice->message = strdup(message);
    if (NULL == notice->message) {
        free(notice);
        return;
    }

    if (NULL == self->last_notice) {
        self->notice_pending = self->last_notice = notice;
    }
    else {
        self->last_notice->next = notice;
        self->last_notice = notice;
    }
}

/* Expose the notices received as Python objects.
 *
 * The function should be called with the connection lock and the GIL.
 */
void
conn_notice_process(connectionObject *self)
{
    struct connectionObject_notice *notice;
    PyObject *msg = NULL;
    PyObject *tmp = NULL;
    static PyObject *append;

    if (NULL == self->notice_pending) {
        return;
    }

    if (!append) {
        if (!(append = Text_FromUTF8("append"))) {
            goto error;
        }
    }

    notice = self->notice_pending;
    while (notice != NULL) {
        Dprintf("conn_notice_process: %s", notice->message);

        if (!(msg = conn_text_from_chars(self, notice->message))) { goto error; }

        if (!(tmp = PyObject_CallMethodObjArgs(
                self->notice_list, append, msg, NULL))) {

            goto error;
        }

        Py_DECREF(tmp); tmp = NULL;
        Py_DECREF(msg); msg = NULL;

        notice = notice->next;
    }

    /* Remove the oldest item if the queue is getting too long. */
    if (PyList_Check(self->notice_list)) {
        Py_ssize_t nnotices;
        nnotices = PyList_GET_SIZE(self->notice_list);
        if (nnotices > CONN_NOTICES_LIMIT) {
            if (-1 == PySequence_DelSlice(self->notice_list,
                    0, nnotices - CONN_NOTICES_LIMIT)) {
                PyErr_Clear();
            }
        }
    }

    conn_notice_clean(self);
    return;

error:
    Py_XDECREF(tmp);
    Py_XDECREF(msg);
    conn_notice_clean(self);

    /* TODO: the caller doesn't expects errors from us */
    PyErr_Clear();
}

void
conn_notice_clean(connectionObject *self)
{
    struct connectionObject_notice *tmp, *notice;

    notice = self->notice_pending;

    while (notice != NULL) {
        tmp = notice;
        notice = notice->next;
        free(tmp->message);
        free(tmp);
    }

    self->last_notice = self->notice_pending = NULL;
}


/* conn_notifies_process - make received notification available
 *
 * The function should be called with the connection lock and holding the GIL.
 */

void
conn_notifies_process(connectionObject *self)
{
    PGnotify *pgn = NULL;
    PyObject *notify = NULL;
    PyObject *pid = NULL, *channel = NULL, *payload = NULL;
    PyObject *tmp = NULL;

    static PyObject *append;

    if (!append) {
        if (!(append = Text_FromUTF8("append"))) {
            goto error;
        }
    }

    while ((pgn = PQnotifies(self->pgconn)) != NULL) {

        Dprintf("conn_notifies_process: got NOTIFY from pid %d, msg = %s",
                (int) pgn->be_pid, pgn->relname);

        if (!(pid = PyInt_FromLong((long)pgn->be_pid))) { goto error; }
        if (!(channel = conn_text_from_chars(self, pgn->relname))) { goto error; }
        if (!(payload = conn_text_from_chars(self, pgn->extra))) { goto error; }

        if (!(notify = PyObject_CallFunctionObjArgs((PyObject *)&notifyType,
                pid, channel, payload, NULL))) {
            goto error;
        }

        Py_DECREF(pid); pid = NULL;
        Py_DECREF(channel); channel = NULL;
        Py_DECREF(payload); payload = NULL;

        if (!(tmp = PyObject_CallMethodObjArgs(
                self->notifies, append, notify, NULL))) {
            goto error;
        }
        Py_DECREF(tmp); tmp = NULL;

        Py_DECREF(notify); notify = NULL;
        PQfreemem(pgn); pgn = NULL;
    }
    return;  /* no error */

error:
    if (pgn) { PQfreemem(pgn); }
    Py_XDECREF(tmp);
    Py_XDECREF(notify);
    Py_XDECREF(pid);
    Py_XDECREF(channel);
    Py_XDECREF(payload);

    /* TODO: callers currently don't expect an error from us */
    PyErr_Clear();

}


/*
 * the conn_get_* family of functions makes it easier to obtain the connection
 * parameters from query results or by interrogating the connection itself
*/

int
conn_get_standard_conforming_strings(PGconn *pgconn)
{
    int equote;
    const char *scs;
    /*
     * The presence of the 'standard_conforming_strings' parameter
     * means that the server _accepts_ the E'' quote.
     *
     * If the parameter is off, the PQescapeByteaConn returns
     * backslash escaped strings (e.g. '\001' -> "\\001"),
     * so the E'' quotes are required to avoid warnings
     * if 'escape_string_warning' is set.
     *
     * If the parameter is on, the PQescapeByteaConn returns
     * not escaped strings (e.g. '\001' -> "\001"), relying on the
     * fact that the '\' will pass untouched the string parser.
     * In this case the E'' quotes are NOT to be used.
     */
    scs = PQparameterStatus(pgconn, "standard_conforming_strings");
    Dprintf("conn_connect: server standard_conforming_strings parameter: %s",
        scs ? scs : "unavailable");

    equote = (scs && (0 == strcmp("off", scs)));
    Dprintf("conn_connect: server requires E'' quotes: %s",
            equote ? "YES" : "NO");

    return equote;
}


/* Remove irrelevant chars from encoding name and turn it uppercase.
 *
 * Return a buffer allocated on Python heap into 'clean' and return 0 on
 * success, otherwise return -1 and set an exception.
 */
RAISES_NEG static int
clear_encoding_name(const char *enc, char **clean)
{
    const char *i = enc;
    char *j, *buf;
    int rv = -1;

    /* convert to upper case and remove '-' and '_' from string */
    if (!(j = buf = PyMem_Malloc(strlen(enc) + 1))) {
        PyErr_NoMemory();
        goto exit;
    }

    while (*i) {
        if (!isalnum(*i)) {
            ++i;
        }
        else {
            *j++ = toupper(*i++);
        }
    }
    *j = '\0';

    Dprintf("clear_encoding_name: %s -> %s", enc, buf);
    *clean = buf;
    rv = 0;

exit:
    return rv;
}

/* set fast access functions according to the currently selected encoding
 */
static void
conn_set_fast_codec(connectionObject *self)
{
    Dprintf("conn_set_fast_codec: encoding=%s", self->encoding);

    if (0 == strcmp(self->encoding, "UTF8")) {
        Dprintf("conn_set_fast_codec: PyUnicode_DecodeUTF8");
        self->cdecoder = PyUnicode_DecodeUTF8;
        return;
    }

    if (0 == strcmp(self->encoding, "LATIN1")) {
        Dprintf("conn_set_fast_codec: PyUnicode_DecodeLatin1");
        self->cdecoder = PyUnicode_DecodeLatin1;
        return;
    }

    Dprintf("conn_set_fast_codec: no fast codec");
    self->cdecoder = NULL;
}


/* Return the Python encoding from a PostgreSQL encoding.
 *
 * Optionally return the clean version of the postgres encoding too
 */
PyObject *
conn_pgenc_to_pyenc(const char *encoding, char **clean_encoding)
{
    char *pgenc = NULL;
    PyObject *rv = NULL;

    if (0 > clear_encoding_name(encoding, &pgenc)) { goto exit; }
    if (!(rv = PyDict_GetItemString(psycoEncodings, pgenc))) {
        PyErr_Format(OperationalError,
            "no Python encoding for PostgreSQL encoding '%s'", pgenc);
        goto exit;
    }
    Py_INCREF(rv);

    if (clean_encoding) {
        *clean_encoding = pgenc;
    }
    else {
        PyMem_Free(pgenc);
    }

exit:
    return rv;
}

/* Convert a Postgres encoding into Python encoding and decoding functions.
 *
 * Set clean_encoding to a clean version of the Postgres encoding name
 * and pyenc and pydec to python codec functions.
 *
 * Return 0 on success, else -1 and set an exception.
 */
RAISES_NEG static int
conn_get_python_codec(const char *encoding,
    char **clean_encoding, PyObject **pyenc, PyObject **pydec)
{
    int rv = -1;
    char *pgenc = NULL;
    PyObject *encname = NULL;
    PyObject *enc_tmp = NULL, *dec_tmp = NULL;

    /* get the Python name of the encoding as a C string */
    if (!(encname = conn_pgenc_to_pyenc(encoding, &pgenc))) { goto exit; }
    if (!(encname = psyco_ensure_bytes(encname))) { goto exit; }

    /* Look up the codec functions */
    if (!(enc_tmp = PyCodec_Encoder(Bytes_AS_STRING(encname)))) { goto exit; }
    if (!(dec_tmp = PyCodec_Decoder(Bytes_AS_STRING(encname)))) { goto exit; }

    /* success */
    *pyenc = enc_tmp; enc_tmp = NULL;
    *pydec = dec_tmp; dec_tmp = NULL;
    *clean_encoding = pgenc; pgenc = NULL;
    rv = 0;

exit:
    Py_XDECREF(enc_tmp);
    Py_XDECREF(dec_tmp);
    Py_XDECREF(encname);
    PyMem_Free(pgenc);

    return rv;
}


/* Store the encoding in the pgconn->encoding field and set the other related
 * encoding fields in the connection structure.
 *
 * Return 0 on success, else -1 and set an exception.
 */
RAISES_NEG static int
conn_store_encoding(connectionObject *self, const char *encoding)
{
    int rv = -1;
    char *pgenc = NULL;
    PyObject *enc_tmp = NULL, *dec_tmp = NULL;

    if (0 > conn_get_python_codec(encoding, &pgenc, &enc_tmp, &dec_tmp)) {
        goto exit;
    }

    /* Good, success: store the encoding/codec in the connection. */
    {
        char *tmp = self->encoding;
        self->encoding = pgenc;
        PyMem_Free(tmp);
        pgenc = NULL;
    }

    Py_CLEAR(self->pyencoder);
    self->pyencoder = enc_tmp;
    enc_tmp = NULL;

    Py_CLEAR(self->pydecoder);
    self->pydecoder = dec_tmp;
    dec_tmp = NULL;

    conn_set_fast_codec(self);

    rv = 0;

exit:
    Py_XDECREF(enc_tmp);
    Py_XDECREF(dec_tmp);
    PyMem_Free(pgenc);
    return rv;
}


/* Read the client encoding from the backend and store it in the connection.
 *
 * Return 0 on success, else -1.
 */
RAISES_NEG static int
conn_read_encoding(connectionObject *self, PGconn *pgconn)
{
    const char *encoding;
    int rv = -1;

    encoding = PQparameterStatus(pgconn, "client_encoding");
    Dprintf("conn_connect: client encoding: %s", encoding ? encoding : "(none)");
    if (!encoding) {
        PyErr_SetString(OperationalError,
            "server didn't return client encoding");
        goto exit;
    }

    if (0 > conn_store_encoding(self, encoding)) {
        goto exit;
    }

    rv = 0;

exit:
    return rv;
}


int
conn_get_protocol_version(PGconn *pgconn)
{
    int ret;
    ret = PQprotocolVersion(pgconn);
    Dprintf("conn_connect: using protocol %d", ret);
    return ret;
}

int
conn_get_server_version(PGconn *pgconn)
{
    return (int)PQserverVersion(pgconn);
}

/* set up the cancel key of the connection.
 * On success return 0, else set an exception and return -1
 */
RAISES_NEG static int
conn_setup_cancel(connectionObject *self, PGconn *pgconn)
{
    if (self->cancel) {
        PQfreeCancel(self->cancel);
    }

    if (!(self->cancel = PQgetCancel(self->pgconn))) {
        PyErr_SetString(OperationalError, "can't get cancellation key");
        return -1;
    }

    return 0;
}

/* Return 1 if the "replication" keyword is set in the DSN, 0 otherwise */
static int
dsn_has_replication(char *pgdsn)
{
    int ret = 0;
    PQconninfoOption *connopts, *ptr;

    connopts = PQconninfoParse(pgdsn, NULL);

    for(ptr = connopts; ptr->keyword != NULL; ptr++) {
      if(strcmp(ptr->keyword, "replication") == 0 && ptr->val != NULL)
        ret = 1;
    }

    PQconninfoFree(connopts);

    return ret;
}


/* Return 1 if the server datestyle allows us to work without problems,
   0 if it needs to be set to something better, e.g. ISO. */
static int
conn_is_datestyle_ok(PGconn *pgconn)
{
    const char *ds;

    ds = PQparameterStatus(pgconn, "DateStyle");
    Dprintf("conn_connect: DateStyle %s", ds);

    /* pgbouncer does not pass on DateStyle */
    if (ds == NULL)
      return 0;

    /* Return true if ds starts with "ISO"
     * e.g. "ISO, DMY" is fine, "German" not. */
    return (ds[0] == 'I' && ds[1] == 'S' && ds[2] == 'O');
}


/* conn_setup - setup and read basic information about the connection */

RAISES_NEG int
conn_setup(connectionObject *self)
{
    int rv = -1;

    self->equote = conn_get_standard_conforming_strings(self->pgconn);
    self->server_version = conn_get_server_version(self->pgconn);
    self->protocol = conn_get_protocol_version(self->pgconn);
    if (3 != self->protocol) {
        PyErr_SetString(InterfaceError, "only protocol 3 supported");
        goto exit;
    }

    if (0 > conn_read_encoding(self, self->pgconn)) {
        goto exit;
    }

    if (0 > conn_setup_cancel(self, self->pgconn)) {
        goto exit;
    }

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&self->lock);
    Py_BLOCK_THREADS;

    if (!dsn_has_replication(self->dsn) && !conn_is_datestyle_ok(self->pgconn)) {
        int res;
        Py_UNBLOCK_THREADS;
        res = pq_set_guc_locked(self, "datestyle", "ISO", &_save);
        Py_BLOCK_THREADS;
        if (res < 0) {
            pq_complete_error(self);
            goto unlock;
        }
    }

    /* for reset */
    self->autocommit = 0;
    self->isolevel = ISOLATION_LEVEL_DEFAULT;
    self->readonly = STATE_DEFAULT;
    self->deferrable = STATE_DEFAULT;

    /* success */
    rv = 0;

unlock:
    Py_UNBLOCK_THREADS;
    pthread_mutex_unlock(&self->lock);
    Py_END_ALLOW_THREADS;

exit:
    return rv;
}

/* conn_connect - execute a connection to the database */

static int
_conn_sync_connect(connectionObject *self, const char *dsn)
{
    int green;

    /* store this value to prevent inconsistencies due to a change
     * in the middle of the function. */
    green = psyco_green();
    if (!green) {
        Py_BEGIN_ALLOW_THREADS;
        self->pgconn = PQconnectdb(dsn);
        Py_END_ALLOW_THREADS;
        Dprintf("conn_connect: new PG connection at %p", self->pgconn);
    }
    else {
        Py_BEGIN_ALLOW_THREADS;
        self->pgconn = PQconnectStart(dsn);
        Py_END_ALLOW_THREADS;
        Dprintf("conn_connect: new green PG connection at %p", self->pgconn);
    }

    if (!self->pgconn)
    {
        Dprintf("conn_connect: PQconnectdb(%s) FAILED", dsn);
        PyErr_SetString(OperationalError, "PQconnectdb() failed");
        return -1;
    }
    else if (PQstatus(self->pgconn) == CONNECTION_BAD)
    {
        Dprintf("conn_connect: PQconnectdb(%s) returned BAD", dsn);
        PyErr_SetString(OperationalError, PQerrorMessage(self->pgconn));
        return -1;
    }

    PQsetNoticeProcessor(self->pgconn, conn_notice_callback, (void*)self);

    /* if the connection is green, wait to finish connection */
    if (green) {
        if (0 > pq_set_non_blocking(self, 1)) {
            return -1;
        }
        if (0 != psyco_wait(self)) {
            return -1;
        }
    }

    /* From here the connection is considered ready: with the new status,
     * poll() will use PQisBusy instead of PQconnectPoll.
     */
    self->status = CONN_STATUS_READY;

    if (conn_setup(self) == -1) {
        return -1;
    }

    return 0;
}

static int
_conn_async_connect(connectionObject *self, const char *dsn)
{
    PGconn *pgconn;

    self->pgconn = pgconn = PQconnectStart(dsn);

    Dprintf("conn_connect: new postgresql connection at %p", pgconn);

    if (pgconn == NULL)
    {
        Dprintf("conn_connect: PQconnectStart(%s) FAILED", dsn);
        PyErr_SetString(OperationalError, "PQconnectStart() failed");
        return -1;
    }
    else if (PQstatus(pgconn) == CONNECTION_BAD)
    {
        Dprintf("conn_connect: PQconnectdb(%s) returned BAD", dsn);
        PyErr_SetString(OperationalError, PQerrorMessage(pgconn));
        return -1;
    }

    PQsetNoticeProcessor(pgconn, conn_notice_callback, (void*)self);

    /* Set the connection to nonblocking now. */
    if (pq_set_non_blocking(self, 1) != 0) {
        return -1;
    }

    /* The connection will be completed banging on poll():
     * First with _conn_poll_connecting() that will finish connection,
     * then with _conn_poll_setup_async() that will do the same job
     * of setup_async(). */

    return 0;
}

int
conn_connect(connectionObject *self, const char *dsn, long int async)
{
    int rv;

    if (async == 1) {
      Dprintf("con_connect: connecting in ASYNC mode");
      rv = _conn_async_connect(self, dsn);
    }
    else {
      Dprintf("con_connect: connecting in SYNC mode");
      rv = _conn_sync_connect(self, dsn);
    }

    if (rv != 0) {
        /* connection failed, so let's close ourselves */
        self->closed = 2;
    }

    return rv;
}


/* poll during a connection attempt until the connection has established. */

static int
_conn_poll_connecting(connectionObject *self)
{
    int res = PSYCO_POLL_ERROR;
    const char *msg;

    Dprintf("conn_poll: poll connecting");
    switch (PQconnectPoll(self->pgconn)) {
    case PGRES_POLLING_OK:
        res = PSYCO_POLL_OK;
        break;
    case PGRES_POLLING_READING:
        res = PSYCO_POLL_READ;
        break;
    case PGRES_POLLING_WRITING:
        res = PSYCO_POLL_WRITE;
        break;
    case PGRES_POLLING_FAILED:
    case PGRES_POLLING_ACTIVE:
        msg = PQerrorMessage(self->pgconn);
        if (!(msg && *msg)) {
            msg = "asynchronous connection failed";
        }
        PyErr_SetString(OperationalError, msg);
        res = PSYCO_POLL_ERROR;
        break;
    }

    return res;
}


/* Advance to the next state after an attempt of flushing output */

static int
_conn_poll_advance_write(connectionObject *self)
{
    int res;
    int flush;

    Dprintf("conn_poll: poll writing");

    flush = PQflush(self->pgconn);
    Dprintf("conn_poll: PQflush() = %i", flush);

    switch (flush) {
    case  0:  /* success */
        /* we've finished pushing the query to the server. Let's start
          reading the results. */
        Dprintf("conn_poll: async_status -> ASYNC_READ");
        self->async_status = ASYNC_READ;
        res = PSYCO_POLL_READ;
        break;
    case  1:  /* would block */
        res = PSYCO_POLL_WRITE;
        break;
    case -1:  /* error */
        PyErr_SetString(OperationalError, PQerrorMessage(self->pgconn));
        res = PSYCO_POLL_ERROR;
        break;
    default:
        Dprintf("conn_poll: unexpected result from flush: %d", flush);
        res = PSYCO_POLL_ERROR;
        break;
    }
    return res;
}


/* Advance to the next state after reading results */

static int
_conn_poll_advance_read(connectionObject *self)
{
    int res;
    int busy;

    Dprintf("conn_poll: poll reading");

    busy = pq_get_result_async(self);

    switch (busy) {
    case 0: /* result is ready */
        Dprintf("conn_poll: async_status -> ASYNC_DONE");
        self->async_status = ASYNC_DONE;
        res = PSYCO_POLL_OK;
        break;
    case 1: /* result not ready: fd would block */
        res = PSYCO_POLL_READ;
        break;
    case -1: /* ouch, error */
        res = PSYCO_POLL_ERROR;
        break;
    default:
        Dprintf("conn_poll: unexpected result from pq_get_result_async: %d",
            busy);
        res = PSYCO_POLL_ERROR;
        break;
    }
    return res;
}


/* Poll the connection for the send query/retrieve result phase

  Advance the async_status (usually going WRITE -> READ -> DONE) but don't
  mess with the connection status. */

static int
_conn_poll_query(connectionObject *self)
{
    int res = PSYCO_POLL_ERROR;

    switch (self->async_status) {
    case ASYNC_WRITE:
        Dprintf("conn_poll: async_status = ASYNC_WRITE");
        res = _conn_poll_advance_write(self);
        break;

    case ASYNC_READ:
        Dprintf("conn_poll: async_status = ASYNC_READ");
        res = _conn_poll_advance_read(self);
        break;

    case ASYNC_DONE:
        Dprintf("conn_poll: async_status = ASYNC_DONE");
        /* We haven't asked anything: just check for notifications. */
        res = _conn_poll_advance_read(self);
        break;

    default:
        Dprintf("conn_poll: in unexpected async status: %d",
                self->async_status);
        res = PSYCO_POLL_ERROR;
        break;
    }

    return res;
}

/* Advance to the next state during an async connection setup
 *
 * If the connection is green, this is performed by the regular
 * sync code so the queries are sent by conn_setup() while in
 * CONN_STATUS_READY state.
 */
static int
_conn_poll_setup_async(connectionObject *self)
{
    int res = PSYCO_POLL_ERROR;

    switch (self->status) {
    case CONN_STATUS_CONNECTING:
        self->equote = conn_get_standard_conforming_strings(self->pgconn);
        self->protocol = conn_get_protocol_version(self->pgconn);
        self->server_version = conn_get_server_version(self->pgconn);
        if (3 != self->protocol) {
            PyErr_SetString(InterfaceError, "only protocol 3 supported");
            break;
        }
        if (0 > conn_read_encoding(self, self->pgconn)) {
            break;
        }
        if (0 > conn_setup_cancel(self, self->pgconn)) {
            return -1;
        }

        /* asynchronous connections always use isolation level 0, the user is
         * expected to manage the transactions himself, by sending
         * (asynchronously) BEGIN and COMMIT statements.
         */
        self->autocommit = 1;

        /* If the datestyle is ISO or anything else good,
         * we can skip the CONN_STATUS_DATESTYLE step.
         * Note that we cannot change the datestyle on a replication
         * connection.
         */
        if (!dsn_has_replication(self->dsn) && !conn_is_datestyle_ok(self->pgconn)) {
            Dprintf("conn_poll: status -> CONN_STATUS_DATESTYLE");
            self->status = CONN_STATUS_DATESTYLE;
            if (0 == pq_send_query(self, psyco_datestyle)) {
                PyErr_SetString(OperationalError, PQerrorMessage(self->pgconn));
                break;
            }
            Dprintf("conn_poll: async_status -> ASYNC_WRITE");
            self->async_status = ASYNC_WRITE;
            res = PSYCO_POLL_WRITE;
        }
        else {
            Dprintf("conn_poll: status -> CONN_STATUS_READY");
            self->status = CONN_STATUS_READY;
            res = PSYCO_POLL_OK;
        }
        break;

    case CONN_STATUS_DATESTYLE:
        res = _conn_poll_query(self);
        if (res == PSYCO_POLL_OK) {
            res = PSYCO_POLL_ERROR;
            if (self->pgres == NULL
                    || PQresultStatus(self->pgres) != PGRES_COMMAND_OK ) {
                PyErr_SetString(OperationalError, "can't set datestyle to ISO");
                break;
            }
            CLEARPGRES(self->pgres);

            Dprintf("conn_poll: status -> CONN_STATUS_READY");
            self->status = CONN_STATUS_READY;
            res = PSYCO_POLL_OK;
        }
        break;
    }
    return res;
}


static cursorObject *
_conn_get_async_cursor(connectionObject *self) {
    PyObject *py_curs;

    if (!(py_curs = PyWeakref_GetObject(self->async_cursor))) {
        PyErr_SetString(PyExc_SystemError,
            "got null dereferencing cursor weakref");
        goto error;
    }
    if (Py_None == py_curs) {
        PyErr_SetString(InterfaceError,
            "the asynchronous cursor has disappeared");
        goto error;
    }

    Py_INCREF(py_curs);
    return (cursorObject *)py_curs;

error:
    pq_clear_async(self);
    return NULL;
}

/* conn_poll - Main polling switch
 *
 * The function is called in all the states and connection types and invokes
 * the right "next step".
 */

int
conn_poll(connectionObject *self)
{
    int res = PSYCO_POLL_ERROR;
    Dprintf("conn_poll: status = %d", self->status);

    switch (self->status) {
    case CONN_STATUS_SETUP:
        Dprintf("conn_poll: status -> CONN_STATUS_SETUP");
        self->status = CONN_STATUS_CONNECTING;
        res = PSYCO_POLL_WRITE;
        break;

    case CONN_STATUS_CONNECTING:
        Dprintf("conn_poll: status -> CONN_STATUS_CONNECTING");
        res = _conn_poll_connecting(self);
        if (res == PSYCO_POLL_OK && self->async) {
            res = _conn_poll_setup_async(self);
        }
        break;

    case CONN_STATUS_DATESTYLE:
        Dprintf("conn_poll: status -> CONN_STATUS_DATESTYLE");
        res = _conn_poll_setup_async(self);
        break;

    case CONN_STATUS_READY:
    case CONN_STATUS_BEGIN:
    case CONN_STATUS_PREPARED:
        Dprintf("conn_poll: status -> CONN_STATUS_*");
        res = _conn_poll_query(self);

        if (res == PSYCO_POLL_OK && self->async && self->async_cursor) {
            cursorObject *curs;

            /* An async query has just finished: parse the tuple in the
             * target cursor. */
            if (!(curs = _conn_get_async_cursor(self))) {
                res = PSYCO_POLL_ERROR;
                break;
            }

            curs_set_result(curs, self->pgres);
            self->pgres = NULL;

            /* fetch the tuples (if there are any) and build the result. We
             * don't care if pq_fetch return 0 or 1, but if there was an error,
             * we want to signal it to the caller. */
            if (pq_fetch(curs, 0) == -1) {
               res = PSYCO_POLL_ERROR;
            }

            /* We have finished with our async_cursor */
            Py_DECREF(curs);
            Py_CLEAR(self->async_cursor);
        }
        break;

    default:
        Dprintf("conn_poll: in unexpected state");
        res = PSYCO_POLL_ERROR;
    }

    Dprintf("conn_poll: returning %d", res);
    return res;
}

/* conn_close - do anything needed to shut down the connection */

void
conn_close(connectionObject *self)
{
    /* a connection with closed == 2 still requires cleanup */
    if (self->closed == 1) {
        return;
    }

    /* sets this connection as closed even for other threads; */
    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&self->lock);

    conn_close_locked(self);

    pthread_mutex_unlock(&self->lock);
    Py_END_ALLOW_THREADS;
}


/* Return a copy of the 'dsn' string with the password scrubbed.
 *
 * The string returned is allocated on the Python heap.
 *
 * In case of error return NULL and raise an exception.
 */
char *
conn_obscure_password(const char *dsn)
{
    PQconninfoOption *options = NULL;
    PyObject *d = NULL, *v = NULL, *pydsn = NULL;
    char *rv = NULL;

    if (!dsn) {
        PyErr_SetString(InternalError, "unexpected null string");
        goto exit;
    }

    if (!(options = PQconninfoParse(dsn, NULL))) {
        /* unlikely: the dsn was already tested valid */
        PyErr_SetString(InternalError, "the connection string is not valid");
        goto exit;
    }

    if (!(d = psyco_dict_from_conninfo_options(
            options, /* include_password = */ 1))) {
        goto exit;
    }
    if (NULL == PyDict_GetItemString(d, "password")) {
        /* the dsn doesn't have a password */
        psyco_strdup(&rv, dsn, -1);
        goto exit;
    }

    /* scrub the password and put back the connection string together */
    if (!(v = Text_FromUTF8("xxx"))) { goto exit; }
    if (0 > PyDict_SetItemString(d, "password", v)) { goto exit; }
    if (!(pydsn = psyco_make_dsn(Py_None, d))) { goto exit; }
    if (!(pydsn = psyco_ensure_bytes(pydsn))) { goto exit; }

    /* Return the connection string with the password replaced */
    psyco_strdup(&rv, Bytes_AS_STRING(pydsn), -1);

exit:
    PQconninfoFree(options);
    Py_XDECREF(v);
    Py_XDECREF(d);
    Py_XDECREF(pydsn);

    return rv;
}


/* conn_close_locked - shut down the connection with the lock already taken */

void conn_close_locked(connectionObject *self)
{
    if (self->closed == 1) {
        return;
    }

    /* We used to call pq_abort_locked here, but the idea of issuing a
     * rollback on close/GC has been considered inappropriate.
     *
     * Dropping the connection on the server has the same effect as the
     * transaction is automatically rolled back. Some middleware, such as
     * PgBouncer, have problem with connections closed in the middle of the
     * transaction though: to avoid these problems the transaction should be
     * closed only in status CONN_STATUS_READY.
     */
    self->closed = 1;

    /* we need to check the value of pgconn, because we get called even when
     * the connection fails! */
    if (self->pgconn) {
        PQfinish(self->pgconn);
        self->pgconn = NULL;
        Dprintf("conn_close: PQfinish called");
    }
}

/* conn_commit - commit on a connection */

RAISES_NEG int
conn_commit(connectionObject *self)
{
    int res;

    res = pq_commit(self);
    return res;
}

/* conn_rollback - rollback a connection */

RAISES_NEG int
conn_rollback(connectionObject *self)
{
    int res;

    res = pq_abort(self);
    return res;
}


/* Change the state of the session */
RAISES_NEG int
conn_set_session(connectionObject *self, int autocommit,
        int isolevel, int readonly, int deferrable)
{
    int rv = -1;
    int want_autocommit = autocommit == SRV_STATE_UNCHANGED ?
        self->autocommit : autocommit;

    if (deferrable != SRV_STATE_UNCHANGED && self->server_version < 90100) {
        PyErr_SetString(ProgrammingError,
            "the 'deferrable' setting is only available"
            " from PostgreSQL 9.1");
        goto exit;
    }

    /* Promote an isolation level to one of the levels supported by the server */
    if (self->server_version < 80000) {
        if (isolevel == ISOLATION_LEVEL_READ_UNCOMMITTED) {
            isolevel = ISOLATION_LEVEL_READ_COMMITTED;
        }
        else if (isolevel == ISOLATION_LEVEL_REPEATABLE_READ) {
            isolevel = ISOLATION_LEVEL_SERIALIZABLE;
        }
    }

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&self->lock);

    if (want_autocommit) {
        /* we are or are going in autocommit state, so no BEGIN will be issued:
         * configure the session with the characteristics requested */
        if (isolevel != SRV_STATE_UNCHANGED) {
            if (0 > pq_set_guc_locked(self,
                    "default_transaction_isolation", srv_isolevels[isolevel],
                    &_save)) {
                goto endlock;
            }
        }
        if (readonly != SRV_STATE_UNCHANGED) {
            if (0 > pq_set_guc_locked(self,
                    "default_transaction_read_only", srv_state_guc[readonly],
                    &_save)) {
                goto endlock;
            }
        }
        if (deferrable != SRV_STATE_UNCHANGED) {
            if (0 > pq_set_guc_locked(self,
                    "default_transaction_deferrable", srv_state_guc[deferrable],
                    &_save)) {
                goto endlock;
            }
        }
    }
    else if (self->autocommit) {
        /* we are moving from autocommit to not autocommit, so revert the
         * characteristics to defaults to let BEGIN do its work */
        if (self->isolevel != ISOLATION_LEVEL_DEFAULT) {
            if (0 > pq_set_guc_locked(self,
                    "default_transaction_isolation", "default",
                    &_save)) {
                goto endlock;
            }
        }
        if (self->readonly != STATE_DEFAULT) {
            if (0 > pq_set_guc_locked(self,
                    "default_transaction_read_only", "default",
                    &_save)) {
                goto endlock;
            }
        }
        if (self->server_version >= 90100 && self->deferrable != STATE_DEFAULT) {
            if (0 > pq_set_guc_locked(self,
                    "default_transaction_deferrable", "default",
                    &_save)) {
                goto endlock;
            }
        }
    }

    if (autocommit != SRV_STATE_UNCHANGED) {
        self->autocommit = autocommit;
    }
    if (isolevel != SRV_STATE_UNCHANGED) {
        self->isolevel = isolevel;
    }
    if (readonly != SRV_STATE_UNCHANGED) {
        self->readonly = readonly;
    }
    if (deferrable != SRV_STATE_UNCHANGED) {
        self->deferrable = deferrable;
    }
    rv = 0;

endlock:
    pthread_mutex_unlock(&self->lock);
    Py_END_ALLOW_THREADS;

    if (rv < 0) {
        pq_complete_error(self);
        goto exit;
    }

    Dprintf(
        "conn_set_session: autocommit %d, isolevel %d, readonly %d, deferrable %d",
        autocommit, isolevel, readonly, deferrable);


exit:
    return rv;
}


/* conn_set_client_encoding - switch client encoding on connection */

RAISES_NEG int
conn_set_client_encoding(connectionObject *self, const char *pgenc)
{
    int res = -1;
    char *clean_enc = NULL;

    /* We must know what python encoding this encoding is. */
    if (0 > clear_encoding_name(pgenc, &clean_enc)) { goto exit; }

    /* If the current encoding is equal to the requested one we don't
       issue any query to the backend */
    if (strcmp(self->encoding, clean_enc) == 0) {
        res = 0;
        goto exit;
    }

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&self->lock);

    /* abort the current transaction, to set the encoding ouside of
       transactions */
    if ((res = pq_abort_locked(self, &_save))) {
        goto endlock;
    }

    if ((res = pq_set_guc_locked(self, "client_encoding", clean_enc, &_save))) {
        goto endlock;
    }

endlock:
    pthread_mutex_unlock(&self->lock);
    Py_END_ALLOW_THREADS;

    if (res < 0) {
        pq_complete_error(self);
        goto exit;
    }

    res = conn_store_encoding(self, pgenc);

    Dprintf("conn_set_client_encoding: encoding set to %s", self->encoding);

exit:
    PyMem_Free(clean_enc);

    return res;
}


/* conn_tpc_begin -- begin a two-phase commit.
 *
 * The state of a connection in the middle of a TPC is exactly the same
 * of a normal transaction, in CONN_STATUS_BEGIN, but with the tpc_xid
 * member set to the xid used. This allows to reuse all the code paths used
 * in regular transactions, as PostgreSQL won't even know we are in a TPC
 * until PREPARE. */

RAISES_NEG int
conn_tpc_begin(connectionObject *self, xidObject *xid)
{
    Dprintf("conn_tpc_begin: starting transaction");

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&self->lock);

    if (pq_begin_locked(self, &_save) < 0) {
        pthread_mutex_unlock(&(self->lock));
        Py_BLOCK_THREADS;
        pq_complete_error(self);
        return -1;
    }

    pthread_mutex_unlock(&self->lock);
    Py_END_ALLOW_THREADS;

    /* The transaction started ok, let's store this xid. */
    Py_INCREF(xid);
    self->tpc_xid = xid;

    return 0;
}


/* conn_tpc_command -- run one of the TPC-related PostgreSQL commands.
 *
 * The function doesn't change the connection state as it can be used
 * for many commands and for recovered transactions. */

RAISES_NEG int
conn_tpc_command(connectionObject *self, const char *cmd, xidObject *xid)
{
    PyObject *tid = NULL;
    const char *ctid;
    int rv = -1;

    Dprintf("conn_tpc_command: %s", cmd);

    /* convert the xid into PostgreSQL transaction id while keeping the GIL */
    if (!(tid = psyco_ensure_bytes(xid_get_tid(xid)))) { goto exit; }
    if (!(ctid = Bytes_AsString(tid))) { goto exit; }

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&self->lock);

    if (0 > (rv = pq_tpc_command_locked(self, cmd, ctid, &_save))) {
        pthread_mutex_unlock(&self->lock);
        Py_BLOCK_THREADS;
        pq_complete_error(self);
        goto exit;
    }

    pthread_mutex_unlock(&self->lock);
    Py_END_ALLOW_THREADS;

exit:
    Py_XDECREF(tid);
    return rv;
}

/* conn_tpc_recover -- return a list of pending TPC Xid */

PyObject *
conn_tpc_recover(connectionObject *self)
{
    int status;
    PyObject *xids = NULL;
    PyObject *rv = NULL;
    PyObject *tmp;

    /* store the status to restore it. */
    status = self->status;

    if (!(xids = xid_recover((PyObject *)self))) { goto exit; }

    if (status == CONN_STATUS_READY && self->status == CONN_STATUS_BEGIN) {
        /* recover began a transaction: let's abort it. */
        if (!(tmp = PyObject_CallMethod((PyObject *)self, "rollback", NULL))) {
            goto exit;
        }
        Py_DECREF(tmp);
    }

    /* all fine */
    rv = xids;
    xids = NULL;

exit:
    Py_XDECREF(xids);

    return rv;

}


void
conn_set_result(connectionObject *self, PGresult *pgres)
{
    PQclear(self->pgres);
    self->pgres = pgres;
}


void
conn_set_error(connectionObject *self, const char *msg)
{
    if (self->error) {
        free(self->error);
        self->error = NULL;
    }
    if (msg && *msg) {
        self->error = strdup(msg);
    }
}
