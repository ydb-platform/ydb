/* connection.h - definition for the psycopg connection type
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

#ifndef PSYCOPG_CONNECTION_H
#define PSYCOPG_CONNECTION_H 1

#include "psycopg/xid.h"

#ifdef __cplusplus
extern "C" {
#endif

/* isolation levels */
#define ISOLATION_LEVEL_AUTOCOMMIT          0
#define ISOLATION_LEVEL_READ_UNCOMMITTED    4
#define ISOLATION_LEVEL_READ_COMMITTED      1
#define ISOLATION_LEVEL_REPEATABLE_READ     2
#define ISOLATION_LEVEL_SERIALIZABLE        3
#define ISOLATION_LEVEL_DEFAULT             5

/* 3-state values on/off/default */
#define STATE_OFF               0
#define STATE_ON                1
#define STATE_DEFAULT           2

/* connection status */
#define CONN_STATUS_SETUP       0
#define CONN_STATUS_READY       1
#define CONN_STATUS_BEGIN       2
#define CONN_STATUS_PREPARED    5
/* async connection building statuses */
#define CONN_STATUS_CONNECTING            20
#define CONN_STATUS_DATESTYLE             21

/* async query execution status */
#define ASYNC_DONE  0
#define ASYNC_READ  1
#define ASYNC_WRITE 2

/* polling result */
#define PSYCO_POLL_OK    0
#define PSYCO_POLL_READ  1
#define PSYCO_POLL_WRITE 2
#define PSYCO_POLL_ERROR 3

/* Hard limit on the notices stored by the Python connection */
#define CONN_NOTICES_LIMIT 50

/* we need the initial date style to be ISO, for typecasters; if the user
   later change it, she must know what she's doing... these are the queries we
   need to issue */
#define psyco_datestyle "SET DATESTYLE TO 'ISO'"

extern HIDDEN PyTypeObject connectionType;

struct connectionObject_notice {
    struct connectionObject_notice *next;
    char *message;
};

/* the typedef is forward-declared in psycopg.h */
struct connectionObject {
    PyObject_HEAD

    pthread_mutex_t lock;   /* the global connection lock */

    char *dsn;              /* data source name */
    char *error;            /* temporarily stored error before raising */
    char *encoding;         /* current backend encoding */

    long int closed;          /* 1 means connection has been closed;
                                 2 that something horrible happened */
    long int mark;            /* number of commits/rollbacks done so far */
    int status;               /* status of the connection */
    xidObject *tpc_xid;       /* Transaction ID in two-phase commit */

    long int async;           /* 1 means the connection is async */
    int protocol;             /* protocol version */
    int server_version;       /* server version */

    PGconn *pgconn;           /* the postgresql connection */
    PGcancel *cancel;         /* the cancellation structure */

    /* Weakref to the object executing an asynchronous query. The object
     * is a cursor for async connections, but it may be something else
     * for a green connection. If NULL, the connection is idle. */
    PyObject *async_cursor;
    int async_status;         /* asynchronous execution status */
    PGresult *pgres;          /* temporary result across async calls */

    /* notice processing */
    PyObject *notice_list;
    struct connectionObject_notice *notice_pending;
    struct connectionObject_notice *last_notice;

    /* notifies */
    PyObject *notifies;

    /* per-connection typecasters */
    PyObject *string_types;   /* a set of typecasters for string types */
    PyObject *binary_types;   /* a set of typecasters for binary types */

    int equote;               /* use E''-style quotes for escaped strings */
    PyObject *weakreflist;    /* list of weak references */

    int autocommit;

    PyObject *cursor_factory;    /* default cursor factory from cursor() */

    /* Optional pointer to a decoding C function, e.g. PyUnicode_DecodeUTF8 */
    PyObject *(*cdecoder)(const char *, Py_ssize_t, const char *);

    /* Pointers to python encoding/decoding functions, e.g.
     * codecs.getdecoder('utf8') */
    PyObject *pyencoder;        /* python codec encoding function */
    PyObject *pydecoder;        /* python codec decoding function */

    /* Values for the transactions characteristics */
    int isolevel;
    int readonly;
    int deferrable;

    /* the pid this connection was created into */
    pid_t procpid;

    /* inside a with block */
    int entered;
};

/* map isolation level values into a numeric const */
typedef struct {
    char *name;
    int value;
} IsolationLevel;

/* C-callable functions in connection_int.c and connection_ext.c */
HIDDEN PyObject *conn_text_from_chars(connectionObject *pgconn, const char *str);
HIDDEN PyObject *conn_encode(connectionObject *self, PyObject *b);
HIDDEN PyObject *conn_decode(connectionObject *self, const char *str, Py_ssize_t len);
HIDDEN int  conn_get_standard_conforming_strings(PGconn *pgconn);
HIDDEN PyObject *conn_pgenc_to_pyenc(const char *encoding, char **clean_encoding);
HIDDEN int  conn_get_protocol_version(PGconn *pgconn);
HIDDEN int  conn_get_server_version(PGconn *pgconn);
HIDDEN void conn_notice_process(connectionObject *self);
HIDDEN void conn_notice_clean(connectionObject *self);
HIDDEN void conn_notifies_process(connectionObject *self);
RAISES_NEG HIDDEN int conn_setup(connectionObject *self);
HIDDEN int  conn_connect(connectionObject *self, const char *dsn, long int async);
HIDDEN char *conn_obscure_password(const char *dsn);
HIDDEN void conn_close(connectionObject *self);
HIDDEN void conn_close_locked(connectionObject *self);
RAISES_NEG HIDDEN int  conn_commit(connectionObject *self);
RAISES_NEG HIDDEN int  conn_rollback(connectionObject *self);
RAISES_NEG HIDDEN int conn_set_session(connectionObject *self, int autocommit,
        int isolevel, int readonly, int deferrable);
RAISES_NEG HIDDEN int  conn_set_client_encoding(connectionObject *self, const char *enc);
HIDDEN int  conn_poll(connectionObject *self);
RAISES_NEG HIDDEN int  conn_tpc_begin(connectionObject *self, xidObject *xid);
RAISES_NEG HIDDEN int  conn_tpc_command(connectionObject *self,
                             const char *cmd, xidObject *xid);
HIDDEN PyObject *conn_tpc_recover(connectionObject *self);
HIDDEN void conn_set_result(connectionObject *self, PGresult *pgres);
HIDDEN void conn_set_error(connectionObject *self, const char *msg);

/* exception-raising macros */
#define EXC_IF_CONN_CLOSED(self) if ((self)->closed > 0) { \
    PyErr_SetString(InterfaceError, "connection already closed"); \
    return NULL; }

#define EXC_IF_CONN_ASYNC(self, cmd) if ((self)->async == 1) { \
    PyErr_SetString(ProgrammingError, #cmd " cannot be used "  \
    "in asynchronous mode");                                   \
    return NULL; }

#define EXC_IF_IN_TRANSACTION(self, cmd)                        \
    if (self->status != CONN_STATUS_READY) {                    \
        PyErr_Format(ProgrammingError,                          \
            "%s cannot be used inside a transaction", #cmd);    \
        return NULL;                                            \
    }

#define EXC_IF_TPC_NOT_SUPPORTED(self)              \
    if ((self)->server_version < 80100) {           \
        PyErr_Format(NotSupportedError,             \
            "server version %d: "                   \
            "two-phase transactions not supported", \
            (self)->server_version);                \
        return NULL;                                \
    }

#define EXC_IF_TPC_BEGIN(self, cmd) if ((self)->tpc_xid) {  \
    PyErr_Format(ProgrammingError, "%s cannot be used "     \
    "during a two-phase transaction", #cmd);                \
    return NULL; }

#define EXC_IF_TPC_PREPARED(self, cmd)                        \
    if ((self)->status == CONN_STATUS_PREPARED) {             \
        PyErr_Format(ProgrammingError, "%s cannot be used "   \
            "with a prepared two-phase transaction", #cmd);   \
        return NULL; }

#ifdef __cplusplus
}
#endif

#endif /* !defined(PSYCOPG_CONNECTION_H) */
