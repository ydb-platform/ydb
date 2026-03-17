/* cursor.h - definition for the psycopg cursor type
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

#ifndef PSYCOPG_CURSOR_H
#define PSYCOPG_CURSOR_H 1

#include "psycopg/connection.h"

#ifdef __cplusplus
extern "C" {
#endif

extern HIDDEN PyTypeObject cursorType;

/* the typedef is forward-declared in psycopg.h */
struct cursorObject {
    PyObject_HEAD

    connectionObject *conn; /* connection owning the cursor */

    int closed:1;            /* 1 if the cursor is closed */
    int notuples:1;          /* 1 if the command was not a SELECT query */
    int withhold:1;          /* 1 if the cursor is named and uses WITH HOLD */

    int scrollable;          /* 1 if the cursor is named and SCROLLABLE,
                                0 if not scrollable
                               -1 if undefined (PG may decide scrollable or not)
                              */

    long int rowcount;       /* number of rows affected by last execute */
    long int columns;        /* number of columns fetched from the db */
    long int arraysize;      /* how many rows should fetchmany() return */
    long int itersize;       /* how many rows should iter(cur) fetch in named cursors */
    long int row;            /* the row counter for fetch*() operations */
    long int mark;           /* transaction marker, copied from conn */

    PyObject *description;   /* read-only attribute: sequence of 7-item
                                sequences.*/

    /* postgres connection stuff */
    PGresult   *pgres;     /* result of last query */
    PyObject   *pgstatus;  /* last message from the server after an execute */
    Oid         lastoid;   /* last oid from an insert or InvalidOid */

    PyObject *casts;       /* an array (tuple) of typecast functions */
    PyObject *caster;      /* the current typecaster object */

    PyObject  *copyfile;   /* file-like used during COPY TO/FROM ops */
    Py_ssize_t copysize;   /* size of the copy buffer during COPY TO/FROM ops */
#define DEFAULT_COPYSIZE 16384
#define DEFAULT_COPYBUFF  8192

    PyObject *tuple_factory;    /* factory for result tuples */
    PyObject *tzinfo_factory;   /* factory for tzinfo objects */

    PyObject *query;      /* last query executed */

    char *qattr;          /* quoting attr, used when quoting strings */
    char *notice;         /* a notice from the backend */
    char *name;           /* this cursor name */
    char *qname;          /* this cursor name, quoted */

    PyObject *string_types;   /* a set of typecasters for string types */
    PyObject *binary_types;   /* a set of typecasters for binary types */

    PyObject *weakreflist;    /* list of weak references */

};


/* C-callable functions in cursor_int.c and cursor_type.c */
BORROWED HIDDEN PyObject *curs_get_cast(cursorObject *self, PyObject *oid);
HIDDEN void curs_reset(cursorObject *self);
RAISES_NEG HIDDEN int curs_withhold_set(cursorObject *self, PyObject *pyvalue);
RAISES_NEG HIDDEN int curs_scrollable_set(cursorObject *self, PyObject *pyvalue);
HIDDEN PyObject *curs_validate_sql_basic(cursorObject *self, PyObject *sql);
HIDDEN void curs_set_result(cursorObject *self, PGresult *pgres);

/* exception-raising macros */
#define EXC_IF_CURS_CLOSED(self) \
do { \
    if (!(self)->conn) { \
        PyErr_SetString(InterfaceError, "the cursor has no connection"); \
        return NULL; } \
    if ((self)->closed || (self)->conn->closed) { \
        PyErr_SetString(InterfaceError, "cursor already closed"); \
        return NULL; } \
} while (0)

#define EXC_IF_NO_TUPLES(self) \
do \
    if ((self)->notuples && (self)->name == NULL) { \
        PyErr_SetString(ProgrammingError, "no results to fetch"); \
        return NULL; } \
while (0)

#define EXC_IF_NO_MARK(self) \
do \
    if ((self)->mark != (self)->conn->mark && (self)->withhold == 0) { \
        PyErr_SetString(ProgrammingError, "named cursor isn't valid anymore"); \
        return NULL; } \
while (0)

#define EXC_IF_CURS_ASYNC(self, cmd) \
do \
    if ((self)->conn->async == 1) { \
        PyErr_SetString(ProgrammingError, \
            #cmd " cannot be used in asynchronous mode"); \
        return NULL; } \
while (0)

#define EXC_IF_ASYNC_IN_PROGRESS(self, cmd) \
do \
    if ((self)->conn->async_cursor != NULL) { \
        PyErr_SetString(ProgrammingError, \
            #cmd " cannot be used while an asynchronous query is underway"); \
    return NULL; } \
while (0)

#ifdef __cplusplus
}
#endif

#endif /* !defined(PSYCOPG_CURSOR_H) */
