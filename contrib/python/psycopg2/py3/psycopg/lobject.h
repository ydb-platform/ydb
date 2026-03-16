/* lobject.h - definition for the psycopg lobject type
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

#ifndef PSYCOPG_LOBJECT_H
#define PSYCOPG_LOBJECT_H 1

#include <libpq/libpq-fs.h>

#include "psycopg/connection.h"

#ifdef __cplusplus
extern "C" {
#endif

extern HIDDEN PyTypeObject lobjectType;

typedef struct {
    PyObject HEAD;

    connectionObject *conn;  /* connection owning the lobject */
    long int mark;           /* copied from conn->mark */

    char *smode;             /* string mode if lobject was opened */
    int mode;                /* numeric version of smode */

    int fd;                  /* the file descriptor for file-like ops */
    Oid oid;                 /* the oid for this lobject */
} lobjectObject;

/* functions exported from lobject_int.c */

RAISES_NEG HIDDEN int lobject_open(lobjectObject *self, connectionObject *conn,
                                   Oid oid, const char *smode, Oid new_oid,
                                   const char *new_file);
RAISES_NEG HIDDEN int lobject_unlink(lobjectObject *self);
RAISES_NEG HIDDEN int lobject_export(lobjectObject *self, const char *filename);

RAISES_NEG HIDDEN Py_ssize_t lobject_read(lobjectObject *self, char *buf, size_t len);
RAISES_NEG HIDDEN Py_ssize_t lobject_write(lobjectObject *self, const char *buf,
                                size_t len);
RAISES_NEG HIDDEN Py_ssize_t lobject_seek(lobjectObject *self, Py_ssize_t pos, int whence);
RAISES_NEG HIDDEN Py_ssize_t lobject_tell(lobjectObject *self);
RAISES_NEG HIDDEN int lobject_truncate(lobjectObject *self, size_t len);
RAISES_NEG HIDDEN int lobject_close(lobjectObject *self);

#define lobject_is_closed(self) \
    ((self)->fd < 0 || !(self)->conn || (self)->conn->closed)

/* exception-raising macros */

#define EXC_IF_LOBJ_CLOSED(self) \
  if (lobject_is_closed(self)) { \
    PyErr_SetString(InterfaceError, "lobject already closed");  \
    return NULL; }

#define EXC_IF_LOBJ_LEVEL0(self) \
if (self->conn->autocommit) {                                       \
    psyco_set_error(ProgrammingError, NULL,                         \
        "can't use a lobject outside of transactions");             \
    return NULL;                                                    \
}
#define EXC_IF_LOBJ_UNMARKED(self) \
if (self->conn->mark != self->mark) {                  \
    psyco_set_error(ProgrammingError, NULL,            \
        "lobject isn't valid anymore");                \
    return NULL;                                       \
}

/* Values for the lobject mode */
#define LOBJECT_READ  1
#define LOBJECT_WRITE 2
#define LOBJECT_BINARY 4
#define LOBJECT_TEXT 8

#ifdef __cplusplus
}
#endif

#endif /* !defined(PSYCOPG_LOBJECT_H) */
