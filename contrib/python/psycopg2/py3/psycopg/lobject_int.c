/* lobject_int.c - code used by the lobject object
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
#include "psycopg/pqpath.h"

#include <string.h>

static void
collect_error(connectionObject *conn)
{
    conn_set_error(conn, PQerrorMessage(conn->pgconn));
}


/* Check if the mode passed to the large object is valid.
 * In case of success return a value >= 0
 * On error return a value < 0 and set an exception.
 *
 * Valid mode are [r|w|rw|n][t|b]
 */
RAISES_NEG static int
_lobject_parse_mode(const char *mode)
{
    int rv = 0;
    size_t pos = 0;

    if (0 == strncmp("rw", mode, 2)) {
        rv |= LOBJECT_READ | LOBJECT_WRITE;
        pos += 2;
    }
    else {
        switch (mode[0]) {
        case 'r':
            rv |= LOBJECT_READ;
            pos += 1;
            break;
        case 'w':
            rv |= LOBJECT_WRITE;
            pos += 1;
            break;
        case 'n':
            pos += 1;
            break;
        default:
            rv |= LOBJECT_READ;
            break;
        }
    }

    switch (mode[pos]) {
        case 't':
            rv |= LOBJECT_TEXT;
            pos += 1;
            break;
        case 'b':
            rv |= LOBJECT_BINARY;
            pos += 1;
            break;
        default:
            rv |= LOBJECT_TEXT;
            break;
    }

    if (pos != strlen(mode)) {
        PyErr_Format(PyExc_ValueError,
            "bad mode for lobject: '%s'", mode);
        rv = -1;
    }

    return rv;
}


/* Return a string representing the lobject mode.
 *
 * The return value is a new string allocated on the Python heap.
 *
 * The function must be called holding the GIL.
 */
static char *
_lobject_unparse_mode(int mode)
{
    char *buf;
    char *c;

    /* the longest is 'rwt' */
    if (!(c = buf = PyMem_Malloc(4))) {
        PyErr_NoMemory();
        return NULL;
    }

    if (mode & LOBJECT_READ) { *c++ = 'r'; }
    if (mode & LOBJECT_WRITE) { *c++ = 'w'; }

    if (buf == c) {
        /* neither read nor write */
        *c++ = 'n';
    }
    else {
        if (mode & LOBJECT_TEXT) {
            *c++ = 't';
        }
        else {
            *c++ = 'b';
        }
    }
    *c = '\0';

    return buf;
}

/* lobject_open - create a new/open an existing lo */

RAISES_NEG int
lobject_open(lobjectObject *self, connectionObject *conn,
              Oid oid, const char *smode, Oid new_oid, const char *new_file)
{
    int retvalue = -1;
    int pgmode = 0;
    int mode;

    if (0 > (mode = _lobject_parse_mode(smode))) {
        return -1;
    }

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

    retvalue = pq_begin_locked(self->conn, &_save);
    if (retvalue < 0)
        goto end;

    /* if the oid is InvalidOid we create a new lob before opening it
       or we import a file from the FS, depending on the value of
       new_file */
    if (oid == InvalidOid) {
        if (new_file)
            self->oid = lo_import(self->conn->pgconn, new_file);
        else {
            /* Use lo_creat when possible to be more middleware-friendly.
               See ticket #88. */
            if (new_oid != InvalidOid)
                self->oid = lo_create(self->conn->pgconn, new_oid);
            else
                self->oid = lo_creat(self->conn->pgconn, INV_READ | INV_WRITE);
        }

        Dprintf("lobject_open: large object created with oid = %u",
                self->oid);

        if (self->oid == InvalidOid) {
            collect_error(self->conn);
            retvalue = -1;
            goto end;
        }

        mode = (mode & ~LOBJECT_READ) | LOBJECT_WRITE;
    }
    else {
        self->oid = oid;
    }

    /* if the oid is a real one we try to open with the given mode */
    if (mode & LOBJECT_READ) { pgmode |= INV_READ; }
    if (mode & LOBJECT_WRITE) { pgmode |= INV_WRITE; }
    if (pgmode) {
        self->fd = lo_open(self->conn->pgconn, self->oid, pgmode);
        Dprintf("lobject_open: large object opened with mode = %i fd = %d",
            pgmode, self->fd);

        if (self->fd == -1) {
            collect_error(self->conn);
            retvalue = -1;
            goto end;
        }
    }

    /* set the mode for future reference */
    self->mode = mode;
    Py_BLOCK_THREADS;
    self->smode = _lobject_unparse_mode(mode);
    Py_UNBLOCK_THREADS;
    if (NULL == self->smode) {
        retvalue = 1;  /* exception already set */
        goto end;
    }

    retvalue = 0;

 end:
    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (retvalue < 0)
        pq_complete_error(self->conn);
    /* if retvalue > 0, an exception is already set */

    return retvalue;
}

/* lobject_close - close an existing lo */

RAISES_NEG static int
lobject_close_locked(lobjectObject *self)
{
    int retvalue;

    Dprintf("lobject_close_locked: conn->closed %ld", self->conn->closed);
    switch (self->conn->closed) {
    case 0:
        /* Connection is open, go ahead */
        break;
    case 1:
        /* Connection is closed, return a success */
        return 0;
        break;
    default:
        conn_set_error(self->conn, "the connection is broken");
        return -1;
        break;
    }

    if (self->conn->autocommit ||
        self->conn->mark != self->mark ||
        self->fd == -1)
        return 0;

    retvalue = lo_close(self->conn->pgconn, self->fd);
    self->fd = -1;
    if (retvalue < 0)
        collect_error(self->conn);

    return retvalue;
}

RAISES_NEG int
lobject_close(lobjectObject *self)
{
    int retvalue;

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

    retvalue = lobject_close_locked(self);

    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (retvalue < 0)
        pq_complete_error(self->conn);
    return retvalue;
}

/* lobject_unlink - remove an lo from database */

RAISES_NEG int
lobject_unlink(lobjectObject *self)
{
    int retvalue = -1;

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

    retvalue = pq_begin_locked(self->conn, &_save);
    if (retvalue < 0)
        goto end;

    /* first we make sure the lobject is closed and then we unlink */
    retvalue = lobject_close_locked(self);
    if (retvalue < 0)
        goto end;

    retvalue = lo_unlink(self->conn->pgconn, self->oid);
    if (retvalue < 0)
        collect_error(self->conn);

 end:
    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (retvalue < 0)
        pq_complete_error(self->conn);
    return retvalue;
}

/* lobject_write - write bytes to a lo */

RAISES_NEG Py_ssize_t
lobject_write(lobjectObject *self, const char *buf, size_t len)
{
    Py_ssize_t written;

    Dprintf("lobject_writing: fd = %d, len = " FORMAT_CODE_SIZE_T,
            self->fd, len);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

    written = lo_write(self->conn->pgconn, self->fd, buf, len);
    if (written < 0)
        collect_error(self->conn);

    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (written < 0)
        pq_complete_error(self->conn);
    return written;
}

/* lobject_read - read bytes from a lo */

RAISES_NEG Py_ssize_t
lobject_read(lobjectObject *self, char *buf, size_t len)
{
    Py_ssize_t n_read;

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

    n_read = lo_read(self->conn->pgconn, self->fd, buf, len);
    if (n_read < 0)
        collect_error(self->conn);

    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (n_read < 0)
        pq_complete_error(self->conn);
    return n_read;
}

/* lobject_seek - move the current position in the lo */

RAISES_NEG Py_ssize_t
lobject_seek(lobjectObject *self, Py_ssize_t pos, int whence)
{
    Py_ssize_t where;

    Dprintf("lobject_seek: fd = %d, pos = " FORMAT_CODE_PY_SSIZE_T ", whence = %d",
            self->fd, pos, whence);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

#ifdef HAVE_LO64
    if (self->conn->server_version < 90300) {
        where = (Py_ssize_t)lo_lseek(self->conn->pgconn, self->fd, (int)pos, whence);
    } else {
        where = (Py_ssize_t)lo_lseek64(self->conn->pgconn, self->fd, pos, whence);
    }
#else
    where = (Py_ssize_t)lo_lseek(self->conn->pgconn, self->fd, (int)pos, whence);
#endif
    Dprintf("lobject_seek: where = " FORMAT_CODE_PY_SSIZE_T, where);
    if (where < 0)
        collect_error(self->conn);

    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (where < 0)
        pq_complete_error(self->conn);
    return where;
}

/* lobject_tell - tell the current position in the lo */

RAISES_NEG Py_ssize_t
lobject_tell(lobjectObject *self)
{
    Py_ssize_t where;

    Dprintf("lobject_tell: fd = %d", self->fd);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

#ifdef HAVE_LO64
    if (self->conn->server_version < 90300) {
        where = (Py_ssize_t)lo_tell(self->conn->pgconn, self->fd);
    } else {
        where = (Py_ssize_t)lo_tell64(self->conn->pgconn, self->fd);
    }
#else
    where = (Py_ssize_t)lo_tell(self->conn->pgconn, self->fd);
#endif
    Dprintf("lobject_tell: where = " FORMAT_CODE_PY_SSIZE_T, where);
    if (where < 0)
        collect_error(self->conn);

    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (where < 0)
        pq_complete_error(self->conn);
    return where;
}

/* lobject_export - export to a local file */

RAISES_NEG int
lobject_export(lobjectObject *self, const char *filename)
{
    int retvalue;

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

    retvalue = pq_begin_locked(self->conn, &_save);
    if (retvalue < 0)
        goto end;

    retvalue = lo_export(self->conn->pgconn, self->oid, filename);
    if (retvalue < 0)
        collect_error(self->conn);

 end:
    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (retvalue < 0)
        pq_complete_error(self->conn);
    return retvalue;
}

RAISES_NEG int
lobject_truncate(lobjectObject *self, size_t len)
{
    int retvalue;

    Dprintf("lobject_truncate: fd = %d, len = " FORMAT_CODE_SIZE_T,
            self->fd, len);

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&(self->conn->lock));

#ifdef HAVE_LO64
    if (self->conn->server_version < 90300) {
        retvalue = lo_truncate(self->conn->pgconn, self->fd, len);
    } else {
        retvalue = lo_truncate64(self->conn->pgconn, self->fd, len);
    }
#else
    retvalue = lo_truncate(self->conn->pgconn, self->fd, len);
#endif
    Dprintf("lobject_truncate: result = %d", retvalue);
    if (retvalue < 0)
        collect_error(self->conn);

    pthread_mutex_unlock(&(self->conn->lock));
    Py_END_ALLOW_THREADS;

    if (retvalue < 0)
        pq_complete_error(self->conn);
    return retvalue;

}
