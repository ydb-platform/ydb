/* utils.h - function definitions for utility file
 *
 * Copyright (C) 2018-2019 Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

#ifndef UTILS_H
#define UTILS_H 1

/* forward declarations */
typedef struct cursorObject cursorObject;
typedef struct connectionObject connectionObject;
typedef struct replicationMessageObject replicationMessageObject;

HIDDEN char *psyco_escape_string(
    connectionObject *conn,
    const char *from, Py_ssize_t len, char *to, Py_ssize_t *tolen);

HIDDEN char *psyco_escape_identifier(
    connectionObject *conn, const char *str, Py_ssize_t len);

HIDDEN int psyco_strdup(char **to, const char *from, Py_ssize_t len);

STEALS(1) HIDDEN PyObject * psyco_ensure_bytes(PyObject *obj);
STEALS(1) HIDDEN PyObject * psyco_ensure_text(PyObject *obj);

HIDDEN int psyco_is_text_file(PyObject *f);

HIDDEN PyObject *psyco_dict_from_conninfo_options(
    PQconninfoOption *options, int include_password);

HIDDEN PyObject *psyco_make_dsn(PyObject *dsn, PyObject *kwargs);

HIDDEN PyObject *psyco_text_from_chars_safe(
    const char *str, Py_ssize_t len, PyObject *decoder);

HIDDEN RAISES BORROWED PyObject *psyco_set_error(
    PyObject *exc, cursorObject *curs, const char *msg);

HIDDEN PyObject *psyco_get_decimal_type(void);

HIDDEN PyObject *Bytes_Format(PyObject *format, PyObject *args);


#endif /* !defined(UTILS_H) */
