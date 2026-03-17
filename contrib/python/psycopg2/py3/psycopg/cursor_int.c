/* cursor_int.c - code used by the cursor object
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

#include "psycopg/cursor.h"
#include "psycopg/pqpath.h"
#include "psycopg/typecast.h"

/* curs_get_cast - return the type caster for an oid.
 *
 * Return the most specific type caster, from cursor to connection to global.
 * If no type caster is found, return the default one.
 *
 * Return a borrowed reference.
 */

BORROWED PyObject *
curs_get_cast(cursorObject *self, PyObject *oid)
{
    PyObject *cast;

    /* cursor lookup */
    if (self->string_types != NULL && self->string_types != Py_None) {
        cast = PyDict_GetItem(self->string_types, oid);
        Dprintf("curs_get_cast:        per-cursor dict: %p", cast);
        if (cast) { return cast; }
    }

    /* connection lookup */
    cast = PyDict_GetItem(self->conn->string_types, oid);
    Dprintf("curs_get_cast:        per-connection dict: %p", cast);
    if (cast) { return cast; }

    /* global lookup */
    cast = PyDict_GetItem(psyco_types, oid);
    Dprintf("curs_get_cast:        global dict: %p", cast);
    if (cast) { return cast; }

    /* fallback */
    return psyco_default_cast;
}

#include <string.h>


/* curs_reset - reset the cursor to a clean state */

void
curs_reset(cursorObject *self)
{
    /* initialize some variables to default values */
    self->notuples = 1;
    self->rowcount = -1;
    self->row = 0;

    Py_CLEAR(self->description);
    Py_CLEAR(self->casts);
}


/* Return 1 if `obj` is a `psycopg2.sql.Composable` instance, else 0
 * Set an exception and return -1 in case of error.
 */
RAISES_NEG static int
_curs_is_composible(PyObject *obj)
{
    int rv = -1;
    PyObject *m = NULL;
    PyObject *comp = NULL;

    if (!(m = PyImport_ImportModule("psycopg2.sql"))) { goto exit; }
    if (!(comp = PyObject_GetAttrString(m, "Composable"))) { goto exit; }
    rv = PyObject_IsInstance(obj, comp);

exit:
    Py_XDECREF(comp);
    Py_XDECREF(m);
    return rv;

}

/* Performs very basic validation on an incoming SQL string.
 * Returns a new reference to a str instance on success; NULL on failure,
 * after having set an exception.
 */
PyObject *
curs_validate_sql_basic(cursorObject *self, PyObject *sql)
{
    PyObject *rv = NULL;
    PyObject *comp = NULL;
    int iscomp;

    if (!sql || !PyObject_IsTrue(sql)) {
        psyco_set_error(ProgrammingError, self,
                         "can't execute an empty query");
        goto exit;
    }

    if (Bytes_Check(sql)) {
        /* Necessary for ref-count symmetry with the unicode case: */
        Py_INCREF(sql);
        rv = sql;
    }
    else if (PyUnicode_Check(sql)) {
        if (!(rv = conn_encode(self->conn, sql))) { goto exit; }
    }
    else if (0 != (iscomp = _curs_is_composible(sql))) {
        if (iscomp < 0) { goto exit; }
        if (!(comp = PyObject_CallMethod(sql, "as_string", "O", self->conn))) {
            goto exit;
        }

        if (Bytes_Check(comp)) {
            rv = comp;
            comp = NULL;
        }
        else if (PyUnicode_Check(comp)) {
            if (!(rv = conn_encode(self->conn, comp))) { goto exit; }
        }
        else {
            PyErr_Format(PyExc_TypeError,
                "as_string() should return a string: got %s instead",
                Py_TYPE(comp)->tp_name);
            goto exit;
        }
    }
    else {
        /* the  is not unicode or string, raise an error */
        PyErr_Format(PyExc_TypeError,
            "argument 1 must be a string or unicode object: got %s instead",
            Py_TYPE(sql)->tp_name);
        goto exit;
    }

exit:
    Py_XDECREF(comp);
    return rv;
}


void
curs_set_result(cursorObject *self, PGresult *pgres)
{
    PQclear(self->pgres);
    self->pgres = pgres;
}
