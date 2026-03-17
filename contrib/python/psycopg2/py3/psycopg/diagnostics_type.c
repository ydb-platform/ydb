/* diagnostics.c - present information from libpq error responses
 *
 * Copyright (C) 2013-2019 Matthew Woodcraft <matthew@woodcraft.me.uk>
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

#include "psycopg/diagnostics.h"
#include "psycopg/error.h"


/* These constants are defined in src/include/postgres_ext.h but some may not
 * be available with the libpq we currently support at compile time. */

/* Available from PG 9.3 */
#ifndef PG_DIAG_SCHEMA_NAME
#define PG_DIAG_SCHEMA_NAME     's'
#endif
#ifndef PG_DIAG_TABLE_NAME
#define PG_DIAG_TABLE_NAME      't'
#endif
#ifndef PG_DIAG_COLUMN_NAME
#define PG_DIAG_COLUMN_NAME     'c'
#endif
#ifndef PG_DIAG_DATATYPE_NAME
#define PG_DIAG_DATATYPE_NAME   'd'
#endif
#ifndef PG_DIAG_CONSTRAINT_NAME
#define PG_DIAG_CONSTRAINT_NAME 'n'
#endif

/* Available from PG 9.6 */
#ifndef PG_DIAG_SEVERITY_NONLOCALIZED
#define PG_DIAG_SEVERITY_NONLOCALIZED 'V'
#endif


/* Retrieve an error string from the exception's cursor.
 *
 * If the cursor or its result isn't available, return None.
 */
static PyObject *
diagnostics_get_field(diagnosticsObject *self, void *closure)
{
    const char *errortext;

    if (!self->err->pgres) {
        Py_RETURN_NONE;
    }

    errortext = PQresultErrorField(self->err->pgres, (int)(Py_intptr_t)closure);
    return error_text_from_chars(self->err, errortext);
}


/* object calculated member list */
static struct PyGetSetDef diagnosticsObject_getsets[] = {
    { "severity", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_SEVERITY },
    { "severity_nonlocalized", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_SEVERITY_NONLOCALIZED },
    { "sqlstate", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_SQLSTATE },
    { "message_primary", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_MESSAGE_PRIMARY },
    { "message_detail", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_MESSAGE_DETAIL },
    { "message_hint", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_MESSAGE_HINT },
    { "statement_position", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_STATEMENT_POSITION },
    { "internal_position", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_INTERNAL_POSITION },
    { "internal_query", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_INTERNAL_QUERY },
    { "context", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_CONTEXT },
    { "schema_name", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_SCHEMA_NAME },
    { "table_name", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_TABLE_NAME },
    { "column_name", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_COLUMN_NAME },
    { "datatype_name", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_DATATYPE_NAME },
    { "constraint_name", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_CONSTRAINT_NAME },
    { "source_file", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_SOURCE_FILE },
    { "source_line", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_SOURCE_LINE },
    { "source_function", (getter)diagnostics_get_field, NULL,
      NULL, (void*) PG_DIAG_SOURCE_FUNCTION },
    {NULL}
};

/* initialization and finalization methods */

static PyObject *
diagnostics_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static int
diagnostics_init(diagnosticsObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *err = NULL;

    if (!PyArg_ParseTuple(args, "O", &err))
        return -1;

    if (!PyObject_TypeCheck(err, &errorType)) {
        PyErr_SetString(PyExc_TypeError,
            "The argument must be a psycopg2.Error");
        return -1;
    }

    Py_INCREF(err);
    self->err = (errorObject *)err;
    return 0;
}

static void
diagnostics_dealloc(diagnosticsObject* self)
{
    Py_CLEAR(self->err);
    Py_TYPE(self)->tp_free((PyObject *)self);
}


/* object type */

static const char diagnosticsType_doc[] =
    "Details from a database error report.\n\n"
    "The object is returned by the `~psycopg2.Error.diag` attribute of the\n"
    "`!Error` object.\n"
    "All the information available from the |PQresultErrorField|_ function\n"
    "are exposed as attributes by the object, e.g. the `!severity` attribute\n"
    "returns the `!PG_DIAG_SEVERITY` code. "
    "Please refer to the `PostgreSQL documentation`__ for the meaning of all"
        " the attributes.\n\n"
    ".. |PQresultErrorField| replace:: `!PQresultErrorField()`\n"
    ".. _PQresultErrorField: https://www.postgresql.org/docs/current/static/"
        "libpq-exec.html#LIBPQ-PQRESULTERRORFIELD\n"
    ".. __: PQresultErrorField_\n";

PyTypeObject diagnosticsType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.Diagnostics",
    sizeof(diagnosticsObject), 0,
    (destructor)diagnostics_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    0,          /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    diagnosticsType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    0,          /*tp_methods*/
    0,          /*tp_members*/
    diagnosticsObject_getsets, /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    (initproc)diagnostics_init, /*tp_init*/
    0,          /*tp_alloc*/
    diagnostics_new, /*tp_new*/
};
