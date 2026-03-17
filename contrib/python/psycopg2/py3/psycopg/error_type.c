/* error_type.c - python interface to the Error objects
 *
 * Copyright (C) 2013-2019  Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

#include "psycopg/error.h"
#include "psycopg/diagnostics.h"
#include "psycopg/pqpath.h"


PyObject *
error_text_from_chars(errorObject *self, const char *str)
{
    return psyco_text_from_chars_safe(str, -1, self->pydecoder);
}


/* Return the Python exception corresponding to an SQLSTATE error
 * code.  A list of error codes can be found at:
 * https://www.postgresql.org/docs/current/static/errcodes-appendix.html
 */
BORROWED PyObject *
exception_from_sqlstate(const char *sqlstate)
{
    PyObject *exc;

    /* First look up an exception of the proper class */
    exc = PyDict_GetItemString(sqlstate_errors, sqlstate);
    if (exc) {
        return exc;
    }
    else {
        PyErr_Clear();
        return base_exception_from_sqlstate(sqlstate);
    }
}

BORROWED PyObject *
base_exception_from_sqlstate(const char *sqlstate)
{
    switch (sqlstate[0]) {
    case '0':
        switch (sqlstate[1]) {
        case '8': /* Class 08 - Connection Exception */
            return OperationalError;
        case 'A': /* Class 0A - Feature Not Supported */
            return NotSupportedError;
        }
        break;
    case '2':
        switch (sqlstate[1]) {
        case '0': /* Class 20 - Case Not Found */
        case '1': /* Class 21 - Cardinality Violation */
            return ProgrammingError;
        case '2': /* Class 22 - Data Exception */
            return DataError;
        case '3': /* Class 23 - Integrity Constraint Violation */
            return IntegrityError;
        case '4': /* Class 24 - Invalid Cursor State */
        case '5': /* Class 25 - Invalid Transaction State */
            return InternalError;
        case '6': /* Class 26 - Invalid SQL Statement Name */
        case '7': /* Class 27 - Triggered Data Change Violation */
        case '8': /* Class 28 - Invalid Authorization Specification */
            return OperationalError;
        case 'B': /* Class 2B - Dependent Privilege Descriptors Still Exist */
        case 'D': /* Class 2D - Invalid Transaction Termination */
        case 'F': /* Class 2F - SQL Routine Exception */
            return InternalError;
        }
        break;
    case '3':
        switch (sqlstate[1]) {
        case '4': /* Class 34 - Invalid Cursor Name */
            return OperationalError;
        case '8': /* Class 38 - External Routine Exception */
        case '9': /* Class 39 - External Routine Invocation Exception */
        case 'B': /* Class 3B - Savepoint Exception */
            return InternalError;
        case 'D': /* Class 3D - Invalid Catalog Name */
        case 'F': /* Class 3F - Invalid Schema Name */
            return ProgrammingError;
        }
        break;
    case '4':
        switch (sqlstate[1]) {
        case '0': /* Class 40 - Transaction Rollback */
            return TransactionRollbackError;
        case '2': /* Class 42 - Syntax Error or Access Rule Violation */
        case '4': /* Class 44 - WITH CHECK OPTION Violation */
            return ProgrammingError;
        }
        break;
    case '5':
        /* Class 53 - Insufficient Resources
           Class 54 - Program Limit Exceeded
           Class 55 - Object Not In Prerequisite State
           Class 57 - Operator Intervention
           Class 58 - System Error (errors external to PostgreSQL itself) */
        if (!strcmp(sqlstate, "57014"))
            return QueryCanceledError;
        else
            return OperationalError;
    case 'F': /* Class F0 - Configuration File Error */
        return InternalError;
    case 'H': /* Class HV - Foreign Data Wrapper Error (SQL/MED) */
        return OperationalError;
    case 'P': /* Class P0 - PL/pgSQL Error */
        return InternalError;
    case 'X': /* Class XX - Internal Error */
        return InternalError;
    }
    /* return DatabaseError as a fallback */
    return DatabaseError;
}


static const char pgerror_doc[] =
    "The error message returned by the backend, if available, else None";

static const char pgcode_doc[] =
    "The error code returned by the backend, if available, else None";

static const char cursor_doc[] =
    "The cursor that raised the exception, if available, else None";

static const char diag_doc[] =
    "A Diagnostics object to get further information about the error";

static PyMemberDef error_members[] = {
    { "pgerror", T_OBJECT, offsetof(errorObject, pgerror),
        READONLY, (char *)pgerror_doc },
    { "pgcode", T_OBJECT, offsetof(errorObject, pgcode),
        READONLY, (char *)pgcode_doc },
    { "cursor", T_OBJECT, offsetof(errorObject, cursor),
        READONLY, (char *)cursor_doc },
    { NULL }
};

static PyObject *
error_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    return ((PyTypeObject *)PyExc_StandardError)->tp_new(
            type, args, kwargs);
}

static int
error_init(errorObject *self, PyObject *args, PyObject *kwargs)
{
    if (((PyTypeObject *)PyExc_StandardError)->tp_init(
            (PyObject *)self, args, kwargs) < 0) {
        return -1;
    }
    return 0;
}

static int
error_traverse(errorObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->pgerror);
    Py_VISIT(self->pgcode);
    Py_VISIT(self->cursor);
    Py_VISIT(self->pydecoder);

    return ((PyTypeObject *)PyExc_StandardError)->tp_traverse(
        (PyObject *)self, visit, arg);
}

static int
error_clear(errorObject *self)
{
    Py_CLEAR(self->pgerror);
    Py_CLEAR(self->pgcode);
    Py_CLEAR(self->cursor);
    Py_CLEAR(self->pydecoder);

    return ((PyTypeObject *)PyExc_StandardError)->tp_clear((PyObject *)self);
}

static void
error_dealloc(errorObject *self)
{
    PyObject_GC_UnTrack((PyObject *)self);
    error_clear(self);
    CLEARPGRES(self->pgres);

    Py_TYPE(self)->tp_free((PyObject *)self);
}


static PyObject *
error_get_diag(errorObject *self, void *closure)
{
    return PyObject_CallFunctionObjArgs(
        (PyObject *)&diagnosticsType, (PyObject *)self, NULL);
}

static struct PyGetSetDef error_getsets[] = {
    { "diag", (getter)error_get_diag, NULL, (char *)diag_doc },
    { NULL }
};


/* Error.__reduce__
 *
 * The method is required to make exceptions picklable: set the cursor
 * attribute to None. Only working from Py 2.5: previous versions
 * would require implementing __getstate__, and as of 2012 it's a little
 * bit too late to care. */
static PyObject *
error_reduce(errorObject *self, PyObject *dummy)
{
    PyObject *meth = NULL;
    PyObject *tuple = NULL;
    PyObject *dict = NULL;
    PyObject *rv = NULL;

    if (!(meth = PyObject_GetAttrString(PyExc_StandardError, "__reduce__"))) {
        goto error;
    }
    if (!(tuple = PyObject_CallFunctionObjArgs(meth, self, NULL))) {
        goto error;
    }

    /* tuple is (type, args): convert it to (type, args, dict)
     * with our extra items in the dict.
     *
     * If these checks fail, we can still return a valid object. Pickle
     * will likely fail downstream, but there's nothing else we can do here */
    if (!PyTuple_Check(tuple)) { goto exit; }
    if (2 != PyTuple_GET_SIZE(tuple)) { goto exit; }

    if (!(dict = PyDict_New())) { goto error; }
    if (self->pgerror) {
        if (0 != PyDict_SetItemString(dict, "pgerror", self->pgerror)) {
            goto error;
        }
    }
    if (self->pgcode) {
        if (0 != PyDict_SetItemString(dict, "pgcode", self->pgcode)) {
            goto error;
        }
    }

    {
        PyObject *newtuple;
        if (!(newtuple = PyTuple_Pack(3,
                PyTuple_GET_ITEM(tuple, 0),
                PyTuple_GET_ITEM(tuple, 1),
                dict))) {
            goto error;
        }
        Py_DECREF(tuple);
        tuple = newtuple;
    }

exit:
    rv = tuple;
    tuple = NULL;

error:
    Py_XDECREF(dict);
    Py_XDECREF(tuple);
    Py_XDECREF(meth);

    return rv;
}

PyObject *
error_setstate(errorObject *self, PyObject *state)
{
    PyObject *rv = NULL;

    /* we don't call the StandartError's setstate as it would try to load the
     * dict content as attributes */

    if (state == Py_None) {
        goto exit;
    }
    if (!PyDict_Check(state)) {
        PyErr_SetString(PyExc_TypeError, "state is not a dictionary");
        goto error;
    }

    /* load the dict content in the structure */
    Py_CLEAR(self->pgerror);
    self->pgerror = PyDict_GetItemString(state, "pgerror");
    Py_XINCREF(self->pgerror);

    Py_CLEAR(self->pgcode);
    self->pgcode = PyDict_GetItemString(state, "pgcode");
    Py_XINCREF(self->pgcode);

    Py_CLEAR(self->cursor);
    /* We never expect a cursor in the state as it's not picklable.
     * at most there could be a None here, coming from Psycopg < 2.5 */

exit:
    rv = Py_None;
    Py_INCREF(rv);

error:
    return rv;
}

static PyMethodDef error_methods[] = {
    /* Make Error and all its subclasses picklable. */
    {"__reduce__", (PyCFunction)error_reduce, METH_NOARGS },
    {"__setstate__", (PyCFunction)error_setstate, METH_O },
    {NULL}
};


PyTypeObject errorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.Error",
    sizeof(errorObject), 0,
    (destructor)error_dealloc, /* tp_dealloc */
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
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    Error_doc, /*tp_doc*/
    (traverseproc)error_traverse, /*tp_traverse*/
    (inquiry)error_clear, /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    error_methods, /*tp_methods*/
    error_members, /*tp_members*/
    error_getsets, /*tp_getset*/
    0,          /*tp_base Will be set to StandardError in module init */
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    (initproc)error_init, /*tp_init*/
    0,          /*tp_alloc*/
    error_new, /*tp_new*/
};
