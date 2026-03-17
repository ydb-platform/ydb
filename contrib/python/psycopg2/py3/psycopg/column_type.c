/* column_type.c - python interface to cursor.description objects
 *
 * Copyright (C) 2018-2019  Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

#include "psycopg/column.h"


static const char column_doc[] =
    "Description of a column returned by a query.\n\n"
    "The DBAPI demands this object to be a 7-items sequence. This object\n"
    "respects this interface, but adds names for the exposed attributes\n"
    "and adds attribute not requested by the DBAPI.";

static const char name_doc[] =
    "The name of the column returned.";

static const char type_code_doc[] =
    "The PostgreSQL OID of the column.\n\n"
    "You can use the pg_type system table to get more informations about the\n"
    "type. This is the value used by Psycopg to decide what Python type use\n"
    "to represent the value";

static const char display_size_doc[] =
    "The actual length of the column in bytes.\n\n"
    "Obtaining this value is computationally intensive, so it is always None";

static const char internal_size_doc[] =
    "The size in bytes of the column associated to this column on the server.\n\n"
    "Set to a negative value for variable-size types.";

static const char precision_doc[] =
    "Total number of significant digits in columns of type NUMERIC.\n\n"
    "None for other types.";

static const char scale_doc[] =
    "Count of decimal digits in the fractional part in columns of type NUMERIC.\n\n"
    "None for other types.";

static const char null_ok_doc[] =
    "Always none.";

static const char table_oid_doc[] =
    "The OID of the table from which the column was fetched.\n\n"
    "None if not available";

static const char table_column_doc[] =
    "The number (within its table) of the column making up the result\n\n"
    "None if not available. Note that PostgreSQL column numbers start at 1";


static PyMemberDef column_members[] = {
    { "name", T_OBJECT, offsetof(columnObject, name), READONLY, (char *)name_doc },
    { "type_code", T_OBJECT, offsetof(columnObject, type_code), READONLY, (char *)type_code_doc },
    { "display_size", T_OBJECT, offsetof(columnObject, display_size), READONLY, (char *)display_size_doc },
    { "internal_size", T_OBJECT, offsetof(columnObject, internal_size), READONLY, (char *)internal_size_doc },
    { "precision", T_OBJECT, offsetof(columnObject, precision), READONLY, (char *)precision_doc },
    { "scale", T_OBJECT, offsetof(columnObject, scale), READONLY, (char *)scale_doc },
    { "null_ok", T_OBJECT, offsetof(columnObject, null_ok), READONLY, (char *)null_ok_doc },
    { "table_oid", T_OBJECT, offsetof(columnObject, table_oid), READONLY, (char *)table_oid_doc },
    { "table_column", T_OBJECT, offsetof(columnObject, table_column), READONLY, (char *)table_column_doc },
    { NULL }
};


static PyObject *
column_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    return type->tp_alloc(type, 0);
}


static int
column_init(columnObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *name = NULL;
    PyObject *type_code = NULL;
    PyObject *display_size = NULL;
    PyObject *internal_size = NULL;
    PyObject *precision = NULL;
    PyObject *scale = NULL;
    PyObject *null_ok = NULL;
    PyObject *table_oid = NULL;
    PyObject *table_column = NULL;

    static char *kwlist[] = {
        "name", "type_code", "display_size", "internal_size",
        "precision", "scale", "null_ok", "table_oid", "table_column", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOOOOOOOO", kwlist,
            &name, &type_code, &display_size, &internal_size, &precision,
            &scale, &null_ok, &table_oid, &table_column)) {
        return -1;
    }

    Py_XINCREF(name); self->name = name;
    Py_XINCREF(type_code); self->type_code = type_code;
    Py_XINCREF(display_size); self->display_size = display_size;
    Py_XINCREF(internal_size); self->internal_size = internal_size;
    Py_XINCREF(precision); self->precision = precision;
    Py_XINCREF(scale); self->scale = scale;
    Py_XINCREF(null_ok); self->null_ok = null_ok;
    Py_XINCREF(table_oid); self->table_oid = table_oid;
    Py_XINCREF(table_column); self->table_column = table_column;

    return 0;
}


static void
column_dealloc(columnObject *self)
{
    Py_CLEAR(self->name);
    Py_CLEAR(self->type_code);
    Py_CLEAR(self->display_size);
    Py_CLEAR(self->internal_size);
    Py_CLEAR(self->precision);
    Py_CLEAR(self->scale);
    Py_CLEAR(self->null_ok);
    Py_CLEAR(self->table_oid);
    Py_CLEAR(self->table_column);

    Py_TYPE(self)->tp_free((PyObject *)self);
}


static PyObject*
column_repr(columnObject *self)
{
    PyObject *rv = NULL;
    PyObject *format = NULL;
    PyObject *args = NULL;
    PyObject *tmp;

    if (!(format = Text_FromUTF8("Column(name=%r, type_code=%r)"))) {
        goto exit;
    }

    if (!(args = PyTuple_New(2))) { goto exit; }

    tmp = self->name ? self->name : Py_None;
    Py_INCREF(tmp);
    PyTuple_SET_ITEM(args, 0, tmp);

    tmp = self->type_code ? self->type_code : Py_None;
    Py_INCREF(tmp);
    PyTuple_SET_ITEM(args, 1, tmp);

    rv = Text_Format(format, args);

exit:
    Py_XDECREF(args);
    Py_XDECREF(format);

    return rv;
}


static PyObject *
column_richcompare(columnObject *self, PyObject *other, int op)
{
    PyObject *rv = NULL;
    PyObject *tself = NULL;

    if (!(tself = PyObject_CallFunctionObjArgs(
            (PyObject *)&PyTuple_Type, (PyObject *)self, NULL))) {
        goto exit;
    }

    rv = PyObject_RichCompare(tself, other, op);

exit:
    Py_XDECREF(tself);
    return rv;
}


/* column description can be accessed as a 7 items tuple for DBAPI compatibility */

static Py_ssize_t
column_len(columnObject *self)
{
    return 7;
}


static PyObject *
column_getitem(columnObject *self, Py_ssize_t item)
{
    PyObject *rv = NULL;

    if (item < 0)
        item += 7;

    switch (item) {
    case 0:
        rv = self->name;
        break;
    case 1:
        rv = self->type_code;
        break;
    case 2:
        rv = self->display_size;
        break;
    case 3:
        rv = self->internal_size;
        break;
    case 4:
        rv = self->precision;
        break;
    case 5:
        rv = self->scale;
        break;
    case 6:
        rv = self->null_ok;
        break;
    default:
        PyErr_SetString(PyExc_IndexError, "index out of range");
        return NULL;
    }

    if (!rv) {
        rv = Py_None;
    }

    Py_INCREF(rv);
    return rv;
}


static PyObject*
column_subscript(columnObject* self, PyObject* item)
{
    PyObject *t = NULL;
    PyObject *rv = NULL;

    /* t = tuple(self) */
    if (!(t = PyObject_CallFunctionObjArgs(
            (PyObject *)&PyTuple_Type, (PyObject *)self, NULL))) {
        goto exit;
    }

    /* rv = t[item] */
    rv = PyObject_GetItem(t, item);

exit:
    Py_XDECREF(t);
    return rv;
}

static PyMappingMethods column_mapping = {
    (lenfunc)column_len,            /* mp_length */
    (binaryfunc)column_subscript,   /* mp_subscript */
    0                               /* mp_ass_subscript */
};

static PySequenceMethods column_sequence = {
    (lenfunc)column_len,       /* sq_length */
    0,                         /* sq_concat */
    0,                         /* sq_repeat */
    (ssizeargfunc)column_getitem, /* sq_item */
    0,                         /* sq_slice */
    0,                         /* sq_ass_item */
    0,                         /* sq_ass_slice */
    0,                         /* sq_contains */
    0,                         /* sq_inplace_concat */
    0,                         /* sq_inplace_repeat */
};


static PyObject *
column_getstate(columnObject *self, PyObject *dummy)
{
    return PyObject_CallFunctionObjArgs(
        (PyObject *)&PyTuple_Type, (PyObject *)self, NULL);
}


PyObject *
column_setstate(columnObject *self, PyObject *state)
{
    Py_ssize_t size;
    PyObject *rv = NULL;

    if (state == Py_None) {
        goto exit;
    }
    if (!PyTuple_Check(state)) {
        PyErr_SetString(PyExc_TypeError, "state is not a tuple");
        goto error;
    }

    size = PyTuple_GET_SIZE(state);

    if (size > 0) {
        Py_CLEAR(self->name);
        self->name = PyTuple_GET_ITEM(state, 0);
        Py_INCREF(self->name);
    }
    if (size > 1) {
        Py_CLEAR(self->type_code);
        self->type_code = PyTuple_GET_ITEM(state, 1);
        Py_INCREF(self->type_code);
    }
    if (size > 2) {
        Py_CLEAR(self->display_size);
        self->display_size = PyTuple_GET_ITEM(state, 2);
        Py_INCREF(self->display_size);
    }
    if (size > 3) {
        Py_CLEAR(self->internal_size);
        self->internal_size = PyTuple_GET_ITEM(state, 3);
        Py_INCREF(self->internal_size);
    }
    if (size > 4) {
        Py_CLEAR(self->precision);
        self->precision = PyTuple_GET_ITEM(state, 4);
        Py_INCREF(self->precision);
    }
    if (size > 5) {
        Py_CLEAR(self->scale);
        self->scale = PyTuple_GET_ITEM(state, 5);
        Py_INCREF(self->scale);
    }
    if (size > 6) {
        Py_CLEAR(self->null_ok);
        self->null_ok = PyTuple_GET_ITEM(state, 6);
        Py_INCREF(self->null_ok);
    }
    if (size > 7) {
        Py_CLEAR(self->table_oid);
        self->table_oid = PyTuple_GET_ITEM(state, 7);
        Py_INCREF(self->table_oid);
    }
    if (size > 8) {
        Py_CLEAR(self->table_column);
        self->table_column = PyTuple_GET_ITEM(state, 8);
        Py_INCREF(self->table_column);
    }

exit:
    rv = Py_None;
    Py_INCREF(rv);

error:
    return rv;
}


static PyMethodDef column_methods[] = {
    /* Make Column picklable. */
    {"__getstate__", (PyCFunction)column_getstate, METH_NOARGS },
    {"__setstate__", (PyCFunction)column_setstate, METH_O },
    {NULL}
};


PyTypeObject columnType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.Column",
    sizeof(columnObject), 0,
    (destructor)column_dealloc, /* tp_dealloc */
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)column_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    &column_sequence, /*tp_as_sequence*/
    &column_mapping,  /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    column_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    (richcmpfunc)column_richcompare, /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    column_methods, /*tp_methods*/
    column_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    (initproc)column_init, /*tp_init*/
    0,          /*tp_alloc*/
    column_new, /*tp_new*/
};
