/* microprotocol_proto.c - psycopg protocols
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

#include "psycopg/microprotocols_proto.h"

#include <string.h>


/** void protocol implementation **/


/* getquoted - return quoted representation for object */

#define isqlquote_getquoted_doc \
"getquoted() -- return SQL-quoted representation of this object"

static PyObject *
isqlquote_getquoted(isqlquoteObject *self, PyObject *args)
{
    Py_RETURN_NONE;
}

/* getbinary - return quoted representation for object */

#define isqlquote_getbinary_doc \
"getbinary() -- return SQL-quoted binary representation of this object"

static PyObject *
isqlquote_getbinary(isqlquoteObject *self, PyObject *args)
{
    Py_RETURN_NONE;
}

/* getbuffer - return quoted representation for object */

#define isqlquote_getbuffer_doc \
"getbuffer() -- return this object"

static PyObject *
isqlquote_getbuffer(isqlquoteObject *self, PyObject *args)
{
    Py_RETURN_NONE;
}



/** the ISQLQuote object **/


/* object method list */

static struct PyMethodDef isqlquoteObject_methods[] = {
    {"getquoted", (PyCFunction)isqlquote_getquoted,
     METH_NOARGS, isqlquote_getquoted_doc},
    {"getbinary", (PyCFunction)isqlquote_getbinary,
     METH_NOARGS, isqlquote_getbinary_doc},
    {"getbuffer", (PyCFunction)isqlquote_getbuffer,
     METH_NOARGS, isqlquote_getbuffer_doc},
    {NULL}
};

/* object member list */

static struct PyMemberDef isqlquoteObject_members[] = {
    /* DBAPI-2.0 extensions (exception objects) */
    {"_wrapped", T_OBJECT, offsetof(isqlquoteObject, wrapped), READONLY},
    {NULL}
};

/* initialization and finalization methods */

static int
isqlquote_setup(isqlquoteObject *self, PyObject *wrapped)
{
    self->wrapped = wrapped;
    Py_INCREF(wrapped);

    return 0;
}

static void
isqlquote_dealloc(PyObject* obj)
{
    isqlquoteObject *self = (isqlquoteObject *)obj;

    Py_XDECREF(self->wrapped);

    Py_TYPE(obj)->tp_free(obj);
}

static int
isqlquote_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *wrapped = NULL;

    if (!PyArg_ParseTuple(args, "O", &wrapped))
        return -1;

    return isqlquote_setup((isqlquoteObject *)obj, wrapped);
}

static PyObject *
isqlquote_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define isqlquoteType_doc \
"Abstract ISQLQuote protocol\n\n" \
"An object conform to this protocol should expose a ``getquoted()`` method\n" \
"returning the SQL representation of the object.\n\n"

PyTypeObject isqlquoteType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.ISQLQuote",
    sizeof(isqlquoteObject), 0,
    isqlquote_dealloc, /*tp_dealloc*/
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
    isqlquoteType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    isqlquoteObject_methods, /*tp_methods*/
    isqlquoteObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    isqlquote_init, /*tp_init*/
    0,          /*tp_alloc*/
    isqlquote_new, /*tp_new*/
};
