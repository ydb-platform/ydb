/* adapter_asis.c - adapt types as they are
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

#include "psycopg/adapter_asis.h"
#include "psycopg/microprotocols_proto.h"

#include <string.h>


/** the AsIs object **/

static PyObject *
asis_getquoted(asisObject *self, PyObject *args)
{
    PyObject *rv;
    if (self->wrapped == Py_None) {
        Py_INCREF(psyco_null);
        rv = psyco_null;
    }
    else {
        rv = PyObject_Str(self->wrapped);
        /* unicode to bytes */
        if (rv) {
            PyObject *tmp = PyUnicode_AsUTF8String(rv);
            Py_DECREF(rv);
            rv = tmp;
        }
    }

    return rv;
}

static PyObject *
asis_str(asisObject *self)
{
    return psyco_ensure_text(asis_getquoted(self, NULL));
}

static PyObject *
asis_conform(asisObject *self, PyObject *args)
{
    PyObject *res, *proto;

    if (!PyArg_ParseTuple(args, "O", &proto)) return NULL;

    if (proto == (PyObject*)&isqlquoteType)
        res = (PyObject*)self;
    else
        res = Py_None;

    Py_INCREF(res);
    return res;
}

/** the AsIs object */

/* object member list */

static struct PyMemberDef asisObject_members[] = {
    {"adapted", T_OBJECT, offsetof(asisObject, wrapped), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef asisObject_methods[] = {
    {"getquoted", (PyCFunction)asis_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL-quoted string"},
    {"__conform__", (PyCFunction)asis_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
asis_setup(asisObject *self, PyObject *obj)
{
    Dprintf("asis_setup: init asis object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );

    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("asis_setup: good asis object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );
    return 0;
}

static void
asis_dealloc(PyObject* obj)
{
    asisObject *self = (asisObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("asis_dealloc: deleted asis object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
      );

    Py_TYPE(obj)->tp_free(obj);
}

static int
asis_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *o;

    if (!PyArg_ParseTuple(args, "O", &o))
        return -1;

    return asis_setup((asisObject *)obj, o);
}

static PyObject *
asis_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define asisType_doc \
"AsIs(str) -> new AsIs adapter object"

PyTypeObject asisType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.AsIs",
    sizeof(asisObject), 0,
    asis_dealloc, /*tp_dealloc*/
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
    (reprfunc)asis_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    asisType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    asisObject_methods, /*tp_methods*/
    asisObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    asis_init, /*tp_init*/
    0,          /*tp_alloc*/
    asis_new, /*tp_new*/
};
