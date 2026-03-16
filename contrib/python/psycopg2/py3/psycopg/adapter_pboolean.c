/* adapter_pboolean.c - psycopg boolean type wrapper implementation
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

#include "psycopg/adapter_pboolean.h"
#include "psycopg/microprotocols_proto.h"

#include <string.h>


/** the Boolean object **/

static PyObject *
pboolean_getquoted(pbooleanObject *self, PyObject *args)
{
    if (PyObject_IsTrue(self->wrapped)) {
        return Bytes_FromString("true");
    }
    else {
        return Bytes_FromString("false");
    }
}

static PyObject *
pboolean_str(pbooleanObject *self)
{
    return psyco_ensure_text(pboolean_getquoted(self, NULL));
}

static PyObject *
pboolean_conform(pbooleanObject *self, PyObject *args)
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

/** the Boolean object */

/* object member list */

static struct PyMemberDef pbooleanObject_members[] = {
    {"adapted", T_OBJECT, offsetof(pbooleanObject, wrapped), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef pbooleanObject_methods[] = {
    {"getquoted", (PyCFunction)pboolean_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL-quoted string"},
    {"__conform__", (PyCFunction)pboolean_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
pboolean_setup(pbooleanObject *self, PyObject *obj)
{
    Dprintf("pboolean_setup: init pboolean object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );

    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("pboolean_setup: good pboolean object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );
    return 0;
}

static void
pboolean_dealloc(PyObject* obj)
{
    pbooleanObject *self = (pbooleanObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("pboolean_dealloc: deleted pboolean object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
      );

    Py_TYPE(obj)->tp_free(obj);
}

static int
pboolean_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *o;

    if (!PyArg_ParseTuple(args, "O", &o))
        return -1;

    return pboolean_setup((pbooleanObject *)obj, o);
}

static PyObject *
pboolean_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define pbooleanType_doc \
"Boolean(str) -> new Boolean adapter object"

PyTypeObject pbooleanType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.Boolean",
    sizeof(pbooleanObject), 0,
    pboolean_dealloc, /*tp_dealloc*/
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
    (reprfunc)pboolean_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    pbooleanType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    pbooleanObject_methods, /*tp_methods*/
    pbooleanObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    pboolean_init, /*tp_init*/
    0,          /*tp_alloc*/
    pboolean_new, /*tp_new*/
};
