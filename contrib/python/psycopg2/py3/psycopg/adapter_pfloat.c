/* adapter_float.c - psycopg pfloat type wrapper implementation
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

#include "psycopg/adapter_pfloat.h"
#include "psycopg/microprotocols_proto.h"

#include <floatobject.h>
#include <math.h>


/** the Float object **/

static PyObject *
pfloat_getquoted(pfloatObject *self, PyObject *args)
{
    PyObject *rv;
    double n = PyFloat_AsDouble(self->wrapped);
    if (isnan(n))
        rv = Bytes_FromString("'NaN'::float");
    else if (isinf(n)) {
        if (n > 0)
            rv = Bytes_FromString("'Infinity'::float");
        else
            rv = Bytes_FromString("'-Infinity'::float");
    }
    else {
        if (!(rv = PyObject_Repr(self->wrapped))) {
            goto exit;
        }

        /* unicode to bytes */
        {
            PyObject *tmp = PyUnicode_AsUTF8String(rv);
            Py_DECREF(rv);
            if (!(rv = tmp)) {
                goto exit;
            }
        }

        if ('-' == Bytes_AS_STRING(rv)[0]) {
            /* Prepend a space in front of negative numbers (ticket #57) */
            PyObject *tmp;
            if (!(tmp = Bytes_FromString(" "))) {
                Py_DECREF(rv);
                rv = NULL;
                goto exit;
            }
            Bytes_ConcatAndDel(&tmp, rv);
            if (!(rv = tmp)) {
                goto exit;
            }
        }
    }

exit:
    return rv;
}

static PyObject *
pfloat_str(pfloatObject *self)
{
    return psyco_ensure_text(pfloat_getquoted(self, NULL));
}

static PyObject *
pfloat_conform(pfloatObject *self, PyObject *args)
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

/** the Float object */

/* object member list */

static struct PyMemberDef pfloatObject_members[] = {
    {"adapted", T_OBJECT, offsetof(pfloatObject, wrapped), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef pfloatObject_methods[] = {
    {"getquoted", (PyCFunction)pfloat_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL-quoted string"},
    {"__conform__", (PyCFunction)pfloat_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
pfloat_setup(pfloatObject *self, PyObject *obj)
{
    Dprintf("pfloat_setup: init pfloat object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );

    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("pfloat_setup: good pfloat object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );
    return 0;
}

static void
pfloat_dealloc(PyObject* obj)
{
    pfloatObject *self = (pfloatObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("pfloat_dealloc: deleted pfloat object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
      );

    Py_TYPE(obj)->tp_free(obj);
}

static int
pfloat_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *o;

    if (!PyArg_ParseTuple(args, "O", &o))
        return -1;

    return pfloat_setup((pfloatObject *)obj, o);
}

static PyObject *
pfloat_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define pfloatType_doc \
"Float(str) -> new Float adapter object"

PyTypeObject pfloatType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.Float",
    sizeof(pfloatObject), 0,
    pfloat_dealloc, /*tp_dealloc*/
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
    (reprfunc)pfloat_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    pfloatType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    pfloatObject_methods, /*tp_methods*/
    pfloatObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    pfloat_init, /*tp_init*/
    0,          /*tp_alloc*/
    pfloat_new, /*tp_new*/
};
