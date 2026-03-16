/* adapter_int.c - psycopg pint type wrapper implementation
 *
 * Copyright (C) 2011-2019 Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

#include "psycopg/adapter_pint.h"
#include "psycopg/microprotocols_proto.h"


/** the Int object **/

static PyObject *
pint_getquoted(pintObject *self, PyObject *args)
{
    PyObject *res = NULL;

    /* Convert subclass to int to handle IntEnum and other subclasses
     * whose str() is not the number. */
    if (PyLong_CheckExact(self->wrapped)) {
        res = PyObject_Str(self->wrapped);
    } else {
        PyObject *tmp;
        if (!(tmp = PyObject_CallFunctionObjArgs(
                (PyObject *)&PyLong_Type, self->wrapped, NULL))) {
            goto exit;
        }
        res = PyObject_Str(tmp);
        Py_DECREF(tmp);
    }

    if (!res) {
        goto exit;
    }

    /* unicode to bytes */
    {
        PyObject *tmp = PyUnicode_AsUTF8String(res);
        Py_DECREF(res);
        if (!(res = tmp)) {
            goto exit;
        }
    }

    if ('-' == Bytes_AS_STRING(res)[0]) {
        /* Prepend a space in front of negative numbers (ticket #57) */
        PyObject *tmp;
        if (!(tmp = Bytes_FromString(" "))) {
            Py_DECREF(res);
            res = NULL;
            goto exit;
        }
        Bytes_ConcatAndDel(&tmp, res);
        if (!(res = tmp)) {
            goto exit;
        }
    }

exit:
    return res;
}

static PyObject *
pint_str(pintObject *self)
{
    return psyco_ensure_text(pint_getquoted(self, NULL));
}

static PyObject *
pint_conform(pintObject *self, PyObject *args)
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

/** the int object */

/* object member list */

static struct PyMemberDef pintObject_members[] = {
    {"adapted", T_OBJECT, offsetof(pintObject, wrapped), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef pintObject_methods[] = {
    {"getquoted", (PyCFunction)pint_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL-quoted string"},
    {"__conform__", (PyCFunction)pint_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
pint_setup(pintObject *self, PyObject *obj)
{
    Dprintf("pint_setup: init pint object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );

    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("pint_setup: good pint object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );
    return 0;
}

static void
pint_dealloc(PyObject* obj)
{
    pintObject *self = (pintObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("pint_dealloc: deleted pint object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
      );

    Py_TYPE(obj)->tp_free(obj);
}

static int
pint_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *o;

    if (!PyArg_ParseTuple(args, "O", &o))
        return -1;

    return pint_setup((pintObject *)obj, o);
}

static PyObject *
pint_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define pintType_doc \
"Int(str) -> new Int adapter object"

PyTypeObject pintType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.Int",
    sizeof(pintObject), 0,
    pint_dealloc, /*tp_dealloc*/
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
    (reprfunc)pint_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    pintType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    pintObject_methods, /*tp_methods*/
    pintObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    pint_init, /*tp_init*/
    0,          /*tp_alloc*/
    pint_new, /*tp_new*/
};
