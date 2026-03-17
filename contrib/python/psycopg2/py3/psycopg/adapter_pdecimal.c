/* adapter_pdecimal.c - psycopg Decimal type wrapper implementation
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

#include "psycopg/adapter_pdecimal.h"
#include "psycopg/microprotocols_proto.h"

#include <floatobject.h>
#include <math.h>


/** the Decimal object **/

static PyObject *
pdecimal_getquoted(pdecimalObject *self, PyObject *args)
{
    PyObject *check, *res = NULL;
    check = PyObject_CallMethod(self->wrapped, "is_finite", NULL);
    if (check == Py_True) {
        if (!(res = PyObject_Str(self->wrapped))) {
            goto end;
        }
        goto output;
    }
    else if (check) {
        res = Bytes_FromString("'NaN'::numeric");
        goto end;
    }

    /* is_finite() was introduced 2.5.1 < somewhere <= 2.5.4.
     * We assume we are here because we didn't find the method. */
    PyErr_Clear();

    if (!(check = PyObject_CallMethod(self->wrapped, "_isnan", NULL))) {
        goto end;
    }
    if (PyObject_IsTrue(check)) {
        res = Bytes_FromString("'NaN'::numeric");
        goto end;
    }

    Py_DECREF(check);
    if (!(check = PyObject_CallMethod(self->wrapped, "_isinfinity", NULL))) {
        goto end;
    }
    if (PyObject_IsTrue(check)) {
        res = Bytes_FromString("'NaN'::numeric");
        goto end;
    }

    /* wrapped is finite */
    if (!(res = PyObject_Str(self->wrapped))) {
        goto end;
    }

    /* res may be unicode and may suffer for issue #57 */
output:

    /* unicode to bytes */
    {
        PyObject *tmp = PyUnicode_AsUTF8String(res);
        Py_DECREF(res);
        if (!(res = tmp)) {
            goto end;
        }
    }

    if ('-' == Bytes_AS_STRING(res)[0]) {
        /* Prepend a space in front of negative numbers (ticket #57) */
        PyObject *tmp;
        if (!(tmp = Bytes_FromString(" "))) {
            Py_DECREF(res);
            res = NULL;
            goto end;
        }
        Bytes_ConcatAndDel(&tmp, res);
        if (!(res = tmp)) {
            goto end;
        }
    }

end:
    Py_XDECREF(check);
    return res;
}

static PyObject *
pdecimal_str(pdecimalObject *self)
{
    return psyco_ensure_text(pdecimal_getquoted(self, NULL));
}

static PyObject *
pdecimal_conform(pdecimalObject *self, PyObject *args)
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

/** the Decimal object */

/* object member list */

static struct PyMemberDef pdecimalObject_members[] = {
    {"adapted", T_OBJECT, offsetof(pdecimalObject, wrapped), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef pdecimalObject_methods[] = {
    {"getquoted", (PyCFunction)pdecimal_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL-quoted string"},
    {"__conform__", (PyCFunction)pdecimal_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
pdecimal_setup(pdecimalObject *self, PyObject *obj)
{
    Dprintf("pdecimal_setup: init pdecimal object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );

    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("pdecimal_setup: good pdecimal object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );
    return 0;
}

static void
pdecimal_dealloc(PyObject* obj)
{
    pdecimalObject *self = (pdecimalObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("pdecimal_dealloc: deleted pdecimal object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
      );

    Py_TYPE(obj)->tp_free(obj);
}

static int
pdecimal_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *o;

    if (!PyArg_ParseTuple(args, "O", &o))
        return -1;

    return pdecimal_setup((pdecimalObject *)obj, o);
}

static PyObject *
pdecimal_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define pdecimalType_doc \
"Decimal(str) -> new Decimal adapter object"

PyTypeObject pdecimalType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2._psycopg.Decimal",
    sizeof(pdecimalObject), 0,
    pdecimal_dealloc, /*tp_dealloc*/
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
    (reprfunc)pdecimal_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    pdecimalType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    pdecimalObject_methods, /*tp_methods*/
    pdecimalObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    pdecimal_init, /*tp_init*/
    0,          /*tp_alloc*/
    pdecimal_new, /*tp_new*/
};
