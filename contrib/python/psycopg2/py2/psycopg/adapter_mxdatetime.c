/* adapter_mxdatetime.c - mx date/time objects
 *
 * Copyright (C) 2003-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020 The Psycopg Team
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

#include "psycopg/adapter_mxdatetime.h"
#include "psycopg/microprotocols_proto.h"

#include <mxDateTime.h>
#include <string.h>


/* Return 0 on success, -1 on failure, but don't set an exception */

int
psyco_adapter_mxdatetime_init(void)
{
    if (mxDateTime_ImportModuleAndAPI()) {
        Dprintf("psyco_adapter_mxdatetime_init: mx.DateTime initialization failed");
        PyErr_Clear();
        return -1;
    }
    return 0;
}


/* mxdatetime_str, mxdatetime_getquoted - return result of quoting */

static PyObject *
mxdatetime_str(mxdatetimeObject *self)
{
    mxDateTimeObject *dt;
    mxDateTimeDeltaObject *dtd;
    char buf[128] = { 0, };

    switch (self->type) {

    case PSYCO_MXDATETIME_DATE:
        dt = (mxDateTimeObject *)self->wrapped;
        if (dt->year >= 1)
            PyOS_snprintf(buf, sizeof(buf) - 1, "'%04ld-%02d-%02d'::date",
                          dt->year, (int)dt->month, (int)dt->day);
        else
            PyOS_snprintf(buf, sizeof(buf) - 1, "'%04ld-%02d-%02d BC'::date",
                          1 - dt->year, (int)dt->month, (int)dt->day);
        break;

    case PSYCO_MXDATETIME_TIMESTAMP:
        dt = (mxDateTimeObject *)self->wrapped;
        if (dt->year >= 1)
            PyOS_snprintf(buf, sizeof(buf) - 1,
                          "'%04ld-%02d-%02dT%02d:%02d:%09.6f'::timestamp",
                          dt->year, (int)dt->month, (int)dt->day,
                          (int)dt->hour, (int)dt->minute, dt->second);
        else
            PyOS_snprintf(buf, sizeof(buf) - 1,
                          "'%04ld-%02d-%02dT%02d:%02d:%09.6f BC'::timestamp",
                          1 - dt->year, (int)dt->month, (int)dt->day,
                          (int)dt->hour, (int)dt->minute, dt->second);
        break;

    case PSYCO_MXDATETIME_TIME:
    case PSYCO_MXDATETIME_INTERVAL:
        /* given the limitation of the mx.DateTime module that uses the same
           type for both time and delta values we need to do some black magic
           and make sure we're not using an adapt()ed interval as a simple
           time */
        dtd = (mxDateTimeDeltaObject *)self->wrapped;
        if (0 <= dtd->seconds && dtd->seconds < 24*3600) {
            PyOS_snprintf(buf, sizeof(buf) - 1, "'%02d:%02d:%09.6f'::time",
                          (int)dtd->hour, (int)dtd->minute, dtd->second);
        } else {
            double ss = dtd->hour*3600.0 + dtd->minute*60.0 + dtd->second;

            if (dtd->seconds >= 0)
                PyOS_snprintf(buf, sizeof(buf) - 1, "'%ld days %.6f seconds'::interval",
                              dtd->day, ss);
            else
                PyOS_snprintf(buf, sizeof(buf) - 1, "'-%ld days -%.6f seconds'::interval",
                              dtd->day, ss);
        }
        break;
    }

    return PyString_FromString(buf);
}

static PyObject *
mxdatetime_getquoted(mxdatetimeObject *self, PyObject *args)
{
    return mxdatetime_str(self);
}

static PyObject *
mxdatetime_conform(mxdatetimeObject *self, PyObject *args)
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

/** the MxDateTime object **/

/* object member list */

static struct PyMemberDef mxdatetimeObject_members[] = {
    {"adapted", T_OBJECT, offsetof(mxdatetimeObject, wrapped), READONLY},
    {"type", T_INT, offsetof(mxdatetimeObject, type), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef mxdatetimeObject_methods[] = {
    {"getquoted", (PyCFunction)mxdatetime_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL date/time"},
    {"__conform__", (PyCFunction)mxdatetime_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
mxdatetime_setup(mxdatetimeObject *self, PyObject *obj, int type)
{
    Dprintf("mxdatetime_setup: init mxdatetime object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );

    self->type = type;
    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("mxdatetime_setup: good mxdatetime object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );
    return 0;
}

static void
mxdatetime_dealloc(PyObject* obj)
{
    mxdatetimeObject *self = (mxdatetimeObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("mxdatetime_dealloc: deleted mxdatetime object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj)
      );

    Py_TYPE(obj)->tp_free(obj);
}

static int
mxdatetime_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *mx;
    int type = -1; /* raise an error if type was not passed! */

    if (!PyArg_ParseTuple(args, "O|i", &mx, &type))
        return -1;

    return mxdatetime_setup((mxdatetimeObject *)obj, mx, type);
}

static PyObject *
mxdatetime_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define mxdatetimeType_doc \
"MxDateTime(mx, type) -> new mx.DateTime wrapper object"

PyTypeObject mxdatetimeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2._psycopg.MxDateTime",
    sizeof(mxdatetimeObject), 0,
    mxdatetime_dealloc, /*tp_dealloc*/
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
    (reprfunc)mxdatetime_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    mxdatetimeType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    mxdatetimeObject_methods, /*tp_methods*/
    mxdatetimeObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    mxdatetime_init, /*tp_init*/
    0,          /*tp_alloc*/
    mxdatetime_new, /*tp_new*/
};


/** module-level functions **/

PyObject *
psyco_DateFromMx(PyObject *self, PyObject *args)
{
    PyObject *mx;

    if (!PyArg_ParseTuple(args, "O!", mxDateTime.DateTime_Type, &mx))
        return NULL;

    return PyObject_CallFunction((PyObject *)&mxdatetimeType, "Oi", mx,
                                 PSYCO_MXDATETIME_DATE);
}

PyObject *
psyco_TimeFromMx(PyObject *self, PyObject *args)
{
    PyObject *mx;

    if (!PyArg_ParseTuple(args, "O!", mxDateTime.DateTimeDelta_Type, &mx))
        return NULL;

    return PyObject_CallFunction((PyObject *)&mxdatetimeType, "Oi", mx,
                                 PSYCO_MXDATETIME_TIME);
}

PyObject *
psyco_TimestampFromMx(PyObject *self, PyObject *args)
{
    PyObject *mx;

    if (!PyArg_ParseTuple(args, "O!", mxDateTime.DateTime_Type, &mx))
        return NULL;

    return PyObject_CallFunction((PyObject *)&mxdatetimeType, "Oi", mx,
                                 PSYCO_MXDATETIME_TIMESTAMP);
}

PyObject *
psyco_IntervalFromMx(PyObject *self, PyObject *args)
{
    PyObject *mx;

    if (!PyArg_ParseTuple(args, "O!", mxDateTime.DateTime_Type, &mx))
        return NULL;

    return PyObject_CallFunction((PyObject *)&mxdatetimeType, "Oi", mx,
                                 PSYCO_MXDATETIME_INTERVAL);
}
