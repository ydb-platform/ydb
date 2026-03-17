/* adapter_datetime.c - python date/time objects
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

#include "psycopg/adapter_datetime.h"
#include "psycopg/microprotocols_proto.h"

#include <datetime.h>

#include <time.h>
#include <string.h>


RAISES_NEG int
adapter_datetime_init(void)
{
    PyDateTime_IMPORT;

    if (!PyDateTimeAPI) {
        PyErr_SetString(PyExc_ImportError, "datetime initialization failed");
        return -1;
    }
    return 0;
}

/* datetime_str, datetime_getquoted - return result of quoting */

static PyObject *
_pydatetime_string_date_time(pydatetimeObject *self)
{
    PyObject *rv = NULL;
    PyObject *iso = NULL;
    PyObject *tz;

    /* Select the right PG type to cast into. */
    char *fmt = NULL;
    switch (self->type) {
    case PSYCO_DATETIME_TIME:
        tz = PyObject_GetAttrString(self->wrapped, "tzinfo");
        if (!tz) { goto error; }
        fmt = (tz == Py_None) ? "'%s'::time" : "'%s'::timetz";
        Py_DECREF(tz);
        break;
    case PSYCO_DATETIME_DATE:
        fmt = "'%s'::date";
        break;
    case PSYCO_DATETIME_TIMESTAMP:
        tz = PyObject_GetAttrString(self->wrapped, "tzinfo");
        if (!tz) { goto error; }
        fmt = (tz == Py_None) ? "'%s'::timestamp" : "'%s'::timestamptz";
        Py_DECREF(tz);
        break;
    }

    if (!(iso = psyco_ensure_bytes(
            PyObject_CallMethod(self->wrapped, "isoformat", NULL)))) {
        goto error;
    }

    rv = Bytes_FromFormat(fmt, Bytes_AsString(iso));

    Py_DECREF(iso);
    return rv;

error:
    Py_XDECREF(iso);
    return rv;
}

static PyObject *
_pydatetime_string_delta(pydatetimeObject *self)
{
    PyDateTime_Delta *obj = (PyDateTime_Delta*)self->wrapped;

    char buffer[8];
    int i;
    int a = PyDateTime_DELTA_GET_MICROSECONDS(obj);

    for (i=0; i < 6 ; i++) {
        buffer[5-i] = '0' + (a % 10);
        a /= 10;
    }
    buffer[6] = '\0';

    return Bytes_FromFormat("'%d days %d.%s seconds'::interval",
                            PyDateTime_DELTA_GET_DAYS(obj),
                            PyDateTime_DELTA_GET_SECONDS(obj),
                            buffer);
}

static PyObject *
pydatetime_getquoted(pydatetimeObject *self, PyObject *args)
{
    if (self->type <= PSYCO_DATETIME_TIMESTAMP) {
        return _pydatetime_string_date_time(self);
    }
    else {
        return _pydatetime_string_delta(self);
    }
}

static PyObject *
pydatetime_str(pydatetimeObject *self)
{
    return psyco_ensure_text(pydatetime_getquoted(self, NULL));
}

static PyObject *
pydatetime_conform(pydatetimeObject *self, PyObject *args)
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

/** the DateTime wrapper object **/

/* object member list */

static struct PyMemberDef pydatetimeObject_members[] = {
    {"adapted", T_OBJECT, offsetof(pydatetimeObject, wrapped), READONLY},
    {"type", T_INT, offsetof(pydatetimeObject, type), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef pydatetimeObject_methods[] = {
    {"getquoted", (PyCFunction)pydatetime_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL date/time"},
    {"__conform__", (PyCFunction)pydatetime_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
pydatetime_setup(pydatetimeObject *self, PyObject *obj, int type)
{
    Dprintf("pydatetime_setup: init datetime object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self));

    self->type = type;
    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("pydatetime_setup: good pydatetime object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self));
    return 0;
}

static void
pydatetime_dealloc(PyObject* obj)
{
    pydatetimeObject *self = (pydatetimeObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("mpydatetime_dealloc: deleted pydatetime object at %p, "
            "refcnt = " FORMAT_CODE_PY_SSIZE_T, obj, Py_REFCNT(obj));

    Py_TYPE(obj)->tp_free(obj);
}

static int
pydatetime_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *dt;
    int type = -1; /* raise an error if type was not passed! */

    if (!PyArg_ParseTuple(args, "O|i", &dt, &type))
        return -1;

    return pydatetime_setup((pydatetimeObject *)obj, dt, type);
}

static PyObject *
pydatetime_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define pydatetimeType_doc \
"datetime(datetime, type) -> new datetime wrapper object"

PyTypeObject pydatetimeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2._psycopg.datetime",
    sizeof(pydatetimeObject), 0,
    pydatetime_dealloc, /*tp_dealloc*/
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
    (reprfunc)pydatetime_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    pydatetimeType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    pydatetimeObject_methods, /*tp_methods*/
    pydatetimeObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    pydatetime_init, /*tp_init*/
    0,          /*tp_alloc*/
    pydatetime_new, /*tp_new*/
};


/** module-level functions **/

PyObject *
psyco_Date(PyObject *self, PyObject *args)
{
    PyObject *res = NULL;
    int year, month, day;

    PyObject* obj = NULL;

    if (!PyArg_ParseTuple(args, "iii", &year, &month, &day))
        return NULL;

    obj = PyObject_CallFunction((PyObject*)PyDateTimeAPI->DateType,
                                "iii", year, month, day);

    if (obj) {
        res = PyObject_CallFunction((PyObject *)&pydatetimeType,
                                    "Oi", obj, PSYCO_DATETIME_DATE);
        Py_DECREF(obj);
    }

    return res;
}

PyObject *
psyco_Time(PyObject *self, PyObject *args)
{
    PyObject *res = NULL;
    PyObject *tzinfo = NULL;
    int hours, minutes=0;
    double micro, second=0.0;

    PyObject* obj = NULL;

    if (!PyArg_ParseTuple(args, "iid|O", &hours, &minutes, &second,
                          &tzinfo))
        return NULL;

    micro = (second - floor(second)) * 1000000.0;
    second = floor(second);

    if (tzinfo == NULL)
       obj = PyObject_CallFunction((PyObject*)PyDateTimeAPI->TimeType, "iiii",
            hours, minutes, (int)second, (int)round(micro));
    else
       obj = PyObject_CallFunction((PyObject*)PyDateTimeAPI->TimeType, "iiiiO",
            hours, minutes, (int)second, (int)round(micro), tzinfo);

    if (obj) {
        res = PyObject_CallFunction((PyObject *)&pydatetimeType,
                                    "Oi", obj, PSYCO_DATETIME_TIME);
        Py_DECREF(obj);
    }

    return res;
}

static PyObject *
_psyco_Timestamp(int year, int month, int day,
                 int hour, int minute, double second, PyObject *tzinfo)
{
    double micro;
    PyObject *obj;
    PyObject *res = NULL;

    micro = (second - floor(second)) * 1000000.0;
    second = floor(second);

    if (tzinfo == NULL)
        obj = PyObject_CallFunction((PyObject*)PyDateTimeAPI->DateTimeType,
            "iiiiiii",
            year, month, day, hour, minute, (int)second,
            (int)round(micro));
    else
        obj = PyObject_CallFunction((PyObject*)PyDateTimeAPI->DateTimeType,
            "iiiiiiiO",
            year, month, day, hour, minute, (int)second,
            (int)round(micro), tzinfo);

    if (obj) {
        res = PyObject_CallFunction((PyObject *)&pydatetimeType,
                                    "Oi", obj, PSYCO_DATETIME_TIMESTAMP);
        Py_DECREF(obj);
    }

    return res;
}

PyObject *
psyco_Timestamp(PyObject *self, PyObject *args)
{
    PyObject *tzinfo = NULL;
    int year, month, day;
    int hour=0, minute=0; /* default to midnight */
    double second=0.0;

    if (!PyArg_ParseTuple(args, "iii|iidO", &year, &month, &day,
                          &hour, &minute, &second, &tzinfo))
        return NULL;

    return _psyco_Timestamp(year, month, day, hour, minute, second, tzinfo);
}

PyObject *
psyco_DateFromTicks(PyObject *self, PyObject *args)
{
    PyObject *res = NULL;
    struct tm tm;
    time_t t;
    double ticks;

    if (!PyArg_ParseTuple(args, "d", &ticks))
        return NULL;

    t = (time_t)floor(ticks);
    if (localtime_r(&t, &tm)) {
        args = Py_BuildValue("iii", tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday);
        if (args) {
            res = psyco_Date(self, args);
            Py_DECREF(args);
        }
    }
    else {
        PyErr_SetString(InterfaceError, "failed localtime call");
    }

    return res;
}

PyObject *
psyco_TimeFromTicks(PyObject *self, PyObject *args)
{
    PyObject *res = NULL;
    struct tm tm;
    time_t t;
    double ticks;

    if (!PyArg_ParseTuple(args,"d", &ticks))
        return NULL;

    t = (time_t)floor(ticks);
    ticks -= (double)t;
    if (localtime_r(&t, &tm)) {
        args = Py_BuildValue("iid", tm.tm_hour, tm.tm_min,
                          (double)tm.tm_sec + ticks);
        if (args) {
            res = psyco_Time(self, args);
            Py_DECREF(args);
        }
    }
    else {
        PyErr_SetString(InterfaceError, "failed localtime call");
    }

    return res;
}

PyObject *
psyco_TimestampFromTicks(PyObject *self, PyObject *args)
{
    pydatetimeObject *wrapper = NULL;
    PyObject *dt_aware = NULL;
    PyObject *res = NULL;
    struct tm tm;
    time_t t;
    double ticks;

    if (!PyArg_ParseTuple(args, "d", &ticks))
        return NULL;

    t = (time_t)floor(ticks);
    ticks -= (double)t;
    if (!localtime_r(&t, &tm)) {
        PyErr_SetString(InterfaceError, "failed localtime call");
        goto exit;
    }

    /* Convert the tm to a wrapper containing a naive datetime.datetime */
    if (!(wrapper = (pydatetimeObject *)_psyco_Timestamp(
            tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, (double)tm.tm_sec + ticks, NULL))) {
        goto exit;
    }

    /* Localize the datetime and assign it back to the wrapper */
    if (!(dt_aware = PyObject_CallMethod(
            wrapper->wrapped, "astimezone", NULL))) {
        goto exit;
    }
    Py_CLEAR(wrapper->wrapped);
    wrapper->wrapped = dt_aware;
    dt_aware = NULL;

    /* the wrapper is ready to be returned */
    res = (PyObject *)wrapper;
    wrapper = NULL;

exit:
    Py_XDECREF(dt_aware);
    Py_XDECREF(wrapper);
    return res;
}

PyObject *
psyco_DateFromPy(PyObject *self, PyObject *args)
{
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "O!", PyDateTimeAPI->DateType, &obj))
        return NULL;

    return PyObject_CallFunction((PyObject *)&pydatetimeType, "Oi", obj,
                                 PSYCO_DATETIME_DATE);
}

PyObject *
psyco_TimeFromPy(PyObject *self, PyObject *args)
{
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "O!", PyDateTimeAPI->TimeType, &obj))
        return NULL;

    return PyObject_CallFunction((PyObject *)&pydatetimeType, "Oi", obj,
                                 PSYCO_DATETIME_TIME);
}

PyObject *
psyco_TimestampFromPy(PyObject *self, PyObject *args)
{
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "O!", PyDateTimeAPI->DateTimeType, &obj))
        return NULL;

    return PyObject_CallFunction((PyObject *)&pydatetimeType, "Oi", obj,
                                 PSYCO_DATETIME_TIMESTAMP);
}

PyObject *
psyco_IntervalFromPy(PyObject *self, PyObject *args)
{
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "O!", PyDateTimeAPI->DeltaType, &obj))
        return NULL;

    return PyObject_CallFunction((PyObject *)&pydatetimeType, "Oi", obj,
                                 PSYCO_DATETIME_INTERVAL);
}
