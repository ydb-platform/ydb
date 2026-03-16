/* This code was originally copied from Pendulum
(https://github.com/sdispater/pendulum/blob/13ff4a0250177f77e4ff2e7bd1f442d954e66b22/pendulum/parsing/_iso8601.c#L176)
Pendulum (like ciso8601) is MIT licensed, so we have included a copy of its
license here.
*/

/*
Copyright (c) 2015 Sébastien Eustace

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include "timezone.h"

#include <Python.h>
#include <datetime.h>
#include <structmember.h>

#define SECS_PER_MIN                 60
#define SECS_PER_HOUR                (60 * SECS_PER_MIN)
#define TWENTY_FOUR_HOURS_IN_SECONDS 86400

#define PY_VERSION_AT_LEAST_36 \
    ((PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 6) || PY_MAJOR_VERSION > 3)

/*
 * class FixedOffset(tzinfo):
 */
typedef struct {
    // Seconds offset from UTC.
    // Must be in range (-86400, 86400) seconds exclusive.
    // i.e., (-1440, 1440) minutes exclusive.
    PyObject_HEAD int offset;
} FixedOffset;

static int
FixedOffset_init(FixedOffset *self, PyObject *args, PyObject *kwargs)
{
    int offset;
    if (!PyArg_ParseTuple(args, "i", &offset))
        return -1;

    if (abs(offset) >= TWENTY_FOUR_HOURS_IN_SECONDS) {
        PyErr_Format(PyExc_ValueError,
                     "offset must be an integer in the range (-86400, 86400), "
                     "exclusive");
        return -1;
    }

    self->offset = offset;
    return 0;
}

static PyObject *
FixedOffset_utcoffset(FixedOffset *self, PyObject *dt)
{
    return PyDelta_FromDSU(0, self->offset, 0);
}

static PyObject *
FixedOffset_dst(FixedOffset *self, PyObject *dt)
{
    Py_RETURN_NONE;
}

static PyObject *
FixedOffset_fromutc(FixedOffset *self, PyDateTime_DateTime *dt)
{
    if (!PyDateTime_Check(dt)) {
        PyErr_SetString(PyExc_TypeError,
                        "fromutc: argument must be a datetime");
        return NULL;
    }
    if (!dt->hastzinfo || dt->tzinfo != (PyObject *)self) {
        PyErr_SetString(PyExc_ValueError,
                        "fromutc: dt.tzinfo "
                        "is not self");
        return NULL;
    }

    return PyNumber_Add((PyObject *)dt,
                        FixedOffset_utcoffset(self, (PyObject *)self));
}

static PyObject *
FixedOffset_tzname(FixedOffset *self, PyObject *dt)
{
    int offset = self->offset;

    if (offset == 0) {
#if PY_VERSION_AT_LEAST_36
        return PyUnicode_FromString("UTC");
#elif PY_MAJOR_VERSION >= 3
        return PyUnicode_FromString("UTC+00:00");
#else
        return PyString_FromString("UTC+00:00");
#endif
    }
    else {
        char result_tzname[10] = {0};
        char sign = '+';

        if (offset < 0) {
            sign = '-';
            offset *= -1;
        }
        snprintf(result_tzname, 10, "UTC%c%02u:%02u", sign,
                 (offset / SECS_PER_HOUR) & 31,
                 offset / SECS_PER_MIN % SECS_PER_MIN);
#if PY_MAJOR_VERSION >= 3
        return PyUnicode_FromString(result_tzname);
#else
        return PyString_FromString(result_tzname);
#endif
    }
}

static PyObject *
FixedOffset_repr(FixedOffset *self)
{
    return FixedOffset_tzname(self, NULL);
}

static PyObject *
FixedOffset_getinitargs(FixedOffset *self)
{
    PyObject *args = PyTuple_Pack(1, PyLong_FromLong(self->offset));
    return args;
}

/*
 * Class member / class attributes
 */
static PyMemberDef FixedOffset_members[] = {
    {"offset", T_INT, offsetof(FixedOffset, offset), 0, "UTC offset"}, {NULL}};

/*
 * Class methods
 */
static PyMethodDef FixedOffset_methods[] = {
    {"utcoffset", (PyCFunction)FixedOffset_utcoffset, METH_O,
     PyDoc_STR("Return fixed offset.")},

    {"dst", (PyCFunction)FixedOffset_dst, METH_O, PyDoc_STR("Return None.")},

    {"fromutc", (PyCFunction)FixedOffset_fromutc, METH_O,
     PyDoc_STR("datetime in UTC -> datetime in local time.")},

    {"tzname", (PyCFunction)FixedOffset_tzname, METH_O,
     PyDoc_STR("Returns offset as 'UTC(+|-)HH:MM'")},

    {"__getinitargs__", (PyCFunction)FixedOffset_getinitargs, METH_NOARGS,
     PyDoc_STR("pickle support")},

    {NULL}};

static PyTypeObject FixedOffset_type = {
    PyVarObject_HEAD_INIT(NULL, 0) "ciso8601.FixedOffset", /* tp_name */
    sizeof(FixedOffset),                                   /* tp_basicsize */
    0,                                                     /* tp_itemsize */
    0,                                                     /* tp_dealloc */
    0,                                                     /* tp_print */
    0,                                                     /* tp_getattr */
    0,                                                     /* tp_setattr */
    0,                                                     /* tp_as_async */
    (reprfunc)FixedOffset_repr,                            /* tp_repr */
    0,                                                     /* tp_as_number */
    0,                                                     /* tp_as_sequence */
    0,                                                     /* tp_as_mapping */
    0,                                                     /* tp_hash  */
    0,                                                     /* tp_call */
    (reprfunc)FixedOffset_repr,                            /* tp_str */
    0,                                                     /* tp_getattro */
    0,                                                     /* tp_setattro */
    0,                                                     /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,              /* tp_flags */
    "TZInfo with fixed offset",                            /* tp_doc */
};

/*
 * Instantiate new FixedOffset_type object
 * Skip overhead of calling PyObject_New and PyObject_Init.
 * Directly allocate object.
 * Note that this also doesn't do any validation of the offset parameter.
 * Callers must ensure that offset is within \
 * the range (-86400, 86400), exclusive.
 */
PyObject *
new_fixed_offset_ex(int offset, PyTypeObject *type)
{
    FixedOffset *self = (FixedOffset *)(type->tp_alloc(type, 0));

    if (self != NULL)
        self->offset = offset;

    return (PyObject *)self;
}

PyObject *
new_fixed_offset(int offset)
{
    return new_fixed_offset_ex(offset, &FixedOffset_type);
}

/* ------------------------------------------------------------- */

int
initialize_timezone_code(PyObject *module)
{
    PyDateTime_IMPORT;
    FixedOffset_type.tp_new = PyType_GenericNew;
    FixedOffset_type.tp_base = PyDateTimeAPI->TZInfoType;
    FixedOffset_type.tp_methods = FixedOffset_methods;
    FixedOffset_type.tp_members = FixedOffset_members;
    FixedOffset_type.tp_init = (initproc)FixedOffset_init;

    if (PyType_Ready(&FixedOffset_type) < 0)
        return -1;

    Py_INCREF(&FixedOffset_type);
    if (PyModule_AddObject(module, "FixedOffset",
                           (PyObject *)&FixedOffset_type) < 0) {
        Py_DECREF(module);
        Py_DECREF(&FixedOffset_type);
        return -1;
    }

    return 0;
}
