/* typecast.c - basic utility functions related to typecasting
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

#include "psycopg/typecast.h"
#include "psycopg/cursor.h"

/* useful function used by some typecasters */

static const char *
skip_until_space2(const char *s, Py_ssize_t *len)
{
    while (*len > 0 && *s && *s != ' ') {
        s++; (*len)--;
    }
    return s;
}

static int
typecast_parse_date(const char* s, const char** t, Py_ssize_t* len,
                     int* year, int* month, int* day)
{
    int acc = -1, cz = 0;

    Dprintf("typecast_parse_date: len = " FORMAT_CODE_PY_SSIZE_T ", s = %s",
      *len, s);

    while (cz < 3 && *len > 0 && *s) {
        switch (*s) {
        case '-':
        case ' ':
        case 'T':
            if (cz == 0) *year = acc;
            else if (cz == 1) *month = acc;
            else if (cz == 2) *day = acc;
            acc = -1; cz++;
            break;
        default:
            acc = (acc == -1 ? 0 : acc*10) + ((int)*s - (int)'0');
            break;
        }

        s++; (*len)--;
    }

    if (acc != -1) {
        *day = acc;
        cz += 1;
    }

    /* Is this a BC date?  If so, adjust the year value.  However
     * Python datetime module does not support BC dates, so this will raise
     * an exception downstream. */
    if (*len >= 2 && s[*len-2] == 'B' && s[*len-1] == 'C')
        *year = -(*year);

    if (t != NULL) *t = s;

    return cz;
}

static int
typecast_parse_time(const char* s, const char** t, Py_ssize_t* len,
                     int* hh, int* mm, int* ss, int* us, int* tz)
{
    int acc = -1, cz = 0;
    int tzsign = 1, tzhh = 0, tzmm = 0, tzss = 0;
    int usd = 0;

    /* sets microseconds and timezone to 0 because they may be missing */
    *us = *tz = 0;

    Dprintf("typecast_parse_time: len = " FORMAT_CODE_PY_SSIZE_T ", s = %s",
      *len, s);

    while (cz < 7 && *len > 0 && *s) {
        switch (*s) {
        case ':':
            if (cz == 0) *hh = acc;
            else if (cz == 1) *mm = acc;
            else if (cz == 2) *ss = acc;
            else if (cz == 3) *us = acc;
            else if (cz == 4) tzhh = acc;
            else if (cz == 5) tzmm = acc;
            acc = -1; cz++;
            break;
        case '.':
            /* we expect seconds and if we don't get them we return an error */
            if (cz != 2) return -1;
            *ss = acc;
            acc = -1; cz++;
            break;
        case '+':
        case '-':
            /* seconds or microseconds here, anything else is an error */
            if (cz < 2 || cz > 3) return -1;
            if (*s == '-') tzsign = -1;
            if      (cz == 2) *ss = acc;
            else if (cz == 3) *us = acc;
            acc = -1; cz = 4;
            break;
        case ' ':
        case 'B':
        case 'C':
            /* Ignore the " BC" suffix, if passed -- it is handled
             * when parsing the date portion. */
            break;
        default:
            acc = (acc == -1 ? 0 : acc*10) + ((int)*s - (int)'0');
            if (cz == 3) usd += 1;
            break;
        }

        s++; (*len)--;
    }

    if (acc != -1) {
        if (cz == 0)      { *hh = acc; cz += 1; }
        else if (cz == 1) { *mm = acc; cz += 1; }
        else if (cz == 2) { *ss = acc; cz += 1; }
        else if (cz == 3) { *us = acc; cz += 1; }
        else if (cz == 4) { tzhh = acc; cz += 1; }
        else if (cz == 5) { tzmm = acc; cz += 1; }
        else if (cz == 6) tzss = acc;
    }
    if (t != NULL) *t = s;

    *tz = tzsign * (3600 * tzhh + 60 * tzmm + tzss);

    if (*us != 0) {
        while (usd++ < 6) *us *= 10;
    }

    /* 24:00:00 -> 00:00:00 (ticket #278) */
    if (*hh == 24) { *hh = 0; }

    return cz;
}

/** include casting objects **/
#include "psycopg/typecast_basic.c"
#include "psycopg/typecast_binary.c"
#include "psycopg/typecast_datetime.c"
#include "psycopg/typecast_array.c"

static long int typecast_default_DEFAULT[] = {0};
static typecastObject_initlist typecast_default = {
    "DEFAULT", typecast_default_DEFAULT, typecast_STRING_cast};

static PyObject *
typecast_UNKNOWN_cast(const char *str, Py_ssize_t len, PyObject *curs)
{
    Dprintf("typecast_UNKNOWN_cast: str = '%s',"
            " len = " FORMAT_CODE_PY_SSIZE_T, str, len);

    return typecast_default.cast(str, len, curs);
}

#include "psycopg/typecast_builtins.c"

#define typecast_PYDATETIMEARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_PYDATETIMETZARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_PYDATEARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_PYTIMEARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_PYINTERVALARRAY_cast typecast_GENERIC_ARRAY_cast

/* a list of initializers, used to make the typecasters accessible anyway */
static typecastObject_initlist typecast_pydatetime[] = {
    {"PYDATETIME", typecast_DATETIME_types, typecast_PYDATETIME_cast},
    {"PYDATETIMETZ", typecast_DATETIMETZ_types, typecast_PYDATETIMETZ_cast},
    {"PYTIME", typecast_TIME_types, typecast_PYTIME_cast},
    {"PYDATE", typecast_DATE_types, typecast_PYDATE_cast},
    {"PYINTERVAL", typecast_INTERVAL_types, typecast_PYINTERVAL_cast},
    {"PYDATETIMEARRAY", typecast_DATETIMEARRAY_types, typecast_PYDATETIMEARRAY_cast, "PYDATETIME"},
    {"PYDATETIMETZARRAY", typecast_DATETIMETZARRAY_types, typecast_PYDATETIMETZARRAY_cast, "PYDATETIMETZ"},
    {"PYTIMEARRAY", typecast_TIMEARRAY_types, typecast_PYTIMEARRAY_cast, "PYTIME"},
    {"PYDATEARRAY", typecast_DATEARRAY_types, typecast_PYDATEARRAY_cast, "PYDATE"},
    {"PYINTERVALARRAY", typecast_INTERVALARRAY_types, typecast_PYINTERVALARRAY_cast, "PYINTERVAL"},
    {NULL, NULL, NULL}
};


/** the type dictionary and associated functions **/

PyObject *psyco_types;
PyObject *psyco_default_cast;
PyObject *psyco_binary_types;
PyObject *psyco_default_binary_cast;


/* typecast_init - initialize the dictionary and create default types */

RAISES_NEG int
typecast_init(PyObject *module)
{
    int i;
    int rv = -1;
    typecastObject *t = NULL;
    PyObject *dict = NULL;

    if (!(dict = PyModule_GetDict(module))) { goto exit; }

    /* create type dictionary and put it in module namespace */
    if (!(psyco_types = PyDict_New())) { goto exit; }
    PyDict_SetItemString(dict, "string_types", psyco_types);

    if (!(psyco_binary_types = PyDict_New())) { goto exit; }
    PyDict_SetItemString(dict, "binary_types", psyco_binary_types);

    /* insert the cast types into the 'types' dictionary and register them in
       the module dictionary */
    for (i = 0; typecast_builtins[i].name != NULL; i++) {
        t = (typecastObject *)typecast_from_c(&(typecast_builtins[i]), dict);
        if (t == NULL) { goto exit; }
        if (typecast_add((PyObject *)t, NULL, 0) < 0) { goto exit; }

        PyDict_SetItem(dict, t->name, (PyObject *)t);

        /* export binary object */
        if (typecast_builtins[i].values == typecast_BINARY_types) {
            Py_INCREF((PyObject *)t);
            psyco_default_binary_cast = (PyObject *)t;
        }
        Py_DECREF((PyObject *)t);
        t = NULL;
    }

    /* create and save a default cast object (but do not register it) */
    psyco_default_cast = typecast_from_c(&typecast_default, dict);

    /* register the date/time typecasters with their original names */
    if (0 > typecast_datetime_init()) { goto exit; }
    for (i = 0; typecast_pydatetime[i].name != NULL; i++) {
        t = (typecastObject *)typecast_from_c(&(typecast_pydatetime[i]), dict);
        if (t == NULL) { goto exit; }
        PyDict_SetItem(dict, t->name, (PyObject *)t);
        Py_DECREF((PyObject *)t);
        t = NULL;
    }

    rv = 0;

exit:
    Py_XDECREF((PyObject *)t);
    return rv;
}

/* typecast_add - add a type object to the dictionary */
RAISES_NEG int
typecast_add(PyObject *obj, PyObject *dict, int binary)
{
    PyObject *val;
    Py_ssize_t len, i;

    typecastObject *type = (typecastObject *)obj;

    if (dict == NULL)
        dict = (binary ? psyco_binary_types : psyco_types);

    len = PyTuple_Size(type->values);
    for (i = 0; i < len; i++) {
        val = PyTuple_GetItem(type->values, i);
        PyDict_SetItem(dict, val, obj);
    }

    return 0;
}


/** typecast type **/

#define OFFSETOF(x) offsetof(typecastObject, x)

static int
typecast_cmp(PyObject *obj1, PyObject* obj2)
{
    typecastObject *self = (typecastObject*)obj1;
    typecastObject *other = NULL;
    PyObject *number = NULL;
    Py_ssize_t i, j;
    int res = -1;

    if (PyObject_TypeCheck(obj2, &typecastType)) {
        other = (typecastObject*)obj2;
    }
    else {
        number = PyNumber_Int(obj2);
    }

    Dprintf("typecast_cmp: other = %p, number = %p", other, number);

    for (i=0; i < PyObject_Length(self->values) && res == -1; i++) {
        long int val = PyInt_AsLong(PyTuple_GET_ITEM(self->values, i));

        if (other != NULL) {
            for (j=0; j < PyObject_Length(other->values); j++) {
                if (PyInt_AsLong(PyTuple_GET_ITEM(other->values, j)) == val) {
                    res = 0; break;
                }
            }
        }

        else if (number != NULL) {
            if (PyInt_AsLong(number) == val) {
                res = 0; break;
            }
        }
    }

    Py_XDECREF(number);
    return res;
}

static PyObject*
typecast_richcompare(PyObject *obj1, PyObject* obj2, int opid)
{
    int res = typecast_cmp(obj1, obj2);

    if (PyErr_Occurred()) return NULL;

    return PyBool_FromLong((opid == Py_EQ && res == 0) || (opid != Py_EQ && res != 0));
}

static struct PyMemberDef typecastObject_members[] = {
    {"name", T_OBJECT, OFFSETOF(name), READONLY},
    {"values", T_OBJECT, OFFSETOF(values), READONLY},
    {NULL}
};

static int
typecast_clear(typecastObject *self)
{
    Py_CLEAR(self->values);
    Py_CLEAR(self->name);
    Py_CLEAR(self->pcast);
    Py_CLEAR(self->bcast);
    return 0;
}

static void
typecast_dealloc(typecastObject *self)
{
    PyObject_GC_UnTrack(self);
    typecast_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int
typecast_traverse(typecastObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->values);
    Py_VISIT(self->name);
    Py_VISIT(self->pcast);
    Py_VISIT(self->bcast);
    return 0;
}

static PyObject *
typecast_repr(PyObject *self)
{
    PyObject *name = ((typecastObject *)self)->name;
    PyObject *rv;

    Py_INCREF(name);
    if (!(name = psyco_ensure_bytes(name))) {
        return NULL;
    }

    rv = PyString_FromFormat("<%s '%s' at %p>",
        Py_TYPE(self)->tp_name, Bytes_AS_STRING(name), self);

    Py_DECREF(name);
    return rv;
}

static PyObject *
typecast_call(PyObject *obj, PyObject *args, PyObject *kwargs)
{
    const char *string;
    Py_ssize_t length;
    PyObject *cursor;

    if (!PyArg_ParseTuple(args, "z#O", &string, &length, &cursor)) {
        return NULL;
    }

    // If the string is not a string but a None value we're being called
    // from a Python-defined caster.
    if (!string) {
        Py_RETURN_NONE;
    }

    return typecast_cast(obj, string, length, cursor);
}

PyTypeObject typecastType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2._psycopg.type",
    sizeof(typecastObject), 0,
    (destructor)typecast_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_reserved*/
    typecast_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    typecast_call, /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_RICHCOMPARE |
      Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    "psycopg type-casting object", /*tp_doc*/
    (traverseproc)typecast_traverse, /*tp_traverse*/
    (inquiry)typecast_clear, /*tp_clear*/
    typecast_richcompare, /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    0,          /*tp_methods*/
    typecastObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    0,          /*tp_init*/
    0,          /*tp_alloc*/
    0,          /*tp_new*/
};

static PyObject *
typecast_new(PyObject *name, PyObject *values, PyObject *cast, PyObject *base)
{
    typecastObject *obj;

    obj = PyObject_GC_New(typecastObject, &typecastType);
    if (obj == NULL) return NULL;

    Py_INCREF(values);
    obj->values = values;

    if (name) {
        Py_INCREF(name);
        obj->name = name;
    }
    else {
        Py_INCREF(Py_None);
        obj->name = Py_None;
    }

    obj->pcast = NULL;
    obj->ccast = NULL;
    obj->bcast = base;

    if (obj->bcast) Py_INCREF(obj->bcast);

    /* FIXME: raise an exception when None is passed as Python caster */
    if (cast && cast != Py_None) {
        Py_INCREF(cast);
        obj->pcast = cast;
    }

    PyObject_GC_Track(obj);

    return (PyObject *)obj;
}

PyObject *
typecast_from_python(PyObject *self, PyObject *args, PyObject *keywds)
{
    PyObject *v, *name = NULL, *cast = NULL, *base = NULL;

    static char *kwlist[] = {"values", "name", "castobj", "baseobj", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, keywds, "O!|O!OO", kwlist,
                                     &PyTuple_Type, &v,
                                     &Text_Type, &name,
                                     &cast, &base)) {
        return NULL;
    }

    return typecast_new(name, v, cast, base);
}

PyObject *
typecast_array_from_python(PyObject *self, PyObject *args, PyObject *keywds)
{
    PyObject *values, *name = NULL, *base = NULL;
    typecastObject *obj = NULL;

    static char *kwlist[] = {"values", "name", "baseobj", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, keywds, "O!O!O!", kwlist,
                                     &PyTuple_Type, &values,
                                     &Text_Type, &name,
                                     &typecastType, &base)) {
        return NULL;
    }

    if ((obj = (typecastObject *)typecast_new(name, values, NULL, base))) {
        obj->ccast = typecast_GENERIC_ARRAY_cast;
        obj->pcast = NULL;
    }

    return (PyObject *)obj;
}

PyObject *
typecast_from_c(typecastObject_initlist *type, PyObject *dict)
{
    PyObject *name = NULL, *values = NULL, *base = NULL;
    typecastObject *obj = NULL;
    Py_ssize_t i, len = 0;

    /* before doing anything else we look for the base */
    if (type->base) {
        /* NOTE: base is a borrowed reference! */
        base = PyDict_GetItemString(dict, type->base);
        if (!base) {
            PyErr_Format(Error, "typecast base not found: %s", type->base);
            goto end;
        }
    }

    name = Text_FromUTF8(type->name);
    if (!name) goto end;

    while (type->values[len] != 0) len++;

    values = PyTuple_New(len);
    if (!values) goto end;

    for (i = 0; i < len ; i++) {
        PyTuple_SET_ITEM(values, i, PyInt_FromLong(type->values[i]));
    }

    obj = (typecastObject *)typecast_new(name, values, NULL, base);

    if (obj) {
        obj->ccast = type->cast;
        obj->pcast = NULL;
    }

 end:
    Py_XDECREF(values);
    Py_XDECREF(name);
    return (PyObject *)obj;
}

PyObject *
typecast_cast(PyObject *obj, const char *str, Py_ssize_t len, PyObject *curs)
{
    PyObject *old, *res = NULL;
    typecastObject *self = (typecastObject *)obj;

    Py_INCREF(obj);
    old = ((cursorObject*)curs)->caster;
    ((cursorObject*)curs)->caster = obj;

    if (self->ccast) {
        res = self->ccast(str, len, curs);
    }
    else if (self->pcast) {
        PyObject *s;
        /* XXX we have bytes in the adapters and strings in the typecasters.
         * are you sure this is ok?
         * Notice that this way it is about impossible to create a python
         * typecaster on a binary type. */
        if (str) {
            s = conn_decode(((cursorObject *)curs)->conn, str, len);
        }
        else {
            Py_INCREF(Py_None);
            s = Py_None;
        }
        if (s) {
            res = PyObject_CallFunctionObjArgs(self->pcast, s, curs, NULL);
            Py_DECREF(s);
        }
    }
    else {
        PyErr_SetString(Error, "internal error: no casting function found");
    }

    ((cursorObject*)curs)->caster = old;
    Py_DECREF(obj);

    return res;
}
