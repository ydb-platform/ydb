/* adapter_list.c - python list objects
 *
 * Copyright (C) 2004-2019 Federico Di Gregorio <fog@debian.org>
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

#include "psycopg/adapter_list.h"
#include "psycopg/microprotocols.h"
#include "psycopg/microprotocols_proto.h"


/* list_str, list_getquoted - return result of quoting */

static PyObject *
list_quote(listObject *self)
{
    /*  adapt the list by calling adapt() recursively and then wrapping
        everything into "ARRAY[]" */
    PyObject *res = NULL;
    PyObject **qs = NULL;
    Py_ssize_t bufsize = 0;
    char *buf = NULL, *ptr;

    /*  list consisting of only NULL don't work with the ARRAY[] construct
     *  so we use the {NULL,...} syntax. The same syntax is also necessary
     *  to convert array of arrays containing only nulls. */
    int all_nulls = 1;

    Py_ssize_t i, len;

    len = PyList_GET_SIZE(self->wrapped);

    /* empty arrays are converted to NULLs (still searching for a way to
       insert an empty array in postgresql */
    if (len == 0) {
        /* it cannot be ARRAY[] because it would make empty lists unusable
         * in any() without a cast. But we may convert it into ARRAY[] below */
        res = Bytes_FromString("'{}'");
        goto exit;
    }

    if (!(qs = PyMem_New(PyObject *, len))) {
        PyErr_NoMemory();
        goto exit;
    }
    memset(qs, 0, len * sizeof(PyObject *));

    for (i = 0; i < len; i++) {
        PyObject *wrapped = PyList_GET_ITEM(self->wrapped, i);
        if (wrapped == Py_None) {
            Py_INCREF(psyco_null);
            qs[i] = psyco_null;
        }
        else {
            if (!(qs[i] = microprotocol_getquoted(
                    wrapped, (connectionObject*)self->connection))) {
                goto exit;
            }

            /* Lists of arrays containing only nulls are also not supported
             * by the ARRAY construct so we should do some special casing */
            if (PyList_Check(wrapped)) {
                if (Bytes_AS_STRING(qs[i])[0] == 'A') {
                    all_nulls = 0;
                }
                else if (0 == strcmp(Bytes_AS_STRING(qs[i]), "'{}'")) {
                    /* case of issue #788: '{{}}' is not supported but
                     * array[array[]] is */
                    all_nulls = 0;
                    Py_CLEAR(qs[i]);
                    if (!(qs[i] = Bytes_FromString("ARRAY[]"))) {
                        goto exit;
                    }
                }
            }
            else {
                all_nulls = 0;
            }
        }
        bufsize += Bytes_GET_SIZE(qs[i]) + 1;      /* this, and a comma */
    }

    /* Create an array literal, usually ARRAY[...] but if the contents are
     * all NULL or array of NULL we must use the '{...}' syntax
     */
    if (!(ptr = buf = PyMem_Malloc(bufsize + 8))) {
        PyErr_NoMemory();
        goto exit;
    }

    if (!all_nulls) {
        strcpy(ptr, "ARRAY[");
        ptr += 6;
        for (i = 0; i < len; i++) {
            Py_ssize_t sl;
            sl = Bytes_GET_SIZE(qs[i]);
            memcpy(ptr, Bytes_AS_STRING(qs[i]), sl);
            ptr += sl;
            *ptr++ = ',';
        }
        *(ptr - 1) = ']';
    }
    else {
        *ptr++ = '\'';
        *ptr++ = '{';
        for (i = 0; i < len; i++) {
            /* in case all the adapted things are nulls (or array of nulls),
             * the quoted string is either NULL or an array of the form
             * '{NULL,...}', in which case we have to strip the extra quotes */
            char *s;
            Py_ssize_t sl;
            s = Bytes_AS_STRING(qs[i]);
            sl = Bytes_GET_SIZE(qs[i]);
            if (s[0] != '\'') {
                memcpy(ptr, s, sl);
                ptr += sl;
            }
            else {
                memcpy(ptr, s + 1, sl - 2);
                ptr += sl - 2;
            }
            *ptr++ = ',';
        }
        *(ptr - 1) = '}';
        *ptr++ = '\'';
    }

    res = Bytes_FromStringAndSize(buf, ptr - buf);

exit:
    if (qs) {
        for (i = 0; i < len; i++) {
            PyObject *q = qs[i];
            Py_XDECREF(q);
        }
        PyMem_Free(qs);
    }
    PyMem_Free(buf);

    return res;
}

static PyObject *
list_str(listObject *self)
{
    return psyco_ensure_text(list_quote(self));
}

static PyObject *
list_getquoted(listObject *self, PyObject *args)
{
    return list_quote(self);
}

static PyObject *
list_prepare(listObject *self, PyObject *args)
{
    PyObject *conn;

    if (!PyArg_ParseTuple(args, "O!", &connectionType, &conn))
        return NULL;

    Py_CLEAR(self->connection);
    Py_INCREF(conn);
    self->connection = conn;

    Py_RETURN_NONE;
}

static PyObject *
list_conform(listObject *self, PyObject *args)
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

static struct PyMemberDef listObject_members[] = {
    {"adapted", T_OBJECT, offsetof(listObject, wrapped), READONLY},
    {NULL}
};

/* object method table */

static PyMethodDef listObject_methods[] = {
    {"getquoted", (PyCFunction)list_getquoted, METH_NOARGS,
     "getquoted() -> wrapped object value as SQL date/time"},
    {"prepare", (PyCFunction)list_prepare, METH_VARARGS,
     "prepare(conn) -> set encoding to conn->encoding"},
    {"__conform__", (PyCFunction)list_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
list_setup(listObject *self, PyObject *obj)
{
    Dprintf("list_setup: init list object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );

    if (!PyList_Check(obj))
        return -1;

    self->connection = NULL;
    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("list_setup: good list object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );
    return 0;
}

static int
list_traverse(listObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->wrapped);
    Py_VISIT(self->connection);
    return 0;
}

static int
list_clear(listObject *self)
{
    Py_CLEAR(self->wrapped);
    Py_CLEAR(self->connection);
    return 0;
}

static void
list_dealloc(listObject* self)
{
    PyObject_GC_UnTrack((PyObject *)self);
    list_clear(self);

    Dprintf("list_dealloc: deleted list object at %p, "
            "refcnt = " FORMAT_CODE_PY_SSIZE_T, self, Py_REFCNT(self));

    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int
list_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *l;

    if (!PyArg_ParseTuple(args, "O", &l))
        return -1;

    return list_setup((listObject *)obj, l);
}

static PyObject *
list_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}


/* object type */

#define listType_doc \
"List(list) -> new list wrapper object"

PyTypeObject listType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2._psycopg.List",
    sizeof(listObject), 0,
    (destructor)list_dealloc, /*tp_dealloc*/
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
    (reprfunc)list_str, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    listType_doc, /*tp_doc*/
    (traverseproc)list_traverse, /*tp_traverse*/
    (inquiry)list_clear, /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    listObject_methods, /*tp_methods*/
    listObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    list_init, /*tp_init*/
    0,          /*tp_alloc*/
    list_new, /*tp_new*/
};
