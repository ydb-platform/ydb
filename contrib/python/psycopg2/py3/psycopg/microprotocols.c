/* microprotocols.c - minimalist and non-validating protocols implementation
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

#include "psycopg/microprotocols.h"
#include "psycopg/microprotocols_proto.h"
#include "psycopg/cursor.h"
#include "psycopg/connection.h"


/** the adapters registry **/

PyObject *psyco_adapters;

/* microprotocols_init - initialize the adapters dictionary */

RAISES_NEG int
microprotocols_init(PyObject *module)
{
    /* create adapters dictionary and put it in module namespace */
    if (!(psyco_adapters = PyDict_New())) {
        return -1;
    }

    Py_INCREF(psyco_adapters);
    if (0 > PyModule_AddObject(module, "adapters", psyco_adapters)) {
        Py_DECREF(psyco_adapters);
        return -1;
    }

    return 0;
}


/* microprotocols_add - add a reverse type-caster to the dictionary
 *
 * Return 0 on success, else -1 and set an exception.
 */
RAISES_NEG int
microprotocols_add(PyTypeObject *type, PyObject *proto, PyObject *cast)
{
    PyObject *key = NULL;
    int rv = -1;

    if (proto == NULL) proto = (PyObject*)&isqlquoteType;

    if (!(key = PyTuple_Pack(2, (PyObject*)type, proto))) { goto exit; }
    if (0 != PyDict_SetItem(psyco_adapters, key, cast)) { goto exit; }

    rv = 0;

exit:
    Py_XDECREF(key);
    return rv;
}

/* Check if one of `obj` superclasses has an adapter for `proto`.
 *
 * If it does, return a *borrowed reference* to the adapter, else to None.
 */
BORROWED static PyObject *
_get_superclass_adapter(PyObject *obj, PyObject *proto)
{
    PyTypeObject *type;
    PyObject *mro, *st;
    PyObject *key, *adapter;
    Py_ssize_t i, ii;

    type = Py_TYPE(obj);
    if (!(type->tp_mro)) {
        /* has no mro */
        return Py_None;
    }

    /* Walk the mro from the most specific subclass. */
    mro = type->tp_mro;
    for (i = 1, ii = PyTuple_GET_SIZE(mro); i < ii; ++i) {
        st = PyTuple_GET_ITEM(mro, i);
        if (!(key = PyTuple_Pack(2, st, proto))) { return NULL; }
        adapter = PyDict_GetItem(psyco_adapters, key);
        Py_DECREF(key);

        if (adapter) {
            Dprintf(
                "microprotocols_adapt: using '%s' adapter to adapt '%s'",
                ((PyTypeObject *)st)->tp_name, type->tp_name);

            /* register this adapter as good for the subclass too,
             * so that the next time it will be found in the fast path */

            /* Well, no, maybe this is not a good idea.
             * It would become a leak in case of dynamic
             * classes generated in a loop (think namedtuples). */

            /* key = PyTuple_Pack(2, (PyObject*)type, proto);
             * PyDict_SetItem(psyco_adapters, key, adapter);
             * Py_DECREF(key);
             */
            return adapter;
        }
    }
    return Py_None;
}


/* microprotocols_adapt - adapt an object to the built-in protocol */

PyObject *
microprotocols_adapt(PyObject *obj, PyObject *proto, PyObject *alt)
{
    PyObject *adapter, *adapted, *key, *meth;
    char buffer[256];

    /* we don't check for exact type conformance as specified in PEP 246
       because the ISQLQuote type is abstract and there is no way to get a
       quotable object to be its instance */

    Dprintf("microprotocols_adapt: trying to adapt %s",
        Py_TYPE(obj)->tp_name);

    /* look for an adapter in the registry */
    if (!(key = PyTuple_Pack(2, Py_TYPE(obj), proto))) { return NULL; }
    adapter = PyDict_GetItem(psyco_adapters, key);
    Py_DECREF(key);
    if (adapter) {
        adapted = PyObject_CallFunctionObjArgs(adapter, obj, NULL);
        return adapted;
    }

    /* try to have the protocol adapt this object*/
    if ((meth = PyObject_GetAttrString(proto, "__adapt__"))) {
        adapted = PyObject_CallFunctionObjArgs(meth, obj, NULL);
        Py_DECREF(meth);
        if (adapted && adapted != Py_None) return adapted;
        Py_XDECREF(adapted);
        if (PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_TypeError)) {
               PyErr_Clear();
            } else {
                return NULL;
            }
        }
    }
    else {
        /* proto.__adapt__ not found. */
        PyErr_Clear();
    }

    /* then try to have the object adapt itself */
    if ((meth = PyObject_GetAttrString(obj, "__conform__"))) {
        adapted = PyObject_CallFunctionObjArgs(meth, proto, NULL);
        Py_DECREF(meth);
        if (adapted && adapted != Py_None) return adapted;
        Py_XDECREF(adapted);
        if (PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_TypeError)) {
               PyErr_Clear();
            } else {
                return NULL;
            }
        }
    }
    else {
        /* obj.__conform__ not found. */
        PyErr_Clear();
    }

    /* Finally check if a superclass can be adapted and use the same adapter. */
    if (!(adapter = _get_superclass_adapter(obj, proto))) {
        return NULL;
    }
    if (Py_None != adapter) {
        adapted = PyObject_CallFunctionObjArgs(adapter, obj, NULL);
        return adapted;
    }

    /* else set the right exception and return NULL */
    PyOS_snprintf(buffer, 255, "can't adapt type '%s'",
        Py_TYPE(obj)->tp_name);
    psyco_set_error(ProgrammingError, NULL, buffer);
    return NULL;
}

/* microprotocol_getquoted - utility function that adapt and call getquoted.
 *
 * Return a bytes string, NULL on error.
 */

PyObject *
microprotocol_getquoted(PyObject *obj, connectionObject *conn)
{
    PyObject *res = NULL;
    PyObject *prepare = NULL;
    PyObject *adapted;

    if (!(adapted = microprotocols_adapt(obj, (PyObject*)&isqlquoteType, NULL))) {
       goto exit;
    }

    Dprintf("microprotocol_getquoted: adapted to %s",
        Py_TYPE(adapted)->tp_name);

    /* if requested prepare the object passing it the connection */
    if (conn) {
        if ((prepare = PyObject_GetAttrString(adapted, "prepare"))) {
            res = PyObject_CallFunctionObjArgs(
                prepare, (PyObject *)conn, NULL);
            if (res) {
                Py_DECREF(res);
                res = NULL;
            } else {
                goto exit;
            }
        }
        else {
            /* adapted.prepare not found */
            PyErr_Clear();
        }
    }

    /* call the getquoted method on adapted (that should exist because we
       adapted to the right protocol) */
    res = PyObject_CallMethod(adapted, "getquoted", NULL);

    /* Convert to bytes. */
    if (res && PyUnicode_CheckExact(res)) {
        PyObject *b;
        b = conn_encode(conn, res);
        Py_DECREF(res);
        res = b;
    }

exit:
    Py_XDECREF(adapted);
    Py_XDECREF(prepare);

    /* we return res with one extra reference, the caller shall free it */
    return res;
}


/** module-level functions **/

PyObject *
psyco_microprotocols_adapt(cursorObject *self, PyObject *args)
{
    PyObject *obj, *alt = NULL;
    PyObject *proto = (PyObject*)&isqlquoteType;

    if (!PyArg_ParseTuple(args, "O|OO", &obj, &proto, &alt)) return NULL;
    return microprotocols_adapt(obj, proto, alt);
}
