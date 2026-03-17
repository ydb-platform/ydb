/* cursor_type.c - python interface to cursor objects
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

#include "psycopg/cursor.h"
#include "psycopg/connection.h"
#include "psycopg/green.h"
#include "psycopg/pqpath.h"
#include "psycopg/typecast.h"
#include "psycopg/microprotocols.h"
#include "psycopg/microprotocols_proto.h"

#include <string.h>

#include <stdlib.h>


/** DBAPI methods **/

/* close method - close the cursor */

#define curs_close_doc \
"close() -- Close the cursor."

static PyObject *
curs_close(cursorObject *self, PyObject *dummy)
{
    PyObject *rv = NULL;
    char *lname = NULL;

    if (self->closed) {
        rv = Py_None;
        Py_INCREF(rv);
        goto exit;
    }

    if (self->qname != NULL) {
        char buffer[256];
        PGTransactionStatusType status;

        EXC_IF_ASYNC_IN_PROGRESS(self, close_named);

        if (self->conn) {
            status = PQtransactionStatus(self->conn->pgconn);
        }
        else {
            status = PQTRANS_UNKNOWN;
        }

        if (status == PQTRANS_UNKNOWN || status == PQTRANS_INERROR) {
            Dprintf("skipping named curs close because tx status %d",
                (int)status);
            goto close;
        }

        /* We should close a server-side cursor only if exists, or we get an
         * error (#716). If we execute()d the cursor should exist alright, but
         * if we didn't there is still the expectation that the cursor is
         * closed (#746).
         *
         * So if we didn't execute() check for the cursor existence before
         * closing it (the view exists since PG 8.2 according to docs).
         */
        if (!self->query && self->conn->server_version >= 80200) {
            if (!(lname = psyco_escape_string(
                    self->conn, self->name, -1, NULL, NULL))) {
                goto exit;
            }
            PyOS_snprintf(buffer, sizeof(buffer),
                "SELECT 1 FROM pg_catalog.pg_cursors where name = %s",
                lname);
            if (pq_execute(self, buffer, 0, 0, 1) == -1) { goto exit; }

            if (self->rowcount == 0) {
                Dprintf("skipping named cursor close because not existing");
                goto close;
            }
        }

        EXC_IF_NO_MARK(self);
        PyOS_snprintf(buffer, sizeof(buffer), "CLOSE %s", self->qname);
        if (pq_execute(self, buffer, 0, 0, 1) == -1) { goto exit; }
    }

close:
    CLEARPGRES(self->pgres);

    self->closed = 1;
    Dprintf("curs_close: cursor at %p closed", self);

    rv = Py_None;
    Py_INCREF(rv);

exit:
    PyMem_Free(lname);
    return rv;
}


/* execute method - executes a query */

/* mogrify a query string and build argument array or dict */

RAISES_NEG static int
_mogrify(PyObject *var, PyObject *fmt, cursorObject *curs, PyObject **new)
{
    PyObject *key, *value, *n;
    const char *d, *c;
    Py_ssize_t index = 0;
    int force = 0, kind = 0;

    /* from now on we'll use n and replace its value in *new only at the end,
       just before returning. we also init *new to NULL to exit with an error
       if we can't complete the mogrification */
    n = *new = NULL;
    c = Bytes_AsString(fmt);

    while(*c) {
        if (*c++ != '%') {
            /* a regular character */
            continue;
        }

        switch (*c) {

        /* handle plain percent symbol in format string */
        case '%':
            ++c;
            force = 1;
            break;

        /* if we find '%(' then this is a dictionary, we:
           1/ find the matching ')' and extract the key name
           2/ locate the value in the dictionary (or return an error)
           3/ mogrify the value into something useful (quoting)...
           4/ ...and add it to the new dictionary to be used as argument
        */
        case '(':
            /* check if some crazy guy mixed formats */
            if (kind == 2) {
                Py_XDECREF(n);
                psyco_set_error(ProgrammingError, curs,
                   "argument formats can't be mixed");
                return -1;
            }
            kind = 1;

            /* let's have d point the end of the argument */
            for (d = c + 1; *d && *d != ')' && *d != '%'; d++);

            if (*d == ')') {
                if (!(key = Text_FromUTF8AndSize(c+1, (Py_ssize_t)(d-c-1)))) {
                    Py_XDECREF(n);
                    return -1;
                }

                /*  if value is NULL we did not find the key (or this is not a
                    dictionary): let python raise a KeyError */
                if (!(value = PyObject_GetItem(var, key))) {
                    Py_DECREF(key); /* destroy key */
                    Py_XDECREF(n);  /* destroy n */
                    return -1;
                }
                /* key has refcnt 1, value the original value + 1 */

                Dprintf("_mogrify: value refcnt: "
                  FORMAT_CODE_PY_SSIZE_T " (+1)", Py_REFCNT(value));

                if (n == NULL) {
                    if (!(n = PyDict_New())) {
                        Py_DECREF(key);
                        Py_DECREF(value);
                        return -1;
                    }
                }

                if (0 == PyDict_Contains(n, key)) {
                    PyObject *t = NULL;

                    /* None is always converted to NULL; this is an
                       optimization over the adapting code and can go away in
                       the future if somebody finds a None adapter useful. */
                    if (value == Py_None) {
                        Py_INCREF(psyco_null);
                        t = psyco_null;
                        PyDict_SetItem(n, key, t);
                        /* t is a new object, refcnt = 1, key is at 2 */
                    }
                    else {
                        t = microprotocol_getquoted(value, curs->conn);
                        if (t != NULL) {
                            PyDict_SetItem(n, key, t);
                            /* both key and t refcnt +1, key is at 2 now */
                        }
                        else {
                            /* no adapter found, raise a BIG exception */
                            Py_DECREF(key);
                            Py_DECREF(value);
                            Py_DECREF(n);
                            return -1;
                        }
                    }

                    Py_XDECREF(t); /* t dies here */
                }
                Py_DECREF(value);
                Py_DECREF(key); /* key has the original refcnt now */
                Dprintf("_mogrify: after value refcnt: "
                    FORMAT_CODE_PY_SSIZE_T, Py_REFCNT(value));
            }
            else {
                /* we found %( but not a ) */
                Py_XDECREF(n);
                psyco_set_error(ProgrammingError, curs,
                   "incomplete placeholder: '%(' without ')'");
                return -1;
            }
            c = d + 1;  /* after the ) */
            break;

        default:
            /* this is a format that expects a tuple; it is much easier,
               because we don't need to check the old/new dictionary for
               keys */

            /* check if some crazy guy mixed formats */
            if (kind == 1) {
                Py_XDECREF(n);
                psyco_set_error(ProgrammingError, curs,
                  "argument formats can't be mixed");
                return -1;
            }
            kind = 2;

            value = PySequence_GetItem(var, index);
            /* value has refcnt inc'ed by 1 here */

            /*  if value is NULL this is not a sequence or the index is wrong;
                anyway we let python set its own exception */
            if (value == NULL) {
                Py_XDECREF(n);
                return -1;
            }

            if (n == NULL) {
                if (!(n = PyTuple_New(PyObject_Length(var)))) {
                    Py_DECREF(value);
                    return -1;
                }
            }

            /* let's have d point just after the '%' */
            if (value == Py_None) {
                Py_INCREF(psyco_null);
                PyTuple_SET_ITEM(n, index, psyco_null);
                Py_DECREF(value);
            }
            else {
                PyObject *t = microprotocol_getquoted(value, curs->conn);

                if (t != NULL) {
                    PyTuple_SET_ITEM(n, index, t);
                    Py_DECREF(value);
                }
                else {
                    Py_DECREF(n);
                    Py_DECREF(value);
                    return -1;
                }
            }
            index += 1;
        }
    }

    if (force && n == NULL)
        n = PyTuple_New(0);
    *new = n;

    return 0;
}


/* Merge together a query string and its arguments.
 *
 * The arguments have been already adapted to SQL.
 *
 * Return a new reference to a string with the merged query,
 * NULL and set an exception if any happened.
 */
static PyObject *
_psyco_curs_merge_query_args(cursorObject *self,
                             PyObject *query, PyObject *args)
{
    PyObject *fquery;

    /* if PyString_Format() return NULL an error occurred: if the error is
       a TypeError we need to check the exception.args[0] string for the
       values:

           "not enough arguments for format string"
           "not all arguments converted"

       and return the appropriate ProgrammingError. we do that by grabbing
       the current exception (we will later restore it if the type or the
       strings do not match.) */

    if (!(fquery = Bytes_Format(query, args))) {
        PyObject *err, *arg, *trace;
        int pe = 0;

        PyErr_Fetch(&err, &arg, &trace);

        if (err && PyErr_GivenExceptionMatches(err, PyExc_TypeError)) {
            Dprintf("curs_execute: TypeError exception caught");
            PyErr_NormalizeException(&err, &arg, &trace);

            if (PyObject_HasAttrString(arg, "args")) {
                PyObject *args = PyObject_GetAttrString(arg, "args");
                PyObject *str = PySequence_GetItem(args, 0);
                const char *s = Bytes_AS_STRING(str);

                Dprintf("curs_execute:     -> %s", s);

                if (!strcmp(s, "not enough arguments for format string")
                  || !strcmp(s, "not all arguments converted")) {
                    Dprintf("curs_execute:     -> got a match");
                    psyco_set_error(ProgrammingError, self, s);
                    pe = 1;
                }

                Py_DECREF(args);
                Py_DECREF(str);
            }
        }

        /* if we did not manage our own exception, restore old one */
        if (pe == 1) {
            Py_XDECREF(err); Py_XDECREF(arg); Py_XDECREF(trace);
        }
        else {
            PyErr_Restore(err, arg, trace);
        }
    }

    return fquery;
}

#define curs_execute_doc \
"execute(query, vars=None) -- Execute query with bound vars."

RAISES_NEG static int
_psyco_curs_execute(cursorObject *self,
                    PyObject *query, PyObject *vars,
                    long int async, int no_result)
{
    int res = -1;
    int tmp;
    PyObject *fquery = NULL, *cvt = NULL;

    /* query becomes NULL or refcount +1, so good to XDECREF at the end */
    if (!(query = curs_validate_sql_basic(self, query))) {
        goto exit;
    }

    CLEARPGRES(self->pgres);
    Py_CLEAR(self->query);
    Dprintf("curs_execute: starting execution of new query");

    /* here we are, and we have a sequence or a dictionary filled with
       objects to be substituted (bound variables). we try to be smart and do
       the right thing (i.e., what the user expects) */
    if (vars && vars != Py_None)
    {
        if (0 > _mogrify(vars, query, self, &cvt)) { goto exit; }
    }

    /* Merge the query to the arguments if needed */
    if (cvt) {
        if (!(fquery = _psyco_curs_merge_query_args(self, query, cvt))) {
            goto exit;
        }
    }
    else {
        Py_INCREF(query);
        fquery = query;
    }

    if (self->qname != NULL) {
        const char *scroll;
        switch (self->scrollable) {
            case -1:
                scroll = "";
                break;
            case 0:
                scroll = "NO SCROLL ";
                break;
            case 1:
                scroll = "SCROLL ";
                break;
            default:
                PyErr_SetString(InternalError, "unexpected scrollable value");
                goto exit;
        }

        if (!(self->query = Bytes_FromFormat(
                "DECLARE %s %sCURSOR %s HOLD FOR %s",
                self->qname,
                scroll,
                self->withhold ? "WITH" : "WITHOUT",
                Bytes_AS_STRING(fquery)))) {
            goto exit;
        }
        if (!self->query) { goto exit; }
    }
    else {
        /* Transfer ownership */
        Py_INCREF(fquery);
        self->query = fquery;
    }

    /* At this point, the SQL statement must be str, not unicode */
    tmp = pq_execute(self, Bytes_AS_STRING(self->query), async, no_result, 0);
    Dprintf("curs_execute: res = %d, pgres = %p", tmp, self->pgres);
    if (tmp < 0) { goto exit; }

    res = 0; /* Success */

exit:
    Py_XDECREF(query);
    Py_XDECREF(fquery);
    Py_XDECREF(cvt);

    return res;
}

static PyObject *
curs_execute(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *vars = NULL, *operation = NULL;

    static char *kwlist[] = {"query", "vars", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O", kwlist,
                                     &operation, &vars)) {
        return NULL;
    }

    if (self->name != NULL) {
        if (self->query) {
            psyco_set_error(ProgrammingError, self,
                "can't call .execute() on named cursors more than once");
            return NULL;
        }
        if (self->conn->autocommit && !self->withhold) {
            psyco_set_error(ProgrammingError, self,
                "can't use a named cursor outside of transactions");
            return NULL;
        }
        EXC_IF_NO_MARK(self);
    }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, execute);
    EXC_IF_TPC_PREPARED(self->conn, execute);

    if (0 > _psyco_curs_execute(self, operation, vars, self->conn->async, 0)) {
        return NULL;
    }

    /* success */
    Py_RETURN_NONE;
}

#define curs_executemany_doc \
"executemany(query, vars_list) -- Execute many queries with bound vars."

static PyObject *
curs_executemany(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *operation = NULL, *vars = NULL;
    PyObject *v, *iter = NULL;
    long rowcount = 0;

    static char *kwlist[] = {"query", "vars_list", NULL};

    /* reset rowcount to -1 to avoid setting it when an exception is raised */
    self->rowcount = -1;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", kwlist,
                                     &operation, &vars)) {
        return NULL;
    }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_CURS_ASYNC(self, executemany);
    EXC_IF_TPC_PREPARED(self->conn, executemany);

    if (self->name != NULL) {
        psyco_set_error(ProgrammingError, self,
                "can't call .executemany() on named cursors");
        return NULL;
    }

    if (!PyIter_Check(vars)) {
        vars = iter = PyObject_GetIter(vars);
        if (iter == NULL) return NULL;
    }

    while ((v = PyIter_Next(vars)) != NULL) {
        if (0 > _psyco_curs_execute(self, operation, v, 0, 1)) {
            Py_DECREF(v);
            Py_XDECREF(iter);
            return NULL;
        }
        else {
            if (self->rowcount == -1)
                rowcount = -1;
            else if (rowcount >= 0)
                rowcount += self->rowcount;
            Py_DECREF(v);
        }
    }
    Py_XDECREF(iter);
    self->rowcount = rowcount;

    if (!PyErr_Occurred()) {
        Py_RETURN_NONE;
    }
    else {
        return NULL;
    }
}


#define curs_mogrify_doc \
"mogrify(query, vars=None) -> str -- Return query after vars binding."

static PyObject *
_psyco_curs_mogrify(cursorObject *self,
                   PyObject *operation, PyObject *vars)
{
    PyObject *fquery = NULL, *cvt = NULL;

    operation = curs_validate_sql_basic(self, operation);
    if (operation == NULL) { goto cleanup; }

    Dprintf("curs_mogrify: starting mogrify");

    /* here we are, and we have a sequence or a dictionary filled with
       objects to be substituted (bound variables). we try to be smart and do
       the right thing (i.e., what the user expects) */

    if (vars && vars != Py_None)
    {
        if (0 > _mogrify(vars, operation, self, &cvt)) {
            goto cleanup;
        }
    }

    if (vars && cvt) {
        if (!(fquery = _psyco_curs_merge_query_args(self, operation, cvt))) {
            goto cleanup;
        }

        Dprintf("curs_mogrify: cvt->refcnt = " FORMAT_CODE_PY_SSIZE_T
            ", fquery->refcnt = " FORMAT_CODE_PY_SSIZE_T,
            Py_REFCNT(cvt), Py_REFCNT(fquery));
    }
    else {
        fquery = operation;
        Py_INCREF(fquery);
    }

cleanup:
    Py_XDECREF(operation);
    Py_XDECREF(cvt);

    return fquery;
}

static PyObject *
curs_mogrify(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *vars = NULL, *operation = NULL;

    static char *kwlist[] = {"query", "vars", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O", kwlist,
                                     &operation, &vars)) {
        return NULL;
    }

    return _psyco_curs_mogrify(self, operation, vars);
}


/* cast method - convert an oid/string into a Python object */
#define curs_cast_doc \
"cast(oid, s) -> value\n\n" \
"Convert the string s to a Python object according to its oid.\n\n" \
"Look for a typecaster first in the cursor, then in its connection," \
"then in the global register. If no suitable typecaster is found," \
"leave the value as a string."

static PyObject *
curs_cast(cursorObject *self, PyObject *args)
{
    PyObject *oid;
    PyObject *s;
    PyObject *cast;

    if (!PyArg_ParseTuple(args, "OO", &oid, &s))
        return NULL;

    cast = curs_get_cast(self, oid);
    return PyObject_CallFunctionObjArgs(cast, s, (PyObject *)self, NULL);
}


/* fetchone method - fetch one row of results */

#define curs_fetchone_doc \
"fetchone() -> tuple or None\n\n" \
"Return the next row of a query result set in the form of a tuple (by\n" \
"default) or using the sequence factory previously set in the\n" \
"`row_factory` attribute. Return `!None` when no more data is available.\n"

RAISES_NEG static int
_psyco_curs_prefetch(cursorObject *self)
{
    int i = 0;

    if (self->pgres == NULL) {
        Dprintf("_psyco_curs_prefetch: trying to fetch data");
        do {
            i = pq_fetch(self, 0);
            Dprintf("_psycopg_curs_prefetch: result = %d", i);
        } while(i == 1);
    }

    Dprintf("_psyco_curs_prefetch: result = %d", i);
    return i;
}

RAISES_NEG static int
_psyco_curs_buildrow_fill(cursorObject *self, PyObject *res,
                          int row, int n, int istuple)
{
    int i, len, err;
    const char *str;
    PyObject *val;
    int rv = -1;

    for (i=0; i < n; i++) {
        if (PQgetisnull(self->pgres, row, i)) {
            str = NULL;
            len = 0;
        }
        else {
            str = PQgetvalue(self->pgres, row, i);
            len = PQgetlength(self->pgres, row, i);
        }

        Dprintf("_psyco_curs_buildrow: row %ld, element %d, len %d",
                self->row, i, len);

        if (!(val = typecast_cast(PyTuple_GET_ITEM(self->casts, i), str, len,
                            (PyObject*)self))) {
            goto exit;
        }

        Dprintf("_psyco_curs_buildrow: val->refcnt = "
            FORMAT_CODE_PY_SSIZE_T,
            Py_REFCNT(val)
          );
        if (istuple) {
            PyTuple_SET_ITEM(res, i, val);
        }
        else {
            err = PySequence_SetItem(res, i, val);
            Py_DECREF(val);
            if (err == -1) { goto exit; }
        }
    }

    rv = 0;

exit:
    return rv;
}

static PyObject *
_psyco_curs_buildrow(cursorObject *self, int row)
{
    int n;
    int istuple;
    PyObject *t = NULL;
    PyObject *rv = NULL;

    n = PQnfields(self->pgres);
    istuple = (self->tuple_factory == Py_None);

    if (istuple) {
        t = PyTuple_New(n);
    }
    else {
        t = PyObject_CallFunctionObjArgs(self->tuple_factory, self, NULL);
    }
    if (!t) { goto exit; }

    if (0 <= _psyco_curs_buildrow_fill(self, t, row, n, istuple)) {
        rv = t;
        t = NULL;
    }

exit:
    Py_XDECREF(t);
    return rv;

}

static PyObject *
curs_fetchone(cursorObject *self, PyObject *dummy)
{
    PyObject *res;

    EXC_IF_CURS_CLOSED(self);
    if (_psyco_curs_prefetch(self) < 0) return NULL;
    EXC_IF_NO_TUPLES(self);

    if (self->qname != NULL) {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        EXC_IF_ASYNC_IN_PROGRESS(self, fetchone);
        EXC_IF_TPC_PREPARED(self->conn, fetchone);
        PyOS_snprintf(buffer, sizeof(buffer), "FETCH FORWARD 1 FROM %s", self->qname);
        if (pq_execute(self, buffer, 0, 0, self->withhold) == -1) return NULL;
        if (_psyco_curs_prefetch(self) < 0) return NULL;
    }

    Dprintf("curs_fetchone: fetching row %ld", self->row);
    Dprintf("curs_fetchone: rowcount = %ld", self->rowcount);

    if (self->row >= self->rowcount) {
        /* we exausted available data: return None */
        Py_RETURN_NONE;
    }

    res = _psyco_curs_buildrow(self, self->row);
    self->row++; /* move the counter to next line */

    /* if the query was async aggresively free pgres, to allow
       successive requests to reallocate it */
    if (self->row >= self->rowcount
        && self->conn->async_cursor
        && PyWeakref_GetObject(self->conn->async_cursor) == (PyObject*)self)
        CLEARPGRES(self->pgres);

    return res;
}

/* Efficient cursor.next() implementation for named cursors.
 *
 * Fetch several records at time. Return NULL when the cursor is exhausted.
 */
static PyObject *
curs_next_named(cursorObject *self)
{
    PyObject *res;

    Dprintf("curs_next_named");
    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, next);
    if (_psyco_curs_prefetch(self) < 0) return NULL;
    EXC_IF_NO_TUPLES(self);

    EXC_IF_NO_MARK(self);
    EXC_IF_TPC_PREPARED(self->conn, next);

    Dprintf("curs_next_named: row %ld", self->row);
    Dprintf("curs_next_named: rowcount = %ld", self->rowcount);
    if (self->row >= self->rowcount) {
        char buffer[128];

        PyOS_snprintf(buffer, sizeof(buffer), "FETCH FORWARD %ld FROM %s",
            self->itersize, self->qname);
        if (pq_execute(self, buffer, 0, 0, self->withhold) == -1) return NULL;
        if (_psyco_curs_prefetch(self) < 0) return NULL;
    }

    /* We exhausted the data: return NULL to stop iteration. */
    if (self->row >= self->rowcount) {
        return NULL;
    }

    res = _psyco_curs_buildrow(self, self->row);
    self->row++; /* move the counter to next line */

    /* if the query was async aggresively free pgres, to allow
       successive requests to reallocate it */
    if (self->row >= self->rowcount
        && self->conn->async_cursor
        && PyWeakref_GetObject(self->conn->async_cursor) == (PyObject*)self)
        CLEARPGRES(self->pgres);

    return res;
}


/* fetch many - fetch some results */

#define curs_fetchmany_doc \
"fetchmany(size=self.arraysize) -> list of tuple\n\n" \
"Return the next `size` rows of a query result set in the form of a list\n" \
"of tuples (by default) or using the sequence factory previously set in\n" \
"the `row_factory` attribute.\n\n" \
"Return an empty list when no more data is available.\n"

static PyObject *
curs_fetchmany(cursorObject *self, PyObject *args, PyObject *kwords)
{
    int i;
    PyObject *list = NULL;
    PyObject *row = NULL;
    PyObject *rv = NULL;

    PyObject *pysize = NULL;
    long int size = self->arraysize;
    static char *kwlist[] = {"size", NULL};

    /* allow passing None instead of omitting the *size* argument,
     * or using the method from subclasses would be a problem */
    if (!PyArg_ParseTupleAndKeywords(args, kwords, "|O", kwlist, &pysize)) {
        return NULL;
    }

    if (pysize && pysize != Py_None) {
        size = PyInt_AsLong(pysize);
        if (size == -1 && PyErr_Occurred()) {
            return NULL;
        }
    }

    EXC_IF_CURS_CLOSED(self);
    if (_psyco_curs_prefetch(self) < 0) return NULL;
    EXC_IF_NO_TUPLES(self);

    if (self->qname != NULL) {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        EXC_IF_ASYNC_IN_PROGRESS(self, fetchmany);
        EXC_IF_TPC_PREPARED(self->conn, fetchone);
        PyOS_snprintf(buffer, sizeof(buffer), "FETCH FORWARD %d FROM %s",
            (int)size, self->qname);
        if (pq_execute(self, buffer, 0, 0, self->withhold) == -1) { goto exit; }
        if (_psyco_curs_prefetch(self) < 0) { goto exit; }
    }

    /* make sure size is not > than the available number of rows */
    if (size > self->rowcount - self->row || size < 0) {
        size = self->rowcount - self->row;
    }

    Dprintf("curs_fetchmany: size = %ld", size);

    if (size <= 0) {
        rv = PyList_New(0);
        goto exit;
    }

    if (!(list = PyList_New(size))) { goto exit; }

    for (i = 0; i < size; i++) {
        row = _psyco_curs_buildrow(self, self->row);
        self->row++;

        if (row == NULL) { goto exit; }

        PyList_SET_ITEM(list, i, row);
    }
    row = NULL;

    /* if the query was async aggresively free pgres, to allow
       successive requests to reallocate it */
    if (self->row >= self->rowcount
        && self->conn->async_cursor
        && PyWeakref_GetObject(self->conn->async_cursor) == (PyObject*)self)
        CLEARPGRES(self->pgres);

    /* success */
    rv = list;
    list = NULL;

exit:
    Py_XDECREF(list);
    Py_XDECREF(row);

    return rv;
}


/* fetch all - fetch all results */

#define curs_fetchall_doc \
"fetchall() -> list of tuple\n\n" \
"Return all the remaining rows of a query result set.\n\n" \
"Rows are returned in the form of a list of tuples (by default) or using\n" \
"the sequence factory previously set in the `row_factory` attribute.\n" \
"Return `!None` when no more data is available.\n"

static PyObject *
curs_fetchall(cursorObject *self, PyObject *dummy)
{
    int i, size;
    PyObject *list = NULL;
    PyObject *row = NULL;
    PyObject *rv = NULL;

    EXC_IF_CURS_CLOSED(self);
    if (_psyco_curs_prefetch(self) < 0) return NULL;
    EXC_IF_NO_TUPLES(self);

    if (self->qname != NULL) {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        EXC_IF_ASYNC_IN_PROGRESS(self, fetchall);
        EXC_IF_TPC_PREPARED(self->conn, fetchall);
        PyOS_snprintf(buffer, sizeof(buffer), "FETCH FORWARD ALL FROM %s", self->qname);
        if (pq_execute(self, buffer, 0, 0, self->withhold) == -1) { goto exit; }
        if (_psyco_curs_prefetch(self) < 0) { goto exit; }
    }

    size = self->rowcount - self->row;

    if (size <= 0) {
        rv = PyList_New(0);
        goto exit;
    }

    if (!(list = PyList_New(size))) { goto exit; }

    for (i = 0; i < size; i++) {
        row = _psyco_curs_buildrow(self, self->row);
        self->row++;
        if (row == NULL) { goto exit; }

        PyList_SET_ITEM(list, i, row);
    }
    row = NULL;

    /* if the query was async aggresively free pgres, to allow
       successive requests to reallocate it */
    if (self->row >= self->rowcount
        && self->conn->async_cursor
        && PyWeakref_GetObject(self->conn->async_cursor) == (PyObject*)self)
        CLEARPGRES(self->pgres);

    /* success */
    rv = list;
    list = NULL;

exit:
    Py_XDECREF(list);
    Py_XDECREF(row);

    return rv;
}


/* callproc method - execute a stored procedure */

#define curs_callproc_doc \
"callproc(procname, parameters=None) -- Execute stored procedure."

static PyObject *
curs_callproc(cursorObject *self, PyObject *args)
{
    const char *procname = NULL;
    char *sql = NULL;
    Py_ssize_t procname_len, i, nparameters = 0, sl = 0;
    PyObject *parameters = Py_None;
    PyObject *operation = NULL;
    PyObject *res = NULL;

    int using_dict;
    PyObject *pname = NULL;
    PyObject *pnames = NULL;
    PyObject *pvals = NULL;
    char *cpname = NULL;
    char **scpnames = NULL;

    if (!PyArg_ParseTuple(args, "s#|O", &procname, &procname_len,
                &parameters)) {
        goto exit;
    }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, callproc);
    EXC_IF_TPC_PREPARED(self->conn, callproc);

    if (self->name != NULL) {
        psyco_set_error(ProgrammingError, self,
                "can't call .callproc() on named cursors");
        goto exit;
    }

    if (parameters != Py_None) {
        if (-1 == (nparameters = PyObject_Length(parameters))) { goto exit; }
    }

    using_dict = nparameters > 0 && PyDict_Check(parameters);

    /* a Dict is complicated; the parameter names go into the query */
    if (using_dict) {
        if (!(pnames = PyDict_Keys(parameters))) { goto exit; }
        if (!(pvals = PyDict_Values(parameters))) { goto exit; }

        sl = procname_len + 17 + nparameters * 5 - (nparameters ? 1 : 0);

        if (!(scpnames = PyMem_New(char *, nparameters))) {
            PyErr_NoMemory();
            goto exit;
        }

        memset(scpnames, 0, sizeof(char *) * nparameters);

        /* each parameter has to be processed; it's a few steps. */
        for (i = 0; i < nparameters; i++) {
            /* all errors are RuntimeErrors as they should never occur */

            if (!(pname = PyList_GetItem(pnames, i))) { goto exit; }
            Py_INCREF(pname);   /* was borrowed */

            /* this also makes a check for keys being strings */
            if (!(pname = psyco_ensure_bytes(pname))) { goto exit; }
            if (!(cpname = Bytes_AsString(pname))) { goto exit; }

            if (!(scpnames[i] = psyco_escape_identifier(
                    self->conn, cpname, -1))) {
                Py_CLEAR(pname);
                goto exit;
            }

            Py_CLEAR(pname);

            sl += strlen(scpnames[i]);
        }

        if (!(sql = (char*)PyMem_Malloc(sl))) {
            PyErr_NoMemory();
            goto exit;
        }

        sprintf(sql, "SELECT * FROM %s(", procname);
        for (i = 0; i < nparameters; i++) {
            strcat(sql, scpnames[i]);
            strcat(sql, ":=%s,");
        }
        sql[sl-2] = ')';
        sql[sl-1] = '\0';
    }

    /* a list (or None, or empty data structure) is a little bit simpler */
    else {
        Py_INCREF(parameters);
        pvals = parameters;

        sl = procname_len + 17 + nparameters * 3 - (nparameters ? 1 : 0);

        sql = (char*)PyMem_Malloc(sl);
        if (sql == NULL) {
            PyErr_NoMemory();
            goto exit;
        }

        sprintf(sql, "SELECT * FROM %s(", procname);
        for (i = 0; i < nparameters; i++) {
            strcat(sql, "%s,");
        }
        sql[sl-2] = ')';
        sql[sl-1] = '\0';
    }

    if (!(operation = Bytes_FromString(sql))) {
        goto exit;
    }

    if (0 <= _psyco_curs_execute(
            self, operation, pvals, self->conn->async, 0)) {
        /* The dict case is outside DBAPI scope anyway, so simply return None */
        if (using_dict) {
            res = Py_None;
        }
        else {
            res = pvals;
        }
        Py_INCREF(res);
    }

exit:
    if (scpnames != NULL) {
        for (i = 0; i < nparameters; i++) {
            if (scpnames[i] != NULL) {
                PQfreemem(scpnames[i]);
            }
        }
    }
    PyMem_Del(scpnames);
    Py_XDECREF(pname);
    Py_XDECREF(pnames);
    Py_XDECREF(operation);
    Py_XDECREF(pvals);
    PyMem_Free((void*)sql);
    return res;
}


/* nextset method - return the next set of data (not supported) */

#define curs_nextset_doc \
"nextset() -- Skip to next set of data.\n\n" \
"This method is not supported (PostgreSQL does not have multiple data \n" \
"sets) and will raise a NotSupportedError exception."

static PyObject *
curs_nextset(cursorObject *self, PyObject *dummy)
{
    EXC_IF_CURS_CLOSED(self);

    PyErr_SetString(NotSupportedError, "not supported by PostgreSQL");
    return NULL;
}


/* setinputsizes - predefine memory areas for execute (does nothing) */

#define curs_setinputsizes_doc \
"setinputsizes(sizes) -- Set memory areas before execute.\n\n" \
"This method currently does nothing but it is safe to call it."

static PyObject *
curs_setinputsizes(cursorObject *self, PyObject *args)
{
    PyObject *sizes;

    if (!PyArg_ParseTuple(args, "O", &sizes))
        return NULL;

    EXC_IF_CURS_CLOSED(self);

    Py_RETURN_NONE;
}


/* setoutputsize - predefine memory areas for execute (does nothing) */

#define curs_setoutputsize_doc \
"setoutputsize(size, column=None) -- Set column buffer size.\n\n" \
"This method currently does nothing but it is safe to call it."

static PyObject *
curs_setoutputsize(cursorObject *self, PyObject *args)
{
    long int size, column;

    if (!PyArg_ParseTuple(args, "l|l", &size, &column))
        return NULL;

    EXC_IF_CURS_CLOSED(self);

    Py_RETURN_NONE;
}


/* scroll - scroll position in result list */

#define curs_scroll_doc \
"scroll(value, mode='relative') -- Scroll to new position according to mode."

static PyObject *
curs_scroll(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    int value, newpos;
    const char *mode = "relative";

    static char *kwlist[] = {"value", "mode", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|s",
                                     kwlist, &value, &mode))
        return NULL;

    EXC_IF_CURS_CLOSED(self);

    /* if the cursor is not named we have the full result set and we can do
       our own calculations to scroll; else we just delegate the scrolling
       to the MOVE SQL statement */
    if (self->qname == NULL) {
        if (strcmp(mode, "relative") == 0) {
            newpos = self->row + value;
        } else if (strcmp( mode, "absolute") == 0) {
            newpos = value;
        } else {
            psyco_set_error(ProgrammingError, self,
                "scroll mode must be 'relative' or 'absolute'");
            return NULL;
        }

        if (newpos < 0 || newpos >= self->rowcount ) {
            psyco_set_error(ProgrammingError, self,
                             "scroll destination out of bounds");
            return NULL;
        }

        self->row = newpos;
    }

    else {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        EXC_IF_ASYNC_IN_PROGRESS(self, scroll);
        EXC_IF_TPC_PREPARED(self->conn, scroll);

        if (strcmp(mode, "absolute") == 0) {
            PyOS_snprintf(buffer, sizeof(buffer), "MOVE ABSOLUTE %d FROM %s",
                value, self->qname);
        }
        else {
            PyOS_snprintf(buffer, sizeof(buffer), "MOVE %d FROM %s", value, self->qname);
        }
        if (pq_execute(self, buffer, 0, 0, self->withhold) == -1) return NULL;
        if (_psyco_curs_prefetch(self) < 0) return NULL;
    }

    Py_RETURN_NONE;
}


#define curs_enter_doc \
"__enter__ -> self"

static PyObject *
curs_enter(cursorObject *self, PyObject *dummy)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

#define curs_exit_doc \
"__exit__ -- close the cursor"

static PyObject *
curs_exit(cursorObject *self, PyObject *args)
{
    PyObject *tmp = NULL;
    PyObject *rv = NULL;

    /* don't care about the arguments here: don't need to parse them */

    if (!(tmp = PyObject_CallMethod((PyObject *)self, "close", ""))) {
        goto exit;
    }

    /* success (of curs.close()).
     * Return None to avoid swallowing the exception */
    rv = Py_None;
    Py_INCREF(rv);

exit:
    Py_XDECREF(tmp);
    return rv;
}


/* Return a newly allocated buffer containing the list of columns to be
 * copied. On error return NULL and set an exception.
 */
static char *_psyco_curs_copy_columns(PyObject *columns)
{
    PyObject *col, *coliter;
    Py_ssize_t collen;
    char *colname;
    char *columnlist = NULL;
    Py_ssize_t bufsize = 512;
    Py_ssize_t offset = 1;

    if (columns == NULL || columns == Py_None) {
        if (NULL == (columnlist = PyMem_Malloc(2))) {
            PyErr_NoMemory();
            goto error;
        }
        columnlist[0] = '\0';
        goto exit;
    }

    if (NULL == (coliter = PyObject_GetIter(columns))) {
        goto error;
    }

    if (NULL == (columnlist = PyMem_Malloc(bufsize))) {
        Py_DECREF(coliter);
        PyErr_NoMemory();
        goto error;
    }
    columnlist[0] = '(';

    while ((col = PyIter_Next(coliter)) != NULL) {
        if (!(col = psyco_ensure_bytes(col))) {
            Py_DECREF(coliter);
            goto error;
        }
        Bytes_AsStringAndSize(col, &colname, &collen);
        while (offset + collen > bufsize - 2) {
            char *tmp;
            bufsize *= 2;
            if (NULL == (tmp = PyMem_Realloc(columnlist, bufsize))) {
                Py_DECREF(col);
                Py_DECREF(coliter);
                PyErr_NoMemory();
                goto error;
            }
            columnlist = tmp;
        }
        strncpy(&columnlist[offset], colname, collen);
        offset += collen;
        columnlist[offset++] = ',';
        Py_DECREF(col);
    }
    Py_DECREF(coliter);

    /* Error raised by the coliter generator */
    if (PyErr_Occurred()) {
        goto error;
    }

    if (offset == 2) {
        goto exit;
    }
    else {
        columnlist[offset - 1] = ')';
        columnlist[offset] = '\0';
        goto exit;
    }

error:
    PyMem_Free(columnlist);
    columnlist = NULL;

exit:
    return columnlist;
}

/* extension: copy_from - implements COPY FROM */

#define curs_copy_from_doc \
"copy_from(file, table, sep='\\t', null='\\\\N', size=8192, columns=None) -- Copy table from file."

static PyObject *
curs_copy_from(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {
            "file", "table", "sep", "null", "size", "columns", NULL};

    const char *sep = "\t";
    const char *null = "\\N";
    const char *command =
        "COPY %s%s FROM stdin WITH DELIMITER AS %s NULL AS %s";

    Py_ssize_t query_size;
    char *query = NULL;
    char *columnlist = NULL;
    char *quoted_delimiter = NULL;
    char *quoted_null = NULL;

    const char *table_name;
    Py_ssize_t bufsize = DEFAULT_COPYBUFF;
    PyObject *file, *columns = NULL, *res = NULL;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "Os|ssnO", kwlist,
            &file, &table_name, &sep, &null, &bufsize, &columns)) {
        return NULL;
    }

    if (!PyObject_HasAttrString(file, "read")) {
        PyErr_SetString(PyExc_TypeError,
            "argument 1 must have a .read() method");
        return NULL;
    }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_CURS_ASYNC(self, copy_from);
    EXC_IF_GREEN(copy_from);
    EXC_IF_TPC_PREPARED(self->conn, copy_from);

    if (NULL == (columnlist = _psyco_curs_copy_columns(columns)))
        goto exit;

    if (!(quoted_delimiter = psyco_escape_string(
            self->conn, sep, -1, NULL, NULL))) {
        goto exit;
    }

    if (!(quoted_null = psyco_escape_string(
            self->conn, null, -1, NULL, NULL))) {
        goto exit;
    }

    query_size = strlen(command) + strlen(table_name) + strlen(columnlist)
        + strlen(quoted_delimiter) + strlen(quoted_null) + 1;
    if (!(query = PyMem_New(char, query_size))) {
        PyErr_NoMemory();
        goto exit;
    }

    PyOS_snprintf(query, query_size, command,
        table_name, columnlist, quoted_delimiter, quoted_null);

    Dprintf("curs_copy_from: query = %s", query);

    Py_CLEAR(self->query);
    if (!(self->query = Bytes_FromString(query))) {
        goto exit;
    }

    /* This routine stores a borrowed reference.  Although it is only held
     * for the duration of curs_copy_from, nested invocations of
     * Py_BEGIN_ALLOW_THREADS could surrender control to another thread,
     * which could invoke the garbage collector.  We thus need an
     * INCREF/DECREF pair if we store this pointer in a GC object, such as
     * a cursorObject */
    self->copysize = bufsize;
    Py_INCREF(file);
    self->copyfile = file;

    if (pq_execute(self, query, 0, 0, 0) >= 0) {
        res = Py_None;
        Py_INCREF(Py_None);
    }

    Py_CLEAR(self->copyfile);

exit:
    PyMem_Free(columnlist);
    PyMem_Free(quoted_delimiter);
    PyMem_Free(quoted_null);
    PyMem_Free(query);

    return res;
}

/* extension: copy_to - implements COPY TO */

#define curs_copy_to_doc \
"copy_to(file, table, sep='\\t', null='\\\\N', columns=None) -- Copy table to file."

static PyObject *
curs_copy_to(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"file", "table", "sep", "null", "columns", NULL};

    const char *sep = "\t";
    const char *null = "\\N";
    const char *command =
        "COPY %s%s TO stdout WITH DELIMITER AS %s NULL AS %s";

    Py_ssize_t query_size;
    char *query = NULL;
    char *columnlist = NULL;
    char *quoted_delimiter = NULL;
    char *quoted_null = NULL;

    const char *table_name;
    PyObject *file = NULL, *columns = NULL, *res = NULL;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "Os|ssO", kwlist,
            &file, &table_name, &sep, &null, &columns)) {
        return NULL;
    }

    if (!PyObject_HasAttrString(file, "write")) {
        PyErr_SetString(PyExc_TypeError,
            "argument 1 must have a .write() method");
        return NULL;
    }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_CURS_ASYNC(self, copy_to);
    EXC_IF_GREEN(copy_to);
    EXC_IF_TPC_PREPARED(self->conn, copy_to);

    if (NULL == (columnlist = _psyco_curs_copy_columns(columns)))
        goto exit;

    if (!(quoted_delimiter = psyco_escape_string(
            self->conn, sep, -1, NULL, NULL))) {
        goto exit;
    }

    if (!(quoted_null = psyco_escape_string(
            self->conn, null, -1, NULL, NULL))) {
        goto exit;
    }

    query_size = strlen(command) + strlen(table_name) + strlen(columnlist)
        + strlen(quoted_delimiter) + strlen(quoted_null) + 1;
    if (!(query = PyMem_New(char, query_size))) {
        PyErr_NoMemory();
        goto exit;
    }

    PyOS_snprintf(query, query_size, command,
        table_name, columnlist, quoted_delimiter, quoted_null);

    Dprintf("curs_copy_to: query = %s", query);

    Py_CLEAR(self->query);
    if (!(self->query = Bytes_FromString(query))) {
        goto exit;
    }

    self->copysize = 0;
    Py_INCREF(file);
    self->copyfile = file;

    if (pq_execute(self, query, 0, 0, 0) >= 0) {
        res = Py_None;
        Py_INCREF(Py_None);
    }

    Py_CLEAR(self->copyfile);

exit:
    PyMem_Free(columnlist);
    PyMem_Free(quoted_delimiter);
    PyMem_Free(quoted_null);
    PyMem_Free(query);

    return res;
}

/* extension: copy_expert - implements extended COPY FROM/TO

   This method supports both COPY FROM and COPY TO with user-specifiable
   SQL statement, rather than composing the statement from parameters.
*/

#define curs_copy_expert_doc \
"copy_expert(sql, file, size=8192) -- Submit a user-composed COPY statement.\n" \
"`file` must be an open, readable file for COPY FROM or an open, writable\n"   \
"file for COPY TO. The optional `size` argument, when specified for a COPY\n"   \
"FROM statement, will be passed to file's read method to control the read\n"    \
"buffer size."

static PyObject *
curs_copy_expert(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    Py_ssize_t bufsize = DEFAULT_COPYBUFF;
    PyObject *sql, *file, *res = NULL;

    static char *kwlist[] = {"sql", "file", "size", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
        "OO|n", kwlist, &sql, &file, &bufsize))
    { return NULL; }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_CURS_ASYNC(self, copy_expert);
    EXC_IF_GREEN(copy_expert);
    EXC_IF_TPC_PREPARED(self->conn, copy_expert);

    sql = curs_validate_sql_basic(self, sql);

    /* Any failure from here forward should 'goto exit' rather than
       'return NULL' directly. */

    if (sql == NULL) { goto exit; }

    /* This validation of file is rather weak, in that it doesn't enforce the
       association between "COPY FROM" -> "read" and "COPY TO" -> "write".
       However, the error handling in _pq_copy_[in|out] must be able to handle
       the case where the attempt to call file.read|write fails, so no harm
       done. */

    if (   !PyObject_HasAttrString(file, "read")
        && !PyObject_HasAttrString(file, "write")
      )
    {
        PyErr_SetString(PyExc_TypeError, "file must be a readable file-like"
            " object for COPY FROM; a writable file-like object for COPY TO."
          );
        goto exit;
    }

    self->copysize = bufsize;
    Py_INCREF(file);
    self->copyfile = file;

    Py_CLEAR(self->query);
    Py_INCREF(sql);
    self->query = sql;

    /* At this point, the SQL statement must be str, not unicode */
    if (pq_execute(self, Bytes_AS_STRING(sql), 0, 0, 0) >= 0) {
        res = Py_None;
        Py_INCREF(res);
    }

    Py_CLEAR(self->copyfile);

exit:
    Py_XDECREF(sql);

    return res;
}

/* extension: closed - return true if cursor is closed */

#define curs_closed_doc \
"True if cursor is closed, False if cursor is open"

static PyObject *
curs_closed_get(cursorObject *self, void *closure)
{
    return PyBool_FromLong(self->closed || (self->conn && self->conn->closed));
}

/* extension: withhold - get or set "WITH HOLD" for named cursors */

#define curs_withhold_doc \
"Set or return cursor use of WITH HOLD"

static PyObject *
curs_withhold_get(cursorObject *self)
{
    return PyBool_FromLong(self->withhold);
}

RAISES_NEG int
curs_withhold_set(cursorObject *self, PyObject *pyvalue)
{
    int value;

    if (pyvalue != Py_False && self->name == NULL) {
        PyErr_SetString(ProgrammingError,
            "trying to set .withhold on unnamed cursor");
        return -1;
    }

    if ((value = PyObject_IsTrue(pyvalue)) == -1)
        return -1;

    self->withhold = value;

    return 0;
}

#define curs_scrollable_doc \
"Set or return cursor use of SCROLL"

static PyObject *
curs_scrollable_get(cursorObject *self)
{
    PyObject *ret = NULL;

    switch (self->scrollable) {
        case -1:
            ret = Py_None;
            break;
        case 0:
            ret = Py_False;
            break;
        case 1:
            ret = Py_True;
            break;
        default:
            PyErr_SetString(InternalError, "unexpected scrollable value");
    }

    Py_XINCREF(ret);
    return ret;
}

RAISES_NEG int
curs_scrollable_set(cursorObject *self, PyObject *pyvalue)
{
    int value;

    if (pyvalue != Py_None && self->name == NULL) {
        PyErr_SetString(ProgrammingError,
            "trying to set .scrollable on unnamed cursor");
        return -1;
    }

    if (pyvalue == Py_None) {
        value = -1;
    } else if ((value = PyObject_IsTrue(pyvalue)) == -1) {
        return -1;
    }

    self->scrollable = value;

    return 0;
}


#define curs_pgresult_ptr_doc \
"pgresult_ptr -- Get the PGresult structure pointer."

static PyObject *
curs_pgresult_ptr_get(cursorObject *self)
{
    if (self->pgres) {
        return PyLong_FromVoidPtr((void *)self->pgres);
    }
    else {
        Py_RETURN_NONE;
    }
}


/** the cursor object **/

/* iterator protocol */

static PyObject *
cursor_iter(PyObject *self)
{
    EXC_IF_CURS_CLOSED((cursorObject*)self);
    Py_INCREF(self);
    return self;
}

static PyObject *
cursor_next(PyObject *self)
{
    PyObject *res;

    if (NULL == ((cursorObject*)self)->name) {
        /* we don't parse arguments: curs_fetchone will do that for us */
        res = curs_fetchone((cursorObject*)self, NULL);

        /* convert a None to NULL to signal the end of iteration */
        if (res && res == Py_None) {
            Py_DECREF(res);
            res = NULL;
        }
    }
    else {
        res = curs_next_named((cursorObject*)self);
    }

    return res;
}

/* object method list */

static struct PyMethodDef cursorObject_methods[] = {
    /* DBAPI-2.0 core */
    {"close", (PyCFunction)curs_close,
     METH_NOARGS, curs_close_doc},
    {"execute", (PyCFunction)curs_execute,
     METH_VARARGS|METH_KEYWORDS, curs_execute_doc},
    {"executemany", (PyCFunction)curs_executemany,
     METH_VARARGS|METH_KEYWORDS, curs_executemany_doc},
    {"fetchone", (PyCFunction)curs_fetchone,
     METH_NOARGS, curs_fetchone_doc},
    {"fetchmany", (PyCFunction)curs_fetchmany,
     METH_VARARGS|METH_KEYWORDS, curs_fetchmany_doc},
    {"fetchall", (PyCFunction)curs_fetchall,
     METH_NOARGS, curs_fetchall_doc},
    {"callproc", (PyCFunction)curs_callproc,
     METH_VARARGS, curs_callproc_doc},
    {"nextset", (PyCFunction)curs_nextset,
     METH_NOARGS, curs_nextset_doc},
    {"setinputsizes", (PyCFunction)curs_setinputsizes,
     METH_VARARGS, curs_setinputsizes_doc},
    {"setoutputsize", (PyCFunction)curs_setoutputsize,
     METH_VARARGS, curs_setoutputsize_doc},
    /* DBAPI-2.0 extensions */
    {"scroll", (PyCFunction)curs_scroll,
     METH_VARARGS|METH_KEYWORDS, curs_scroll_doc},
    {"__enter__", (PyCFunction)curs_enter,
     METH_NOARGS, curs_enter_doc},
    {"__exit__", (PyCFunction)curs_exit,
     METH_VARARGS, curs_exit_doc},
    /* psycopg extensions */
    {"cast", (PyCFunction)curs_cast,
     METH_VARARGS, curs_cast_doc},
    {"mogrify", (PyCFunction)curs_mogrify,
     METH_VARARGS|METH_KEYWORDS, curs_mogrify_doc},
    {"copy_from", (PyCFunction)curs_copy_from,
     METH_VARARGS|METH_KEYWORDS, curs_copy_from_doc},
    {"copy_to", (PyCFunction)curs_copy_to,
     METH_VARARGS|METH_KEYWORDS, curs_copy_to_doc},
    {"copy_expert", (PyCFunction)curs_copy_expert,
     METH_VARARGS|METH_KEYWORDS, curs_copy_expert_doc},
    {NULL}
};

/* object member list */

#define OFFSETOF(x) offsetof(cursorObject, x)

static struct PyMemberDef cursorObject_members[] = {
    /* DBAPI-2.0 basics */
    {"rowcount", T_LONG, OFFSETOF(rowcount), READONLY,
        "Number of rows read from the backend in the last command."},
    {"arraysize", T_LONG, OFFSETOF(arraysize), 0,
        "Number of records `fetchmany()` must fetch if not explicitly " \
        "specified."},
    {"itersize", T_LONG, OFFSETOF(itersize), 0,
        "Number of records ``iter(cur)`` must fetch per network roundtrip."},
    {"description", T_OBJECT, OFFSETOF(description), READONLY,
        "Cursor description as defined in DBAPI-2.0."},
    {"lastrowid", T_OID, OFFSETOF(lastoid), READONLY,
        "The ``oid`` of the last row inserted by the cursor."},
    /* DBAPI-2.0 extensions */
    {"rownumber", T_LONG, OFFSETOF(row), READONLY,
        "The current row position."},
    {"connection", T_OBJECT, OFFSETOF(conn), READONLY,
        "The connection where the cursor comes from."},
    {"name", T_STRING, OFFSETOF(name), READONLY},
    {"statusmessage", T_OBJECT, OFFSETOF(pgstatus), READONLY,
        "The return message of the last command."},
    {"query", T_OBJECT, OFFSETOF(query), READONLY,
        "The last query text sent to the backend."},
    {"row_factory", T_OBJECT, OFFSETOF(tuple_factory), 0},
    {"tzinfo_factory", T_OBJECT, OFFSETOF(tzinfo_factory), 0},
    {"typecaster", T_OBJECT, OFFSETOF(caster), READONLY},
    {"string_types", T_OBJECT, OFFSETOF(string_types), 0},
    {"binary_types", T_OBJECT, OFFSETOF(binary_types), 0},
    {NULL}
};

/* object calculated member list */
static struct PyGetSetDef cursorObject_getsets[] = {
    { "closed", (getter)curs_closed_get, NULL,
      curs_closed_doc, NULL },
    { "withhold",
      (getter)curs_withhold_get,
      (setter)curs_withhold_set,
      curs_withhold_doc, NULL },
    { "scrollable",
      (getter)curs_scrollable_get,
      (setter)curs_scrollable_set,
      curs_scrollable_doc, NULL },
    { "pgresult_ptr",
      (getter)curs_pgresult_ptr_get, NULL,
      curs_pgresult_ptr_doc, NULL },
    {NULL}
};

/* initialization and finalization methods */

static int
cursor_setup(cursorObject *self, connectionObject *conn, const char *name)
{
    Dprintf("cursor_setup: init cursor object at %p", self);
    Dprintf("cursor_setup: parameters: name = %s, conn = %p", name, conn);

    if (name) {
        if (0 > psyco_strdup(&self->name, name, -1)) {
            return -1;
        }
        if (!(self->qname = psyco_escape_identifier(conn, name, -1))) {
            return -1;
        }
    }

    /* FIXME: why does this raise an exception on the _next_ line of code?
    if (PyObject_IsInstance((PyObject*)conn,
                             (PyObject *)&connectionType) == 0) {
        PyErr_SetString(PyExc_TypeError,
            "argument 1 must be subclass of psycopg2.extensions.connection");
        return -1;
    } */
    Py_INCREF(conn);
    self->conn = conn;

    self->mark = conn->mark;
    self->notuples = 1;
    self->arraysize = 1;
    self->itersize = 2000;
    self->rowcount = -1;
    self->lastoid = InvalidOid;

    Py_INCREF(Py_None);
    self->tuple_factory = Py_None;

    /* default tzinfo factory */
    {
        PyObject *m = NULL;
        if ((m = PyImport_ImportModule("psycopg2.tz"))) {
            self->tzinfo_factory = PyObject_GetAttrString(
                    m, "FixedOffsetTimezone");
            Py_DECREF(m);
        }
        if (!self->tzinfo_factory) {
            return -1;
        }
    }

    Dprintf("cursor_setup: good cursor object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, Py_REFCNT(self)
      );
    return 0;
}

static int
cursor_clear(cursorObject *self)
{
    Py_CLEAR(self->conn);
    Py_CLEAR(self->description);
    Py_CLEAR(self->pgstatus);
    Py_CLEAR(self->casts);
    Py_CLEAR(self->caster);
    Py_CLEAR(self->copyfile);
    Py_CLEAR(self->tuple_factory);
    Py_CLEAR(self->tzinfo_factory);
    Py_CLEAR(self->query);
    Py_CLEAR(self->string_types);
    Py_CLEAR(self->binary_types);
    return 0;
}

static void
cursor_dealloc(PyObject* obj)
{
    cursorObject *self = (cursorObject *)obj;

    PyObject_GC_UnTrack(self);

    if (self->weakreflist) {
        PyObject_ClearWeakRefs(obj);
    }

    cursor_clear(self);

    PyMem_Free(self->name);
    PQfreemem(self->qname);

    CLEARPGRES(self->pgres);

    Dprintf("cursor_dealloc: deleted cursor object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, Py_REFCNT(obj));

    Py_TYPE(obj)->tp_free(obj);
}

static int
cursor_init(PyObject *obj, PyObject *args, PyObject *kwargs)
{
    PyObject *conn;
    PyObject *name = Py_None;
    PyObject *bname = NULL;
    const char *cname = NULL;
    int rv = -1;

    static char *kwlist[] = {"conn", "name", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!|O", kwlist,
            &connectionType, &conn, &name)) {
        goto exit;
    }

    if (name != Py_None) {
        Py_INCREF(name);   /* for ensure_bytes */
        if (!(bname = psyco_ensure_bytes(name))) {
            /* name has had a ref stolen */
            goto exit;
        }

        if (!(cname = Bytes_AsString(bname))) {
            goto exit;
        }
    }

    rv = cursor_setup((cursorObject *)obj, (connectionObject *)conn, cname);

exit:
    Py_XDECREF(bname);
    return rv;
}

static PyObject *
cursor_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static PyObject *
cursor_repr(cursorObject *self)
{
    return PyString_FromFormat(
        "<cursor object at %p; closed: %d>", self, self->closed);
}

static int
cursor_traverse(cursorObject *self, visitproc visit, void *arg)
{
    Py_VISIT((PyObject *)self->conn);
    Py_VISIT(self->description);
    Py_VISIT(self->pgstatus);
    Py_VISIT(self->casts);
    Py_VISIT(self->caster);
    Py_VISIT(self->copyfile);
    Py_VISIT(self->tuple_factory);
    Py_VISIT(self->tzinfo_factory);
    Py_VISIT(self->query);
    Py_VISIT(self->string_types);
    Py_VISIT(self->binary_types);
    return 0;
}


/* object type */

#define cursorType_doc \
"A database cursor."

PyTypeObject cursorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.cursor",
    sizeof(cursorObject), 0,
    cursor_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)cursor_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */
    0,          /*tp_call*/
    (reprfunc)cursor_repr, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_ITER |
      Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_HAVE_WEAKREFS ,
                /*tp_flags*/
    cursorType_doc, /*tp_doc*/
    (traverseproc)cursor_traverse, /*tp_traverse*/
    (inquiry)cursor_clear, /*tp_clear*/
    0,          /*tp_richcompare*/
    offsetof(cursorObject, weakreflist), /*tp_weaklistoffset*/
    cursor_iter, /*tp_iter*/
    cursor_next, /*tp_iternext*/
    cursorObject_methods, /*tp_methods*/
    cursorObject_members, /*tp_members*/
    cursorObject_getsets, /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    cursor_init, /*tp_init*/
    0,          /*tp_alloc*/
    cursor_new, /*tp_new*/
};
