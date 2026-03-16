/* conninfo_type.c - present information about the libpq connection
 *
 * Copyright (C) 2018-2019  Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

#include "psycopg/conninfo.h"


static const char connInfoType_doc[] =
"Details about the native PostgreSQL database connection.\n"
"\n"
"This class exposes several `informative functions`__ about the status\n"
"of the libpq connection.\n"
"\n"
"Objects of this class are exposed as the `connection.info` attribute.\n"
"\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html";


static const char dbname_doc[] =
"The database name of the connection.\n"
"\n"
".. seealso:: libpq docs for `PQdb()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQDB";

static PyObject *
dbname_get(connInfoObject *self)
{
    const char *val;

    val = PQdb(self->conn->pgconn);
    if (!val) {
        Py_RETURN_NONE;
    }
    return conn_text_from_chars(self->conn, val);
}


static const char user_doc[] =
"The user name of the connection.\n"
"\n"
".. seealso:: libpq docs for `PQuser()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQUSER";

static PyObject *
user_get(connInfoObject *self)
{
    const char *val;

    val = PQuser(self->conn->pgconn);
    if (!val) {
        Py_RETURN_NONE;
    }
    return conn_text_from_chars(self->conn, val);
}


static const char password_doc[] =
"The password of the connection.\n"
"\n"
".. seealso:: libpq docs for `PQpass()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQPASS";

static PyObject *
password_get(connInfoObject *self)
{
    const char *val;

    val = PQpass(self->conn->pgconn);
    if (!val) {
        Py_RETURN_NONE;
    }
    return conn_text_from_chars(self->conn, val);
}


static const char host_doc[] =
"The server host name of the connection.\n"
"\n"
"This can be a host name, an IP address, or a directory path if the\n"
"connection is via Unix socket. (The path case can be distinguished\n"
"because it will always be an absolute path, beginning with ``/``.)\n"
"\n"
".. seealso:: libpq docs for `PQhost()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQHOST";

static PyObject *
host_get(connInfoObject *self)
{
    const char *val;

    val = PQhost(self->conn->pgconn);
    if (!val) {
        Py_RETURN_NONE;
    }
    return conn_text_from_chars(self->conn, val);
}


static const char port_doc[] =
"The port of the connection.\n"
"\n"
":type: `!int`\n"
"\n"
".. seealso:: libpq docs for `PQport()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQPORT";

static PyObject *
port_get(connInfoObject *self)
{
    const char *val;

    val = PQport(self->conn->pgconn);
    if (!val || !val[0]) {
        Py_RETURN_NONE;
    }
    return PyInt_FromString((char *)val, NULL, 10);
}


static const char options_doc[] =
"The command-line options passed in the connection request.\n"
"\n"
".. seealso:: libpq docs for `PQoptions()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQOPTIONS";

static PyObject *
options_get(connInfoObject *self)
{
    const char *val;

    val = PQoptions(self->conn->pgconn);
    if (!val) {
        Py_RETURN_NONE;
    }
    return conn_text_from_chars(self->conn, val);
}


static const char dsn_parameters_doc[] =
"The effective connection parameters.\n"
"\n"
":type: `!dict`\n"
"\n"
"The results include values which weren't explicitly set by the connection\n"
"string, such as defaults, environment variables, etc.\n"
"The *password* parameter is removed from the results.\n"
"\n"
".. seealso:: libpq docs for `PQconninfo()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/libpq-connect.html"
    "#LIBPQ-PQCONNINFO";

static PyObject *
dsn_parameters_get(connInfoObject *self)
{
#if PG_VERSION_NUM >= 90300
    PyObject *res = NULL;
    PQconninfoOption *options = NULL;

    EXC_IF_CONN_CLOSED(self->conn);

    if (!(options = PQconninfo(self->conn->pgconn))) {
        PyErr_NoMemory();
        goto exit;
    }

    res = psyco_dict_from_conninfo_options(options, /* include_password = */ 0);

exit:
    PQconninfoFree(options);

    return res;
#else
    PyErr_SetString(NotSupportedError, "PQconninfo not available in libpq < 9.3");
    return NULL;
#endif
}


static const char status_doc[] =
"The status of the connection.\n"
"\n"
":type: `!int`\n"
"\n"
".. seealso:: libpq docs for `PQstatus()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQSTATUS";

static PyObject *
status_get(connInfoObject *self)
{
    ConnStatusType val;

    val = PQstatus(self->conn->pgconn);
    return PyInt_FromLong((long)val);
}


static const char transaction_status_doc[] =
"The current in-transaction status of the connection.\n"
"\n"
"Symbolic constants for the values are defined in the module\n"
"`psycopg2.extensions`: see :ref:`transaction-status-constants` for the\n"
"available values.\n"
"\n"
":type: `!int`\n"
"\n"
".. seealso:: libpq docs for `PQtransactionStatus()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQTRANSACTIONSTATUS";

static PyObject *
transaction_status_get(connInfoObject *self)
{
    PGTransactionStatusType val;

    val = PQtransactionStatus(self->conn->pgconn);
    return PyInt_FromLong((long)val);
}


static const char parameter_status_doc[] =
"Looks up a current parameter setting of the server.\n"
"\n"
":param name: The name of the parameter to return.\n"
":type name: `!str`\n"
":return: The parameter value, `!None` if the parameter is unknown.\n"
":rtype: `!str`\n"
"\n"
".. seealso:: libpq docs for `PQparameterStatus()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQPARAMETERSTATUS";

static PyObject *
parameter_status(connInfoObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"name", NULL};
    const char *name;
    const char *val;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &name)) {
        return NULL;
    }

    val = PQparameterStatus(self->conn->pgconn, name);

    if (!val) {
        Py_RETURN_NONE;
    }
    else {
        return conn_text_from_chars(self->conn, val);
    }
}


static const char protocol_version_doc[] =
"The frontend/backend protocol being used.\n"
"\n"
":type: `!int`\n"
"\n"
".. seealso:: libpq docs for `PQprotocolVersion()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQPROTOCOLVERSION";

static PyObject *
protocol_version_get(connInfoObject *self)
{
    int val;

    val = PQprotocolVersion(self->conn->pgconn);
    return PyInt_FromLong((long)val);
}


static const char server_version_doc[] =
"Returns an integer representing the server version.\n"
"\n"
":type: `!int`\n"
"\n"
".. seealso:: libpq docs for `PQserverVersion()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQSERVERVERSION";

static PyObject *
server_version_get(connInfoObject *self)
{
    int val;

    val = PQserverVersion(self->conn->pgconn);
    return PyInt_FromLong((long)val);
}


static const char error_message_doc[] =
"The error message most recently generated by an operation on the connection.\n"
"\n"
"`!None` if there is no current message.\n"
"\n"
".. seealso:: libpq docs for `PQerrorMessage()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQERRORMESSAGE";

static PyObject *
error_message_get(connInfoObject *self)
{
    const char *val;

    val = PQerrorMessage(self->conn->pgconn);
    if (!val || !val[0]) {
        Py_RETURN_NONE;
    }
    return conn_text_from_chars(self->conn, val);
}


static const char socket_doc[] =
"The file descriptor number of the connection socket to the server.\n"
"\n"
":type: `!int`\n"
"\n"
".. seealso:: libpq docs for `PQsocket()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQSOCKET";

static PyObject *
socket_get(connInfoObject *self)
{
    int val;

    val = PQsocket(self->conn->pgconn);
    return PyInt_FromLong((long)val);
}


static const char backend_pid_doc[] =
"The process ID (PID) of the backend process you connected to.\n"
"\n"
":type: `!int`\n"
"\n"
".. seealso:: libpq docs for `PQbackendPID()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQBACKENDPID";

static PyObject *
backend_pid_get(connInfoObject *self)
{
    int val;

    val = PQbackendPID(self->conn->pgconn);
    return PyInt_FromLong((long)val);
}


static const char needs_password_doc[] =
"The connection authentication method required a password, but none was available.\n"
"\n"
":type: `!bool`\n"
"\n"
".. seealso:: libpq docs for `PQconnectionNeedsPassword()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQCONNECTIONNEEDSPASSWORD";

static PyObject *
needs_password_get(connInfoObject *self)
{
    return PyBool_FromLong(PQconnectionNeedsPassword(self->conn->pgconn));
}


static const char used_password_doc[] =
"The connection authentication method used a password.\n"
"\n"
":type: `!bool`\n"
"\n"
".. seealso:: libpq docs for `PQconnectionUsedPassword()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQCONNECTIONUSEDPASSWORD";

static PyObject *
used_password_get(connInfoObject *self)
{
    return PyBool_FromLong(PQconnectionUsedPassword(self->conn->pgconn));
}


static const char ssl_in_use_doc[] =
"`!True` if the connection uses SSL, `!False` if not.\n"
"\n"
"Only available if psycopg was built with libpq >= 9.5; raise\n"
"`~psycopg2.NotSupportedError` otherwise.\n"
"\n"
":type: `!bool`\n"
"\n"
".. seealso:: libpq docs for `PQsslInUse()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQSSLINUSE";

static PyObject *
ssl_in_use_get(connInfoObject *self)
{
    PyObject *rv = NULL;

#if PG_VERSION_NUM >= 90500
    rv = PyBool_FromLong(PQsslInUse(self->conn->pgconn));
#else
    PyErr_SetString(NotSupportedError,
        "'ssl_in_use' not available in libpq < 9.5");
#endif
    return rv;
}


static const char ssl_attribute_doc[] =
"Returns SSL-related information about the connection.\n"
"\n"
":param name: The name of the attribute to return.\n"
":type name: `!str`\n"
":return: The attribute value, `!None` if unknown.\n"
":rtype: `!str`\n"
"\n"
"Only available if psycopg was built with libpq >= 9.5; raise\n"
"`~psycopg2.NotSupportedError` otherwise.\n"
"\n"
"Valid names are available in `ssl_attribute_names`.\n"
"\n"
".. seealso:: libpq docs for `PQsslAttribute()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQSSLATTRIBUTE";

static PyObject *
ssl_attribute(connInfoObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"name", NULL};
    const char *name;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &name)) {
        return NULL;
    }

#if PG_VERSION_NUM >= 90500
    {
        const char *val;

        val = PQsslAttribute(self->conn->pgconn, name);

        if (!val) {
            Py_RETURN_NONE;
        }
        else {
            return conn_text_from_chars(self->conn, val);
        }
    }
#else
    PyErr_SetString(NotSupportedError,
        "'ssl_attribute()' not available in libpq < 9.5");
    return NULL;
#endif
}

static const char ssl_attribute_names_doc[] =
"The list of the SSL attribute names available.\n"
"\n"
":type: `!list` of `!str`\n"
"\n"
"Only available if psycopg was built with libpq >= 9.5; raise\n"
"`~psycopg2.NotSupportedError` otherwise.\n"
"\n"
".. seealso:: libpq docs for `PQsslAttributeNames()`__ for details.\n"
".. __: https://www.postgresql.org/docs/current/static/libpq-status.html"
    "#LIBPQ-PQSSLATTRIBUTENAMES";

static PyObject *
ssl_attribute_names_get(connInfoObject *self)
{
#if PG_VERSION_NUM >= 90500
    const char* const* names;
    int i;
    PyObject *l = NULL, *s = NULL, *rv = NULL;

    names = PQsslAttributeNames(self->conn->pgconn);
    if (!(l = PyList_New(0))) { goto exit; }

    for (i = 0; names[i]; i++) {
        if (!(s = conn_text_from_chars(self->conn, names[i]))) { goto exit; }
        if (0 != PyList_Append(l, s)) { goto exit; }
        Py_CLEAR(s);
    }

    rv = l;
    l = NULL;

exit:
    Py_XDECREF(l);
    Py_XDECREF(s);
    return rv;
#else
    PyErr_SetString(NotSupportedError,
        "'ssl_attribute_names not available in libpq < 9.5");
    return NULL;
#endif
}


static struct PyGetSetDef connInfoObject_getsets[] = {
    { "dbname", (getter)dbname_get, NULL, (char *)dbname_doc },
    { "user", (getter)user_get, NULL, (char *)user_doc },
    { "password", (getter)password_get, NULL, (char *)password_doc },
    { "host", (getter)host_get, NULL, (char *)host_doc },
    { "port", (getter)port_get, NULL, (char *)port_doc },
    { "options", (getter)options_get, NULL, (char *)options_doc },
    { "dsn_parameters", (getter)dsn_parameters_get, NULL,
        (char *)dsn_parameters_doc },
    { "status", (getter)status_get, NULL, (char *)status_doc },
    { "transaction_status", (getter)transaction_status_get, NULL,
        (char *)transaction_status_doc },
    { "protocol_version", (getter)protocol_version_get, NULL,
        (char *)protocol_version_doc },
    { "server_version", (getter)server_version_get, NULL,
        (char *)server_version_doc },
    { "error_message", (getter)error_message_get, NULL,
        (char *)error_message_doc },
    { "socket", (getter)socket_get, NULL, (char *)socket_doc },
    { "backend_pid", (getter)backend_pid_get, NULL, (char *)backend_pid_doc },
    { "used_password", (getter)used_password_get, NULL,
        (char *)used_password_doc },
    { "needs_password", (getter)needs_password_get, NULL,
        (char *)needs_password_doc },
    { "ssl_in_use", (getter)ssl_in_use_get, NULL,
        (char *)ssl_in_use_doc },
    { "ssl_attribute_names", (getter)ssl_attribute_names_get, NULL,
        (char *)ssl_attribute_names_doc },
    {NULL}
};

static struct PyMethodDef connInfoObject_methods[] = {
    {"ssl_attribute", (PyCFunction)ssl_attribute,
     METH_VARARGS|METH_KEYWORDS, ssl_attribute_doc},
    {"parameter_status", (PyCFunction)parameter_status,
     METH_VARARGS|METH_KEYWORDS, parameter_status_doc},
    {NULL}
};

/* initialization and finalization methods */

static PyObject *
conninfo_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static int
conninfo_init(connInfoObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *conn = NULL;

    if (!PyArg_ParseTuple(args, "O", &conn))
        return -1;

    if (!PyObject_TypeCheck(conn, &connectionType)) {
        PyErr_SetString(PyExc_TypeError,
            "The argument must be a psycopg2 connection");
        return -1;
    }

    Py_INCREF(conn);
    self->conn = (connectionObject *)conn;
    return 0;
}

static void
conninfo_dealloc(connInfoObject* self)
{
    Py_CLEAR(self->conn);
    Py_TYPE(self)->tp_free((PyObject *)self);
}


/* object type */

PyTypeObject connInfoType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2.extensions.ConnectionInfo",
    sizeof(connInfoObject), 0,
    (destructor)conninfo_dealloc, /*tp_dealloc*/
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
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    connInfoType_doc, /*tp_doc*/
    0,          /*tp_traverse*/
    0,          /*tp_clear*/
    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/
    0,          /*tp_iter*/
    0,          /*tp_iternext*/
    connInfoObject_methods, /*tp_methods*/
    0,          /*tp_members*/
    connInfoObject_getsets, /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/
    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/
    (initproc)conninfo_init, /*tp_init*/
    0,          /*tp_alloc*/
    conninfo_new, /*tp_new*/
};
