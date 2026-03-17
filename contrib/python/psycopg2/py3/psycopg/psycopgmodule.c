/* psycopgmodule.c - psycopg module (will import other C classes)
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

#include "psycopg/connection.h"
#include "psycopg/cursor.h"
#include "psycopg/replication_connection.h"
#include "psycopg/replication_cursor.h"
#include "psycopg/replication_message.h"
#include "psycopg/green.h"
#include "psycopg/column.h"
#include "psycopg/lobject.h"
#include "psycopg/notify.h"
#include "psycopg/xid.h"
#include "psycopg/typecast.h"
#include "psycopg/microprotocols.h"
#include "psycopg/microprotocols_proto.h"
#include "psycopg/conninfo.h"
#include "psycopg/diagnostics.h"

#include "psycopg/adapter_qstring.h"
#include "psycopg/adapter_binary.h"
#include "psycopg/adapter_pboolean.h"
#include "psycopg/adapter_pint.h"
#include "psycopg/adapter_pfloat.h"
#include "psycopg/adapter_pdecimal.h"
#include "psycopg/adapter_asis.h"
#include "psycopg/adapter_list.h"
#include "psycopg/typecast_binary.h"

/* some module-level variables, like the datetime module */
#include <datetime.h>
#include "psycopg/adapter_datetime.h"

#include <stdlib.h>

HIDDEN PyObject *psycoEncodings = NULL;
HIDDEN PyObject *sqlstate_errors = NULL;

#ifdef PSYCOPG_DEBUG
HIDDEN int psycopg_debug_enabled = 0;
#endif

/* Python representation of SQL NULL */
HIDDEN PyObject *psyco_null = NULL;

/* macro trick to stringify a macro expansion */
#define xstr(s) str(s)
#define str(s) #s

/** connect module-level function **/
#define psyco_connect_doc \
"_connect(dsn, [connection_factory], [async]) -- New database connection.\n\n"

static PyObject *
psyco_connect(PyObject *self, PyObject *args, PyObject *keywds)
{
    PyObject *conn = NULL;
    PyObject *factory = NULL;
    const char *dsn = NULL;
    int async = 0, async_ = 0;

    static char *kwlist[] = {"dsn", "connection_factory", "async", "async_", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, keywds, "s|Oii", kwlist,
            &dsn, &factory, &async, &async_)) {
        return NULL;
    }

    if (async_) { async = async_; }

    Dprintf("psyco_connect: dsn = '%s', async = %d", dsn, async);

    /* allocate connection, fill with errors and return it */
    if (factory == NULL || factory == Py_None) {
        factory = (PyObject *)&connectionType;
    }

    /* Here we are breaking the connection.__init__ interface defined
     * by psycopg2. So, if not requiring an async conn, avoid passing
     * the async parameter. */
    /* TODO: would it be possible to avoid an additional parameter
     * to the conn constructor? A subclass? (but it would require mixins
     * to further subclass) Another dsn parameter (but is not really
     * a connection parameter that can be configured) */
    if (!async) {
        conn = PyObject_CallFunction(factory, "s", dsn);
    } else {
        conn = PyObject_CallFunction(factory, "si", dsn, async);
    }

    return conn;
}


#define parse_dsn_doc \
"parse_dsn(dsn) -> dict -- parse a connection string into parameters"

static PyObject *
parse_dsn(PyObject *self, PyObject *args, PyObject *kwargs)
{
    char *err = NULL;
    PQconninfoOption *options = NULL;
    PyObject *res = NULL, *dsn;

    static char *kwlist[] = {"dsn", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &dsn)) {
        return NULL;
    }

    Py_INCREF(dsn); /* for ensure_bytes */
    if (!(dsn = psyco_ensure_bytes(dsn))) { goto exit; }

    options = PQconninfoParse(Bytes_AS_STRING(dsn), &err);
    if (options == NULL) {
        if (err != NULL) {
            PyErr_Format(ProgrammingError, "invalid dsn: %s", err);
            PQfreemem(err);
        } else {
            PyErr_SetString(OperationalError, "PQconninfoParse() failed");
        }
        goto exit;
    }

    res = psyco_dict_from_conninfo_options(options, /* include_password = */ 1);

exit:
    PQconninfoFree(options);    /* safe on null */
    Py_XDECREF(dsn);

    return res;
}


#define quote_ident_doc \
"quote_ident(str, conn_or_curs) -> str -- wrapper around PQescapeIdentifier\n\n" \
":Parameters:\n" \
"  * `str`: A bytes or unicode object\n" \
"  * `conn_or_curs`: A connection or cursor, required"

static PyObject *
quote_ident(PyObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *ident = NULL, *obj = NULL, *result = NULL;
    connectionObject *conn;
    char *quoted = NULL;

    static char *kwlist[] = {"ident", "scope", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", kwlist, &ident, &obj)) {
        return NULL;
    }

    if (PyObject_TypeCheck(obj, &cursorType)) {
        conn = ((cursorObject*)obj)->conn;
    }
    else if (PyObject_TypeCheck(obj, &connectionType)) {
        conn = (connectionObject*)obj;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "argument 2 must be a connection or a cursor");
        return NULL;
    }

    Py_INCREF(ident); /* for ensure_bytes */
    if (!(ident = psyco_ensure_bytes(ident))) { goto exit; }

    if (!(quoted = psyco_escape_identifier(conn,
        Bytes_AS_STRING(ident), Bytes_GET_SIZE(ident)))) { goto exit; }

    result = conn_text_from_chars(conn, quoted);

exit:
    PQfreemem(quoted);
    Py_XDECREF(ident);

    return result;
}

/** type registration **/
#define register_type_doc \
"register_type(obj, conn_or_curs) -> None -- register obj with psycopg type system\n\n" \
":Parameters:\n" \
"  * `obj`: A type adapter created by `new_type()`\n" \
"  * `conn_or_curs`: A connection, cursor or None"

#define typecast_from_python_doc \
"new_type(oids, name, castobj) -> new type object\n\n" \
"Create a new binding object. The object can be used with the\n" \
"`register_type()` function to bind PostgreSQL objects to python objects.\n\n" \
":Parameters:\n" \
"  * `oids`: Tuple of ``oid`` of the PostgreSQL types to convert.\n" \
"  * `name`: Name for the new type\n" \
"  * `adapter`: Callable to perform type conversion.\n" \
"    It must have the signature ``fun(value, cur)`` where ``value`` is\n" \
"    the string representation returned by PostgreSQL (`!None` if ``NULL``)\n" \
"    and ``cur`` is the cursor from which data are read."

#define typecast_array_from_python_doc \
"new_array_type(oids, name, baseobj) -> new type object\n\n" \
"Create a new binding object to parse an array.\n\n" \
"The object can be used with `register_type()`.\n\n" \
":Parameters:\n" \
"  * `oids`: Tuple of ``oid`` of the PostgreSQL types to convert.\n" \
"  * `name`: Name for the new type\n" \
"  * `baseobj`: Adapter to perform type conversion of a single array item."

static PyObject *
register_type(PyObject *self, PyObject *args)
{
    PyObject *type, *obj = NULL;

    if (!PyArg_ParseTuple(args, "O!|O", &typecastType, &type, &obj)) {
        return NULL;
    }

    if (obj != NULL && obj != Py_None) {
        if (PyObject_TypeCheck(obj, &cursorType)) {
            PyObject **dict = &(((cursorObject*)obj)->string_types);
            if (*dict == NULL) {
                if (!(*dict = PyDict_New())) { return NULL; }
            }
            if (0 > typecast_add(type, *dict, 0)) { return NULL; }
        }
        else if (PyObject_TypeCheck(obj, &connectionType)) {
            if (0 > typecast_add(type, ((connectionObject*)obj)->string_types, 0)) {
                return NULL;
            }
        }
        else {
            PyErr_SetString(PyExc_TypeError,
                "argument 2 must be a connection, cursor or None");
            return NULL;
        }
    }
    else {
        if (0 > typecast_add(type, NULL, 0)) { return NULL; }
    }

    Py_RETURN_NONE;
}



/* Make sure libcrypto thread callbacks are set up. */
static void
libcrypto_threads_init(void)
{
    PyObject *m;

    Dprintf("psycopgmodule: configuring libpq libcrypto callbacks ");

    /* importing the ssl module sets up Python's libcrypto callbacks */
    if ((m = PyImport_ImportModule("ssl"))) {
        /* disable libcrypto setup in libpq, so it won't stomp on the callbacks
           that have already been set up */
        PQinitOpenSSL(1, 0);
        Py_DECREF(m);
    }
    else {
        /* might mean that Python has been compiled without OpenSSL support,
           fall back to relying on libpq's libcrypto locking */
        PyErr_Clear();
    }
}

/* Initialize the default adapters map
 *
 * Return 0 on success, else -1 and set an exception.
 */
RAISES_NEG static int
adapters_init(PyObject *module)
{
    PyObject *dict = NULL, *obj = NULL;
    int rv = -1;

    if (0 > microprotocols_init(module)) { goto exit; }

    Dprintf("psycopgmodule: initializing adapters");

    if (0 > microprotocols_add(&PyFloat_Type, NULL, (PyObject*)&pfloatType)) {
        goto exit;
    }
    if (0 > microprotocols_add(&PyLong_Type, NULL, (PyObject*)&pintType)) {
        goto exit;
    }
    if (0 > microprotocols_add(&PyBool_Type, NULL, (PyObject*)&pbooleanType)) {
        goto exit;
    }

    /* strings */
    if (0 > microprotocols_add(&PyUnicode_Type, NULL, (PyObject*)&qstringType)) {
        goto exit;
    }

    /* binary */
    if (0 > microprotocols_add(&PyBytes_Type, NULL, (PyObject*)&binaryType)) {
        goto exit;
    }

    if (0 > microprotocols_add(&PyByteArray_Type, NULL, (PyObject*)&binaryType)) {
        goto exit;
    }

    if (0 > microprotocols_add(&PyMemoryView_Type, NULL, (PyObject*)&binaryType)) {
        goto exit;
    }

    if (0 > microprotocols_add(&PyList_Type, NULL, (PyObject*)&listType)) {
        goto exit;
    }

    /* the module has already been initialized, so we can obtain the callable
       objects directly from its dictionary :) */
    if (!(dict = PyModule_GetDict(module))) { goto exit; }

    if (!(obj = PyMapping_GetItemString(dict, "DateFromPy"))) { goto exit; }
    if (0 > microprotocols_add(PyDateTimeAPI->DateType, NULL, obj)) { goto exit; }
    Py_CLEAR(obj);

    if (!(obj = PyMapping_GetItemString(dict, "TimeFromPy"))) { goto exit; }
    if (0 > microprotocols_add(PyDateTimeAPI->TimeType, NULL, obj)) { goto exit; }
    Py_CLEAR(obj);

    if (!(obj = PyMapping_GetItemString(dict, "TimestampFromPy"))) { goto exit; }
    if (0 > microprotocols_add(PyDateTimeAPI->DateTimeType, NULL, obj)) { goto exit; }
    Py_CLEAR(obj);

    if (!(obj = PyMapping_GetItemString(dict, "IntervalFromPy"))) { goto exit; }
    if (0 > microprotocols_add(PyDateTimeAPI->DeltaType, NULL, obj)) { goto exit; }
    Py_CLEAR(obj);

    /* Success! */
    rv = 0;

exit:
    Py_XDECREF(obj);

    return rv;
}

#define libpq_version_doc "Query actual libpq version loaded."

static PyObject*
libpq_version(PyObject *self, PyObject *dummy)
{
    return PyInt_FromLong(PQlibVersion());
}

/* encrypt_password - Prepare the encrypted password form */
#define encrypt_password_doc \
"encrypt_password(password, user, [scope], [algorithm]) -- Prepares the encrypted form of a PostgreSQL password.\n\n"

static PyObject *
encrypt_password(PyObject *self, PyObject *args, PyObject *kwargs)
{
    char *encrypted = NULL;
    PyObject *password = NULL, *user = NULL;
    PyObject *scope = Py_None, *algorithm = Py_None;
    PyObject *res = NULL;
    connectionObject *conn = NULL;

    static char *kwlist[] = {"password", "user", "scope", "algorithm", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|OO", kwlist,
            &password, &user, &scope, &algorithm)) {
        return NULL;
    }

    /* for ensure_bytes */
    Py_INCREF(user);
    Py_INCREF(password);
    Py_INCREF(algorithm);

    if (scope != Py_None) {
        if (PyObject_TypeCheck(scope, &cursorType)) {
            conn = ((cursorObject*)scope)->conn;
        }
        else if (PyObject_TypeCheck(scope, &connectionType)) {
            conn = (connectionObject*)scope;
        }
        else {
            PyErr_SetString(PyExc_TypeError,
                "the scope must be a connection or a cursor");
            goto exit;
        }
    }

    if (!(user = psyco_ensure_bytes(user))) { goto exit; }
    if (!(password = psyco_ensure_bytes(password))) { goto exit; }
    if (algorithm != Py_None) {
        if (!(algorithm = psyco_ensure_bytes(algorithm))) {
            goto exit;
        }
    }

    /* If we have to encrypt md5 we can use the libpq < 10 API */
    if (algorithm != Py_None &&
            strcmp(Bytes_AS_STRING(algorithm), "md5") == 0) {
        encrypted = PQencryptPassword(
            Bytes_AS_STRING(password), Bytes_AS_STRING(user));
    }

    /* If the algorithm is not md5 we have to use the API available from
     * libpq 10. */
    else {
#if PG_VERSION_NUM >= 100000
        if (!conn) {
            PyErr_SetString(ProgrammingError,
                "password encryption (other than 'md5' algorithm)"
                " requires a connection or cursor");
            goto exit;
        }

        /* TODO: algo = None will block: forbid on async/green conn? */
        encrypted = PQencryptPasswordConn(conn->pgconn,
            Bytes_AS_STRING(password), Bytes_AS_STRING(user),
            algorithm != Py_None ? Bytes_AS_STRING(algorithm) : NULL);
#else
        PyErr_SetString(NotSupportedError,
            "password encryption (other than 'md5' algorithm)"
            " requires libpq 10");
        goto exit;
#endif
    }

    if (encrypted) {
        res = Text_FromUTF8(encrypted);
    }
    else {
        const char *msg = PQerrorMessage(conn->pgconn);
        PyErr_Format(ProgrammingError,
            "password encryption failed: %s", msg ? msg : "no reason given");
        goto exit;
    }

exit:
    if (encrypted) {
        PQfreemem(encrypted);
    }
    Py_XDECREF(user);
    Py_XDECREF(password);
    Py_XDECREF(algorithm);

    return res;
}


/* Fill the module's postgresql<->python encoding table */
static struct {
    char *pgenc;
    char *pyenc;
} enctable[] = {
    {"ABC",          "cp1258"},
    {"ALT",          "cp866"},
    {"BIG5",         "big5"},
    {"EUC_CN",       "euccn"},
    {"EUC_JIS_2004", "euc_jis_2004"},
    {"EUC_JP",       "euc_jp"},
    {"EUC_KR",       "euc_kr"},
    {"GB18030",      "gb18030"},
    {"GBK",          "gbk"},
    {"ISO_8859_1",   "iso8859_1"},
    {"ISO_8859_2",   "iso8859_2"},
    {"ISO_8859_3",   "iso8859_3"},
    {"ISO_8859_5",   "iso8859_5"},
    {"ISO_8859_6",   "iso8859_6"},
    {"ISO_8859_7",   "iso8859_7"},
    {"ISO_8859_8",   "iso8859_8"},
    {"ISO_8859_9",   "iso8859_9"},
    {"ISO_8859_10",  "iso8859_10"},
    {"ISO_8859_13",  "iso8859_13"},
    {"ISO_8859_14",  "iso8859_14"},
    {"ISO_8859_15",  "iso8859_15"},
    {"ISO_8859_16",  "iso8859_16"},
    {"JOHAB",        "johab"},
    {"KOI8",         "koi8_r"},
    {"KOI8R",        "koi8_r"},
    {"KOI8U",        "koi8_u"},
    {"LATIN1",       "iso8859_1"},
    {"LATIN2",       "iso8859_2"},
    {"LATIN3",       "iso8859_3"},
    {"LATIN4",       "iso8859_4"},
    {"LATIN5",       "iso8859_9"},
    {"LATIN6",       "iso8859_10"},
    {"LATIN7",       "iso8859_13"},
    {"LATIN8",       "iso8859_14"},
    {"LATIN9",       "iso8859_15"},
    {"LATIN10",      "iso8859_16"},
    {"Mskanji",      "cp932"},
    {"ShiftJIS",     "cp932"},
    {"SHIFT_JIS_2004", "shift_jis_2004"},
    {"SJIS",         "cp932"},
    {"SQL_ASCII",    "ascii"},  /* XXX this is wrong: SQL_ASCII means "no
                                 *  encoding" we should fix the unicode
                                 *  typecaster to return a str or bytes in Py3
                                 */
    {"TCVN",         "cp1258"},
    {"TCVN5712",     "cp1258"},
    {"UHC",          "cp949"},
    {"UNICODE",      "utf_8"}, /* Not valid in 8.2, backward compatibility */
    {"UTF8",         "utf_8"},
    {"VSCII",        "cp1258"},
    {"WIN",          "cp1251"},
    {"WIN866",       "cp866"},
    {"WIN874",       "cp874"},
    {"WIN932",       "cp932"},
    {"WIN936",       "gbk"},
    {"WIN949",       "cp949"},
    {"WIN950",       "cp950"},
    {"WIN1250",      "cp1250"},
    {"WIN1251",      "cp1251"},
    {"WIN1252",      "cp1252"},
    {"WIN1253",      "cp1253"},
    {"WIN1254",      "cp1254"},
    {"WIN1255",      "cp1255"},
    {"WIN1256",      "cp1256"},
    {"WIN1257",      "cp1257"},
    {"WIN1258",      "cp1258"},
    {"Windows932",   "cp932"},
    {"Windows936",   "gbk"},
    {"Windows949",   "cp949"},
    {"Windows950",   "cp950"},

/* those are missing from Python:                */
/*    {"EUC_TW", "?"},                           */
/*    {"MULE_INTERNAL", "?"},                    */
    {NULL, NULL}
};

/* Initialize the encodings table.
 *
 * Return 0 on success, else -1 and set an exception.
 */
RAISES_NEG static int
encodings_init(PyObject *module)
{
    PyObject *value = NULL;
    int i;
    int rv = -1;

    Dprintf("psycopgmodule: initializing encodings table");
    if (psycoEncodings) {
        Dprintf("encodings_init(): already called");
        return 0;
    }

    if (!(psycoEncodings = PyDict_New())) { goto exit; }
    Py_INCREF(psycoEncodings);
    if (0 > PyModule_AddObject(module, "encodings", psycoEncodings)) {
        Py_DECREF(psycoEncodings);
        goto exit;
    }

    for (i = 0; enctable[i].pgenc != NULL; i++) {
        if (!(value = Text_FromUTF8(enctable[i].pyenc))) { goto exit; }
        if (0 > PyDict_SetItemString(
                psycoEncodings, enctable[i].pgenc, value)) {
            goto exit;
        }
        Py_CLEAR(value);
    }
    rv = 0;

exit:
    Py_XDECREF(value);

    return rv;
}

/* Initialize the module's exceptions and after that a dictionary with a full
   set of exceptions. */

PyObject *Error, *Warning, *InterfaceError, *DatabaseError,
    *InternalError, *OperationalError, *ProgrammingError,
    *IntegrityError, *DataError, *NotSupportedError;
PyObject *QueryCanceledError, *TransactionRollbackError;

/* mapping between exception names and their PyObject */
static struct {
    char *name;
    PyObject **exc;
    PyObject **base;
    const char *docstr;
} exctable[] = {
    { "psycopg2.Error", &Error, NULL, Error_doc },
    { "psycopg2.Warning", &Warning, NULL, Warning_doc },
    { "psycopg2.InterfaceError", &InterfaceError, &Error, InterfaceError_doc },
    { "psycopg2.DatabaseError", &DatabaseError, &Error, DatabaseError_doc },
    { "psycopg2.InternalError", &InternalError, &DatabaseError, InternalError_doc },
    { "psycopg2.OperationalError", &OperationalError, &DatabaseError,
        OperationalError_doc },
    { "psycopg2.ProgrammingError", &ProgrammingError, &DatabaseError,
        ProgrammingError_doc },
    { "psycopg2.IntegrityError", &IntegrityError, &DatabaseError,
        IntegrityError_doc },
    { "psycopg2.DataError", &DataError, &DatabaseError, DataError_doc },
    { "psycopg2.NotSupportedError", &NotSupportedError, &DatabaseError,
        NotSupportedError_doc },
    { "psycopg2.extensions.QueryCanceledError", &QueryCanceledError,
      &OperationalError, QueryCanceledError_doc },
    { "psycopg2.extensions.TransactionRollbackError",
      &TransactionRollbackError, &OperationalError,
      TransactionRollbackError_doc },
    {NULL}  /* Sentinel */
};


RAISES_NEG static int
basic_errors_init(PyObject *module)
{
    /* the names of the exceptions here reflect the organization of the
       psycopg2 module and not the fact the original error objects live in
       _psycopg */

    int i;
    PyObject *dict = NULL;
    PyObject *str = NULL;
    PyObject *errmodule = NULL;
    int rv = -1;

    Dprintf("psycopgmodule: initializing basic exceptions");

    /* 'Error' has been defined elsewhere: only init the other classes */
    Error = (PyObject *)&errorType;

    for (i = 1; exctable[i].name; i++) {
        if (!(dict = PyDict_New())) { goto exit; }

        if (exctable[i].docstr) {
            if (!(str = Text_FromUTF8(exctable[i].docstr))) { goto exit; }
            if (0 > PyDict_SetItemString(dict, "__doc__", str)) { goto exit; }
            Py_CLEAR(str);
        }

        /* can't put PyExc_StandardError in the static exctable:
         * windows build will fail */
        if (!(*exctable[i].exc = PyErr_NewException(
                exctable[i].name,
                exctable[i].base ? *exctable[i].base : PyExc_StandardError,
                dict))) {
            goto exit;
        }
        Py_CLEAR(dict);
    }

    if (!(errmodule = PyImport_ImportModule("psycopg2.errors"))) {
        /* don't inject the exceptions into the errors module */
        PyErr_Clear();
    }

    for (i = 0; exctable[i].name; i++) {
        char *name;
        if (NULL == exctable[i].exc) { continue; }

        /* the name is the part after the last dot */
        name = strrchr(exctable[i].name, '.');
        name = name ? name + 1 : exctable[i].name;

        Py_INCREF(*exctable[i].exc);
        if (0 > PyModule_AddObject(module, name, *exctable[i].exc)) {
            Py_DECREF(*exctable[i].exc);
            goto exit;
        }
        if (errmodule) {
            Py_INCREF(*exctable[i].exc);
            if (0 > PyModule_AddObject(errmodule, name, *exctable[i].exc)) {
                Py_DECREF(*exctable[i].exc);
                goto exit;
            }
        }
    }

    rv = 0;

exit:
    Py_XDECREF(errmodule);
    Py_XDECREF(str);
    Py_XDECREF(dict);
    return rv;
}


/* mapping between sqlstate and exception name */
static struct {
    char *sqlstate;
    char *name;
} sqlstate_table[] = {
#include "sqlstate_errors.h"
    {NULL}  /* Sentinel */
};


RAISES_NEG static int
sqlstate_errors_init(PyObject *module)
{
    int i;
    char namebuf[120];
    char prefix[] = "psycopg2.errors.";
    char *suffix;
    size_t bufsize;
    PyObject *exc = NULL;
    PyObject *errmodule = NULL;
    int rv = -1;

    Dprintf("psycopgmodule: initializing sqlstate exceptions");

    if (sqlstate_errors) {
		Dprintf("sqlstate_errors_init(): already called");
        return 0;
    }
    if (!(errmodule = PyImport_ImportModule("psycopg2.errors"))) {
        /* don't inject the exceptions into the errors module */
        PyErr_Clear();
    }
    if (!(sqlstate_errors = PyDict_New())) {
        goto exit;
    }
    Py_INCREF(sqlstate_errors);
    if (0 > PyModule_AddObject(module, "sqlstate_errors", sqlstate_errors)) {
        Py_DECREF(sqlstate_errors);
        return -1;
    }

    strcpy(namebuf, prefix);
    suffix = namebuf + sizeof(prefix) - 1;
    bufsize = sizeof(namebuf) - sizeof(prefix) - 1;
    /* If this 0 gets deleted the buffer was too small. */
    namebuf[sizeof(namebuf) - 1] = '\0';

    for (i = 0; sqlstate_table[i].sqlstate; i++) {
        PyObject *base;

        base = base_exception_from_sqlstate(sqlstate_table[i].sqlstate);
        strncpy(suffix, sqlstate_table[i].name, bufsize);
        if (namebuf[sizeof(namebuf) - 1] != '\0') {
            PyErr_SetString(
                PyExc_SystemError, "sqlstate_errors_init(): buffer too small");
            goto exit;
        }
        if (!(exc = PyErr_NewException(namebuf, base, NULL))) {
            goto exit;
        }
        if (0 > PyDict_SetItemString(
                sqlstate_errors, sqlstate_table[i].sqlstate, exc)) {
            goto exit;
        }

        /* Expose the exceptions to psycopg2.errors */
        if (errmodule) {
            if (0 > PyModule_AddObject(
                    errmodule, sqlstate_table[i].name, exc)) {
                goto exit;
            }
            else {
                exc = NULL;     /* ref stolen by the module */
            }
        }
        else {
            Py_CLEAR(exc);
        }
    }

    rv = 0;

exit:
    Py_XDECREF(errmodule);
    Py_XDECREF(exc);
    return rv;
}


RAISES_NEG static int
add_module_constants(PyObject *module)
{
    PyObject *tmp;
    Dprintf("psycopgmodule: initializing module constants");

    if (0 > PyModule_AddStringConstant(module,
        "__version__", xstr(PSYCOPG_VERSION)))
    { return -1; }

    if (0 > PyModule_AddStringConstant(module,
        "__doc__", "psycopg2 PostgreSQL driver"))
    { return -1; }

    if (0 > PyModule_AddIntConstant(module,
        "__libpq_version__", PG_VERSION_NUM))
    { return -1; }

    if (0 > PyModule_AddObject(module,
        "apilevel", tmp = Text_FromUTF8(APILEVEL)))
    {
        Py_XDECREF(tmp);
        return -1;
    }

    if (0 > PyModule_AddObject(module,
        "threadsafety", tmp = PyInt_FromLong(THREADSAFETY)))
    {
        Py_XDECREF(tmp);
        return -1;
    }

    if (0 > PyModule_AddObject(module,
        "paramstyle", tmp = Text_FromUTF8(PARAMSTYLE)))
    {
        Py_XDECREF(tmp);
        return -1;
    }

    if (0 > PyModule_AddIntMacro(module, REPLICATION_PHYSICAL)) { return -1; }
    if (0 > PyModule_AddIntMacro(module, REPLICATION_LOGICAL)) { return -1; }

    return 0;
}


static struct {
    char *name;
    PyTypeObject *type;
} typetable[] = {
    { "connection", &connectionType },
    { "cursor", &cursorType },
    { "ReplicationConnection", &replicationConnectionType },
    { "ReplicationCursor", &replicationCursorType },
    { "ReplicationMessage", &replicationMessageType },
    { "ISQLQuote", &isqlquoteType },
    { "Column", &columnType },
    { "Notify", &notifyType },
    { "Xid", &xidType },
    { "ConnectionInfo", &connInfoType },
    { "Diagnostics", &diagnosticsType },
    { "AsIs", &asisType },
    { "Binary", &binaryType },
    { "Boolean", &pbooleanType },
    { "Decimal", &pdecimalType },
    { "Int", &pintType },
    { "Float", &pfloatType },
    { "List", &listType },
    { "QuotedString", &qstringType },
    { "lobject", &lobjectType },
    {NULL}  /* Sentinel */
};

RAISES_NEG static int
add_module_types(PyObject *module)
{
    int i;

    Dprintf("psycopgmodule: initializing module types");

    for (i = 0; typetable[i].name; i++) {
        PyObject *type = (PyObject *)typetable[i].type;

        Py_SET_TYPE(typetable[i].type, &PyType_Type);
        if (0 > PyType_Ready(typetable[i].type)) { return -1; }

        Py_INCREF(type);
        if (0 > PyModule_AddObject(module, typetable[i].name, type)) {
            Py_DECREF(type);
            return -1;
        }
    }
    return 0;
}


RAISES_NEG static int
datetime_init(void)
{
    PyObject *dt = NULL;

    Dprintf("psycopgmodule: initializing datetime module");

    /* import python builtin datetime module, if available */
    if (!(dt = PyImport_ImportModule("datetime"))) {
        return -1;
    }
    Py_DECREF(dt);

    /* Initialize the PyDateTimeAPI everywhere is used */
    PyDateTime_IMPORT;
    if (0 > adapter_datetime_init()) { return -1; }
    if (0 > repl_curs_datetime_init()) { return -1; }
    if (0 > replmsg_datetime_init()) { return -1; }

    Py_SET_TYPE(&pydatetimeType, &PyType_Type);
    if (0 > PyType_Ready(&pydatetimeType)) { return -1; }

    return 0;
}

/** method table and module initialization **/

static PyMethodDef psycopgMethods[] = {
    {"_connect",  (PyCFunction)psyco_connect,
     METH_VARARGS|METH_KEYWORDS, psyco_connect_doc},
    {"parse_dsn",  (PyCFunction)parse_dsn,
     METH_VARARGS|METH_KEYWORDS, parse_dsn_doc},
    {"quote_ident", (PyCFunction)quote_ident,
     METH_VARARGS|METH_KEYWORDS, quote_ident_doc},
    {"adapt",  (PyCFunction)psyco_microprotocols_adapt,
     METH_VARARGS, psyco_microprotocols_adapt_doc},

    {"register_type", (PyCFunction)register_type,
     METH_VARARGS, register_type_doc},
    {"new_type", (PyCFunction)typecast_from_python,
     METH_VARARGS|METH_KEYWORDS, typecast_from_python_doc},
    {"new_array_type", (PyCFunction)typecast_array_from_python,
     METH_VARARGS|METH_KEYWORDS, typecast_array_from_python_doc},
    {"libpq_version", (PyCFunction)libpq_version,
     METH_NOARGS, libpq_version_doc},

    {"Date",  (PyCFunction)psyco_Date,
     METH_VARARGS, psyco_Date_doc},
    {"Time",  (PyCFunction)psyco_Time,
     METH_VARARGS, psyco_Time_doc},
    {"Timestamp",  (PyCFunction)psyco_Timestamp,
     METH_VARARGS, psyco_Timestamp_doc},
    {"DateFromTicks",  (PyCFunction)psyco_DateFromTicks,
     METH_VARARGS, psyco_DateFromTicks_doc},
    {"TimeFromTicks",  (PyCFunction)psyco_TimeFromTicks,
     METH_VARARGS, psyco_TimeFromTicks_doc},
    {"TimestampFromTicks",  (PyCFunction)psyco_TimestampFromTicks,
     METH_VARARGS, psyco_TimestampFromTicks_doc},

    {"DateFromPy",  (PyCFunction)psyco_DateFromPy,
     METH_VARARGS, psyco_DateFromPy_doc},
    {"TimeFromPy",  (PyCFunction)psyco_TimeFromPy,
     METH_VARARGS, psyco_TimeFromPy_doc},
    {"TimestampFromPy",  (PyCFunction)psyco_TimestampFromPy,
     METH_VARARGS, psyco_TimestampFromPy_doc},
    {"IntervalFromPy",  (PyCFunction)psyco_IntervalFromPy,
     METH_VARARGS, psyco_IntervalFromPy_doc},

    {"set_wait_callback",  (PyCFunction)psyco_set_wait_callback,
     METH_O, psyco_set_wait_callback_doc},
    {"get_wait_callback",  (PyCFunction)psyco_get_wait_callback,
     METH_NOARGS, psyco_get_wait_callback_doc},
    {"encrypt_password", (PyCFunction)encrypt_password,
     METH_VARARGS|METH_KEYWORDS, encrypt_password_doc},

    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef psycopgmodule = {
        PyModuleDef_HEAD_INIT,
        "_psycopg",
        NULL,
        -1,
        psycopgMethods,
        NULL,
        NULL,
        NULL,
        NULL
};

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
INIT_MODULE(_psycopg)(void)
{
    PyObject *module = NULL;

#ifdef PSYCOPG_DEBUG
    if (getenv("PSYCOPG_DEBUG"))
        psycopg_debug_enabled = 1;
#endif

    Dprintf("psycopgmodule: initializing psycopg %s", xstr(PSYCOPG_VERSION));

    /* initialize libcrypto threading callbacks */
    libcrypto_threads_init();

    /* initialize types and objects not exposed to the module */
    Py_SET_TYPE(&typecastType, &PyType_Type);
    if (0 > PyType_Ready(&typecastType)) { goto error; }

    Py_SET_TYPE(&chunkType, &PyType_Type);
    if (0 > PyType_Ready(&chunkType)) { goto error; }

    Py_SET_TYPE(&errorType, &PyType_Type);
    errorType.tp_base = (PyTypeObject *)PyExc_StandardError;
    if (0 > PyType_Ready(&errorType)) { goto error; }

    if (!(psyco_null = Bytes_FromString("NULL"))) { goto error; }

    /* initialize the module */
    module = PyModule_Create(&psycopgmodule);
    if (!module) { goto error; }

    if (0 > add_module_constants(module)) { goto error; }
    if (0 > add_module_types(module)) { goto error; }
    if (0 > datetime_init()) { goto error; }
    if (0 > encodings_init(module)) { goto error; }
    if (0 > typecast_init(module)) { goto error; }
    if (0 > adapters_init(module)) { goto error; }
    if (0 > basic_errors_init(module)) { goto error; }
    if (0 > sqlstate_errors_init(module)) { goto error; }

    Dprintf("psycopgmodule: module initialization complete");
    return module;

error:
	if (module)
		Py_DECREF(module);
	return NULL;
}
