/* green.c - cooperation with coroutine libraries.
 *
 * Copyright (C) 2010-2019 Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

#ifndef PSYCOPG_GREEN_H
#define PSYCOPG_GREEN_H 1

#include <libpq-fe.h>
#include "psycopg/connection.h"

#ifdef __cplusplus
extern "C" {
#endif

#define psyco_set_wait_callback_doc \
"Register a callback function to block waiting for data.\n" \
"\n" \
"The callback should have signature :samp:`fun({conn})` and\n" \
"is called to wait for data available whenever a blocking function from the\n" \
"libpq is called.  Use `!set_wait_callback(None)` to revert to the\n" \
"original behaviour (i.e. using blocking libpq functions).\n" \
"\n" \
"The function is an hook to allow coroutine-based libraries (such as\n" \
"Eventlet_ or gevent_) to switch when Psycopg is blocked, allowing\n" \
"other coroutines to run concurrently.\n" \
"\n" \
"See `~psycopg2.extras.wait_select()` for an example of a wait callback\n" \
"implementation.\n" \
"\n" \
".. _Eventlet: https://eventlet.net/\n" \
".. _gevent: http://www.gevent.org/\n"
HIDDEN PyObject *psyco_set_wait_callback(PyObject *self, PyObject *obj);

#define psyco_get_wait_callback_doc \
"Return the currently registered wait callback.\n" \
"\n" \
"Return `!None` if no callback is currently registered.\n"
HIDDEN PyObject *psyco_get_wait_callback(PyObject *self, PyObject *obj);

HIDDEN int psyco_green(void);
HIDDEN int psyco_wait(connectionObject *conn);
HIDDEN PGresult *psyco_exec_green(connectionObject *conn, const char *command);

#define EXC_IF_GREEN(cmd) \
if (psyco_green()) {   \
    PyErr_SetString(ProgrammingError, #cmd " cannot be used "       \
    "with an asynchronous callback.");                              \
    return NULL; }

#ifdef __cplusplus
}
#endif

#endif /* !defined(PSYCOPG_GREEN_H) */
