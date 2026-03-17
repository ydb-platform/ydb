/* microprotocols.c - definitions for minimalist and non-validating protocols
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

#ifndef PSYCOPG_MICROPROTOCOLS_H
#define PSYCOPG_MICROPROTOCOLS_H 1

#include "psycopg/connection.h"
#include "psycopg/cursor.h"

#ifdef __cplusplus
extern "C" {
#endif

/** adapters registry **/

extern HIDDEN PyObject *psyco_adapters;

/** the names of the three mandatory methods **/

#define MICROPROTOCOLS_GETQUOTED_NAME "getquoted"
#define MICROPROTOCOLS_GETSTRING_NAME "getstring"
#define MICROPROTOCOLS_GETBINARY_NAME "getbinary"

/** exported functions **/

/* used by module.c to init the microprotocols system */
HIDDEN RAISES_NEG int microprotocols_init(PyObject *dict);
HIDDEN RAISES_NEG int microprotocols_add(
    PyTypeObject *type, PyObject *proto, PyObject *cast);

HIDDEN PyObject *microprotocols_adapt(
    PyObject *obj, PyObject *proto, PyObject *alt);
HIDDEN PyObject *microprotocol_getquoted(
    PyObject *obj, connectionObject *conn);

HIDDEN PyObject *
    psyco_microprotocols_adapt(cursorObject *self, PyObject *args);
#define psyco_microprotocols_adapt_doc \
    "adapt(obj, protocol, alternate) -> object -- adapt obj to given protocol"

#endif /* !defined(PSYCOPG_MICROPROTOCOLS_H) */
