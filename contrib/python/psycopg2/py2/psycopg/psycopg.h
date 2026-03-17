/* psycopg.h - definitions for the psycopg python module
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

#ifndef PSYCOPG_H
#define PSYCOPG_H 1

#if PG_VERSION_NUM < 90100
#error "Psycopg requires PostgreSQL client library (libpq) >= 9.1"
#endif

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>

#include "psycopg/config.h"
#include "psycopg/python.h"
#include "psycopg/utils.h"

#ifdef __cplusplus
extern "C" {
#endif

/* DBAPI compliance parameters */
#define APILEVEL "2.0"
#define THREADSAFETY 2
#define PARAMSTYLE "pyformat"

/* global exceptions */
extern HIDDEN PyObject *Error, *Warning, *InterfaceError, *DatabaseError,
    *InternalError, *OperationalError, *ProgrammingError,
    *IntegrityError, *DataError, *NotSupportedError;
extern HIDDEN PyObject *QueryCanceledError, *TransactionRollbackError;

/* sqlstate -> exception map */
extern HIDDEN PyObject *sqlstate_errors;

/* postgresql<->python encoding map */
extern HIDDEN PyObject *psycoEncodings;

/* SQL NULL */
extern HIDDEN PyObject *psyco_null;

/* Exceptions docstrings */
#define Error_doc \
"Base class for error exceptions."

#define Warning_doc \
"A database warning."

#define InterfaceError_doc \
"Error related to the database interface."

#define DatabaseError_doc \
"Error related to the database engine."

#define InternalError_doc \
"The database encountered an internal error."

#define OperationalError_doc \
"Error related to database operation (disconnect, memory allocation etc)."

#define ProgrammingError_doc \
"Error related to database programming (SQL error, table not found etc)."

#define IntegrityError_doc \
"Error related to database integrity."

#define DataError_doc \
"Error related to problems with the processed data."

#define NotSupportedError_doc \
"A method or database API was used which is not supported by the database."

#define QueryCanceledError_doc \
"Error related to SQL query cancellation."

#define TransactionRollbackError_doc \
"Error causing transaction rollback (deadlocks, serialization failures, etc)."

#ifdef __cplusplus
}
#endif

#endif /* !defined(PSYCOPG_H) */
