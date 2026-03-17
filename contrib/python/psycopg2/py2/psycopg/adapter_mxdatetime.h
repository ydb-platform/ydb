/* adapter_mxdatetime.h - definition for the mx date/time types
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

#ifndef PSYCOPG_MXDATETIME_H
#define PSYCOPG_MXDATETIME_H 1

#ifdef __cplusplus
extern "C" {
#endif

extern HIDDEN PyTypeObject mxdatetimeType;

typedef struct {
    PyObject_HEAD

    PyObject *wrapped;
    int       type;
#define       PSYCO_MXDATETIME_TIME       0
#define       PSYCO_MXDATETIME_DATE       1
#define       PSYCO_MXDATETIME_TIMESTAMP  2
#define       PSYCO_MXDATETIME_INTERVAL   3

} mxdatetimeObject;

HIDDEN int psyco_adapter_mxdatetime_init(void);

HIDDEN PyObject *psyco_DateFromMx(PyObject *module, PyObject *args);
#define psyco_DateFromMx_doc \
    "DateFromMx(mx) -> new date"

HIDDEN PyObject *psyco_TimeFromMx(PyObject *module, PyObject *args);
#define psyco_TimeFromMx_doc \
    "TimeFromMx(mx) -> new time"

HIDDEN PyObject *psyco_TimestampFromMx(PyObject *module, PyObject *args);
#define psyco_TimestampFromMx_doc \
    "TimestampFromMx(mx) -> new timestamp"

HIDDEN PyObject *psyco_IntervalFromMx(PyObject *module, PyObject *args);
#define psyco_IntervalFromMx_doc \
    "IntervalFromMx(mx) -> new interval"

#ifdef __cplusplus
}
#endif

#endif /* !defined(PSYCOPG_MXDATETIME_H) */
