/* adapter_datetime.h - definition for the python date/time types
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

#ifndef PSYCOPG_DATETIME_H
#define PSYCOPG_DATETIME_H 1

#ifdef __cplusplus
extern "C" {
#endif

extern HIDDEN PyTypeObject pydatetimeType;

typedef struct {
    PyObject_HEAD

    PyObject *wrapped;
    int       type;
#define       PSYCO_DATETIME_TIME       0
#define       PSYCO_DATETIME_DATE       1
#define       PSYCO_DATETIME_TIMESTAMP  2
#define       PSYCO_DATETIME_INTERVAL   3

} pydatetimeObject;


RAISES_NEG HIDDEN int adapter_datetime_init(void);

HIDDEN PyObject *psyco_Date(PyObject *module, PyObject *args);
#define psyco_Date_doc \
    "Date(year, month, day) -> new date\n\n" \
    "Build an object holding a date value."

HIDDEN PyObject *psyco_Time(PyObject *module, PyObject *args);
#define psyco_Time_doc \
    "Time(hour, minutes, seconds, tzinfo=None) -> new time\n\n" \
    "Build an object holding a time value."

HIDDEN PyObject *psyco_Timestamp(PyObject *module, PyObject *args);
#define psyco_Timestamp_doc \
    "Timestamp(year, month, day, hour, minutes, seconds, tzinfo=None) -> new timestamp\n\n" \
    "Build an object holding a timestamp value."

HIDDEN PyObject *psyco_DateFromTicks(PyObject *module, PyObject *args);
#define psyco_DateFromTicks_doc \
    "DateFromTicks(ticks) -> new date\n\n" \
    "Build an object holding a date value from the given ticks value.\n\n" \
    "Ticks are the number of seconds since the epoch; see the documentation " \
    "of the standard Python time module for details)."

HIDDEN PyObject *psyco_TimeFromTicks(PyObject *module, PyObject *args);
#define psyco_TimeFromTicks_doc \
    "TimeFromTicks(ticks) -> new time\n\n" \
    "Build an object holding a time value from the given ticks value.\n\n" \
    "Ticks are the number of seconds since the epoch; see the documentation " \
    "of the standard Python time module for details)."

HIDDEN PyObject *psyco_TimestampFromTicks(PyObject *module, PyObject *args);
#define psyco_TimestampFromTicks_doc \
    "TimestampFromTicks(ticks) -> new timestamp\n\n" \
    "Build an object holding a timestamp value from the given ticks value.\n\n" \
    "Ticks are the number of seconds since the epoch; see the documentation " \
    "of the standard Python time module for details)."

HIDDEN PyObject *psyco_DateFromPy(PyObject *module, PyObject *args);
#define psyco_DateFromPy_doc \
    "DateFromPy(datetime.date) -> new wrapper"

HIDDEN PyObject *psyco_TimeFromPy(PyObject *module, PyObject *args);
#define psyco_TimeFromPy_doc \
    "TimeFromPy(datetime.time) -> new wrapper"

HIDDEN PyObject *psyco_TimestampFromPy(PyObject *module, PyObject *args);
#define psyco_TimestampFromPy_doc \
    "TimestampFromPy(datetime.datetime) -> new wrapper"

HIDDEN PyObject *psyco_IntervalFromPy(PyObject *module, PyObject *args);
#define psyco_IntervalFromPy_doc \
    "IntervalFromPy(datetime.timedelta) -> new wrapper"

#ifdef __cplusplus
}
#endif

#endif /* !defined(PSYCOPG_DATETIME_H) */
