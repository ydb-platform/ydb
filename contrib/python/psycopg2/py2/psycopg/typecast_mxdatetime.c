/* typecast_mxdatetime.c - date and time typecasting functions to mx types
 *
 * Copyright (C) 2001-2019 Federico Di Gregorio <fog@debian.org>
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

// #include "mxDateTime.h"


/* Return 0 on success, -1 on failure, but don't set an exception */

static int
typecast_mxdatetime_init(void)
{
    if (mxDateTime_ImportModuleAndAPI()) {
        Dprintf("typecast_mxdatetime_init: mx.DateTime initialization failed");
        PyErr_Clear();
        return -1;
    }
    return 0;
}


/** DATE - cast a date into mx.DateTime python object **/

static PyObject *
typecast_MXDATE_cast(const char *str, Py_ssize_t len, PyObject *curs)
{
    int n, y=0, m=0, d=0;
    int hh=0, mm=0, ss=0, us=0, tz=0;
    const char *tp = NULL;

    if (str == NULL) { Py_RETURN_NONE; }

    Dprintf("typecast_MXDATE_cast: s = %s", str);

    /* check for infinity */
    if (!strcmp(str, "infinity") || !strcmp(str, "-infinity")) {
        if (str[0] == '-') {
            return mxDateTime.DateTime_FromDateAndTime(-999998,1,1, 0,0,0);
        }
        else {
            return mxDateTime.DateTime_FromDateAndTime(999999,12,31, 0,0,0);
        }
    }

    n = typecast_parse_date(str, &tp, &len, &y, &m, &d);
    Dprintf("typecast_MXDATE_cast: tp = %p n = %d,"
            " len = " FORMAT_CODE_PY_SSIZE_T ","
            " y = %d, m = %d, d = %d", tp, n, len, y, m, d);
    if (n != 3) {
        PyErr_SetString(DataError, "unable to parse date");
        return NULL;
    }

    if (len > 0) {
        n = typecast_parse_time(tp, NULL, &len, &hh, &mm, &ss, &us, &tz);
        Dprintf("typecast_MXDATE_cast: n = %d,"
            " len = " FORMAT_CODE_PY_SSIZE_T ","
            " hh = %d, mm = %d, ss = %d, us = %d, tz = %d",
            n, len, hh, mm, ss, us, tz);
        if (n != 0 && (n < 3 || n > 6)) {
            PyErr_SetString(DataError, "unable to parse time");
            return NULL;
        }
    }

    Dprintf("typecast_MXDATE_cast: fractionary seconds: %lf",
        (double)ss + (double)us/(double)1000000.0);
    return mxDateTime.DateTime_FromDateAndTime(y, m, d, hh, mm,
        (double)ss + (double)us/(double)1000000.0);
}

/** TIME - parse time into an mx.DateTime object **/

static PyObject *
typecast_MXTIME_cast(const char *str, Py_ssize_t len, PyObject *curs)
{
    int n, hh=0, mm=0, ss=0, us=0, tz=0;

    if (str == NULL) { Py_RETURN_NONE; }

    Dprintf("typecast_MXTIME_cast: s = %s", str);

    n = typecast_parse_time(str, NULL, &len, &hh, &mm, &ss, &us, &tz);
    Dprintf("typecast_MXTIME_cast: time parsed, %d components", n);
    Dprintf("typecast_MXTIME_cast: hh = %d, mm = %d, ss = %d, us = %d",
             hh, mm, ss, us);

    if (n < 3 || n > 6) {
        PyErr_SetString(DataError, "unable to parse time");
        return NULL;
    }

    Dprintf("typecast_MXTIME_cast: fractionary seconds: %lf",
        (double)ss + (double)us/(double)1000000.0);
    return mxDateTime.DateTimeDelta_FromTime(hh, mm,
        (double)ss + (double)us/(double)1000000.0);
}

/** INTERVAL - parse an interval into an mx.DateTimeDelta **/

static PyObject *
typecast_MXINTERVAL_cast(const char *str, Py_ssize_t len, PyObject *curs)
{
    long years = 0, months = 0, days = 0, denominator = 1;
    double hours = 0.0, minutes = 0.0, seconds = 0.0, hundredths = 0.0;
    double v = 0.0, sign = 1.0;
    int part = 0;

    if (str == NULL) { Py_RETURN_NONE; }

    Dprintf("typecast_MXINTERVAL_cast: s = %s", str);

    while (*str) {
        switch (*str) {

        case '-':
            sign = -1.0;
            break;

        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
            v = v * 10.0 + (double)(*str - '0');
            Dprintf("typecast_MXINTERVAL_cast: v = %f", v);
            if (part == 6){
                denominator *= 10;
                Dprintf("typecast_MXINTERVAL_cast: denominator = %ld",
                        denominator);
            }
            break;

        case 'y':
            if (part == 0) {
                years = (long)(v*sign);
                str = skip_until_space(str);
                Dprintf("typecast_MXINTERVAL_cast: years = %ld, rest = %s",
                        years, str);
                v = 0.0; sign = 1.0; part = 1;
            }
            break;

        case 'm':
            if (part <= 1) {
                months = (long)(v*sign);
                str = skip_until_space(str);
                Dprintf("typecast_MXINTERVAL_cast: months = %ld, rest = %s",
                        months, str);
                v = 0.0; sign = 1.0; part = 2;
            }
            break;

        case 'd':
            if (part <= 2) {
                days = (long)(v*sign);
                str = skip_until_space(str);
                Dprintf("typecast_MXINTERVAL_cast: days = %ld, rest = %s",
                        days, str);
                v = 0.0; sign = 1.0; part = 3;
            }
            break;

        case ':':
            if (part <= 3) {
                hours = v;
                Dprintf("typecast_MXINTERVAL_cast: hours = %f", hours);
                v = 0.0; part = 4;
            }
            else if (part == 4) {
                minutes = v;
                Dprintf("typecast_MXINTERVAL_cast: minutes = %f", minutes);
                v = 0.0; part = 5;
            }
            break;

        case '.':
            if (part == 5) {
                seconds = v;
                Dprintf("typecast_MXINTERVAL_cast: seconds = %f", seconds);
                v = 0.0; part = 6;
            }
            break;

        default:
            break;
        }

        str++;
    }

    /* manage last value, be it minutes or seconds or hundredths of a second */
    if (part == 4) {
        minutes = v;
        Dprintf("typecast_MXINTERVAL_cast: minutes = %f", minutes);
    }
    else if (part == 5) {
        seconds = v;
        Dprintf("typecast_MXINTERVAL_cast: seconds = %f", seconds);
    }
    else if (part == 6) {
        hundredths = v;
        Dprintf("typecast_MXINTERVAL_cast: hundredths = %f", hundredths);
        hundredths = hundredths/denominator;
        Dprintf("typecast_MXINTERVAL_cast: fractions = %.20f", hundredths);
    }

    /* calculates seconds */
    if (sign < 0.0) {
        seconds = - (hundredths + seconds + minutes*60 + hours*3600);
    }
    else {
        seconds += hundredths + minutes*60 + hours*3600;
    }

    /* calculates days */
    days += years*365 + months*30;

    Dprintf("typecast_MXINTERVAL_cast: days = %ld, seconds = %f",
            days, seconds);
    return mxDateTime.DateTimeDelta_FromDaysAndSeconds(days, seconds);
}
