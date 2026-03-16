/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/***************************************************************************
 *
 * The Julian date of any instant is the Julian day number plus the fraction of a day
 * since the preceding noon in Universal Time.
 * Julian dates are expressed as a Julian day number with a decimal fraction added.
 *
 ***************************************************************************/
#include "grib_api_internal.h"

#define ROUND(a) ((a) >= 0 ? (long)((a) + 0.5) : (long)((a)-0.5))
int grib_julian_to_datetime(double jd, long* year, long* month, long* day,
                            long* hour, long* minute, long* second)
{
    long z, a, alpha, b, c, d, e;
    double dday;
    double f;
    long s;

    jd += 0.5;
    z = (long)jd;
    f = jd - z;

    if (z < 2299161)
        a = z;
    else {
        alpha = (long)((z - 1867216.25) / 36524.25);
        a     = z + 1 + alpha - (long)(((double)alpha) / 4);
    }
    b = a + 1524;
    c = (long)((b - 122.1) / 365.25);
    d = (long)(365.25 * c);
    e = (long)(((double)(b - d)) / 30.6001);

    dday = b - d - (long)(30.6001 * e) + f;
    *day = (long)dday;
    dday -= *day;

    /* ANF-CG 02.03.2012 */
    s       = ROUND((double)(dday * 86400)); /* total in sec , no msec*/
    *hour   = (long)s / 3600;
    *minute = (long)((s % 3600) / 60);
    *second = (long)(s % 60);

    // Old algorithm, now replaced by above. See GRIB-180
    // dhour = dday * 24;
    // *hour = (long)dhour;
    // dhour -= *hour;
    // dminute = dhour * 60;
    // *minute = (long)dminute;
    // *second = (long)((dminute - *minute) * 60);

    if (e < 14)
        *month = e - 1;
    else
        *month = e - 13;

    if (*month > 2)
        *year = c - 4716;
    else
        *year = c - 4715;

    return GRIB_SUCCESS;
}

int grib_datetime_to_julian(long year, long month, long day,
                            long hour, long minute, long second, double* jd)
{
    return grib_datetime_to_julian_d(year, month, day, hour, minute, second, jd);
}

/* This version can deal with seconds provided as a double. Supporting milliseconds etc */
int grib_datetime_to_julian_d(
    long year, long month, long day, long hour, long minute,
    double second, double* jd)
{
    double a, b = 0, dday;
    long y, m;

    dday = (double)(hour * 3600 + minute * 60 + second) / 86400.0 + day;

    if (month < 3) {
        y = year - 1;
        m = month + 12;
    }
    else {
        y = year;
        m = month;
    }
    a = (long)(((double)y) / 100);

    if (y > 1582 ||
        (y == 1582 && ((m > 10) || (m == 10 && day > 14)))) {
        b = 2 - a + (long)(a / 4);
    }

    *jd = (long)(365.25 * (y + 4716)) + (long)(30.6001 * (m + 1)) + dday + b - 1524.5;

    return GRIB_SUCCESS;
}

long grib_julian_to_date(long jdate)
{
    long x, y, d, m, e;
    long day, month, year;

    x = 4 * jdate - 6884477;
    y = (x / 146097) * 100;
    e = x % 146097;
    d = e / 4;

    x = 4 * d + 3;
    y = (x / 1461) + y;
    e = x % 1461;
    d = e / 4 + 1;

    x = 5 * d - 3;
    m = x / 153 + 1;
    e = x % 153;
    d = e / 5 + 1;

    if (m < 11)
        month = m + 2;
    else
        month = m - 10;

    day  = d;
    year = y + m / 11;

    return year * 10000 + month * 100 + day;
}

long grib_date_to_julian(long ddate)
{
    long m1, y1, a, b, c, d, j1;

    long month, day, year;

    /*Asserts(ddate > 0);*/

    year = ddate / 10000;
    ddate %= 10000;
    month = ddate / 100;
    ddate %= 100;
    day = ddate;

    /*  if (year < 100) year = year + 1900; */

    if (month > 2) {
        m1 = month - 3;
        y1 = year;
    }
    else {
        m1 = month + 9;
        y1 = year - 1;
    }
    a  = 146097 * (y1 / 100) / 4;
    d  = y1 % 100;
    b  = 1461 * d / 4;
    c  = (153 * m1 + 2) / 5 + day + 1721119;
    j1 = a + b + c;

    return (j1);
}

/*
   void basedate_to_verifydate(gribsec1 *s1,request *r)
   {
   int bdate,vdate,cdate,vd,vtime,vstep;

   bdate = (s1->century-1)*1000000 + s1->year * 10000 + s1->month * 100 + s1->day;
   cdate = date_to_julian (bdate);
   vtime = cdate * 24 + s1->p1*units[s1->time_unit];
   vd    = vtime / 24;
   vdate = julian_to_date (vd,mars.y2k);
   vtime = vtime % 24;
   vstep = 0;

   set_value(r,"DATE","%d",vdate);
   set_value(r,"TIME","%02d00",vtime);
   set_value(r,"STEP","%d",vstep);
   }
 */
